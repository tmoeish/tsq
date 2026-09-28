package cmd

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"go/types"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"text/template"

	"mvdan.cc/gofumpt/format"

	"github.com/tmoeish/tsq/v5/internal/genmodel"
)

type generationModel struct {
	Data       any
	Template   *template.Template
	Filename   string
	ErrorLabel string
}

type generationPlanStatus string

const (
	generationPlanCreate    generationPlanStatus = "create"
	generationPlanUpdate    generationPlanStatus = "update"
	generationPlanUnchanged generationPlanStatus = "unchanged"
	generationPlanStale     generationPlanStatus = "stale"
)

type generationPlanEntry struct {
	Model    generationModel
	Filename string
	Source   []byte
	Status   generationPlanStatus
}

type generationStats struct {
	Tables  int
	Results int
}

func buildGenerationModels(
	list []*genmodel.StructInfo,
	dir string,
	tableTpl *template.Template,
	resultTpl *template.Template,
	runtimeTpl *template.Template,
) ([]generationModel, error) {
	if err := validateGeneratedFilenameCollisions(list); err != nil {
		return nil, err
	}

	if err := validateTableNameCollisions(list); err != nil {
		return nil, err
	}

	if err := validateIndexNameCollisions(list); err != nil {
		return nil, err
	}

	if err := validateGeneratedSymbolCollisions(list); err != nil {
		return nil, err
	}

	structsByName := make(map[string]*genmodel.StructInfo, len(list))
	for _, s := range list {
		structsByName[s.TypeInfo.TypeName] = s
	}

	models := make([]generationModel, 0, len(list))

	var resolver *ddlTypeResolver

	for _, s := range list {
		if s.TableMeta == nil || len(s.Fields) == 0 {
			continue
		}

		if err := validateStructForGeneration(s, structsByName); err != nil {
			return nil, structErr(s, err)
		}

		s.Receiver = receiverName(s.Receiver)

		model := generationModel{
			Data:     s,
			Filename: filepath.Join(dir, generatedFilename(s)),
		}

		if resolver == nil {
			r, err := newDDLTypeResolver(s.TypeInfo.Package.Path, dir)
			if err != nil {
				return nil, err
			}

			resolver = r
		}

		if err := resolveNullValues(s, resolver); err != nil {
			return nil, structErr(s, fmt.Errorf("resolve nullable fields: %w", err))
		}

		if err := sortByDeclaration(s, resolver); err != nil {
			return nil, structErr(s, err)
		}

		if err := validateTextFields(s, resolver); err != nil {
			return nil, structErr(s, err)
		}

		if s.IsResult {
			if err := validateResultTypes(s, structsByName, resolver); err != nil {
				return nil, structErr(s, err)
			}

			normalizeResultColumns(s)

			model.Template = resultTpl
			model.ErrorLabel = "Result template rendering failed"
		} else {
			schema, err := buildSchemaColumns(s, resolver)
			if err != nil {
				return nil, structErr(s, fmt.Errorf("build schema columns: %w", err))
			}

			s.Schema = schema

			if err := validateAutoIncrementKey(s); err != nil {
				return nil, structErr(s, err)
			}

			if err := validateDatabaseFilledFields(s); err != nil {
				return nil, structErr(s, err)
			}

			model.Template = tableTpl
			model.ErrorLabel = "template rendering failed"
		}

		models = append(models, model)
	}

	if resolver != nil {
		if err := validateDeclaredSymbols(list, resolver); err != nil {
			return nil, err
		}
	}

	runtimeModel, err := buildPackageRuntimeModel(list, dir, runtimeTpl)
	if err != nil {
		return nil, err
	}

	if runtimeModel != nil {
		models = append(models, *runtimeModel)
	}

	return models, nil
}

// sortByDeclaration puts s.Fields in the order the struct declares them, an
// embedded struct's fields where it is embedded, so the generated table lists its
// columns (and Columns() selects them) the way the reader wrote them. The parser
// collects fields in a map and cannot tell.
func sortByDeclaration(s *genmodel.StructInfo, resolver *ddlTypeResolver) error {
	named, pkg, err := resolver.lookupNamedStruct(s.TypeInfo)
	if err != nil {
		return err
	}

	position := make(map[string][]int, len(s.Fields))

	for _, field := range s.Fields {
		_, index, _ := types.LookupFieldOrMethod(named, false, pkg, field.Name)
		if index == nil {
			return fmt.Errorf("field %s not found", field.Name)
		}

		position[field.Name] = index
	}

	slices.SortStableFunc(s.Fields, func(a, b genmodel.FieldInfo) int {
		return slices.Compare(position[a.Name], position[b.Name])
	})

	return nil
}

// structErr points err at the struct it is about: file:line:column: Name: err.
func structErr(s *genmodel.StructInfo, err error) error {
	if s.Pos == "" {
		return fmt.Errorf("%s: %w", s.TypeInfo.TypeName, err)
	}

	return fmt.Errorf("%s: %s: %w", s.Pos, s.TypeInfo.TypeName, err)
}

func buildPackageRuntimeModel(
	list []*genmodel.StructInfo,
	dir string,
	runtimeTpl *template.Template,
) (*generationModel, error) {
	if runtimeTpl == nil {
		return nil, nil
	}

	tables := make([]*genmodel.StructInfo, 0, len(list))
	for _, s := range list {
		if s == nil || s.TableMeta == nil || s.IsResult || len(s.Fields) == 0 {
			continue
		}

		tables = append(tables, s)
	}

	if len(tables) == 0 {
		return nil, nil
	}

	sort.Slice(tables, func(i, j int) bool {
		if tables[i].Table == tables[j].Table {
			return tables[i].TypeInfo.TypeName < tables[j].TypeInfo.TypeName
		}

		return tables[i].Table < tables[j].Table
	})

	return &generationModel{
		Data: packageRuntimeTemplateData{
			Package:    tables[0].TypeInfo.Package,
			Tables:     tables,
			TSQVersion: tables[0].TSQVersion,
		},
		Template:   runtimeTpl,
		Filename:   filepath.Join(dir, runtimeFilename),
		ErrorLabel: "runtime template rendering failed",
	}, nil
}

// resolveNullValues fills FieldInfo.NullValue for the fields that can hold NULL.
func resolveNullValues(s *genmodel.StructInfo, resolver *ddlTypeResolver) error {
	named, pkg, err := resolver.lookupNamedStruct(s.TypeInfo)
	if err != nil {
		return err
	}

	// A package is spelled by the alias this file imports it under: two packages
	// named pkg are imported as pkg and pkg1, and one named like a package the
	// generated code uses (tsq, context) is renamed too.
	qualifier := func(p *types.Package) string {
		switch p.Path() {
		case pkg.Path():
			return ""
		case importPathTime:
			return generatedTimeAlias
		case importPathDatabaseSQL:
			return generatedSQLAlias
		}

		if alias, ok := s.Imports[p.Path()]; ok {
			return alias
		}

		return p.Name()
	}

	for i, field := range s.Fields {
		obj, _, err := lookupDDLField(named, pkg, field.Name)
		if err != nil {
			return err
		}

		changed := false

		if value, ok := nullableValueType(obj.Type()); ok {
			field.NullValue = types.TypeString(value, qualifier)
			changed = true
		}

		// The parser reads types from the AST and records a generic type by its
		// base name, and a package by its name rather than this file's alias; the
		// spelling of a type from another package, sql.Null[time.Time] or pkg1.V,
		// comes from go/types.
		// The AST records [N]T as a slice of T, which is another type.
		_, array := obj.Type().(*types.Array)

		if field.TypeArgs != "" || field.Type.Package.Path != "" || array {
			field.Spelled = types.TypeString(obj.Type(), qualifier)
			changed = true
		}

		if changed {
			s.Fields[i] = field

			if mapped, ok := s.FieldsByName[field.Name]; ok {
				mapped.NullValue = field.NullValue
				mapped.Spelled = field.Spelled
				s.FieldsByName[field.Name] = mapped
			}
		}
	}

	return nil
}

// nullableValueType mirrors the library's rule for a nullable form: the element of
// a pointer, or the single data field of a scannable struct with a Valid bool.
func nullableValueType(t types.Type) (types.Type, bool) {
	t = types.Unalias(t)

	if ptr, ok := t.(*types.Pointer); ok {
		return ptr.Elem(), true
	}

	st, ok := t.Underlying().(*types.Struct)
	if !ok {
		return nil, false
	}

	if scan, _, _ := types.LookupFieldOrMethod(types.NewPointer(t), true, nil, "Scan"); scan == nil {
		return nil, false
	}

	var (
		valid bool
		value types.Type
		n     int
	)

	var walk func(st *types.Struct)
	walk = func(st *types.Struct) {
		for f := range st.Fields() {
			switch {
			case f.Embedded():
				if inner, ok := f.Type().Underlying().(*types.Struct); ok {
					walk(inner)
				}
			case !f.Exported():
			case f.Name() == "Valid" && types.Identical(f.Type(), types.Typ[types.Bool]):
				valid = true
			default:
				value = f.Type()
				n++
			}
		}
	}

	walk(st)

	return value, valid && n == 1
}

func buildSchemaColumns(
	table *genmodel.StructInfo,
	resolver *ddlTypeResolver,
) ([]genmodel.SchemaColumn, error) {
	columns := make([]genmodel.SchemaColumn, 0, len(table.Fields))
	for _, field := range orderedDDLFields(table) {
		desc, err := resolver.describeField(table, field)
		if err != nil {
			return nil, err
		}

		columns = append(columns, genmodel.SchemaColumn{
			Name:          field.Column,
			Kind:          string(desc.kind),
			Bits:          desc.bits,
			Unsigned:      desc.unsigned,
			Nullable:      desc.nullable,
			Size:          desc.size,
			RawType:       desc.rawType,
			PrimaryKey:    field.Name == table.PrimaryKey,
			AutoIncrement: field.Name == table.PrimaryKey && table.AutoIncrement,
			Default:       ddlColumnDefault(table, field, desc),
			Fill:          desc.fill,
			Generated:     desc.generatedSQL,
		})
	}

	return columns, nil
}

func summarizeGenerationModels(models []generationModel) generationStats {
	stats := generationStats{}

	for _, model := range models {
		if data, ok := model.Data.(*genmodel.StructInfo); ok && data != nil {
			if data.IsResult {
				stats.Results++
				continue
			}

			stats.Tables++
		}
	}

	return stats
}

func renderGenerationModelSource(model generationModel) ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := model.Template.Execute(buf, model.Data); err != nil {
		bs := prettyJSON(model.Data)
		return nil, fmt.Errorf("%s: %s, data: %s"+": %w", model.ErrorLabel, model.Filename, bs, err)
	}

	src, err := format.Source(buf.Bytes(), format.Options{})
	if err != nil {
		return nil, fmt.Errorf("go code formatting failed: %s: %w", model.Filename, err)
	}

	return src, nil
}

func prettyJSON(v any) string {
	bs, err := json.MarshalIndent(v, "", "    ")
	if err != nil {
		return ""
	}

	return string(bs)
}

func renderGenerationModel(model generationModel) error {
	if v {
		if _, err := fmt.Fprintf(os.Stderr, "gen %s\n", model.Filename); err != nil {
			return err
		}
	}

	src, err := renderGenerationModelSource(model)
	if err != nil {
		return err
	}

	if err := writeGeneratedFile(model.Filename, src); err != nil {
		return fmt.Errorf("failed to write file: %s"+": %w", model.Filename, err)
	}

	return nil
}

func buildGenerationPlan(models []generationModel, dir string) ([]generationPlanEntry, error) {
	plan := make([]generationPlanEntry, 0, len(models))
	plannedFiles := make(map[string]struct{}, len(models))

	for _, model := range models {
		src, err := renderGenerationModelSource(model)
		if err != nil {
			return nil, err
		}

		status, err := generationPlanStatusFor(model.Filename, src)
		if err != nil {
			return nil, fmt.Errorf("failed to plan file: %s"+": %w", model.Filename, err)
		}

		plan = append(plan, generationPlanEntry{
			Model:    model,
			Filename: model.Filename,
			Source:   src,
			Status:   status,
		})
		plannedFiles[model.Filename] = struct{}{}
	}

	staleFiles, err := findStaleGeneratedFiles(dir, plannedFiles)
	if err != nil {
		return nil, err
	}

	for _, staleFile := range staleFiles {
		plan = append(plan, generationPlanEntry{
			Filename: staleFile,
			Status:   generationPlanStale,
		})
	}

	return plan, nil
}

func findStaleGeneratedFiles(dir string, plannedFiles map[string]struct{}) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	stale := make([]string, 0)

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if !isGeneratedFilename(name) {
			continue
		}

		filename := filepath.Join(dir, name)
		if _, ok := plannedFiles[filename]; ok {
			continue
		}

		content, err := os.ReadFile(filename)
		if err != nil {
			return nil, err
		}

		if !bytes.HasPrefix(content, []byte(generatedFileHeaderPrefix)) {
			continue
		}

		stale = append(stale, filename)
	}

	sort.Strings(stale)

	return stale, nil
}

func isGeneratedFilename(name string) bool {
	return strings.HasSuffix(name, ".tsq.go") || strings.HasSuffix(name, ".result.tsq.go")
}

func generationPlanStatusFor(filename string, src []byte) (generationPlanStatus, error) {
	existing, err := os.ReadFile(filename)
	if os.IsNotExist(err) {
		return generationPlanCreate, nil
	}

	if err != nil {
		return "", err
	}

	if bytes.Equal(existing, src) {
		return generationPlanUnchanged, nil
	}

	if err := ensureWritableGeneratedFile(filename); err != nil {
		return "", err
	}

	return generationPlanUpdate, nil
}

func ensureGenerationPlanUpToDate(plan []generationPlanEntry) error {
	outdated := make([]string, 0)

	for _, entry := range plan {
		if entry.Status == generationPlanUnchanged {
			continue
		}

		outdated = append(outdated, fmt.Sprintf("%s %s", strings.ToUpper(string(entry.Status)), entry.Filename))
	}

	if len(outdated) == 0 {
		return nil
	}

	return fmt.Errorf("%w:\n%s", ErrOutOfDate, strings.Join(outdated, "\n"))
}

// ErrOutOfDate is what gen --check fails with when generating again would change
// files; the tsq command exits with status 2 for it, and 1 for every other error.
var ErrOutOfDate = errors.New("generated files are out of date")

func printGenerationPlan(w io.Writer, plan []generationPlanEntry) {
	for _, entry := range plan {
		if _, err := fmt.Fprintf(w, "%s %s\n", strings.ToUpper(string(entry.Status)), entry.Filename); err != nil {
			return
		}
	}
}

func printGenerationSummary(w io.Writer, plan []generationPlanEntry) {
	createCount := 0
	updateCount := 0
	unchangedCount := 0
	staleCount := 0

	for _, entry := range plan {
		switch entry.Status {
		case generationPlanCreate:
			createCount++
		case generationPlanUpdate:
			updateCount++
		case generationPlanUnchanged:
			unchangedCount++
		case generationPlanStale:
			staleCount++
		}
	}

	if _, err := fmt.Fprintf(
		w,
		"files: %d create, %d update, %d unchanged, %d stale\n",
		createCount,
		updateCount,
		unchangedCount,
		staleCount,
	); err != nil {
		return
	}
}

// validateAutoIncrementKey refuses a key the database is to generate that is not
// an integer column: the DDL has no way to write it, and pk=Code on a string used
// to reach it as a panic.
func validateAutoIncrementKey(s *genmodel.StructInfo) error {
	for _, column := range s.Schema {
		if column.PrimaryKey && column.AutoIncrement && column.Kind != string(ddlColumnInt) {
			return fmt.Errorf("primary key %s is a %s column, and a key the database generates is an integer; "+
				"write pk=%s assigned when the caller sets it", s.PrimaryKey, column.Kind, s.PrimaryKey)
		}
	}

	return nil
}
