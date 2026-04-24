package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"text/template"

	"github.com/tmoeish/tsq"
)

func TestValidateDTOFieldsRejectsUnknownTargetField(t *testing.T) {
	dto := &tsq.StructInfo{
		TableInfo: &tsq.TableInfo{IsDTO: true},
		TypeInfo:  tsq.TypeInfo{TypeName: "UserDTO"},
		Fields: []tsq.FieldInfo{
			{Name: "UserName", Column: "User.Missing"},
		},
	}

	structsByName := map[string]*tsq.StructInfo{
		"User": {
			TableInfo: &tsq.TableInfo{Table: "user"},
			FieldMap: map[string]tsq.FieldInfo{
				"ID": {Name: "ID", Column: "id"},
			},
		},
	}

	if err := validateDTOFields(dto, structsByName); err == nil {
		t.Fatal("expected invalid DTO reference to return an error")
	}
}

func TestValidateDTOFieldsRejectsNormalizedReferenceCollisions(t *testing.T) {
	dto := &tsq.StructInfo{
		TableInfo: &tsq.TableInfo{IsDTO: true},
		TypeInfo:  tsq.TypeInfo{TypeName: "UserDTO"},
		Fields: []tsq.FieldInfo{
			{Name: "A", Column: "User.Profile_ID"},
			{Name: "B", Column: "User_Profile.ID"},
		},
	}

	structsByName := map[string]*tsq.StructInfo{
		"User": {
			TableInfo: &tsq.TableInfo{Table: "user"},
			FieldMap: map[string]tsq.FieldInfo{
				"Profile_ID": {Name: "Profile_ID", Column: "profile_id"},
			},
		},
		"User_Profile": {
			TableInfo: &tsq.TableInfo{Table: "user_profile"},
			FieldMap: map[string]tsq.FieldInfo{
				"ID": {Name: "ID", Column: "id"},
			},
		},
	}

	if err := validateDTOFields(dto, structsByName); err == nil {
		t.Fatal("expected normalized DTO reference collision to return an error")
	}
}

func TestValidateDTOFieldsRejectsIncompatibleTypes(t *testing.T) {
	dto := &tsq.StructInfo{
		TableInfo: &tsq.TableInfo{IsDTO: true},
		TypeInfo:  tsq.TypeInfo{TypeName: "UserDTO"},
		Fields: []tsq.FieldInfo{
			{Name: "OrderTime", Column: "Order.CT", Type: tsq.TypeInfo{TypeName: "string"}},
		},
	}

	structsByName := map[string]*tsq.StructInfo{
		"Order": {
			TableInfo: &tsq.TableInfo{Table: "order"},
			FieldMap: map[string]tsq.FieldInfo{
				"CT": {
					Name:   "CT",
					Column: "ct",
					Type: tsq.TypeInfo{
						Package:  tsq.PackageInfo{Path: "time", Name: "time"},
						TypeName: "Time",
					},
				},
			},
		},
	}

	if err := validateDTOFields(dto, structsByName); err == nil {
		t.Fatal("expected incompatible DTO field type to return an error")
	}
}

func TestValidateDTOFieldsAcceptsMatchingTypes(t *testing.T) {
	timeType := tsq.TypeInfo{
		Package:  tsq.PackageInfo{Path: "time", Name: "time"},
		TypeName: "Time",
	}
	dto := &tsq.StructInfo{
		TableInfo: &tsq.TableInfo{IsDTO: true},
		TypeInfo:  tsq.TypeInfo{TypeName: "UserDTO"},
		Fields: []tsq.FieldInfo{
			{Name: "OrderTime", Column: "Order.CT", Type: timeType},
		},
	}

	structsByName := map[string]*tsq.StructInfo{
		"Order": {
			TableInfo: &tsq.TableInfo{Table: "order"},
			FieldMap: map[string]tsq.FieldInfo{
				"CT": {Name: "CT", Column: "ct", Type: timeType},
			},
		},
	}

	if err := validateDTOFields(dto, structsByName); err != nil {
		t.Fatalf("expected matching DTO field type to pass, got %v", err)
	}
}

func TestNormalizeDTOColumnsUpdatesFieldMap(t *testing.T) {
	dto := &tsq.StructInfo{
		TableInfo: &tsq.TableInfo{IsDTO: true},
		Fields: []tsq.FieldInfo{
			{Name: "UserID", Column: "User.ID"},
		},
		FieldMap: map[string]tsq.FieldInfo{
			"UserID": {Name: "UserID", Column: "User.ID"},
		},
	}

	normalizeDTOColumns(dto)

	if got := dto.Fields[0].Column; got != "User_ID" {
		t.Fatalf("expected DTO field column to be normalized, got %q", got)
	}

	if got := dto.FieldMap["UserID"].Column; got != "User_ID" {
		t.Fatalf("expected DTO field map column to be normalized, got %q", got)
	}
}

func TestValidateGeneratedFilenameCollisionsRejectsCaseConflicts(t *testing.T) {
	list := []*tsq.StructInfo{
		{
			TableInfo: &tsq.TableInfo{Table: "user"},
			TypeInfo:  tsq.TypeInfo{TypeName: "User"},
			Fields:    []tsq.FieldInfo{{Name: "ID"}},
		},
		{
			TableInfo: &tsq.TableInfo{Table: "user_lower"},
			TypeInfo:  tsq.TypeInfo{TypeName: "user"},
			Fields:    []tsq.FieldInfo{{Name: "ID"}},
		},
	}

	if err := validateGeneratedFilenameCollisions(list); err == nil {
		t.Fatal("expected case-insensitive filename collision to return an error")
	}
}

func TestValidateIndexNameCollisionsRejectsCrossTableReuse(t *testing.T) {
	list := []*tsq.StructInfo{
		{
			TableInfo: &tsq.TableInfo{
				Table:  "user",
				UxList: []tsq.IndexInfo{{Name: "ux_name", Fields: []string{"Name"}}},
			},
		},
		{
			TableInfo: &tsq.TableInfo{
				Table:  "org",
				UxList: []tsq.IndexInfo{{Name: "ux_name", Fields: []string{"Name"}}},
			},
		},
	}

	if err := validateIndexNameCollisions(list); err == nil {
		t.Fatal("expected reused index name across tables to return an error")
	}
}

func TestValidateStructForGenerationRejectsPointerPrimaryKeys(t *testing.T) {
	data := &tsq.StructInfo{
		TableInfo: &tsq.TableInfo{
			Table: "user",
			ID:    "ID",
		},
		TypeInfo: tsq.TypeInfo{TypeName: "User"},
		FieldMap: map[string]tsq.FieldInfo{
			"ID": {
				Name:      "ID",
				IsPointer: true,
				Type:      tsq.TypeInfo{TypeName: "string"},
			},
		},
	}

	if err := validateStructForGeneration(data, nil); err == nil {
		t.Fatal("expected pointer primary key to be rejected")
	}
}

func TestValidateStructForGenerationRejectsSlicePrimaryKeys(t *testing.T) {
	data := &tsq.StructInfo{
		TableInfo: &tsq.TableInfo{
			Table: "blob_user",
			ID:    "ID",
		},
		TypeInfo: tsq.TypeInfo{TypeName: "BlobUser"},
		FieldMap: map[string]tsq.FieldInfo{
			"ID": {
				Name:    "ID",
				IsArray: true,
				Type:    tsq.TypeInfo{TypeName: "byte"},
			},
		},
	}

	if err := validateStructForGeneration(data, nil); err == nil {
		t.Fatal("expected slice primary key to be rejected")
	}
}

func TestTableTemplateOrErrPreservesErrNoRows(t *testing.T) {
	want := `if errors.Is(err, {{ GeneratedSQLRef "ErrNoRows" }}) {`
	if count := strings.Count(defaultTableTpl, want); count < 4 {
		t.Fatalf("expected table template to preserve sql.ErrNoRows in every OrErr helper, count=%d", count)
	}
}

func TestTableTemplateAvoidsKeywordParameterNames(t *testing.T) {
	dir := t.TempDir()

	tpl, err := template.New("tsq.go.tmpl").Funcs(TemplateFuncs()).Parse(defaultTableTpl)
	if err != nil {
		t.Fatalf("failed to parse table template: %v", err)
	}

	field := tsq.FieldInfo{Name: "Type", Column: "type", JsonTag: "type", Type: tsq.TypeInfo{TypeName: "int64"}}
	data := &tsq.StructInfo{
		TableInfo: &tsq.TableInfo{
			Table: "keyworded",
			ID:    "Type",
			AI:    true,
		},
		TypeInfo: tsq.TypeInfo{Package: tsq.PackageInfo{Name: "example"}, TypeName: "Keyworded"},
		Fields:   []tsq.FieldInfo{field},
		FieldMap: map[string]tsq.FieldInfo{
			"Type": field,
		},
		Recv:       "k",
		TSQVersion: "test",
	}

	if err := gen(data, tpl, dir); err != nil {
		t.Fatalf("expected template with keyword field to render valid Go, got %v", err)
	}

	contents, err := os.ReadFile(filepath.Join(dir, "keyworded_tsq.go"))
	if err != nil {
		t.Fatalf("failed to read generated file: %v", err)
	}

	rendered := string(contents)
	if !strings.Contains(rendered, "type_ int64") {
		t.Fatalf("expected generated parameter to avoid Go keyword, got:\n%s", rendered)
	}

	if strings.Contains(rendered, "\ttype int64") {
		t.Fatalf("generated code still contains keyword parameter:\n%s", rendered)
	}
}

func TestResolveTemplateTextUsesFallbackWithoutLeakingPreviousOverride(t *testing.T) {
	dir := t.TempDir()

	overridePath := filepath.Join(dir, "custom.tmpl")
	if err := os.WriteFile(overridePath, []byte("custom-template"), 0o644); err != nil {
		t.Fatalf("failed to write override template: %v", err)
	}

	override, err := resolveTemplateText(overridePath, defaultTableTpl, "template")
	if err != nil {
		t.Fatalf("expected override template to load, got %v", err)
	}

	if override != "custom-template" {
		t.Fatalf("unexpected override template: %q", override)
	}

	fallback, err := resolveTemplateText("", defaultTableTpl, "template")
	if err != nil {
		t.Fatalf("expected fallback template to load, got %v", err)
	}

	if fallback != defaultTableTpl {
		t.Fatal("expected empty override path to return embedded template")
	}

	if fallback == override {
		t.Fatal("expected fallback template to ignore previous override content")
	}
}

func TestGenDoesNotWriteBrokenGoOnFormatError(t *testing.T) {
	dir := t.TempDir()

	target := filepath.Join(dir, "user_tsq.go")
	if err := os.WriteFile(target, []byte("// existing\n"), 0o644); err != nil {
		t.Fatalf("failed to seed generated file: %v", err)
	}

	tpl, err := template.New("broken").Parse("package {{.TypeInfo.Package.Name}}\nfunc {")
	if err != nil {
		t.Fatalf("failed to parse broken template: %v", err)
	}

	data := &tsq.StructInfo{
		TableInfo: &tsq.TableInfo{Table: "user"},
		TypeInfo:  tsq.TypeInfo{Package: tsq.PackageInfo{Name: "example"}, TypeName: "User"},
		Fields:    []tsq.FieldInfo{{Name: "ID"}},
	}

	if err := gen(data, tpl, dir); err == nil {
		t.Fatal("expected generation to fail for invalid Go output")
	}

	contents, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("failed to read generated file: %v", err)
	}

	if string(contents) != "// existing\n" {
		t.Fatalf("expected format failure to leave existing file untouched, got %q", string(contents))
	}
}

func TestGenDTODoesNotWriteBrokenGoOnFormatError(t *testing.T) {
	dir := t.TempDir()

	target := filepath.Join(dir, "userdto_dto_tsq.go")
	if err := os.WriteFile(target, []byte("// existing dto\n"), 0o644); err != nil {
		t.Fatalf("failed to seed DTO generated file: %v", err)
	}

	tpl, err := template.New("broken").Parse("package {{.TypeInfo.Package.Name}}\nfunc {")
	if err != nil {
		t.Fatalf("failed to parse broken template: %v", err)
	}

	data := &tsq.StructInfo{
		TableInfo: &tsq.TableInfo{IsDTO: true},
		TypeInfo:  tsq.TypeInfo{Package: tsq.PackageInfo{Name: "example"}, TypeName: "UserDTO"},
		Fields:    []tsq.FieldInfo{{Name: "ID"}},
	}

	if err := genDTO(data, tpl, dir); err == nil {
		t.Fatal("expected DTO generation to fail for invalid Go output")
	}

	contents, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("failed to read DTO generated file: %v", err)
	}

	if string(contents) != "// existing dto\n" {
		t.Fatalf("expected DTO format failure to leave existing file untouched, got %q", string(contents))
	}
}

func TestWriteGeneratedFileReplacesContentsAtomically(t *testing.T) {
	dir := t.TempDir()

	target := filepath.Join(dir, "user_tsq.go")
	if err := os.WriteFile(target, []byte("// Code generated by tsq-test. DO NOT EDIT.\nold"), 0o644); err != nil {
		t.Fatalf("failed to seed target file: %v", err)
	}

	if err := writeGeneratedFile(target, []byte("new")); err != nil {
		t.Fatalf("expected atomic write helper to succeed, got %v", err)
	}

	contents, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("failed to read target file: %v", err)
	}

	if string(contents) != "new" {
		t.Fatalf("expected target file to be replaced, got %q", string(contents))
	}
}

func TestWriteGeneratedFileRejectsNonGeneratedFile(t *testing.T) {
	dir := t.TempDir()

	target := filepath.Join(dir, "user_tsq.go")
	if err := os.WriteFile(target, []byte("package example\n"), 0o644); err != nil {
		t.Fatalf("failed to seed target file: %v", err)
	}

	err := writeGeneratedFile(target, []byte("new"))
	if err == nil {
		t.Fatal("expected non-generated file overwrite to fail")
	}
}

func TestWriteGeneratedFilePreservesPermissions(t *testing.T) {
	dir := t.TempDir()

	target := filepath.Join(dir, "user_tsq.go")
	if err := os.WriteFile(target, []byte("// Code generated by tsq-test. DO NOT EDIT.\n"), 0o600); err != nil {
		t.Fatalf("failed to seed target file: %v", err)
	}

	if err := writeGeneratedFile(target, []byte("// Code generated by tsq-test. DO NOT EDIT.\npackage example\n")); err != nil {
		t.Fatalf("expected generated file rewrite to succeed, got %v", err)
	}

	info, err := os.Stat(target)
	if err != nil {
		t.Fatalf("failed to stat target file: %v", err)
	}

	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("expected permissions to be preserved, got %o", got)
	}
}

// gen and genDTO are thin helpers used by tests to render a template for a
// single struct and write the output to dir.  They are test-only wrappers
// around renderGenerationModel and intentionally not part of the production
// code path.

func gen(data *tsq.StructInfo, t *template.Template, dir string) error {
	return renderGenerationModel(generationModel{
		Data:       data,
		Template:   t,
		Filename:   filepath.Join(dir, generatedFilename(data)),
		ErrorLabel: "template rendering failed",
	})
}

func genDTO(data *tsq.StructInfo, t *template.Template, dir string) error {
	return renderGenerationModel(generationModel{
		Data:       data,
		Template:   t,
		Filename:   filepath.Join(dir, generatedFilename(data)),
		ErrorLabel: "DTO template rendering failed",
	})
}
