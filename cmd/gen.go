package cmd

import (
	"bytes"
	_ "embed"
	"fmt"
	"os"
	"path"
	"strings"
	"text/template"

	"github.com/juju/errors"
	"github.com/spf13/cobra"
	"github.com/tmoeish/tsq"
	"github.com/tmoeish/tsq/internal/parser"
	"mvdan.cc/gofumpt/format"
)

var (
	//go:embed tsq.go.tmpl
	defaultTableTpl string

	//go:embed tsq_dto.go.tmpl
	defaultDTOTpl string

	tplFlag    string
	dtoTplFlag string
	v          bool
)

func init() {
	GenCmd.Flags().StringVarP(&tplFlag, "tpl", "t", "", "tsq template file")
	GenCmd.Flags().StringVarP(&dtoTplFlag, "dtotpl", "", "", "tsq dto template file")
	GenCmd.Flags().BoolVarP(&v, "verbose", "v", false, "verbose output")
}

var GenCmd = &cobra.Command{
	Use:   "gen [package]",
	Short: "`gen` generates tsq.go file for each table in package",
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			_ = cmd.Usage()
			return errors.New("package path is required")
		}

		tableTpl, err := resolveTemplateText(tplFlag, defaultTableTpl, "template")
		if err != nil {
			return errors.Trace(err)
		}

		dtoTpl, err := resolveTemplateText(dtoTplFlag, defaultDTOTpl, "DTO template")
		if err != nil {
			return errors.Trace(err)
		}

		pPath := args[0]
		list, dir, err := parser.Parse(pPath)
		if err != nil {
			return errors.Trace(err)
		}

		for i := range list {
			list[i].SetTSQVersion(tsq.Version)
		}

		if err := validateGeneratedFilenameCollisions(list); err != nil {
			return errors.Trace(err)
		}

		structsByName := make(map[string]*tsq.StructInfo, len(list))
		for _, s := range list {
			structsByName[s.TypeInfo.TypeName] = s
		}

		tpl, err := template.New("tsq.go.tmpl").Funcs(TemplateFuncs()).Parse(tableTpl)
		if err != nil {
			return errors.Annotate(err, "failed to parse table template")
		}

		dtoTplParsed, err := template.New("tsq_dto.go.tmpl").Funcs(TemplateFuncs()).Parse(dtoTpl)
		if err != nil {
			return errors.Annotate(err, "failed to parse DTO template")
		}

		for _, s := range list {
			if s.TableInfo == nil || len(s.Fields) == 0 {
				continue
			}
			if err := validateStructForGeneration(s, structsByName); err != nil {
				return errors.Annotatef(err, "failed to validate %s", s.TypeInfo.TypeName)
			}
			if s.IsDTO {
				normalizeDTOColumns(s)
				if err := genDTO(s, dtoTplParsed, dir); err != nil {
					return errors.Trace(err)
				}
			} else {
				if err := gen(s, tpl, dir); err != nil {
					return errors.Trace(err)
				}
			}
		}

		return nil
	},
}

func resolveTemplateText(overridePath string, fallback string, label string) (string, error) {
	if len(overridePath) == 0 {
		return fallback, nil
	}

	tplBytes, err := os.ReadFile(overridePath)
	if err != nil {
		return "", errors.Annotatef(err, "failed to read %s file: %s", label, overridePath)
	}

	return string(tplBytes), nil
}

func gen(data *tsq.StructInfo, t *template.Template, dir string) error {
	filename := generatedFilename(data)
	filename = path.Join(dir, filename)
	if v {
		_, _ = fmt.Fprintf(os.Stderr, "gen %s\n", filename)
	}

	buf := new(bytes.Buffer)

	if err := t.Execute(buf, data); err != nil {
		bs := tsq.PrettyJSON(data)
		return errors.Annotatef(err, "template rendering failed: %s, data: %s", filename, bs)
	}

	src, err := format.Source(buf.Bytes(), format.Options{})
	if err != nil {
		_ = os.WriteFile(filename, buf.Bytes(), 0o644)
		return errors.Annotatef(err, "Go code formatting failed: %s", filename)
	}

	err = os.WriteFile(filename, src, 0o644)
	if err != nil {
		return errors.Annotatef(err, "failed to write file: %s", filename)
	}

	return nil
}

func genDTO(data *tsq.StructInfo, t *template.Template, dir string) error {
	filename := generatedFilename(data)
	filename = path.Join(dir, filename)
	if v {
		_, _ = fmt.Fprintf(os.Stderr, "gen %s\n", filename)
	}
	buf := new(bytes.Buffer)

	if err := t.Execute(buf, data); err != nil {
		bs := tsq.PrettyJSON(data)
		return errors.Annotatef(err, "DTO template rendering failed: %s, data: %s", filename, bs)
	}

	src, err := format.Source(buf.Bytes(), format.Options{})
	if err != nil {
		_ = os.WriteFile(filename, buf.Bytes(), 0o644)
		return errors.Annotatef(err, "Go code formatting failed: %s", filename)
	}

	err = os.WriteFile(filename, src, 0o644)
	if err != nil {
		return errors.Annotatef(err, "failed to write file: %s", filename)
	}

	return nil
}

func validateStructForGeneration(
	data *tsq.StructInfo,
	structsByName map[string]*tsq.StructInfo,
) error {
	if data == nil || data.TableInfo == nil {
		return nil
	}

	if data.IsDTO {
		return errors.Trace(validateDTOFields(data, structsByName))
	}

	return errors.Trace(ValidateManagedFields(data))
}

func validateDTOFields(
	dto *tsq.StructInfo,
	structsByName map[string]*tsq.StructInfo,
) error {
	normalizedRefs := make(map[string]string, len(dto.Fields))

	for _, field := range dto.Fields {
		parts := strings.Split(field.Column, ".")
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return errors.Errorf(
				"DTO field %s must reference a generated column as Struct.Field, got %q",
				field.Name,
				field.Column,
			)
		}

		normalizedRef := strings.ReplaceAll(field.Column, ".", "_")
		if existing, ok := normalizedRefs[normalizedRef]; ok && existing != field.Column {
			return errors.Errorf(
				"DTO field %s reference %s collides with %s after normalization",
				field.Name,
				field.Column,
				existing,
			)
		}
		normalizedRefs[normalizedRef] = field.Column

		targetStruct, ok := structsByName[parts[0]]
		if !ok || targetStruct.TableInfo == nil {
			return errors.Errorf(
				"DTO field %s references unknown struct %s",
				field.Name,
				parts[0],
			)
		}

		if _, ok := targetStruct.FieldMap[parts[1]]; !ok {
			return errors.Errorf(
				"DTO field %s references unknown field %s.%s",
				field.Name,
				parts[0],
				parts[1],
			)
		}
	}

	return nil
}

func normalizeDTOColumns(data *tsq.StructInfo) {
	for i, field := range data.Fields {
		field.Column = strings.ReplaceAll(field.Column, ".", "_")
		data.Fields[i] = field

		mapped := data.FieldMap[field.Name]
		mapped.Column = field.Column
		data.FieldMap[field.Name] = mapped
	}
}

func validateGeneratedFilenameCollisions(list []*tsq.StructInfo) error {
	seen := make(map[string]string, len(list))

	for _, data := range list {
		if data == nil || data.TableInfo == nil || len(data.Fields) == 0 {
			continue
		}

		filename := generatedFilename(data)
		if existing, ok := seen[filename]; ok && existing != data.TypeInfo.TypeName {
			return errors.Errorf(
				"generated filename %s collides between %s and %s",
				filename,
				existing,
				data.TypeInfo.TypeName,
			)
		}

		seen[filename] = data.TypeInfo.TypeName
	}

	return nil
}

func generatedFilename(data *tsq.StructInfo) string {
	base := strings.ToLower(data.TypeInfo.TypeName)
	if data.IsDTO {
		return fmt.Sprintf("%s_dto_tsq.go", base)
	}

	return fmt.Sprintf("%s_tsq.go", base)
}
