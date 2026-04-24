package cmd

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"text/template"

	"github.com/juju/errors"
	"github.com/tmoeish/tsq"
	"mvdan.cc/gofumpt/format"
)

type generationModel struct {
	Data       *tsq.StructInfo
	Template   *template.Template
	Filename   string
	ErrorLabel string
}

func buildGenerationModels(
	list []*tsq.StructInfo,
	dir string,
	tableTpl *template.Template,
	dtoTpl *template.Template,
) ([]generationModel, error) {
	if err := validateGeneratedFilenameCollisions(list); err != nil {
		return nil, errors.Trace(err)
	}
	if err := validateIndexNameCollisions(list); err != nil {
		return nil, errors.Trace(err)
	}
	if err := validateGeneratedSymbolCollisions(list); err != nil {
		return nil, errors.Trace(err)
	}

	structsByName := make(map[string]*tsq.StructInfo, len(list))
	for _, s := range list {
		structsByName[s.TypeInfo.TypeName] = s
	}

	models := make([]generationModel, 0, len(list))
	for _, s := range list {
		if s.TableInfo == nil || len(s.Fields) == 0 {
			continue
		}

		if err := validateStructForGeneration(s, structsByName); err != nil {
			return nil, errors.Annotatef(err, "failed to validate %s", s.TypeInfo.TypeName)
		}

		model := generationModel{
			Data:     s,
			Filename: filepath.Join(dir, generatedFilename(s)),
		}

		if s.IsDTO {
			normalizeDTOColumns(s)
			model.Template = dtoTpl
			model.ErrorLabel = "DTO template rendering failed"
		} else {
			model.Template = tableTpl
			model.ErrorLabel = "template rendering failed"
		}

		models = append(models, model)
	}

	return models, nil
}

func renderGenerationModel(model generationModel) error {
	if v {
		_, _ = fmt.Fprintf(os.Stderr, "gen %s\n", model.Filename)
	}

	buf := new(bytes.Buffer)
	if err := model.Template.Execute(buf, model.Data); err != nil {
		bs := tsq.PrettyJSON(model.Data)
		return errors.Annotatef(err, "%s: %s, data: %s", model.ErrorLabel, model.Filename, bs)
	}

	src, err := format.Source(buf.Bytes(), format.Options{})
	if err != nil {
		return errors.Annotatef(err, "Go code formatting failed: %s", model.Filename)
	}

	if err := writeGeneratedFile(model.Filename, src); err != nil {
		return errors.Annotatef(err, "failed to write file: %s", model.Filename)
	}

	return nil
}
