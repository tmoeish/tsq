package cmd

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/template"

	"github.com/juju/errors"
	"mvdan.cc/gofumpt/format"

	"github.com/tmoeish/tsq"
)

type generationModel struct {
	Data       *tsq.StructInfo
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
	list []*tsq.StructInfo,
	dir string,
	tableTpl *template.Template,
	resultTpl *template.Template,
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

		if s.IsResult {
			normalizeResultColumns(s)

			model.Template = resultTpl
			model.ErrorLabel = "Result template rendering failed"
		} else {
			model.Template = tableTpl
			model.ErrorLabel = "template rendering failed"
		}

		models = append(models, model)
	}

	return models, nil
}

func summarizeGenerationModels(models []generationModel) generationStats {
	stats := generationStats{}

	for _, model := range models {
		if model.Data != nil && model.Data.IsResult {
			stats.Results++
			continue
		}

		stats.Tables++
	}

	return stats
}

func renderGenerationModelSource(model generationModel) ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := model.Template.Execute(buf, model.Data); err != nil {
		bs := tsq.PrettyJSON(model.Data)
		return nil, errors.Annotatef(err, "%s: %s, data: %s", model.ErrorLabel, model.Filename, bs)
	}

	src, err := format.Source(buf.Bytes(), format.Options{})
	if err != nil {
		return nil, errors.Annotatef(err, "Go code formatting failed: %s", model.Filename)
	}

	return src, nil
}

func renderGenerationModel(model generationModel) error {
	if v {
		if _, err := fmt.Fprintf(os.Stderr, "gen %s\n", model.Filename); err != nil {
			return errors.Trace(err)
		}
	}

	src, err := renderGenerationModelSource(model)
	if err != nil {
		return errors.Trace(err)
	}

	if err := writeGeneratedFile(model.Filename, src); err != nil {
		return errors.Annotatef(err, "failed to write file: %s", model.Filename)
	}

	return nil
}

func buildGenerationPlan(models []generationModel, dir string) ([]generationPlanEntry, error) {
	plan := make([]generationPlanEntry, 0, len(models))
	plannedFiles := make(map[string]struct{}, len(models))

	for _, model := range models {
		src, err := renderGenerationModelSource(model)
		if err != nil {
			return nil, errors.Trace(err)
		}

		status, err := generationPlanStatusFor(model.Filename, src)
		if err != nil {
			return nil, errors.Annotatef(err, "failed to plan file: %s", model.Filename)
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
		return nil, errors.Trace(err)
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
	return strings.HasSuffix(name, "_tsq.go") || strings.HasSuffix(name, "_result_tsq.go")
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

	return errors.Errorf("generated files are out of date:\n%s", strings.Join(outdated, "\n"))
}

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
		"summary: %d create, %d update, %d unchanged, %d stale\n",
		createCount,
		updateCount,
		unchangedCount,
		staleCount,
	); err != nil {
		return
	}
}
