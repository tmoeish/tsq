package parser

import (
	"fmt"
	"go/ast"
	"go/token"
	"slices"
	"strings"

	"github.com/serenize/snaker"

	"github.com/tmoeish/tsq/v5/internal/genmodel"
)

// DirectivePrefix marks a TSQ directive. It follows the //go: convention: no
// space after the slashes and one concern per line, which gofmt leaves alone.
const DirectivePrefix = "//tsq:"

// defaultPrimaryKeyField is the Go field a table uses as its key unless pk= says otherwise.
const defaultPrimaryKeyField = "ID"

// directive is one //tsq: line.
type directive struct {
	name string   // the word after the prefix, such as "table"
	args []string // whitespace-separated arguments
	text string   // the line as written
	pos  token.Position
}

func (d directive) errorf(format string, args ...any) error {
	return fmt.Errorf("%s: %w: %s: %s", d.pos, ErrInvalidDirective, d.text, fmt.Sprintf(format, args...))
}

// parseAnnotations builds the table metadata a struct declares through //tsq:
// directives. It returns nil when the struct carries none.
func parseAnnotations(
	structName string,
	comments []*ast.CommentGroup,
	fields map[string]struct{},
	fileSet *token.FileSet,
) (*genmodel.TableMeta, error) {
	directives := collectDirectives(comments, fileSet)
	if len(directives) == 0 {
		return nil, nil
	}

	declaration, err := findDeclaration(structName, directives)
	if err != nil {
		return nil, err
	}

	meta := &genmodel.TableMeta{IsResult: declaration.name == "result"}
	if !meta.IsResult {
		meta.Table = snaker.CamelToSnake(structName)
		meta.PrimaryKey = defaultPrimaryKeyField
		meta.AutoIncrement = true
	}

	if err := applyDeclaration(meta, declaration); err != nil {
		return nil, err
	}

	for _, d := range directives {
		if d.name == "table" || d.name == "result" {
			continue
		}

		if err := applyDirective(meta, d, fields); err != nil {
			return nil, err
		}
	}

	if err := checkReferencedFields(meta, fields, declaration); err != nil {
		return nil, err
	}

	byName := func(a, b genmodel.IndexInfo) int { return strings.Compare(a.Name, b.Name) }
	slices.SortFunc(meta.Uniques, byName)
	slices.SortFunc(meta.Indexes, byName)

	return meta, nil
}

func collectDirectives(comments []*ast.CommentGroup, fileSet *token.FileSet) []directive {
	var directives []directive

	for _, group := range comments {
		if group == nil {
			continue
		}

		for _, comment := range group.List {
			text := strings.TrimRight(comment.Text, " \t")
			if !strings.HasPrefix(text, DirectivePrefix) {
				continue
			}

			words := strings.Fields(text[len(DirectivePrefix):])
			if len(words) == 0 {
				continue
			}

			d := directive{name: words[0], args: words[1:], text: text}
			if fileSet != nil {
				d.pos = fileSet.Position(comment.Slash)
			}

			directives = append(directives, d)
		}
	}

	return directives
}

func findDeclaration(structName string, directives []directive) (directive, error) {
	var found []directive

	for _, d := range directives {
		if d.name == "table" || d.name == "result" {
			found = append(found, d)
		}
	}

	switch len(found) {
	case 1:
		return found[0], nil
	case 0:
		return directive{}, fmt.Errorf("%s: %w: add a %stable or %sresult line for the other directives to attach to",
			directives[0].pos, ErrInvalidDirective, DirectivePrefix, DirectivePrefix)
	default:
		return directive{}, found[1].errorf("%s already declares %s%s", structName, DirectivePrefix, found[0].name)
	}
}

func applyDeclaration(meta *genmodel.TableMeta, d directive) error {
	for _, arg := range d.args {
		key, value, hasValue := strings.Cut(arg, "=")

		switch {
		case meta.IsResult:
			return d.errorf("a result takes no options, got %q", arg)
		case key == "name" && hasValue && value != "":
			meta.Table = value
		case key == "pk" && hasValue && strings.Contains(value, ","):
			return d.errorf("composite primary keys are not supported; add a single-column key and declare //tsq:unique %s", value)
		case key == "pk" && hasValue && value != "":
			meta.PrimaryKey = value
		case key == "assigned" && !hasValue:
			meta.AutoIncrement = false
		default:
			return d.errorf("unknown option %q", arg)
		}
	}

	return nil
}

// managedRoles maps a role word to its default Go field and the metadata it fills.
var managedRoles = map[string]struct {
	defaultField string
	assign       func(*genmodel.TableMeta, string)
}{
	"version":    {"Version", func(m *genmodel.TableMeta, f string) { m.VersionField = f }},
	"created_at": {"CreatedAt", func(m *genmodel.TableMeta, f string) { m.CreatedAtField = f }},
	"updated_at": {"UpdatedAt", func(m *genmodel.TableMeta, f string) { m.UpdatedAtField = f }},
	"deleted_at": {"DeletedAt", func(m *genmodel.TableMeta, f string) { m.DeletedAtField = f }},
}

func applyDirective(meta *genmodel.TableMeta, d directive, fields map[string]struct{}) error {
	switch d.name {
	case "managed":
		if meta.IsResult {
			return d.errorf("managed fields belong to a table")
		}

		if len(d.args) == 0 {
			return d.errorf("name at least one of version, created_at, updated_at, deleted_at")
		}

		for _, arg := range d.args {
			role, field, hasField := strings.Cut(arg, "=")

			spec, ok := managedRoles[role]
			if !ok {
				return d.errorf("unknown managed field %q", role)
			}

			if !hasField {
				field = spec.defaultField
			}

			if field == "" {
				return d.errorf("%s= needs a Go field name", role)
			}

			spec.assign(meta, field)
		}

		return nil

	case "search":
		list, name, err := fieldList(d, fields)
		if err != nil {
			return err
		}

		if name != "" {
			return d.errorf("search takes no name")
		}

		meta.SearchColumns = append(meta.SearchColumns, list...)

		return nil
	case "unique", "index":
		if meta.IsResult {
			return d.errorf("indexes belong to a table")
		}

		list, name, err := fieldList(d, fields)
		if err != nil {
			return err
		}

		prefix := "idx"
		if d.name == "unique" {
			prefix = "ux"
		}

		if name == "" {
			name = derivedIndexName(prefix, meta.Table, list)
		}

		for _, existing := range slices.Concat(meta.Uniques, meta.Indexes) {
			if slices.Equal(existing.Fields, list) {
				return d.errorf("index %s already covers %s", existing.Name, strings.Join(list, ","))
			}

			if existing.Name == name {
				return d.errorf("index name %s is already used", name)
			}
		}

		index := genmodel.IndexInfo{Name: name, Fields: list}
		if d.name == "unique" {
			meta.Uniques = append(meta.Uniques, index)
		} else {
			meta.Indexes = append(meta.Indexes, index)
		}

		return nil

	default:
		return d.errorf("unknown directive %q", d.name)
	}
}

// fieldList reads a comma-separated Go field list and an optional name= option.
func fieldList(d directive, fields map[string]struct{}) (list []string, name string, err error) {
	for _, arg := range d.args {
		if key, value, hasValue := strings.Cut(arg, "="); hasValue {
			if key != "name" || value == "" {
				return nil, "", d.errorf("unknown option %q", arg)
			}

			name = value

			continue
		}

		for field := range strings.SplitSeq(arg, ",") {
			switch {
			case field == "":
				return nil, "", d.errorf("empty field name")
			case slices.Contains(list, field):
				return nil, "", d.errorf("field %s is listed twice", field)
			}

			if _, ok := fields[field]; fields != nil && !ok {
				return nil, "", d.errorf("struct has no field %s (use Go field names, not column names)", field)
			}

			list = append(list, field)
		}
	}

	if len(list) == 0 {
		return nil, "", d.errorf("name at least one Go field")
	}

	return list, name, nil
}

// checkReferencedFields verifies the fields named by the declaration and the
// managed roles exist.
func checkReferencedFields(meta *genmodel.TableMeta, fields map[string]struct{}, declaration directive) error {
	if fields == nil {
		return nil
	}

	for _, field := range []string{meta.PrimaryKey, meta.VersionField, meta.CreatedAtField, meta.UpdatedAtField, meta.DeletedAtField} {
		if field == "" {
			continue
		}

		if _, ok := fields[field]; !ok {
			return declaration.errorf("struct has no field %s (use Go field names, not column names)", field)
		}
	}

	return nil
}

func derivedIndexName(prefix, table string, fields []string) string {
	parts := []string{prefix, snaker.CamelToSnake(table)}
	for _, field := range fields {
		parts = append(parts, snaker.CamelToSnake(field))
	}

	return strings.Join(parts, "_")
}
