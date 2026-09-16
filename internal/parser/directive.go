package parser

import (
	"fmt"
	"go/ast"
	"strings"

	"github.com/serenize/snaker"

	"github.com/tmoeish/tsq/v4/internal/genmodel"
)

// DirectivePrefix marks a TSQ directive. It follows the //go: convention: no
// space after the slashes, one concern per line.
//
// The previous form was a parenthesised DSL inside a doc comment, which gofmt
// reflowed and re-indented, so the generator shipped a formatter (`tsq fmt`)
// whose only job was to put the annotation back the way the parser wanted it.
// A directive line survives gofmt untouched.
const DirectivePrefix = "//tsq:"

// directive is one parsed //tsq: line.
type directive struct {
	name string   // the word after the prefix, such as "table"
	args []string // whitespace-separated arguments
	line string   // the original line, for error messages
}

// ParseDirectives builds table metadata from the //tsq: lines of a struct's
// comment groups. It returns nil when the struct carries no directive.
func ParseDirectives(
	structName string,
	commentGroups []*ast.CommentGroup,
	structFields map[string]struct{},
) (*genmodel.TableMeta, error) {
	directives := collectDirectives(commentGroups)
	if len(directives) == 0 {
		return nil, nil
	}

	kind, err := declarationDirective(structName, directives)
	if err != nil {
		return nil, err
	}

	info := &genmodel.TableMeta{IsResult: kind.name == "result"}
	if !info.IsResult {
		info.Table = snaker.CamelToSnake(structName)
		info.PK = DefaultPKField
		info.AI = true
	}

	if err := applyDeclaration(info, kind); err != nil {
		return nil, err
	}

	for _, d := range directives {
		if d.name == "table" || d.name == "result" {
			continue
		}

		if err := applyDirective(info, d); err != nil {
			return nil, err
		}
	}

	normalizeIndexNames(info.UxList, "ux", info.Table)
	normalizeIndexNames(info.IdxList, "idx", info.Table)

	if err := validateTableInfoAgainstStruct(info, structFields, structName); err != nil {
		return nil, err
	}

	return info, nil
}

// collectDirectives returns every //tsq: line across the struct's comment groups.
func collectDirectives(commentGroups []*ast.CommentGroup) []directive {
	var directives []directive

	for _, group := range commentGroups {
		if group == nil {
			continue
		}

		for _, comment := range group.List {
			text := strings.TrimRight(comment.Text, " \t")
			if !strings.HasPrefix(text, DirectivePrefix) {
				continue
			}

			fields := strings.Fields(text[len(DirectivePrefix):])
			if len(fields) == 0 {
				continue
			}

			directives = append(directives, directive{
				name: fields[0],
				args: fields[1:],
				line: text,
			})
		}
	}

	return directives
}

// declarationDirective finds the single table or result directive.
func declarationDirective(structName string, directives []directive) (directive, error) {
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
		return directive{}, fmt.Errorf(
			"%s: the other %s directives need a %stable or %sresult line to attach to",
			structName, DirectivePrefix, DirectivePrefix, DirectivePrefix,
		)
	default:
		return directive{}, fmt.Errorf(
			"%s: a struct declares %stable or %sresult once, found %d",
			structName, DirectivePrefix, DirectivePrefix, len(found),
		)
	}
}

// applyDeclaration reads the table or result line.
func applyDeclaration(info *genmodel.TableMeta, d directive) error {
	for _, arg := range d.args {
		key, value, hasValue := strings.Cut(arg, "=")

		switch {
		case key == "name" && hasValue:
			if value == "" {
				return directiveError(d, "name needs a value")
			}

			if info.IsResult {
				continue // the result name is the struct name; name= is accepted for symmetry
			}

			info.Table = value
		case key == "pk" && hasValue:
			if info.IsResult {
				return directiveError(d, "pk belongs to a table, not a result")
			}

			if value == "" {
				return directiveError(d, "pk needs a Go field name")
			}

			info.PK = value
		case key == "assigned" && !hasValue:
			if info.IsResult {
				return directiveError(d, "assigned belongs to a table, not a result")
			}

			info.AI = false
		default:
			return directiveError(d, fmt.Sprintf("unknown option %q", arg))
		}
	}

	return nil
}

// managedRoles maps a directive word to the metadata field it fills and the Go
// field name used when the word carries no explicit one.
var managedRoles = map[string]struct {
	defaultField string
	assign       func(*genmodel.TableMeta, string)
}{
	"version":    {DefaultVersionField, func(m *genmodel.TableMeta, f string) { m.VersionField = f }},
	"created_at": {DefaultCreatedAtField, func(m *genmodel.TableMeta, f string) { m.CreatedAtField = f }},
	"updated_at": {DefaultUpdatedAtField, func(m *genmodel.TableMeta, f string) { m.UpdatedAtField = f }},
	"deleted_at": {DefaultDeletedAtField, func(m *genmodel.TableMeta, f string) { m.DeletedAtField = f }},
}

// applyDirective reads one non-declaration directive.
func applyDirective(info *genmodel.TableMeta, d directive) error {
	switch d.name {
	case "managed":
		return applyManaged(info, d)
	case "search":
		fields, _, err := indexArgs(d)
		if err != nil {
			return err
		}

		info.SearchColumns = append(info.SearchColumns, fields...)

		return nil
	case "unique", "index":
		if info.IsResult {
			return directiveError(d, d.name+" belongs to a table, not a result")
		}

		fields, name, err := indexArgs(d)
		if err != nil {
			return err
		}

		idx := genmodel.IndexInfo{Name: name, Fields: fields}
		if d.name == "unique" {
			info.UxList = append(info.UxList, idx)
		} else {
			info.IdxList = append(info.IdxList, idx)
		}

		return nil
	default:
		return directiveError(d, fmt.Sprintf("unknown directive %q", d.name))
	}
}

// applyManaged reads a managed directive: role words, each optionally naming the
// Go field that carries it.
func applyManaged(info *genmodel.TableMeta, d directive) error {
	if info.IsResult {
		return directiveError(d, "managed fields belong to a table, not a result")
	}

	if len(d.args) == 0 {
		return directiveError(d, "managed needs at least one of version, created_at, updated_at, deleted_at")
	}

	for _, arg := range d.args {
		role, field, hasField := strings.Cut(arg, "=")

		spec, ok := managedRoles[role]
		if !ok {
			return directiveError(d, fmt.Sprintf("unknown managed field %q", role))
		}

		if !hasField {
			field = spec.defaultField
		}

		if field == "" {
			return directiveError(d, fmt.Sprintf("%s needs a Go field name", role))
		}

		spec.assign(info, field)
	}

	return nil
}

// indexArgs splits an index-shaped directive into its field list and optional name.
func indexArgs(d directive) (fields []string, name string, err error) {
	for _, arg := range d.args {
		if key, value, hasValue := strings.Cut(arg, "="); hasValue {
			if key != "name" {
				return nil, "", directiveError(d, fmt.Sprintf("unknown option %q", arg))
			}

			if value == "" {
				return nil, "", directiveError(d, "name needs a value")
			}

			name = value

			continue
		}

		for field := range strings.SplitSeq(arg, ",") {
			field = strings.TrimSpace(field)
			if field == "" {
				return nil, "", directiveError(d, "empty field name in the list")
			}

			fields = append(fields, field)
		}
	}

	if len(fields) == 0 {
		return nil, "", directiveError(d, "needs at least one Go field name")
	}

	return fields, name, nil
}

func directiveError(d directive, reason string) error {
	return fmt.Errorf("%s: %s", strings.TrimSpace(d.line), reason)
}
