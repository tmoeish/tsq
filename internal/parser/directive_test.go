package parser

import (
	"errors"
	"go/parser"
	"go/token"
	"reflect"
	"strings"
	"testing"

	"github.com/tmoeish/tsq/v5/internal/genmodel"
)

// parseSource runs the annotation parser over the first struct in src.
func parseSource(t *testing.T, src string, fields ...string) (*genmodel.TableMeta, error) {
	t.Helper()

	fset := token.NewFileSet()

	file, err := parser.ParseFile(fset, "model.go", "package p\n\n"+src, parser.ParseComments)
	if err != nil {
		t.Fatalf("ParseFile() error = %v", err)
	}

	set := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		set[field] = struct{}{}
	}

	return parseAnnotations("User", file.Comments, set, fset)
}

func TestParseAnnotations(t *testing.T) {
	tests := []struct {
		name   string
		src    string
		fields []string
		want   genmodel.TableMeta
	}{
		{
			name: "every directive",
			src: `//tsq:table name=account pk=Key
//tsq:managed version created_at updated_at=MTime deleted_at
//tsq:unique F1,F2 name=u1
//tsq:unique F3
//tsq:index F4 name=i1
//tsq:index F5,F6
//tsq:search F1,F3
type User struct{}`,
			fields: []string{"Key", "Version", "CreatedAt", "MTime", "DeletedAt", "F1", "F2", "F3", "F4", "F5", "F6"},
			want: genmodel.TableMeta{
				Table:          "account",
				PrimaryKey:     "Key",
				AutoIncrement:  true,
				VersionField:   "Version",
				CreatedAtField: "CreatedAt",
				UpdatedAtField: "MTime",
				DeletedAtField: "DeletedAt",
				Uniques: []genmodel.IndexInfo{
					{Name: "u1", Fields: []string{"F1", "F2"}},
					{Name: "ux_account_f3", Fields: []string{"F3"}},
				},
				Indexes: []genmodel.IndexInfo{
					{Name: "i1", Fields: []string{"F4"}},
					{Name: "idx_account_f5_f6", Fields: []string{"F5", "F6"}},
				},
				SearchColumns: []string{"F1", "F3"},
			},
		},
		{
			name:   "defaults",
			src:    "//tsq:table\ntype User struct{}",
			fields: []string{"ID"},
			want:   genmodel.TableMeta{Table: "user", PrimaryKey: "ID", AutoIncrement: true},
		},
		{
			name:   "assigned primary key",
			src:    "//tsq:table pk=Code assigned\ntype User struct{}",
			fields: []string{"Code"},
			want:   genmodel.TableMeta{Table: "user", PrimaryKey: "Code"},
		},
		{
			name:   "result",
			src:    "//tsq:result\n//tsq:search Name\ntype User struct{}",
			fields: []string{"Name"},
			want:   genmodel.TableMeta{IsResult: true, SearchColumns: []string{"Name"}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := parseSource(t, test.src, test.fields...)
			if err != nil {
				t.Fatalf("parseAnnotations() error = %v", err)
			}

			if !reflect.DeepEqual(*got, test.want) {
				t.Fatalf("got  %+v\nwant %+v", *got, test.want)
			}
		})
	}
}

func TestParseAnnotationsIgnoresStructsWithoutDirectives(t *testing.T) {
	got, err := parseSource(t, "// User mentions //tsq:table only in prose.\ntype User struct{}", "ID")
	if err != nil || got != nil {
		t.Fatalf("expected no metadata and no error, got %+v, %v", got, err)
	}
}

// TestParseAnnotationsReportsTheLine checks that each mistake names the file,
// the line and the directive it came from.
func TestParseAnnotationsReportsTheLine(t *testing.T) {
	tests := []struct {
		name string
		src  string
		line string
		want string
	}{
		{"no declaration", "//tsq:unique Name\ntype User struct{}", "model.go:3", "add a //tsq:table"},
		{"two declarations", "//tsq:table\n//tsq:result\ntype User struct{}", "model.go:4", "already declares"},
		{"unknown directive", "//tsq:table\n//tsq:kw Name\ntype User struct{}", "model.go:4", `unknown directive "kw"`},
		{"unknown option", "//tsq:table oops=1\ntype User struct{}", "model.go:3", `unknown option "oops=1"`},
		{"composite key", "//tsq:table pk=OrgID,Slug\ntype User struct{}", "model.go:3", "//tsq:unique OrgID,Slug"},
		{"result option", "//tsq:result name=x\ntype User struct{}", "model.go:3", "a result takes no options"},
		{"unknown role", "//tsq:table\n//tsq:managed touched_at\ntype User struct{}", "model.go:4", `unknown managed field "touched_at"`},
		{"missing field", "//tsq:table\n//tsq:unique Nickname\ntype User struct{}", "model.go:4", "struct has no field Nickname"},
		{"missing pk", "//tsq:table pk=Code\ntype User struct{}", "model.go:3", "struct has no field Code"},
		{"empty index", "//tsq:table\n//tsq:index name=i\ntype User struct{}", "model.go:4", "name at least one Go field"},
		{"repeated field", "//tsq:table\n//tsq:index Name,Name\ntype User struct{}", "model.go:4", "listed twice"},
		{"same fields twice", "//tsq:table\n//tsq:unique Name\n//tsq:index Name\ntype User struct{}", "model.go:5", "already covers Name"},
		{"index on result", "//tsq:result\n//tsq:index Name\ntype User struct{}", "model.go:4", "indexes belong to a table"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := parseSource(t, test.src, "ID", "Name")
			if err == nil {
				t.Fatal("expected an error")
			}

			if !errors.Is(err, ErrInvalidDirective) {
				t.Fatalf("expected ErrInvalidDirective, got %v", err)
			}

			for _, want := range []string{test.line, test.want} {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("expected the error to mention %q, got %q", want, err)
				}
			}
		})
	}
}
