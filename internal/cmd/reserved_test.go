package cmd

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tmoeish/tsq/v5/internal/genmodel"
)

// TestReservedTableNamesCoverTableOf reads every exported method declared on
// tsq.TableOf from the source, generic ones included, which reflect cannot list.
// A method missing from reservedTableFields would let a column field hide it.
func TestReservedTableNamesCoverTableOf(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("..", "..", "*.go"))
	if err != nil {
		t.Fatal(err)
	}

	reserved := reservedTableFields(&genmodel.StructInfo{TableMeta: &genmodel.TableMeta{}})
	fset := token.NewFileSet()
	found := 0

	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}

		f, err := parser.ParseFile(fset, file, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatal(err)
		}

		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || !fn.Name.IsExported() {
				continue
			}

			star, ok := fn.Recv.List[0].Type.(*ast.StarExpr)
			if !ok {
				continue
			}

			index, ok := star.X.(*ast.IndexListExpr)
			if !ok {
				continue
			}

			if ident, ok := index.X.(*ast.Ident); !ok || ident.Name != "TableOf" {
				continue
			}

			found++

			if _, ok := reserved[fn.Name.Name]; !ok {
				t.Errorf("TableOf.%s is not reserved; a column field of that name would hide it", fn.Name.Name)
			}
		}
	}

	if found < 20 {
		t.Fatalf("found only %d TableOf methods; the source scan is broken", found)
	}
}

func TestValidateFieldNamesRefusesCollisions(t *testing.T) {
	table := func(fields ...string) *genmodel.StructInfo {
		data := &genmodel.StructInfo{
			TableMeta: &genmodel.TableMeta{Uniques: []genmodel.IndexInfo{{Fields: []string{"Email"}}}},
			TypeInfo:  genmodel.TypeInfo{TypeName: "User"},
		}
		for _, name := range fields {
			data.Fields = append(data.Fields, genmodel.FieldInfo{Name: name})
		}

		return data
	}

	for _, name := range []string{"Update", "Query", "FetchBy", "TableOf", "As", "GetByEmail", "Columns"} {
		if err := validateFieldNames(table("ID", name)); err == nil || !strings.Contains(err.Error(), "User."+name) {
			t.Errorf("field %s: err = %v, want a collision", name, err)
		}
	}

	if err := validateFieldNames(table("ID", "Name", "Email", "Table")); err != nil {
		t.Errorf("ordinary fields refused: %v", err)
	}

	result := &genmodel.StructInfo{
		TableMeta: &genmodel.TableMeta{IsResult: true}, TypeInfo: genmodel.TypeInfo{TypeName: "Row"},
		Fields: []genmodel.FieldInfo{{Name: "Columns"}},
	}
	if err := validateFieldNames(result); err == nil {
		t.Error("a result field named Columns must be refused")
	}
}
