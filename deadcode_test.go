package tsq

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// deadcodePackages are the packages whose unexported code must be reachable from
// code that ships, not only from tests.
var deadcodePackages = []string{".", "dialect", "internal/cmd", "internal/genmodel", "internal/parser", "internal/sqldialect"}

// TestNoUnexportedCodeOnlyTestsReach fails on an unexported declaration that no
// file outside _test.go refers to while a test does. Such code is not in the
// library: the test proves it is consistent with itself, not that anything calls
// it. The unused linter cannot see it, because a reference from a test counts as a
// use. This happened four times: tracers behind a key nothing set, a copy of the
// capability canonicalizer, a package-level Runtime in a test, and upsertIndex, a
// second copy of the index policy that had drifted from the real one.
//
// Names are matched as identifiers, without type information, so two declarations
// sharing a name can hide each other; that only lets something through.
func TestNoUnexportedCodeOnlyTestsReach(t *testing.T) {
	for _, dir := range deadcodePackages {
		declared := map[string]token.Position{}
		used := map[string]int{}
		tested := map[string]int{}

		fset := token.NewFileSet()

		files, err := filepath.Glob(filepath.Join(dir, "*.go"))
		if err != nil {
			t.Fatal(err)
		}

		for _, file := range files {
			source, err := os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}

			f, err := parser.ParseFile(fset, file, source, parser.SkipObjectResolution)
			if err != nil {
				t.Fatal(err)
			}

			test := strings.HasSuffix(file, "_test.go")
			if !test {
				for _, decl := range f.Decls {
					for _, name := range unexportedNames(decl) {
						declared[name.Name] = fset.Position(name.Pos())
					}
				}
			}

			ast.Inspect(f, func(n ast.Node) bool {
				if ident, ok := n.(*ast.Ident); ok {
					if test {
						tested[ident.Name]++
					} else {
						used[ident.Name]++
					}
				}

				return true
			})
		}

		for name, at := range declared {
			// The declaration itself is one use.
			if used[name] <= 1 && tested[name] > 0 {
				t.Errorf("%s: %s is referred to only by tests; call it from the package or delete it with its tests", at, name)
			}
		}
	}
}

// unexportedNames are the unexported names decl introduces at package level.
func unexportedNames(decl ast.Decl) []*ast.Ident {
	var names []*ast.Ident

	keep := func(ident *ast.Ident) {
		if ident != nil && !ident.IsExported() && ident.Name != "_" && ident.Name != "init" && ident.Name != "main" {
			names = append(names, ident)
		}
	}

	switch d := decl.(type) {
	case *ast.FuncDecl:
		keep(d.Name)
	case *ast.GenDecl:
		for _, spec := range d.Specs {
			switch s := spec.(type) {
			case *ast.TypeSpec:
				keep(s.Name)
			case *ast.ValueSpec:
				for _, name := range s.Names {
					keep(name)
				}
			}
		}
	}

	return names
}
