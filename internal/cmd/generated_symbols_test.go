package cmd

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"testing"

	"golang.org/x/tools/go/packages"
)

// generatedPackageRefs maps the import alias that generated code uses to the
// package the alias refers to.
var generatedPackageRefs = map[string]string{
	"tsq":        "github.com/tmoeish/tsq/v4",
	"tsqdialect": "github.com/tmoeish/tsq/v4/dialect",
}

// TestGeneratedCodeReferencesOnlyRealSymbols checks every qualified symbol that
// the templates and the template helpers emit into generated code against the
// package that actually defines it.
//
// Neither templates nor helper string literals are type-checked when this
// package compiles, so a reference to a symbol that does not exist reaches the
// user as a compile error in their own tree. Asserting the emitted text instead
// only proves the helper agrees with itself: timestampNowValue emitted
// tsq.TimePtr for pointer-typed managed fields, its unit test asserted that
// exact string, and the root package never had a TimePtr.
func TestGeneratedCodeReferencesOnlyRealSymbols(t *testing.T) {
	sources, err := filepath.Glob("*.go.tmpl")
	if err != nil {
		t.Fatalf("glob templates: %v", err)
	}

	if len(sources) == 0 {
		t.Fatal("no templates found; this test must run in internal/cmd")
	}

	// Helpers build generated code as string literals, so they emit symbols the
	// templates never spell out.
	sources = append(sources, "template_helpers.go")

	refs := make(map[string]map[string][]string) // alias -> symbol -> source files
	for _, source := range sources {
		content, err := os.ReadFile(source)
		if err != nil {
			t.Fatalf("read %s: %v", source, err)
		}

		for alias, symbol := range qualifiedSymbolRefs(string(content)) {
			for _, name := range symbol {
				if refs[alias] == nil {
					refs[alias] = make(map[string][]string)
				}

				refs[alias][name] = append(refs[alias][name], source)
			}
		}
	}

	if len(refs) == 0 {
		t.Fatal("no qualified symbols found; the scanner is broken, not the templates")
	}

	for alias, symbols := range refs {
		importPath, ok := generatedPackageRefs[alias]
		if !ok {
			t.Errorf("generated code references unknown package alias %q; add it to generatedPackageRefs", alias)
			continue
		}

		exported := exportedSymbols(t, importPath)

		names := make([]string, 0, len(symbols))
		for name := range symbols {
			names = append(names, name)
		}

		sort.Strings(names)

		for _, name := range names {
			if !exported[name] {
				t.Errorf(
					"generated code references %s.%s (from %v), but %s exports no such symbol",
					alias, name, symbols[name], importPath,
				)
			}
		}
	}
}

var qualifiedSymbolPattern = regexp.MustCompile(`\b(tsq|tsqdialect)\.([A-Z][A-Za-z0-9_]*)`)

// qualifiedSymbolRefs returns the exported symbols referenced per package alias.
func qualifiedSymbolRefs(content string) map[string][]string {
	refs := make(map[string][]string)
	for _, match := range qualifiedSymbolPattern.FindAllStringSubmatch(content, -1) {
		refs[match[1]] = append(refs[match[1]], match[2])
	}

	return refs
}

// exportedSymbols returns the exported package-scope names of importPath.
func exportedSymbols(t *testing.T, importPath string) map[string]bool {
	t.Helper()

	loaded, err := packages.Load(&packages.Config{Mode: packages.NeedName | packages.NeedTypes}, importPath)
	if err != nil {
		t.Fatalf("load %s: %v", importPath, err)
	}

	if len(loaded) != 1 || loaded[0].Types == nil {
		t.Fatalf("load %s: expected exactly one typed package, got %d", importPath, len(loaded))
	}

	scope := loaded[0].Types.Scope()

	exported := make(map[string]bool, len(scope.Names()))
	for _, name := range scope.Names() {
		if scope.Lookup(name).Exported() {
			exported[name] = true
		}
	}

	return exported
}
