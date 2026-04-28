package parser

import (
	"go/parser"
	"go/token"
	"log"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/tmoeish/tsq"
)

func Test_parsePackageAliases(t *testing.T) {
	src := `
package p

import "strings"

import (
	jsontestify "github.com/stretchr/testify/assert"
	"database/sql"
)
`
	fset := token.NewFileSet()

	f, err := parser.ParseFile(fset, "", src, parser.AllErrors)
	if err != nil {
		log.Fatal(err)
	}

	pkgs, err := parsePackageAliases(f)
	if err != nil {
		t.Fatalf("parsePackageAliases returned error: %v", err)
	}

	assert.Equal(t, 3, len(pkgs))
	assert.Equal(t, "strings", pkgs["strings"].Path)
	assert.Equal(t, "github.com/stretchr/testify/assert", pkgs["jsontestify"].Path)
	assert.Equal(t, "database/sql", pkgs["sql"].Path)
}

func Test_parsePackageAliases_UnresolvedImport(t *testing.T) {
	src := `
package p

import "example.invalid/missingpkg/v2"
`
	fset := token.NewFileSet()

	f, err := parser.ParseFile(fset, "", src, parser.AllErrors)
	if err != nil {
		log.Fatal(err)
	}

	_, err = parsePackageAliases(f)
	if err == nil {
		t.Fatal("expected unresolved import to return an error")
	}

	if !IsErrorType(err, ErrorTypePackageImport) {
		t.Fatalf("expected package import error, got %v", err)
	}
}

func Test_parsePackageAliases_RejectsDotImport(t *testing.T) {
	src := `
package p

import . "strings"
`
	fset := token.NewFileSet()

	f, err := parser.ParseFile(fset, "", src, parser.AllErrors)
	if err != nil {
		log.Fatal(err)
	}

	_, err = parsePackageAliases(f)
	if err == nil {
		t.Fatal("expected dot import to return an error")
	}

	if !strings.Contains(err.Error(), "dot imports are not supported") {
		t.Fatalf("expected dot import error, got %v", err)
	}
}

func Test_parsePackageAliases_RejectsDuplicateAlias(t *testing.T) {
	src := `
package p

import (
	io1 "io"
	io1 "io/fs"
)
`
	fset := token.NewFileSet()

	f, err := parser.ParseFile(fset, "", src, parser.AllErrors)
	if err != nil {
		log.Fatal(err)
	}

	_, err = parsePackageAliases(f)
	if err == nil {
		t.Fatal("expected duplicate import alias to return an error")
	}

	if !strings.Contains(err.Error(), `duplicate import alias "io1"`) {
		t.Fatalf("expected duplicate alias error, got %v", err)
	}
}

func Test_parsePackageAliases_SkipsBlankImports(t *testing.T) {
	src := `
package p

import (
	_ "net/http/pprof"
	"strings"
)
`
	fset := token.NewFileSet()

	f, err := parser.ParseFile(fset, "", src, parser.AllErrors)
	if err != nil {
		log.Fatal(err)
	}

	pkgs, err := parsePackageAliases(f)
	if err != nil {
		t.Fatalf("parsePackageAliases returned error: %v", err)
	}

	if len(pkgs) != 1 {
		t.Fatalf("expected blank import to be skipped, got %+v", pkgs)
	}

	if _, ok := pkgs["_"]; ok {
		t.Fatal("expected blank import alias to be omitted")
	}
}

func Test_importBuildPackage_RelativePath(t *testing.T) {
	buildPkg, err := importBuildPackage("../../examples/database")
	if err != nil {
		t.Fatalf("importBuildPackage returned error: %v", err)
	}

	if got := filepath.ToSlash(buildPkg.Dir); !strings.HasSuffix(got, "/examples/database") {
		t.Fatalf("expected package dir to resolve examples/database, got %q", got)
	}
}

func TestFilterAndProcessResultsOnlyReturnsTargetPackageStructs(t *testing.T) {
	buildPkg, err := importBuildPackage("../../examples/database")
	if err != nil {
		t.Fatalf("importBuildPackage returned error: %v", err)
	}

	targetPkg := tsq.PackageInfo{
		Path: buildPkg.ImportPath,
		Name: buildPkg.Name,
	}
	dependencyPkg := tsq.PackageInfo{
		Path: "example.com/dependency",
		Name: "dependency",
	}

	targetType := tsq.TypeInfo{Package: targetPkg, TypeName: "Target"}
	dependencyType := tsq.TypeInfo{Package: dependencyPkg, TypeName: "Dependency"}

	ps := &ParseState{
		structMap: map[tsq.TypeInfo]*StructInfo{
			targetType: {
				StructInfo: &tsq.StructInfo{
					TableInfo: &tsq.TableInfo{Table: "targets"},
					TypeInfo:  targetType,
					FieldMap: map[string]tsq.FieldInfo{
						"ID": {Name: "ID", Type: tsq.TypeInfo{TypeName: "int64"}},
					},
					Recv: "t",
				},
			},
			dependencyType: {
				StructInfo: &tsq.StructInfo{
					TableInfo: &tsq.TableInfo{Table: "dependencies"},
					TypeInfo:  dependencyType,
					FieldMap: map[string]tsq.FieldInfo{
						"ID": {Name: "ID", Type: tsq.TypeInfo{TypeName: "int64"}},
					},
					Recv: "d",
				},
			},
		},
	}

	result, err := ps.filterAndProcessResults("../../examples/database")
	if err != nil {
		t.Fatalf("filterAndProcessResults returned error: %v", err)
	}

	if got := len(result.Structs); got != 1 {
		t.Fatalf("expected exactly one struct in target package, got %d", got)
	}

	if got := result.Structs[0].TypeInfo.Package; got != targetPkg {
		t.Fatalf("expected only target package structs, got %+v", got)
	}
}
