package parser

import (
	"container/list"
	"go/ast"
	"go/parser"
	"go/token"
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_parseStruct(t *testing.T) {
	pAlias := map[string]Pkg{
		"xxpkg":   {Path: "a.b/c/xxpkg", Name: "xxpkg"},
		"xxpkgv2": {Path: "a.b/c/xxpkg.v2", Name: "xxpkg"},
	}
	p := Pkg{Path: "a.b/c", Name: "p"}
	name := "S"
	structs := map[PkgTyp]*Struct{}
	parsedPkgs := map[Pkg]bool{}
	parsingPkgs := list.New()

	src := `
package p
type S struct {
	xxpkg.Embed
	xxpkgv2.Embed

	A rune ` + "`db:\"a\"`" + `
	B *btype ` + "`db:\"b\"`" + `
	C xxpkg.xxtype ` + "`db:\"c\"`" + `
	D *xxpkgv2.xxtype ` + "`db:\"d\"`" + `
	E []*byte ` + "`db:\"e\"`" + `
	F *xxpkgv2.xxtype ` + "`json:\"-\"`" + `
	f *xxpkgv2.xxtype ` + "`db:\"f\"`" + `
	g fff
}
`
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "", src, parser.AllErrors)
	if err != nil {
		log.Fatal(err)
	}

	st := f.Decls[0].(*ast.GenDecl).Specs[0].(*ast.TypeSpec).Type.(*ast.StructType)
	parseStruct(pAlias, p, name, st, structs, parsedPkgs, parsingPkgs)

	assert.Equal(t, 1, len(structs))

	s := structs[PkgTyp{p, name}]
	assert.Equal(t, p, s.Name.Pkg)
	assert.Equal(t, "S", s.Name.Name)
	assert.Equal(t, 5, len(s.FieldMap))
	assert.Equal(t, 2, len(s.embeddeds))
	assert.False(t, s.embeddedsResolved)
}

func TestStruct_resolveImports(t *testing.T) {
	pAlias := map[string]Pkg{
		"xxpkg":   {Path: "a.b/c/xxpkg", Name: "xxpkg"},
		"xxpkgv2": {Path: "a.b/c/xxpkg.v2", Name: "xxpkg"},
	}
	p := Pkg{Path: "a.b/c", Name: "p"}
	name := "S"
	structs := map[PkgTyp]*Struct{}
	parsedPkgs := map[Pkg]bool{}
	parsingPkgs := list.New()

	src := `
package p
type S struct {
	xxpkg.Embed
	xxpkgv2.Embed

	A rune ` + "`db:\"a\"`" + `
	B *btype ` + "`db:\"b\"`" + `
	C xxpkg.xxtype ` + "`db:\"c\"`" + `
	D *xxpkgv2.xxtype ` + "`db:\"d\"`" + `
	E []*byte ` + "`db:\"e\"`" + `
	F *xxpkgv2.xxtype ` + "`json:\"-\"`" + `
	f *xxpkgv2.xxtype ` + "`db:\"f\"`" + `
	g fff
}
`
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "", src, parser.AllErrors)
	if err != nil {
		log.Fatal(err)
	}

	st := f.Decls[0].(*ast.GenDecl).Specs[0].(*ast.TypeSpec).Type.(*ast.StructType)
	parseStruct(pAlias, p, name, st, structs, parsedPkgs, parsingPkgs)

	assert.Equal(t, 1, len(structs))

	s := structs[PkgTyp{p, name}]
	s.resolveImports()
}

func TestStruct_resolveFields(t *testing.T) {
}
