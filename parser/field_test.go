package parser

import (
	"go/ast"
	"go/parser"
	"go/token"
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_parseNamedFields(t *testing.T) {
	pAlias := map[string]Pkg{
		"xxpkg":   {Path: "a.b/c/xxpkg", Name: "xxpkg"},
		"xxpkgv2": {Path: "a.b/c/xxpkg.v2", Name: "xxpkg"},
	}
	p := Pkg{Path: "a.b/c", Name: "p"}

	src := `
package p
type S struct {
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
	fields := parseNamedFields(pAlias, p, st)
	assert.Equal(t, 5, len(fields))

	assert.Equal(t, Field{
		Name: "A",
		Arr:  false,
		Ptr:  false,
		Typ: PkgTyp{
			Pkg:  Pkg{},
			Name: "rune",
		},
		Column: "a",
	}, fields["A"])

	assert.Equal(t, Field{
		Name: "B",
		Arr:  false,
		Ptr:  true,
		Typ: PkgTyp{
			Pkg:  p,
			Name: "btype",
		},
		Column: "b",
	}, fields["B"])

	assert.Equal(t, Field{
		Name: "C",
		Arr:  false,
		Ptr:  false,
		Typ: PkgTyp{
			Pkg:  pAlias["xxpkg"],
			Name: "xxtype",
		},
		Column: "c",
	}, fields["C"])

	assert.Equal(t, Field{
		Name: "D",
		Arr:  false,
		Ptr:  true,
		Typ: PkgTyp{
			Pkg:  pAlias["xxpkgv2"],
			Name: "xxtype",
		},
		Column: "d",
	}, fields["D"])

	assert.Equal(t, Field{
		Name: "E",
		Arr:  true,
		Ptr:  true,
		Typ: PkgTyp{
			Pkg:  Pkg{},
			Name: "byte",
		},
		Column: "e",
	}, fields["E"])
}

func Test_parseEmbeddedFields(t *testing.T) {
	pAlias := map[string]Pkg{
		"xxpkg":   {Path: "a.b/c/xxpkg", Name: "xxpkg"},
		"xxpkgv2": {Path: "a.b/c/xxpkg.v2", Name: "xxpkg"},
	}
	p := Pkg{Path: "a.b/c", Name: "p"}

	src := `
package p
type S struct {
	structInCurrentPkg
	xxpkg.Struct
	xxpkg.Struct2
	*xxpkgv2.Struct
	*xxpkgv2.Struct2
}
`
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "", src, parser.AllErrors)
	if err != nil {
		log.Fatal(err)
	}

	st := f.Decls[0].(*ast.GenDecl).Specs[0].(*ast.TypeSpec).Type.(*ast.StructType)
	pkgs := parseEmbeddedFields(pAlias, p, st)
	assert.Equal(t, 5, len(pkgs))
	assert.True(t, pkgs[PkgTyp{Pkg{"a.b/c", "p"}, "structInCurrentPkg"}])
	assert.True(t, pkgs[PkgTyp{Pkg{"a.b/c/xxpkg", "xxpkg"}, "Struct"}])
	assert.True(t, pkgs[PkgTyp{Pkg{"a.b/c/xxpkg", "xxpkg"}, "Struct2"}])
	assert.True(t, pkgs[PkgTyp{Pkg{"a.b/c/xxpkg.v2", "xxpkg"}, "Struct"}])
	assert.True(t, pkgs[PkgTyp{Pkg{"a.b/c/xxpkg.v2", "xxpkg"}, "Struct2"}])
}

func Test_parseFieldType(t *testing.T) {
	{
		src := `
package p
type S struct {
	A xxtype
}
`
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, "", src, parser.AllErrors)
		if err != nil {
			log.Fatal(err)
		}

		d := f.Decls[0].(*ast.GenDecl).Specs[0].(*ast.TypeSpec).Type.(*ast.StructType)
		expr := d.Fields.List[0].Type
		star, arr, pkgName, name := parseFieldType(expr)
		assert.Equal(t, false, star)
		assert.Equal(t, false, arr)
		assert.Equal(t, "", pkgName)
		assert.Equal(t, "xxtype", name)
	}
	{
		src := `
package p
type S struct {
	A []int
}
`
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, "", src, parser.AllErrors)
		if err != nil {
			log.Fatal(err)
		}

		d := f.Decls[0].(*ast.GenDecl).Specs[0].(*ast.TypeSpec).Type.(*ast.StructType)
		expr := d.Fields.List[0].Type
		star, arr, pkgName, name := parseFieldType(expr)
		assert.Equal(t, false, star)
		assert.Equal(t, true, arr)
		assert.Equal(t, "", pkgName)
		assert.Equal(t, "int", name)
	}
	{
		src := `
package p
type S struct {
	A *xxtype
}
`
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, "", src, parser.AllErrors)
		if err != nil {
			log.Fatal(err)
		}

		d := f.Decls[0].(*ast.GenDecl).Specs[0].(*ast.TypeSpec).Type.(*ast.StructType)
		expr := d.Fields.List[0].Type
		star, arr, pkgName, name := parseFieldType(expr)
		assert.Equal(t, true, star)
		assert.Equal(t, false, arr)
		assert.Equal(t, "", pkgName)
		assert.Equal(t, "xxtype", name)
	}
	{
		src := `
package p
type S struct {
	A xxpkg.xxtype
}
`
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, "", src, parser.AllErrors)
		if err != nil {
			log.Fatal(err)
		}

		d := f.Decls[0].(*ast.GenDecl).Specs[0].(*ast.TypeSpec).Type.(*ast.StructType)
		expr := d.Fields.List[0].Type
		star, arr, pkgName, name := parseFieldType(expr)
		assert.Equal(t, false, star)
		assert.Equal(t, false, arr)
		assert.Equal(t, "xxpkg", pkgName)
		assert.Equal(t, "xxtype", name)
	}
	{
		src := `
package p
type S struct {
	A *xxpkg.xxtype
}
`
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, "", src, parser.AllErrors)
		if err != nil {
			log.Fatal(err)
		}

		d := f.Decls[0].(*ast.GenDecl).Specs[0].(*ast.TypeSpec).Type.(*ast.StructType)
		expr := d.Fields.List[0].Type
		star, arr, pkgName, name := parseFieldType(expr)
		assert.Equal(t, true, star)
		assert.Equal(t, false, arr)
		assert.Equal(t, "xxpkg", pkgName)
		assert.Equal(t, "xxtype", name)
	}
	{
		src := `
package p
type S struct {
	A []*xxpkg.xxtype
}
`
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, "", src, parser.AllErrors)
		if err != nil {
			log.Fatal(err)
		}

		d := f.Decls[0].(*ast.GenDecl).Specs[0].(*ast.TypeSpec).Type.(*ast.StructType)
		expr := d.Fields.List[0].Type
		star, arr, pkgName, name := parseFieldType(expr)
		assert.Equal(t, true, star)
		assert.Equal(t, true, arr)
		assert.Equal(t, "xxpkg", pkgName)
		assert.Equal(t, "xxtype", name)
	}
}
