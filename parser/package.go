package parser

import (
	"container/list"
	"fmt"
	"go/ast"
	"go/build"
	"go/parser"
	"go/token"
	"path"
	"reflect"
	"strings"

	"github.com/juju/errors"
	"github.com/sirupsen/logrus"
)

type Pkg struct {
	Path string
	Name string
}

func Parse(pPath string) ([]*Struct, string) {
	structs := make(map[PkgTyp]*Struct)
	parsedPkgs := make(map[Pkg]bool)
	parsingPkgs := list.New()

	// parse package recursively
	parsingPkgs.PushBack(pPath)
	for parsingPkgs.Len() > 0 {
		e := parsingPkgs.Front()
		parsingPkgs.Remove(e)
		parsePkg(e.Value.(string), structs, parsedPkgs, parsingPkgs)
	}

	// resolve embedded
	for _, s := range structs {
		resolveEmbeddeds(s, structs)
	}

	bp, err := build.Default.Import(pPath, "", 0)
	if err != nil {
		logrus.Fatalf("process directory failed: %s", err)
	}

	p := Pkg{
		Path: bp.ImportPath,
		Name: bp.Name,
	}

	// fill table meta
	fs := token.NewFileSet()
	for _, filename := range bp.GoFiles {
		if strings.HasSuffix(filename, "_tsq.go") {
			continue
		}
		if !strings.HasSuffix(filename, ".go") {
			continue
		}

		filename = path.Join(bp.Dir, filename)
		file, err := parser.ParseFile(fs, filename, nil, parser.ParseComments)
		if err != nil {
			logrus.Fatalf(errors.ErrorStack(err))
		}

		// ast.Inspect(file, traverses)
		cmap := ast.NewCommentMap(fs, file, file.Comments)
		for node, comments := range cmap {
			switch node.(type) {
			case *ast.GenDecl:
				for _, spec := range node.(*ast.GenDecl).Specs {
					typeSpec, ok := spec.(*ast.TypeSpec)
					if !ok {
						continue
					}
					_, ok = typeSpec.Type.(*ast.StructType)
					if !ok {
						continue
					}

					name := typeSpec.Name.Name
					tm := ParseTableMeta(name, comments)
					if tm != nil {
						structs[PkgTyp{
							Pkg:  p,
							Name: name,
						}].TableMeta = tm
					}
				}

			case *ast.TypeSpec:
				typeSpec := node.(*ast.TypeSpec)
				_, ok := typeSpec.Type.(*ast.StructType)
				if !ok {
					continue
				}
				_, ok = typeSpec.Type.(*ast.StructType)
				if !ok {
					continue
				}

				name := typeSpec.Name.Name
				tm := ParseTableMeta(name, comments)
				if tm != nil {
					structs[PkgTyp{
						Pkg:  p,
						Name: name,
					}].TableMeta = tm
				}

			default:
				logrus.Debugln(reflect.TypeOf(node))
			}
		}
	}

	// filter out table-less struct
	var rs []*Struct
	for _, s := range structs {
		if s.TableMeta == nil {
			continue
		}
		s.resolveImports()
		s.resolveFields()
		rs = append(rs, s)
	}

	return rs, bp.Dir
}

func parsePkg(
	pPath string,
	structs map[PkgTyp]*Struct,
	parsedPkgs map[Pkg]bool,
	parsingPkgs *list.List,
) {
	bp, err := build.Default.Import(pPath, "", 0)
	if err != nil {
		logrus.Fatalf("process pkg failed: %s", err)
	}

	p := Pkg{
		Path: bp.ImportPath,
		Name: bp.Name,
	}
	logrus.Debugf("package: %s", pPath)

	fs := token.NewFileSet()
	for _, filename := range bp.GoFiles {
		if strings.HasSuffix(filename, "_tsq.go") {
			continue
		}
		if !strings.HasSuffix(filename, ".go") {
			continue
		}

		filename = path.Join(bp.Dir, filename)
		logrus.Debugf("file: %s", filename)
		f, err := parser.ParseFile(fs, filename, nil, parser.ParseComments)
		if err != nil {
			logrus.Fatalf(errors.ErrorStack(err))
		}

		pkgAlias := parsePkgAlias(f)

		for _, decl := range f.Decls {
			switch decl.(type) {
			case *ast.GenDecl:
				for _, spec := range decl.(*ast.GenDecl).Specs {
					ts, ok := spec.(*ast.TypeSpec)
					if !ok {
						continue
					}
					st, ok := ts.Type.(*ast.StructType)
					if !ok {
						continue
					}

					parseStruct(
						pkgAlias, p, ts.Name.Name, st,
						structs, parsedPkgs, parsingPkgs,
					)
				}

			default:
			}

		}
	}
}

func parsePkgAlias(f *ast.File) map[string]Pkg {
	pAlias := make(map[string]Pkg)
	for _, i := range f.Imports {
		pPath := strings.Trim(i.Path.Value, `"`)
		p := getPkg(pPath)
		if i.Name != nil {
			pAlias[i.Name.Name] = p
		} else {
			pAlias[p.Name] = p
		}
	}
	return pAlias
}

func getPkg(importPath string) Pkg {
	bPkg, err := build.Default.Import(importPath, "", 0)
	if err != nil {
		logrus.Fatalf("get pkg failed: %s", err)
	}

	return Pkg{
		Path: bPkg.ImportPath,
		Name: bPkg.Name,
	}
}

func resolveEmbeddeds(
	s *Struct,
	structs map[PkgTyp]*Struct,
) {
	if s.embeddedsResolved {
		return
	}

	logrus.Debugln("resolve embeddeds", s.Name)
	for fsn := range s.embeddeds {
		logrus.Debugln("embedded", fsn)
		emb, ok := structs[fsn]
		if !ok {
			logrus.Fatalf("embedded struct %s not found", fsn)
		}

		if !emb.embeddedsResolved {
			resolveEmbeddeds(emb, structs)
		} else {
			logrus.Debugln("embedded struct already resolved", fsn)
		}

		for k, v := range emb.FieldMap {
			if _, ok := s.FieldMap[k]; ok {
				logrus.
					WithField("struct", s.Name).
					WithField("field", v.Name).
					Fatalf(fmt.Sprintf("field %s already exists", k))
			}
			s.FieldMap[k] = v
			logrus.WithField("struct", s.Name).Debugln("embedded field", v)
		}
	}

	s.embeddedsResolved = true
}
