package parser

import (
	"container/list"
	"fmt"
	"go/ast"
	"sort"

	"github.com/sirupsen/logrus"
)

type Struct struct {
	*TableMeta

	Name      PkgTyp
	Imports   map[string]string // pkg path => pkg name
	FieldList []Field
	FieldMap  map[string]Field

	embeddeds         map[PkgTyp]bool
	embeddedsResolved bool
}

func parseStruct(
	pAlias map[string]Pkg,
	p Pkg,
	name string,
	st *ast.StructType,
	structs map[PkgTyp]*Struct,
	parsedPkgs map[Pkg]bool,
	parsingPkgs *list.List,
) {
	typ := PkgTyp{
		Pkg:  p,
		Name: name,
	}

	logrus.Debugf("struct: %s", typ)

	embedded := parseEmbeddedFields(pAlias, p, st)
	for fsn := range embedded {
		if _, ok := parsedPkgs[fsn.Pkg]; !ok {
			parsingPkgs.PushBack(fsn.Pkg.Path)
			parsedPkgs[fsn.Pkg] = true
		}
	}

	structs[typ] = &Struct{
		Name:              typ,
		FieldMap:          parseNamedFields(pAlias, p, st),
		embeddeds:         embedded,
		embeddedsResolved: len(embedded) == 0,
	}
}

func (s *Struct) resolveImports() {
	pkgs := map[Pkg]bool{}
	for _, f := range s.FieldMap {
		if f.Typ.Pkg.Path == "" || f.Typ.Pkg == s.Name.Pkg {
			// skip primitive types and current package
			continue
		}

		pkgs[f.Typ.Pkg] = true
	}

	// same pkg name but different path
	names := map[string][]string{}
	for p := range pkgs {
		names[p.Name] = append(names[p.Name], p.Path)
	}

	s.Imports = make(map[string]string)
	for n, ps := range names {
		s.Imports[ps[0]] = n
		for i := 1; i < len(ps); i++ {
			s.Imports[ps[i]] = fmt.Sprintf("%s%d", n, i)
		}
	}
}

func (s *Struct) resolveFields() {
	for _, f := range s.FieldMap {
		if f.Typ.Pkg.Path == "" || f.Typ.Pkg == s.Name.Pkg {
			f.Typ.Pkg.Name = ""
		} else {
			f.Typ.Pkg.Name = s.Imports[f.Typ.Pkg.Path]
		}
		s.FieldList = append(s.FieldList, f)
	}
	for _, f := range s.FieldList {
		s.FieldMap[f.Name] = f
	}

	sort.Slice(s.FieldList, func(i, j int) bool {
		return s.FieldList[i].Name < s.FieldList[j].Name
	})
}
