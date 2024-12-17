package parser

import (
	"fmt"
	"go/ast"
	"reflect"
	"strings"

	"github.com/sirupsen/logrus"
)

// Field pointer *[] not supported
type Field struct {
	Name   string
	Arr    bool
	Ptr    bool
	Typ    PkgTyp
	Column string
}

func (f Field) String() string {
	sb := new(strings.Builder)
	sb.WriteString(f.Name)
	sb.WriteString(" ")
	if f.Arr {
		sb.WriteString("[]")
	}
	if f.Ptr {
		sb.WriteString("*")
	}
	sb.WriteString(f.Typ.String())
	return sb.String()
}

type PkgTyp struct {
	Pkg  Pkg
	Name string
}

func (t PkgTyp) String() string {
	if t.Pkg.Path == "" {
		return t.Name
	}
	return t.Pkg.Path + "." + t.Name
}

func parseNamedFields(
	pAlias map[string]Pkg,
	p Pkg,
	st *ast.StructType,
) map[string]Field {
	fields := make(map[string]Field)

	for _, af := range st.Fields.List {
		// skip unexported fields
		if len(af.Names) == 0 {
			continue
		}
		// skip fields without db tag
		if af.Tag == nil {
			continue
		}
		s := af.Tag.Value
		if !strings.Contains(s, "db:") {
			continue
		}
		tags := reflect.StructTag(strings.Trim(s, "`"))
		if tags.Get("db") == "-" {
			continue
		}
		name := af.Names[0].Name
		// skip unexported fields
		if !ast.IsExported(name) {
			continue
		}

		column := strings.Split(tags.Get("db"), ",")[0]
		ptr, arr, pPath, tName := parseFieldType(af.Type)
		var tPkg Pkg
		if pPath == "" {
			primitiveTypes := map[string]struct{}{
				"bool":       {},
				"string":     {},
				"int":        {},
				"int8":       {},
				"int16":      {},
				"int32":      {},
				"int64":      {},
				"uint":       {},
				"uint8":      {},
				"uint16":     {},
				"uint32":     {},
				"uint64":     {},
				"uintptr":    {},
				"byte":       {},
				"rune":       {},
				"float32":    {},
				"float64":    {},
				"complex64":  {},
				"complex128": {},
			}
			if _, ok := primitiveTypes[tName]; !ok {
				// if the type is not a primitive type,
				// it must be a type under the same package
				tPkg = p
			}
		} else {
			tPkg = pAlias[pPath]
		}

		if _, ok := fields[name]; ok {
			logrus.Fatalf("duplicated field: %s", name)
		}

		f := Field{
			Name: name,
			Ptr:  ptr,
			Arr:  arr,
			Typ: PkgTyp{
				Pkg:  tPkg,
				Name: tName,
			},
			Column: column,
		}
		fields[name] = f
		logrus.Debugf("field: %s", f)
	}

	return fields
}

// parse embedded fields
func parseEmbeddedFields(
	pAlias map[string]Pkg,
	p Pkg,
	st *ast.StructType,
) map[PkgTyp]bool {
	typs := make(map[PkgTyp]bool)

	for _, f := range st.Fields.List {
		if len(f.Names) != 0 {
			continue
		}

		var t PkgTyp
		_, _, pPath, name := parseFieldType(f.Type)
		if pPath == "" {
			t = PkgTyp{
				Pkg:  p,
				Name: name,
			}
		} else {
			t = PkgTyp{
				Pkg:  pAlias[pPath],
				Name: name,
			}
		}

		if _, ok := typs[t]; ok {
			logrus.Fatalf("duplicated embedded type: %s", t)
		}

		typs[t] = true
		logrus.Debugf("embedded: %s", t)
	}

	return typs
}

func parseFieldType(f ast.Expr) (ptr bool, arr bool, pPath string, name string) {
	switch t := f.(type) {
	case *ast.Ident:
		return false, false, "", t.Name
	case *ast.SelectorExpr:
		return false, false, t.X.(*ast.Ident).Name, t.Sel.Name
	case *ast.ArrayType:
		ptr, arr, pPath, name = parseFieldType(t.Elt)
		return ptr, true, pPath, name
	case *ast.StarExpr:
		ptr, arr, pPath, name = parseFieldType(t.X)
		return true, arr, pPath, name
	default:
		panic(fmt.Sprintf("unexpected type %T", t))
	}
}
