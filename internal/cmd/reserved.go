package cmd

import (
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/tmoeish/tsq/v5"
	"github.com/tmoeish/tsq/v5/internal/genmodel"
)

// genericTableMethods are the generic methods of tsq.TableOf. reflect does not
// list generic methods, so they are named here; TestReservedTableNamesCoverTableOf
// checks the list against the source.
var genericTableMethods = []string{"FetchBy", "FindBy", "GetBy"}

// reservedTableFields returns the names a column field of a generated table struct
// cannot take: the embedded table type (TableOf, or SoftDeleteTableOf for a table
// with deleted_at), every method it promotes, and the methods the table template
// adds. A field with one of these names would hide the method, or fail to compile
// beside it.
func reservedTableFields(data *genmodel.StructInfo) map[string]string {
	reserved := map[string]string{
		"TableOf": "the embedded *tsq.TableOf",
		"As":      "the generated As method",
	}

	table, label := reflect.TypeFor[*tsq.TableOf[struct{}, int]](), "tsq.TableOf"

	if data.DeletedAtField != "" {
		reserved["SoftDeleteTableOf"] = "the embedded *tsq.SoftDeleteTableOf"
		reserved["WithDeleted"] = "the generated WithDeleted method"
		table, label = reflect.TypeFor[*tsq.SoftDeleteTableOf[struct{}, int]](), "tsq.SoftDeleteTableOf"
	}

	for method := range table.Methods() {
		reserved[method.Name] = "the " + label + " method " + method.Name
	}

	for _, name := range genericTableMethods {
		reserved[name] = "the tsq.TableOf method " + name
	}

	for _, ux := range data.Uniques {
		name := joinAnd(ux.Fields)
		reserved["GetBy"+name] = "the generated GetBy" + name + " method"
		reserved["FetchBy"+name] = "the generated FetchBy" + name + " method"
		reserved["FindBy"+name] = "the generated FindBy" + name + " method"
	}

	for _, ft := range data.FullTexts {
		name := "FullText" + joinAnd(ft.Fields)
		reserved[name] = "the generated " + name + " method"
	}

	// The row type gets methods too, and a field of the same name beside them
	// does not compile.
	for _, name := range rowMethods(data) {
		reserved[name] = "the generated row method " + name
	}

	return reserved
}

// rowMethods are the methods table.go.tmpl declares on the row type itself.
func rowMethods(data *genmodel.StructInfo) []string {
	methods := []string{"Insert", "Update", "HardDelete"}
	if data.DeletedAtField != "" {
		methods = append(methods, "Delete", "Restore", "IsDeleted")
	}

	return methods
}

// validateFieldNames refuses a field whose generated column would collide with a
// method of the generated table or result struct.
// validateGeneratedMethodNames refuses two indexes that would generate one method.
// The lookups of a unique index are named after its fields joined by And, so
// //tsq:unique A,B beside //tsq:unique AAndB both gave GetByAAndB, declared twice
// in code that then did not compile.
func validateGeneratedMethodNames(data *genmodel.StructInfo) error {
	if data.IsResult {
		return nil
	}

	owner := map[string]string{}

	claim := func(method, directive string) error {
		if first, taken := owner[method]; taken {
			return fmt.Errorf("%s: %s and %s both generate the method %s; rename one of the Go fields (the db tag keeps the column name)",
				data.TypeInfo.TypeName, first, directive, method)
		}

		owner[method] = directive

		return nil
	}

	for _, ux := range data.Uniques {
		if err := claim("GetBy"+joinAnd(ux.Fields), "//tsq:unique "+strings.Join(ux.Fields, ",")); err != nil {
			return err
		}
	}

	for _, ft := range data.FullTexts {
		if err := claim("FullText"+joinAnd(ft.Fields), "//tsq:fulltext "+strings.Join(ft.Fields, ",")); err != nil {
			return err
		}
	}

	return nil
}

func validateFieldNames(data *genmodel.StructInfo) error {
	reserved := map[string]string{"Columns": "the generated Columns method"}
	if !data.IsResult {
		reserved = reservedTableFields(data)
	}

	names := make([]string, 0, len(data.Fields))
	for _, f := range data.Fields {
		names = append(names, f.Name)
	}

	slices.Sort(names)

	for _, name := range names {
		if what, ok := reserved[name]; ok {
			return fmt.Errorf("%s.%s: the generated column field %s would collide with %s; rename the Go field (the db tag keeps the column name)",
				data.TypeInfo.TypeName, name, name, what)
		}
	}

	return nil
}
