package cmd

import (
	"fmt"
	"reflect"
	"slices"

	"github.com/tmoeish/tsq/v5"
	"github.com/tmoeish/tsq/v5/internal/genmodel"
)

// genericTableMethods are the generic methods of tsq.TableOf. reflect does not
// list generic methods, so they are named here; TestReservedTableNamesCoverTableOf
// checks the list against the source.
var genericTableMethods = []string{"FetchBy", "GetBy"}

// reservedTableFields returns the names a column field of a generated table struct
// cannot take: the embedded TableOf, every method it promotes, and the methods the
// table template adds. A field with one of these names would hide the method, or
// fail to compile beside it.
func reservedTableFields(data *genmodel.StructInfo) map[string]string {
	reserved := map[string]string{
		"TableOf":     "the embedded *tsq.TableOf",
		"As":          "the generated As method",
		"WithDeleted": "the generated WithDeleted method",
	}

	table := reflect.TypeFor[*tsq.TableOf[struct{}, int]]()
	for method := range table.Methods() {
		reserved[method.Name] = "the tsq.TableOf method " + method.Name
	}

	for _, name := range genericTableMethods {
		reserved[name] = "the tsq.TableOf method " + name
	}

	for _, ux := range data.Uniques {
		name := joinAnd(ux.Fields)
		reserved["GetBy"+name] = "the generated GetBy" + name + " method"
		reserved["FetchBy"+name] = "the generated FetchBy" + name + " method"
	}

	return reserved
}

// validateFieldNames refuses a field whose generated column would collide with a
// method of the generated table or result struct.
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
