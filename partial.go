package tsq

import (
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"weak"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
)

// partialRows remembers the rows a query read with only some of their table's
// columns, keyed by a weak pointer so a row that is dropped is forgotten with it.
// Update and Upsert write every column of a row, and on such a row that would
// overwrite the unread columns with zero values; they refuse it instead, and say
// which columns to name.
var partialRows sync.Map // weak.Pointer[R] -> []string

// rowTables maps a table's row type to its definition, for partialColumns.
var rowTables sync.Map // reflect.Type -> *tableDef

// partialColumns returns the columns a query reads when it scans into a table's
// own row type without filling every column TSQ writes back, and nil otherwise.
// What is compared is the fields the scan fills, not where the values come from:
// a row read through a CTE, or projected into the row type with MapInto, is as
// partial as one read with a narrow Select, and saving it whole zeroed the rest.
// A generated column is never written, so leaving it out loses nothing.
func partialColumns[O any](selects []BoundColumn[O]) []string {
	found, ok := rowTables.Load(reflect.TypeFor[O]())
	if !ok {
		return nil
	}

	def := found.(*tableDef)
	holder := new(O)

	filled := make(map[uintptr]bool, len(selects))
	for _, col := range selects {
		if core := col.core(); core != nil && core.scan != nil {
			filled[reflect.ValueOf(core.scan(holder)).Pointer()] = true
		}
	}

	var names []string

	whole := true

	for _, col := range def.columns {
		if filled[reflect.ValueOf(col.scan(holder)).Pointer()] {
			names = append(names, col.name)
		} else if col.fill != tsqdialect.FillGenerated {
			whole = false
		}
	}

	if whole {
		return nil
	}

	return names
}

// markPartial records that row holds only cols.
func markPartial[R any](row *R, cols []string) {
	key := weak.Make(row)
	partialRows.Store(key, cols)
	runtime.AddCleanup(row, func(k weak.Pointer[R]) { partialRows.Delete(k) }, key)
}

// checkFullRow fails for a row a partial query read, naming what it read.
func checkFullRow[R any](op, table string, row *R) error {
	cols, ok := partialRows.Load(weak.Make(row))
	if !ok {
		return nil
	}

	return fmt.Errorf(
		"%s %s: the row was read with only %s, so writing every column would write zero values over the others; read it whole, or name the columns an Update writes (Update(ctx, db, row, cols...))",
		op, table, strings.Join(cols.([]string), ", "))
}
