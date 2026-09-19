package tsq

import (
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync"
	"weak"
)

// partialRows remembers the rows a query read with only some of their table's
// columns, keyed by a weak pointer so a row that is dropped is forgotten with it.
// Update and Upsert write every column of a row, and on such a row that would
// overwrite the unread columns with zero values; they refuse it instead, and say
// which columns to name.
var partialRows sync.Map // weak.Pointer[R] -> []string

// partialColumns returns the columns a query reads when it scans a strict subset
// of one table's columns into that table's own row type, and nil otherwise:
// projections into result types, CTEs and full-width selects are not partial.
func partialColumns[O any](selects []BoundColumn[O]) []string {
	var (
		def   *tableDef
		names = make([]string, 0, len(selects))
	)

	for _, col := range selects {
		core := col.core()
		if core == nil || !core.plain || isNilValue(core.table) {
			return nil
		}

		d := core.table.definition()
		switch {
		case d == nil:
			return nil
		case def == nil:
			def = d
		case def != d:
			return nil
		}

		if !slices.Contains(names, core.name) {
			names = append(names, core.name)
		}
	}

	if def == nil || len(names) >= len(def.columns) {
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
		"%s %s: the row was read with only %s, so writing every column would overwrite the others with zero values; name the columns to write, as in Update(ctx, db, row, cols...)",
		op, table, strings.Join(cols.([]string), ", "))
}
