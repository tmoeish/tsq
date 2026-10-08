package tsq

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
)

func validateIndexIdentifiers(table, idx string, fields []string) error {
	if err := validateBuiltInIdentifier(table); err != nil {
		return fmt.Errorf("%s: %w", "invalid table name", err)
	}

	if err := validateBuiltInIdentifier(idx); err != nil {
		return fmt.Errorf("%s: %w", "invalid index name", err)
	}

	if len(fields) == 0 {
		return errors.New("index fields cannot be empty")
	}

	for _, field := range fields {
		if err := validateBuiltInIdentifier(field); err != nil {
			return fmt.Errorf("invalid index field %s: %w", field, err)
		}
	}

	return nil
}

// DuplicateRowsError reports a unique index a policy was about to create over
// rows that share its values: nothing was changed, since the index would have
// been refused after the table's columns were already altered, and a policy that
// changes a table is not a transaction on every engine.
type DuplicateRowsError struct {
	Table string
	Index string
	// Columns are the index's columns the table has; the others are new and
	// would hold one value in every row.
	Columns []string
	// Values is one set of values the rows share, in the order of Columns.
	Values []any
	// Rows is how many rows share them.
	Rows int64
}

func (e *DuplicateRowsError) Error() string {
	if len(e.Columns) == 0 {
		return fmt.Sprintf("unique index %s on table %s cannot be created: its columns are new and would hold one value in every one of the %d rows; nothing was changed. Fill them in a migration first, or leave the index out",
			e.Index, e.Table, e.Rows)
	}

	return fmt.Sprintf("unique index %s on table %s cannot be created: %d rows share %v = %v; nothing was changed. Remove the duplicates, or leave the index out",
		e.Index, e.Table, e.Rows, e.Columns, e.Values)
}

// refuseUniqueIndexesOverDuplicates runs before any DDL of a policy that creates
// indexes: a unique index that is missing is tried as a query first. Created
// after the table's columns were changed, it failed on the rows that share its
// values and left the table altered without it, which the next start could only
// repeat. A column the index needs and the table does not have yet is filled by
// the table policy with one value for every row (the zero value, or the
// default), so it tells no two rows apart: the rows are compared on the columns
// that are there, and where none is, any two rows conflict. A new column that
// can hold NULL and has no default is NULL in every row, and NULL never
// conflicts, so such an index is left to the engine.
func (r *Runtime) refuseUniqueIndexesOverDuplicates(ctx context.Context) error {
	for _, table := range r.tables {
		columns, found, err := r.dialect.InspectColumns(ctx, r.schemaDB(), table.name)
		if err != nil || !found {
			continue // a table that is not there has no rows to share values
		}

		present := make(map[string]bool, len(columns))
		for _, column := range columns {
			present[strings.ToLower(column.Name)] = true
		}

		declared := make(map[string]tsqdialect.ColumnSpec, len(table.Columns))
		for _, column := range table.Columns {
			declared[strings.ToLower(column.Name)] = column
		}

		indexes, err := r.dialect.ListIndexes(ctx, r.schemaDB(), table.name)
		if err != nil {
			continue
		}

		existing := make(map[string]bool, len(indexes))
		for _, idx := range indexes {
			existing[idx.Name] = true
		}

		for _, idx := range table.Indexes {
			if !idx.Unique || idx.FullText || existing[idx.Name] || len(idx.Columns) == 0 {
				continue
			}

			if err := validateIndexIdentifiers(table.name, idx.Name, idx.Columns); err != nil {
				return err
			}

			var there []string

			nullFilled := false

			for _, column := range idx.Columns {
				if present[strings.ToLower(column)] {
					there = append(there, column)
					continue
				}

				spec, known := declared[strings.ToLower(column)]
				if !known || (spec.Type.Nullable && spec.Default == "") || spec.Fill == tsqdialect.FillGenerated {
					nullFilled = true
				}
			}

			if nullFilled {
				continue
			}

			if err := r.duplicatesOf(ctx, table.name, idx, there); err != nil {
				return err
			}
		}
	}

	return nil
}

// duplicatesOf is the DuplicateRowsError for idx over table, or nil where no
// rows share the index's values on the columns in there, the ones the table has.
// NULL never conflicts in a unique index on any of the engines, so rows holding
// one are left out.
func (r *Runtime) duplicatesOf(ctx context.Context, table string, idx IndexSpec, there []string) error {
	quotedTable := r.dialect.QuoteIdent(table)

	if len(there) == 0 {
		var rows int64
		if err := r.schemaDB().QueryRowContext(ctx, "SELECT COUNT(*) FROM "+quotedTable).Scan(&rows); err != nil {
			return fmt.Errorf("count the rows of %s for unique index %s: %w", table, idx.Name, err)
		}

		if rows < 2 {
			return nil
		}

		return &DuplicateRowsError{Table: table, Index: idx.Name, Columns: slices.Clone(idx.Columns), Rows: rows, Values: []any{}}
	}

	quoted := make([]string, 0, len(there))
	notNull := make([]string, 0, len(there))

	for _, column := range there {
		q := r.dialect.QuoteIdent(column)
		quoted = append(quoted, q)
		notNull = append(notNull, q+" IS NOT NULL")
	}

	list := strings.Join(quoted, ", ")
	query := fmt.Sprintf("SELECT %s, COUNT(*) FROM %s WHERE %s GROUP BY %s HAVING COUNT(*) > 1 LIMIT 1",
		list, quotedTable, strings.Join(notNull, " AND "), list)

	values := make([]any, len(there))
	dest := make([]any, 0, len(there)+1)

	for i := range values {
		dest = append(dest, &values[i])
	}

	var rows int64

	dest = append(dest, &rows)

	err := r.schemaDB().QueryRowContext(ctx, query).Scan(dest...)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}

	if err != nil {
		return fmt.Errorf("look for rows that share the values of unique index %s on %s: %w", idx.Name, table, err)
	}

	for i, v := range values {
		if b, ok := v.([]byte); ok {
			values[i] = string(b)
		}
	}

	return &DuplicateRowsError{Table: table, Index: idx.Name, Columns: slices.Clone(there), Values: values, Rows: rows}
}
