package tsq

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// ensurePostgresIndex delegates index creation to the PostgreSQL-specific DDL path.
func ensurePostgresIndex(db *Engine, table string, unique bool, idx string, fields []string) error {
	return createPostgresIndex(db, table, unique, idx, fields)
}

func inspectPostgresIndexDefinition(db *Engine, idx string) (IndexDefinition, bool, error) {
	type row struct {
		Table   string         `db:"table_name"`
		Unique  bool           `db:"is_unique"`
		Columns sql.NullString `db:"columns_csv"`
	}

	var existing row

	err := db.QueryRowContext(context.Background(), `
		SELECT
			t.relname AS table_name,
			i.indisunique AS is_unique,
			STRING_AGG(a.attname, ',' ORDER BY ord.ord) AS columns_csv
		FROM pg_class idx
		JOIN pg_namespace ns ON ns.oid = idx.relnamespace
		JOIN pg_index i ON i.indexrelid = idx.oid
		JOIN pg_class t ON t.oid = i.indrelid
		JOIN UNNEST(i.indkey) WITH ORDINALITY AS ord(attnum, ord) ON TRUE
		JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ord.attnum
		WHERE ns.nspname = current_schema()
			AND idx.relname = $1
		GROUP BY t.relname, i.indisunique`,
		idx,
	).Scan(&existing.Table, &existing.Unique, &existing.Columns)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return IndexDefinition{}, false, nil
		}

		return IndexDefinition{}, false, err
	}

	return IndexDefinition{
		Table:  existing.Table,
		Unique: existing.Unique,
		Fields: parseColumnsCSV(existing.Columns.String),
	}, true, nil
}

// createPostgresIndex issues the PostgreSQL DDL needed to create an index and emits schema events.
func createPostgresIndex(db *Engine, table string, unique bool, idx string, fields []string) error {
	quotedFields, err := quoteDialectIdentifiers(db.Dialect, fields)
	if err != nil {
		return err
	}

	quotedTable, err := quoteDialectIdentifier(db.Dialect, table)
	if err != nil {
		return err
	}

	quotedIndex, err := quoteDialectIdentifier(db.Dialect, idx)
	if err != nil {
		return err
	}

	uniqueClause := ""
	if unique {
		uniqueClause = "UNIQUE "
	}

	query := fmt.Sprintf(
		"CREATE %sINDEX %s ON %s(%s)",
		uniqueClause, quotedIndex, quotedTable, strings.Join(quotedFields, ", "),
	)

	_, err = db.ExecContext(context.Background(), query)
	if err := finishCreateIndex(db, table, unique, idx, fields, err); err != nil {
		return err
	}

	return db.emitSchemaEvent(SchemaEvent{
		Kind:  SchemaEventCreateIndex,
		Table: table,
		Name:  idx,
		SQL:   query,
	})
}
