package tsq

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// ensureMySQLIndex delegates index creation to the MySQL-specific DDL path.
func ensureMySQLIndex(db *Engine, table string, unique bool, idx string, fields []string) error {
	return createMySQLIndex(db, table, unique, idx, fields)
}

func inspectMySQLIndexDefinition(
	engine *Engine,
	table string,
	idx string,
) (IndexDefinition, bool, error) {
	type row struct {
		Table   string         `db:"table_name"`
		Unique  int            `db:"is_unique"`
		Columns sql.NullString `db:"columns_csv"`
	}

	var existing row

	err := engine.QueryRowContext(context.Background(), `
		SELECT
			table_name,
			CASE WHEN MIN(non_unique) = 0 THEN 1 ELSE 0 END AS is_unique,
			GROUP_CONCAT(column_name ORDER BY seq_in_index SEPARATOR ',') AS columns_csv
		FROM INFORMATION_SCHEMA.STATISTICS
		WHERE
			table_schema = DATABASE()
			AND table_name = ?
			AND index_name = ?
		GROUP BY table_name`,
		table, idx,
	).Scan(&existing.Table, &existing.Unique, &existing.Columns)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return IndexDefinition{}, false, nil
		}

		return IndexDefinition{}, false, err
	}

	return IndexDefinition{
		Table:  existing.Table,
		Unique: existing.Unique == 1,
		Fields: parseColumnsCSV(existing.Columns.String),
	}, true, nil
}

// createMySQLIndex issues the MySQL DDL needed to create an index and emits schema events.
func createMySQLIndex(engine *Engine, table string, unique bool, idx string, fields []string) error {
	quotedFields, err := quoteDialectIdentifiers(engine.Dialect, fields)
	if err != nil {
		return err
	}

	quotedTable, err := quoteDialectIdentifier(engine.Dialect, table)
	if err != nil {
		return err
	}

	quotedIndex, err := quoteDialectIdentifier(engine.Dialect, idx)
	if err != nil {
		return err
	}

	uniqueClause := ""
	if unique {
		uniqueClause = "UNIQUE "
	}

	query := fmt.Sprintf(
		"ALTER TABLE %s ADD %sINDEX %s(%s)",
		quotedTable, uniqueClause, quotedIndex, strings.Join(quotedFields, ", "),
	)

	_, err = engine.ExecContext(context.Background(), query)
	if err := finishCreateIndex(engine, table, unique, idx, fields, err); err != nil {
		return err
	}

	return engine.emitSchemaEvent(SchemaEvent{
		Kind:  SchemaEventCreateIndex,
		Table: table,
		Name:  idx,
		SQL:   query,
	})
}
