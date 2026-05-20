package tsq

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// ensureSQLiteIndex delegates index creation to the SQLite-specific DDL path.
func ensureSQLiteIndex(db *Engine, table string, unique bool, idx string, fields []string) error {
	return createSQLiteIndex(db, table, unique, idx, fields)
}

func inspectSQLiteIndexDefinition(db *Engine, idx string) (IndexDefinition, bool, error) {
	type sqliteMasterRow struct {
		Table string `db:"tbl_name"`
	}

	type sqliteIndexListRow struct {
		Seq     int    `db:"seq"`
		Name    string `db:"name"`
		Unique  int    `db:"unique"`
		Origin  string `db:"origin"`
		Partial int    `db:"partial"`
	}

	var master sqliteMasterRow

	err := db.QueryRowContext(
		context.Background(),
		"SELECT tbl_name FROM sqlite_master WHERE type='index' AND name=?",
		idx,
	).Scan(&master.Table)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return IndexDefinition{}, false, nil
		}

		return IndexDefinition{}, false, err
	}

	quotedTable, err := quoteDialectIdentifier(db.Dialect, master.Table)
	if err != nil {
		return IndexDefinition{}, false, err
	}

	rows, err := db.QueryContext(context.Background(), fmt.Sprintf("PRAGMA index_list(%s)", quotedTable))
	if err != nil {
		return IndexDefinition{}, false, err
	}

	defer func() {
		_ = rows.Close()
	}()

	definition := IndexDefinition{Table: master.Table}

	for rows.Next() {
		var row sqliteIndexListRow
		if err := rows.Scan(&row.Seq, &row.Name, &row.Unique, &row.Origin, &row.Partial); err != nil {
			return IndexDefinition{}, false, err
		}

		if row.Name == idx {
			definition.Unique = row.Unique == 1
		}
	}

	if err := rows.Err(); err != nil {
		return IndexDefinition{}, false, err
	}

	fields, err := inspectSQLiteIndexFields(db, idx)
	if err != nil {
		return IndexDefinition{}, false, err
	}

	definition.Fields = fields

	return definition, true, nil
}

func inspectSQLiteIndexFields(db *Engine, idx string) ([]string, error) {
	type sqliteIndexInfoRow struct {
		SeqNo int    `db:"seqno"`
		CID   int    `db:"cid"`
		Name  string `db:"name"`
	}

	quotedIndex, err := quoteDialectIdentifier(db.Dialect, idx)
	if err != nil {
		return nil, err
	}

	rows, err := db.QueryContext(context.Background(), fmt.Sprintf("PRAGMA index_info(%s)", quotedIndex))
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = rows.Close()
	}()

	fields := make([]string, 0)

	for rows.Next() {
		var row sqliteIndexInfoRow
		if err := rows.Scan(&row.SeqNo, &row.CID, &row.Name); err != nil {
			return nil, err
		}
		fields = append(fields, row.Name)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return fields, nil
}

// createSQLiteIndex issues the SQLite DDL needed to create an index and emits schema events.
func createSQLiteIndex(db *Engine, table string, unique bool, idx string, fields []string) error {
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
