package dialect

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

type SQLiteDialect struct{}

func (d SQLiteDialect) Name() Name {
	return SQLite
}

func (d SQLiteDialect) QuoteField(f string) string {
	return `"` + f + `"`
}

func (d SQLiteDialect) BindVar(i int) string {
	return "?"
}

func (d SQLiteDialect) CreateTableSuffix() string {
	return ";"
}

func (d SQLiteDialect) CreateIndexSuffix() string {
	return ""
}

func (d SQLiteDialect) DropIndexSuffix() string {
	return ""
}

func (d SQLiteDialect) TruncateClause() string {
	return "DELETE FROM"
}

func (d SQLiteDialect) AutoIncrementClause() string {
	return "AUTOINCREMENT"
}

func (d SQLiteDialect) AutoIncrementBindValue() string {
	return "NULL"
}

func (d SQLiteDialect) LastInsertIdReturningSuffix(table, col string) string {
	return ""
}

func (d SQLiteDialect) AllTablesQuery() string {
	return "SELECT name FROM sqlite_master WHERE type='table'"
}

func (d SQLiteDialect) CreateTableIfNotExistsSuffix() string {
	return "IF NOT EXISTS"
}

func (d SQLiteDialect) HasConstraintsQuery(table, column string) string {
	return ""
}

func (d SQLiteDialect) ValidateIdentifier(identifier string) error {
	return validateDialectIdentifier(identifier, d.Name(), 0)
}

func (d SQLiteDialect) SupportsCapability(capability Capability) bool {
	switch canonicalCapabilityName(string(capability)) {
	case CapabilityCTE, CapabilityExcept, CapabilityIntersect:
		return true
	case CapabilityFullOuterJoin,
		CapabilitySelectForUpdate,
		CapabilitySelectForShare,
		CapabilitySelectForNoWait,
		CapabilitySelectForSkipLocked:
		return false
	default:
		return false
	}
}

func (d SQLiteDialect) BatchInsertStartID(lastID, rowsAffected int64) (int64, bool) {
	if rowsAffected <= 0 {
		return 0, false
	}

	return lastID - rowsAffected + 1, true
}

func (d SQLiteDialect) EnsureIndex(ctx context.Context, db Executor, table string, unique bool, idx string, fields []string) (string, error) {
	quotedFields, err := quoteDialectIdentifiers(d, fields)
	if err != nil {
		return "", err
	}

	quotedTable, err := quoteDialectIdentifier(d, table)
	if err != nil {
		return "", err
	}

	quotedIndex, err := quoteDialectIdentifier(d, idx)
	if err != nil {
		return "", err
	}

	uniqueClause := ""
	if unique {
		uniqueClause = "UNIQUE "
	}

	query := fmt.Sprintf(
		"CREATE %sINDEX %s ON %s(%s)",
		uniqueClause, quotedIndex, quotedTable, strings.Join(quotedFields, ", "),
	)

	_, err = db.ExecContext(ctx, query)
	if err != nil {
		definition, found, inspectErr := d.InspectIndexDefinition(ctx, db, table, idx)
		if inspectErr == nil && found && validateIndexDefinition(table, unique, idx, fields, definition) == nil {
			return "", nil
		}

		return "", err
	}

	return query, nil
}

func (d SQLiteDialect) InspectIndexDefinition(ctx context.Context, db Executor, table, idx string) (IndexDefinition, bool, error) {
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
		ctx,
		"SELECT tbl_name FROM sqlite_master WHERE type='index' AND name=?",
		idx,
	).Scan(&master.Table)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return IndexDefinition{}, false, nil
		}

		return IndexDefinition{}, false, err
	}

	quotedTable, err := quoteDialectIdentifier(d, master.Table)
	if err != nil {
		return IndexDefinition{}, false, err
	}

	rows, err := db.QueryContext(ctx, fmt.Sprintf("PRAGMA index_list(%s)", quotedTable))
	if err != nil {
		return IndexDefinition{}, false, err
	}

	definition := IndexDefinition{Table: master.Table}
	found := false

	for rows.Next() {
		var row sqliteIndexListRow
		if err := rows.Scan(&row.Seq, &row.Name, &row.Unique, &row.Origin, &row.Partial); err != nil {
			_ = rows.Close()
			return IndexDefinition{}, false, err
		}

		if row.Name == idx {
			definition.Unique = row.Unique == 1
			found = true

			break
		}
	}

	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return IndexDefinition{}, false, err
	}
	_ = rows.Close()

	if !found {
		return IndexDefinition{}, false, nil
	}

	cols, err := d.inspectSQLiteIndexColumns(ctx, db, idx)
	if err != nil {
		return IndexDefinition{}, false, err
	}
	definition.Fields = cols

	return definition, true, nil
}

func (d SQLiteDialect) inspectSQLiteIndexColumns(ctx context.Context, db Executor, idx string) ([]string, error) {
	quotedIndex, err := quoteDialectIdentifier(d, idx)
	if err != nil {
		return nil, err
	}

	rows, err := db.QueryContext(ctx, fmt.Sprintf("PRAGMA index_info(%s)", quotedIndex))
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = rows.Close()
	}()

	var fields []string

	for rows.Next() {
		var seqno, cid int

		var name string
		if err := rows.Scan(&seqno, &cid, &name); err != nil {
			return nil, err
		}
		fields = append(fields, name)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return fields, nil
}

func (d SQLiteDialect) DDLColumnType(desc DDLColumnType) string {
	switch desc.Kind {
	case DDLColumnKindBool:
		return "BOOLEAN"
	case DDLColumnKindBytes:
		return "BLOB"
	case DDLColumnKindFloat:
		return "REAL"
	case DDLColumnKindInt:
		return "INTEGER"
	case DDLColumnKindString:
		return "TEXT"
	case DDLColumnKindTime:
		return "TIMESTAMP"
	default:
		return "TEXT"
	}
}

func (d SQLiteDialect) DDLAutoIncrementPrimaryKey(quotedColumn string, desc DDLColumnType) (string, error) {
	if desc.Kind != DDLColumnKindInt {
		return "", errors.New("auto-increment primary key requires an integer field")
	}

	return quotedColumn + " INTEGER PRIMARY KEY " + d.AutoIncrementClause(), nil
}

func (d SQLiteDialect) DDLCreateIndex(table, idx string, fields []string, unique bool) string {
	uniqueClause := ""
	if unique {
		uniqueClause = "UNIQUE "
	}

	return fmt.Sprintf(
		"CREATE %sINDEX %s ON %s(%s)%s",
		uniqueClause,
		d.QuoteField(idx),
		d.QuoteField(table),
		strings.Join(fields, ", "),
		d.CreateIndexSuffix(),
	)
}

func (d SQLiteDialect) DDLDropIndex(table, idx string) string {
	return fmt.Sprintf("DROP INDEX %s;", d.QuoteField(idx))
}

func (d SQLiteDialect) DDLAlterColumnMode() DDLAlterColumnMode {
	return DDLAlterColumnRebuild
}

func (d SQLiteDialect) DDLAlterColumnStatements(table string, before, after DDLColumnSpec) []string {
	return nil
}
