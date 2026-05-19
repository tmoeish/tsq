package tsq

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// SQLiteDialect is the SQLite dialect implementation.
type SQLiteDialect struct{}

// Name returns DialectSQLite.
func (d SQLiteDialect) Name() DialectName {
	return DialectSQLite
}

// QuoteField quotes an identifier for SQLite.
func (d SQLiteDialect) QuoteField(f string) string {
	return `"` + f + `"`
}

// BindVar returns SQLite's placeholder at position i.
func (d SQLiteDialect) BindVar(i int) string {
	return "?"
}

// CreateTableSuffix returns the SQLite CREATE TABLE suffix.
func (d SQLiteDialect) CreateTableSuffix() string {
	return ";"
}

// CreateIndexSuffix returns the SQLite CREATE INDEX suffix.
func (d SQLiteDialect) CreateIndexSuffix() string {
	return ""
}

// DropIndexSuffix returns the SQLite DROP INDEX suffix.
func (d SQLiteDialect) DropIndexSuffix() string {
	return ""
}

// TruncateClause returns the SQLite TRUNCATE clause.
func (d SQLiteDialect) TruncateClause() string {
	return "DELETE FROM"
}

// AutoIncrementClause returns the SQLite AUTOINCREMENT clause.
func (d SQLiteDialect) AutoIncrementClause() string {
	return "AUTOINCREMENT"
}

// AutoIncrementBindValue returns the SQLite AUTOINCREMENT bind value.
func (d SQLiteDialect) AutoIncrementBindValue() string {
	return "NULL"
}

// LastInsertIdReturningSuffix returns SQLite's empty returning suffix.
func (d SQLiteDialect) LastInsertIdReturningSuffix(table, col string) string {
	return ""
}

// AllTablesQuery returns the SQLite query used to list all tables.
func (d SQLiteDialect) AllTablesQuery() string {
	return "SELECT name FROM sqlite_master WHERE type='table'"
}

// CreateTableIfNotExistsSuffix returns SQLite's IF NOT EXISTS fragment.
func (d SQLiteDialect) CreateTableIfNotExistsSuffix() string {
	return "IF NOT EXISTS"
}

// HasConstraintsQuery returns the SQLite query used to inspect column constraints.
func (d SQLiteDialect) HasConstraintsQuery(table, column string) string {
	return ""
}

// ValidateIdentifier applies SQLite's identifier validation rules.
func (d SQLiteDialect) ValidateIdentifier(identifier string) error {
	return validateDialectIdentifier(identifier, d.Name(), 0)
}

// SupportsCapability reports whether SQLite supports capability.
func (d SQLiteDialect) SupportsCapability(capability DialectCapability) bool {
	switch canonicalCapabilityName(string(capability)) {
	case DialectCapabilityCTE, DialectCapabilityExcept, DialectCapabilityIntersect:
		return true
	case DialectCapabilityFullOuterJoin,
		DialectCapabilitySelectForUpdate,
		DialectCapabilitySelectForShare,
		DialectCapabilitySelectForNoWait,
		DialectCapabilitySelectForSkipLocked:
		return false
	default:
		return false
	}
}

// BatchInsertStartID derives the first id assigned by a SQLite batch insert when possible.
func (d SQLiteDialect) BatchInsertStartID(lastID, rowsAffected int64) (int64, bool) {
	if rowsAffected <= 0 {
		return 0, false
	}

	return lastID - rowsAffected + 1, true
}

// EnsureIndex creates or updates an index definition for SQLite.
func (d SQLiteDialect) EnsureIndex(ctx context.Context, db SQLExecutor, table string, unique bool, idx string, fields []string) (string, error) {
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
		// Check if it's already created and matches
		definition, found, inspectErr := d.InspectIndexDefinition(ctx, db, table, idx)
		if inspectErr == nil && found && validateIndexDefinition(table, unique, idx, fields, definition) == nil {
			return "", nil
		}

		return "", err
	}

	return query, nil
}

// InspectIndexDefinition reads back an existing SQLite index definition.
func (d SQLiteDialect) InspectIndexDefinition(ctx context.Context, db SQLExecutor, table, idx string) (IndexDefinition, bool, error) {
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

			break // we found it, can stop iterating
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

func (d SQLiteDialect) inspectSQLiteIndexColumns(ctx context.Context, db SQLExecutor, idx string) ([]string, error) {
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

// DDLColumnType renders a SQLite column type for desc.
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

// DDLAutoIncrementPrimaryKey renders a SQLite auto-increment primary key column.
func (d SQLiteDialect) DDLAutoIncrementPrimaryKey(quotedColumn string, desc DDLColumnType) (string, error) {
	if desc.Kind != DDLColumnKindInt {
		return "", errors.New("auto-increment primary key requires an integer field")
	}

	return quotedColumn + " INTEGER PRIMARY KEY " + d.AutoIncrementClause(), nil
}

// DDLCreateIndex renders a SQLite CREATE INDEX statement.
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

// DDLDropIndex renders a SQLite DROP INDEX statement.
func (d SQLiteDialect) DDLDropIndex(table, idx string) string {
	return fmt.Sprintf("DROP INDEX %s;", d.QuoteField(idx))
}

// DDLAlterColumnMode reports that SQLite applies column changes by table rebuild.
func (d SQLiteDialect) DDLAlterColumnMode() DDLAlterColumnMode {
	return DDLAlterColumnRebuild
}

// DDLAlterColumnStatements returns SQLite's direct alter-column statements, if any.
func (d SQLiteDialect) DDLAlterColumnStatements(table string, before, after DDLColumnSpec) []string {
	return nil
}
