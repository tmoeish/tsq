package dialect

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
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
	return ";"
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

func (d SQLiteDialect) ListTables(ctx context.Context, db Executor) ([]string, error) {
	rows, err := db.QueryContext(ctx, "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = rows.Close()
	}()

	var tables []string

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		tables = append(tables, name)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return tables, nil
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

func (d SQLiteDialect) InspectTableColumns(ctx context.Context, db Executor, table string) ([]DDLColumnSpec, bool, error) {
	quotedTable, err := quoteDialectIdentifier(d, table)
	if err != nil {
		return nil, false, err
	}

	var createSQL sql.NullString

	err = db.QueryRowContext(
		ctx,
		"SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
		table,
	).Scan(&createSQL)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, false, nil
		}

		return nil, false, err
	}

	rows, err := db.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%s)", quotedTable))
	if err != nil {
		return nil, false, err
	}

	defer func() {
		_ = rows.Close()
	}()

	type pragmaRow struct {
		CID        int
		Name       string
		Type       string
		NotNull    int
		Default    sql.NullString
		PrimaryKey int
	}

	columns := make([]DDLColumnSpec, 0)
	createStmtUpper := strings.ToUpper(createSQL.String)

	for rows.Next() {
		var row pragmaRow
		if err := rows.Scan(&row.CID, &row.Name, &row.Type, &row.NotNull, &row.Default, &row.PrimaryKey); err != nil {
			return nil, false, err
		}

		colType, err := parseSQLiteDDLColumnType(row.Type)
		if err != nil {
			return nil, false, fmt.Errorf("inspect sqlite column %s.%s: %w", table, row.Name, err)
		}

		quotedColumn := d.QuoteField(row.Name)
		autoincrement := row.PrimaryKey > 0 &&
			strings.Contains(createStmtUpper, quotedColumn+" INTEGER PRIMARY KEY AUTOINCREMENT")

		columns = append(columns, DDLColumnSpec{
			Name:          row.Name,
			Type:          withDDLNullable(colType, row.NotNull == 0 && row.PrimaryKey == 0),
			PrimaryKey:    row.PrimaryKey > 0,
			AutoIncrement: autoincrement,
			Default:       normalizeDDLDefault(row.Default),
		})
	}

	if err := rows.Err(); err != nil {
		return nil, false, err
	}

	return columns, true, nil
}

func (d SQLiteDialect) ListIndexes(ctx context.Context, db Executor, table string) ([]NamedIndexDefinition, error) {
	quotedTable, err := quoteDialectIdentifier(d, table)
	if err != nil {
		return nil, err
	}

	rows, err := db.QueryContext(ctx, fmt.Sprintf("PRAGMA index_list(%s)", quotedTable))
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = rows.Close()
	}()

	type sqliteIndexListRow struct {
		Seq     int
		Name    string
		Unique  int
		Origin  string
		Partial int
	}

	indexes := make([]NamedIndexDefinition, 0)

	for rows.Next() {
		var row sqliteIndexListRow
		if err := rows.Scan(&row.Seq, &row.Name, &row.Unique, &row.Origin, &row.Partial); err != nil {
			return nil, err
		}

		fields, err := d.inspectSQLiteIndexColumns(ctx, db, row.Name)
		if err != nil {
			return nil, err
		}

		indexes = append(indexes, NamedIndexDefinition{
			Name:       row.Name,
			Table:      table,
			Unique:     row.Unique == 1,
			Fields:     fields,
			PrimaryKey: row.Origin == "pk",
			Constraint: row.Origin == "u",
		})
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return indexes, nil
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

func parseSQLiteDDLColumnType(raw string) (DDLColumnType, error) {
	rawType := strings.TrimSpace(raw)
	upper := strings.ToUpper(rawType)

	switch {
	case upper == "", strings.Contains(upper, "VARCHAR"), strings.Contains(upper, "TEXT"), strings.Contains(upper, "CLOB"):
		size := 0

		if strings.HasPrefix(upper, "VARCHAR(") && strings.HasSuffix(upper, ")") {
			value := strings.TrimSuffix(strings.TrimPrefix(upper, "VARCHAR("), ")")
			if n, err := strconv.Atoi(value); err == nil && n > 0 {
				size = n
			}
		}

		return DDLColumnType{Kind: DDLColumnKindString, Size: size}, nil
	case strings.Contains(upper, "BOOLEAN"):
		return DDLColumnType{Kind: DDLColumnKindBool}, nil
	case strings.Contains(upper, "BLOB"):
		return DDLColumnType{Kind: DDLColumnKindBytes}, nil
	case strings.Contains(upper, "REAL"), strings.Contains(upper, "FLOA"), strings.Contains(upper, "DOUB"):
		return DDLColumnType{Kind: DDLColumnKindFloat, Bits: 64}, nil
	case strings.Contains(upper, "TIMESTAMP"), strings.Contains(upper, "DATETIME"), strings.Contains(upper, "DATE"):
		return DDLColumnType{Kind: DDLColumnKindTime}, nil
	case strings.Contains(upper, "INT"):
		return DDLColumnType{Kind: DDLColumnKindInt, Bits: 64}, nil
	default:
		return DDLColumnType{RawType: rawType}, nil
	}
}

func (d SQLiteDialect) DDLColumnType(desc DDLColumnType) string {
	if desc.RawType != "" {
		return desc.RawType
	}

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
		if desc.Size <= 0 {
			return fmt.Sprintf("VARCHAR(%d)", defaultDDLStringSize)
		}

		return fmt.Sprintf("VARCHAR(%d)", desc.Size)
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
