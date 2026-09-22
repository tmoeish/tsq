package sqldialect

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
)

type SQLiteDialect struct{}

func (d SQLiteDialect) Name() Name {
	return SQLite
}

func (d SQLiteDialect) QuoteIdent(f string) string {
	return `"` + f + `"`
}

func (d SQLiteDialect) Placeholder(i int) string {
	return "?"
}

func (d SQLiteDialect) ReturningClause(col string) string {
	return ""
}

func (d SQLiteDialect) ValidateIdentifier(identifier string) error {
	return validateDialectIdentifier(identifier, d.Name(), 0)
}

func (d SQLiteDialect) SupportsCapability(capability Capability) bool {
	return tsqdialect.Supports(d.Name(), capability)
}

func (d SQLiteDialect) BatchInsertStartID(lastID, rowsAffected int64) (int64, bool) {
	if rowsAffected <= 0 {
		return 0, false
	}

	return lastID - rowsAffected + 1, true
}

func (d SQLiteDialect) InspectColumns(ctx context.Context, db Executor, table string) ([]Column, bool, error) {
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

	columns := make([]Column, 0)
	createStmtUpper := strings.ToUpper(createSQL.String)

	for rows.Next() {
		var row pragmaRow
		if err := rows.Scan(&row.CID, &row.Name, &row.Type, &row.NotNull, &row.Default, &row.PrimaryKey); err != nil {
			return nil, false, err
		}

		colType, err := parseSQLiteColumnType(row.Type)
		if err != nil {
			return nil, false, fmt.Errorf("inspect sqlite column %s.%s: %w", table, row.Name, err)
		}

		autoincrement := row.PrimaryKey > 0 &&
			sqliteCreateSQLDeclaresAutoincrement(createStmtUpper, row.Name)

		columns = append(columns, Column{
			Name:          row.Name,
			Type:          withDDLNullable(colType, row.NotNull == 0 && row.PrimaryKey == 0),
			PrimaryKey:    row.PrimaryKey > 0,
			AutoIncrement: autoincrement,
			Default:       normalizeDDLDefault(row.Default),
			NativeType:    strings.TrimSpace(row.Type),
		})
	}

	if err := rows.Err(); err != nil {
		return nil, false, err
	}

	return columns, true, nil
}

func (d SQLiteDialect) ListIndexes(ctx context.Context, db Executor, table string) ([]Index, error) {
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

	indexes := make([]Index, 0)

	for rows.Next() {
		var row sqliteIndexListRow
		if err := rows.Scan(&row.Seq, &row.Name, &row.Unique, &row.Origin, &row.Partial); err != nil {
			return nil, err
		}

		fields, err := d.inspectSQLiteIndexColumns(ctx, db, row.Name)
		if err != nil {
			return nil, err
		}

		indexes = append(indexes, Index{
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

func (d SQLiteDialect) EnsureIndex(ctx context.Context, db Executor, table, idx string, fields []string, unique bool) (string, error) {
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
		definition, found, inspectErr := d.InspectIndex(ctx, db, table, idx)
		if inspectErr == nil && found && validateIndex(table, unique, idx, fields, definition) == nil {
			return "", nil
		}

		return "", err
	}

	return query, nil
}

func (d SQLiteDialect) InspectIndex(ctx context.Context, db Executor, table, idx string) (Index, bool, error) {
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
			return Index{}, false, nil
		}

		return Index{}, false, err
	}

	quotedTable, err := quoteDialectIdentifier(d, master.Table)
	if err != nil {
		return Index{}, false, err
	}

	rows, err := db.QueryContext(ctx, fmt.Sprintf("PRAGMA index_list(%s)", quotedTable))
	if err != nil {
		return Index{}, false, err
	}

	definition := Index{Table: master.Table}
	found := false

	for rows.Next() {
		var row sqliteIndexListRow
		if err := rows.Scan(&row.Seq, &row.Name, &row.Unique, &row.Origin, &row.Partial); err != nil {
			_ = rows.Close()
			return Index{}, false, err
		}

		if row.Name == idx {
			definition.Unique = row.Unique == 1
			found = true

			break
		}
	}

	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return Index{}, false, err
	}
	_ = rows.Close()

	if !found {
		return Index{}, false, nil
	}

	cols, err := d.inspectSQLiteIndexColumns(ctx, db, idx)
	if err != nil {
		return Index{}, false, err
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

		// An expression index reports its expressions with a NULL name, which
		// is left out, as PostgreSQL's listing does.
		var name sql.NullString
		if err := rows.Scan(&seqno, &cid, &name); err != nil {
			return nil, err
		}

		if name.Valid {
			fields = append(fields, name.String)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return fields, nil
}

// sqliteCreateSQLDeclaresAutoincrement reports whether the CREATE TABLE SQL
// declares column as INTEGER PRIMARY KEY AUTOINCREMENT. The match is performed
// case-insensitively and tolerates `"x"`, `[x]`, backtick, or bare identifier
// quoting, since handwritten DDL rarely matches tsq's own quoting style.
func sqliteCreateSQLDeclaresAutoincrement(createStmtUpper, column string) bool {
	normalized := createStmtUpper
	for _, quote := range []string{`"`, "`", "[", "]"} {
		normalized = strings.ReplaceAll(normalized, quote, "")
	}

	needle := strings.ToUpper(column) + " INTEGER PRIMARY KEY AUTOINCREMENT"

	for offset := 0; ; {
		idx := strings.Index(normalized[offset:], needle)
		if idx < 0 {
			return false
		}

		idx += offset
		if idx == 0 || !isSQLIdentifierChar(normalized[idx-1]) {
			return true
		}

		offset = idx + 1
	}
}

func isSQLIdentifierChar(c byte) bool {
	return c == '_' ||
		(c >= '0' && c <= '9') ||
		(c >= 'A' && c <= 'Z') ||
		(c >= 'a' && c <= 'z')
}

func parseSQLiteColumnType(raw string) (ColumnType, error) {
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

		return ColumnType{Kind: KindString, Size: size}, nil
	case strings.Contains(upper, "BOOLEAN"):
		return ColumnType{Kind: KindBool}, nil
	case strings.Contains(upper, "BLOB"):
		return ColumnType{Kind: KindBytes}, nil
	case strings.Contains(upper, "REAL"), strings.Contains(upper, "FLOA"), strings.Contains(upper, "DOUB"):
		return ColumnType{Kind: KindFloat, Bits: 64}, nil
	case strings.Contains(upper, "TIMESTAMP"), strings.Contains(upper, "DATETIME"), strings.Contains(upper, "DATE"):
		return ColumnType{Kind: KindTime}, nil
	case strings.Contains(upper, "INT"):
		return ColumnType{Kind: KindInt, Bits: 64}, nil
	default:
		return ColumnType{RawType: rawType}, nil
	}
}

func (d SQLiteDialect) ColumnTypeSQL(desc ColumnType) string {
	if desc.RawType != "" {
		return desc.RawType
	}

	switch desc.Kind {
	case KindBool:
		return "BOOLEAN"
	case KindBytes:
		return "BLOB"
	case KindFloat:
		return "REAL"
	case KindInt:
		return "INTEGER"
	case KindString:
		if desc.Size <= 0 {
			return fmt.Sprintf("VARCHAR(%d)", defaultDDLStringSize)
		}

		return fmt.Sprintf("VARCHAR(%d)", desc.Size)
	case KindTime:
		return "TIMESTAMP"
	default:
		return "TEXT"
	}
}

func (d SQLiteDialect) AutoIncrementColumnSQL(quotedColumn string, desc ColumnType) (string, error) {
	if desc.Kind != KindInt {
		return "", errors.New("auto-increment primary key requires an integer field")
	}

	return quotedColumn + " INTEGER PRIMARY KEY AUTOINCREMENT", nil
}

// FullTextIndexSQL is empty: SQLite's full-text search lives in an FTS5 virtual
// table with its own triggers, which TSQ does not manage, so there is no index to
// create and the predicate matches substrings instead.
func (d SQLiteDialect) FullTextIndexSQL(table, idx string, quotedFields []string) string { return "" }

// FullTextVectorSQL is empty: the substring predicate names the columns itself.
func (d SQLiteDialect) FullTextVectorSQL(quotedFields []string) string { return "" }

func (d SQLiteDialect) CreateIndexSQL(table, idx string, fields []string, unique bool) string {
	uniqueClause := ""
	if unique {
		uniqueClause = "UNIQUE "
	}

	return fmt.Sprintf(
		"CREATE %sINDEX %s ON %s(%s)%s",
		uniqueClause,
		d.QuoteIdent(idx),
		d.QuoteIdent(table),
		strings.Join(fields, ", "),
		";",
	)
}

func (d SQLiteDialect) DropIndexSQL(table, idx string) string {
	return fmt.Sprintf("DROP INDEX %s;", d.QuoteIdent(idx))
}

// sqliteCheck finds a CHECK constraint in a CREATE TABLE statement. A match inside
// a string literal is a false positive, which refuses a rebuild that was safe; the
// other way round would drop the constraint.
var sqliteCheck = regexp.MustCompile(`(?i)\bCHECK\s*\(`)

// sqliteMentions reports whether a statement names table as an identifier. Like
// sqliteCheck it errs on the side of refusing.
func sqliteMentions(statement, table string) bool {
	name := regexp.QuoteMeta(table)

	return regexp.MustCompile(`(?i)(^|[^A-Za-z0-9_$])["'\x60\[]?` + name + `["'\x60\]]?($|[^A-Za-z0-9_$])`).MatchString(statement)
}

// InspectRebuild reads what a rebuild of table must keep. The rebuilt table is
// created from the declared columns, so anything its CREATE TABLE carries besides
// them (UNIQUE, CHECK and FOREIGN KEY constraints) blocks the rebuild, and so does a
// view, a trigger or a foreign key elsewhere that refers to the table: renaming
// and dropping it would leave them pointing at nothing. Its own indexes and
// triggers are returned with the statements that created them, so expressions,
// partial WHERE clauses and collations survive.
func (d SQLiteDialect) InspectRebuild(ctx context.Context, db Executor, table string) (Rebuild, error) {
	var rebuild Rebuild

	var definition string
	if err := db.QueryRowContext(ctx, "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ? COLLATE NOCASE", table).Scan(&definition); err != nil {
		return Rebuild{}, fmt.Errorf("read the definition of %s: %w", table, err)
	}

	if sqliteCheck.MatchString(definition) {
		rebuild.Blockers = append(rebuild.Blockers, "a CHECK constraint")
	}

	indexes, err := d.ListIndexes(ctx, db, table)
	if err != nil {
		return Rebuild{}, err
	}

	for _, index := range indexes {
		if index.Constraint {
			rebuild.Blockers = append(rebuild.Blockers, fmt.Sprintf("a UNIQUE constraint on (%s)", strings.Join(index.Fields, ", ")))
		}
	}

	keys, err := db.QueryContext(ctx, `SELECT DISTINCT m.name, f."table" FROM sqlite_master AS m, pragma_foreign_key_list(m.name) AS f WHERE m.type = 'table'`)
	if err != nil {
		return Rebuild{}, err
	}

	defer func() { _ = keys.Close() }()

	for keys.Next() {
		var from, to string
		if err := keys.Scan(&from, &to); err != nil {
			return Rebuild{}, err
		}

		switch {
		case strings.EqualFold(from, table):
			rebuild.Blockers = append(rebuild.Blockers, "its foreign key to "+to)
		case strings.EqualFold(to, table):
			rebuild.Blockers = append(rebuild.Blockers, "the foreign key of "+from+" that references it")
		}
	}

	if err := keys.Err(); err != nil {
		return Rebuild{}, err
	}

	objects, err := db.QueryContext(ctx, `SELECT type, name, tbl_name, sql FROM sqlite_master WHERE type IN ('index', 'trigger', 'view') AND sql IS NOT NULL ORDER BY type, name`)
	if err != nil {
		return Rebuild{}, err
	}

	defer func() { _ = objects.Close() }()

	var ownIndexes []int // positions in rebuild.Objects of the table's indexes

	for objects.Next() {
		var kind, name, of, statement string
		if err := objects.Scan(&kind, &name, &of, &statement); err != nil {
			return Rebuild{}, err
		}

		switch {
		case kind != "view" && strings.EqualFold(of, table):
			if kind == "index" {
				ownIndexes = append(ownIndexes, len(rebuild.Objects))
			}

			rebuild.Objects = append(rebuild.Objects, RebuildObject{Name: name, SQL: statement})
		case kind != "index" && sqliteMentions(statement, table):
			rebuild.Blockers = append(rebuild.Blockers, fmt.Sprintf("the %s %s, which refers to it", kind, name))
		}
	}

	if err := objects.Err(); err != nil {
		return Rebuild{}, err
	}

	for _, i := range ownIndexes {
		if rebuild.Objects[i].Columns, err = d.inspectSQLiteIndexColumns(ctx, db, rebuild.Objects[i].Name); err != nil {
			return Rebuild{}, err
		}
	}

	return rebuild, nil
}

func (d SQLiteDialect) AlterMode() AlterMode {
	return AlterRebuild
}

func (d SQLiteDialect) AlterColumnSQL(table string, before Column, after ColumnSpec) []string {
	return nil
}
