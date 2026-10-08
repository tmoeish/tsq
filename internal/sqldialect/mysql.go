package sqldialect

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
)

const (
	mysqlMaxVarcharChars    = 16383
	mysqlMaxMediumTextChars = 4_194_303
)

type MySQLDialect struct{}

func (d MySQLDialect) Name() Name {
	return MySQL
}

func (d MySQLDialect) QuoteIdent(f string) string {
	return "`" + f + "`"
}

func (d MySQLDialect) Placeholder(i int) string {
	return "?"
}

func (d MySQLDialect) ReturningClause(col string) string {
	return ""
}

func (d MySQLDialect) Returning(...string) string {
	return ""
}

func (d MySQLDialect) ValidateIdentifier(identifier string) error {
	return validateDialectIdentifier(identifier, d.Name(), maxIdentifierLengthMySQL)
}

func (d MySQLDialect) SupportsCapability(capability Capability) bool {
	return tsqdialect.Supports(d.Name(), capability)
}

// BatchInsertStartID is the last insert id itself: MySQL reports the first key of
// a multi-row INSERT.
func (d MySQLDialect) BatchInsertStartID(lastID, rowsAffected, step int64) (int64, bool) {
	if rowsAffected <= 0 {
		return 0, false
	}

	return lastID, true
}

// InsertIDStepQuery reads auto_increment_increment, which spaces the keys of one
// INSERT (a multi-primary setup sets it above 1).
func (d MySQLDialect) InsertIDStepQuery() string { return "SELECT @@auto_increment_increment" }

// KeySequenceAdvanceQuery is empty: the counter moves past a key a statement
// writes on its own.
func (d MySQLDialect) KeySequenceAdvanceQuery(string, string) (string, error) { return "", nil }

func (d MySQLDialect) InspectColumns(ctx context.Context, db Executor, table string) ([]Column, bool, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT
			c.column_name,
			c.data_type,
			c.column_type,
			c.is_nullable,
			c.column_default,
			c.column_key,
			c.extra,
			c.character_maximum_length,
			CASE WHEN c.collation_name IS NULL OR c.collation_name = t.table_collation THEN '' ELSE c.collation_name END,
			c.column_comment
		FROM information_schema.columns c
		JOIN information_schema.tables t ON t.table_schema = c.table_schema AND t.table_name = c.table_name
		WHERE c.table_schema = DATABASE() AND c.table_name = ?
		ORDER BY c.ordinal_position`,
		table,
	)
	if err != nil {
		return nil, false, err
	}

	defer func() {
		_ = rows.Close()
	}()

	type row struct {
		Name      string
		Data      string
		Type      string
		Null      string
		Default   sql.NullString
		Key       string
		Extra     string
		Size      sql.NullInt64
		Collation string
		Comment   string
	}

	columns := make([]Column, 0)

	for rows.Next() {
		var item row
		if err := rows.Scan(&item.Name, &item.Data, &item.Type, &item.Null, &item.Default, &item.Key, &item.Extra, &item.Size, &item.Collation, &item.Comment); err != nil {
			return nil, false, err
		}

		desc, err := parseMySQLColumnType(item.Data, item.Type, item.Size)
		if err != nil {
			return nil, false, fmt.Errorf("inspect mysql column %s.%s: %w", table, item.Name, err)
		}

		nullable := strings.EqualFold(item.Null, "YES")
		columns = append(columns, Column{
			Name:          item.Name,
			Type:          withDDLNullable(desc, nullable && item.Key != "PRI"),
			PrimaryKey:    item.Key == "PRI",
			AutoIncrement: strings.Contains(strings.ToLower(item.Extra), "auto_increment"),
			Default:       mysqlDefault(item.Default, item.Extra, item.Data),
			NativeType:    strings.TrimSpace(item.Type),
			Collation:     item.Collation,
			Comment:       item.Comment,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, false, err
	}

	if len(columns) == 0 {
		return nil, false, nil
	}

	return columns, true, nil
}

// mysqlSchemaLock names the lock schema changes are made under. A named lock is
// the server's, not a database's, so the name carries the database (hashed: a
// lock name is at most 64 characters).
const mysqlSchemaLock = "CONCAT('tsq.schema.', MD5(DATABASE()))"

// LockSchema takes a named lock, waiting for it without a limit of its own: ctx
// is what gives up. The session is strict while it holds the lock, whatever mode
// the pool runs in: outside strict mode ALTER TABLE cuts a value that does not fit
// the new type of its column ('abcdefghij' into a VARCHAR(5) is 'abcde', with a
// warning nobody reads), and a change of type is refused or it is a loss of data.
func (d MySQLDialect) LockSchema(ctx context.Context, conn Executor) (func(context.Context) error, bool, error) {
	var got sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK("+mysqlSchemaLock+", -1)").Scan(&got); err != nil {
		return nil, true, err
	}

	if got.Int64 != 1 {
		return nil, true, errors.New("the server did not give the schema lock")
	}

	release := func(ctx context.Context) error {
		_, err := conn.ExecContext(ctx, "DO RELEASE_LOCK("+mysqlSchemaLock+")")

		return err
	}

	mode, err := mysqlSessionMode(ctx, conn)
	if err == nil {
		_, err = conn.ExecContext(ctx, "SET SESSION sql_mode = CONCAT_WS(',', NULLIF(@@SESSION.sql_mode, ''), 'STRICT_ALL_TABLES')")
	}

	if err != nil {
		return nil, true, errors.Join(fmt.Errorf("make the session strict for schema changes: %w", err), release(ctx))
	}

	return func(ctx context.Context) error {
		_, restoreErr := conn.ExecContext(ctx, "SET SESSION sql_mode = "+quoteLiteral(mode))

		return errors.Join(restoreErr, release(ctx))
	}, true, nil
}

// mysqlSessionMode is the sql_mode of the session, as the server lists it.
func mysqlSessionMode(ctx context.Context, conn Executor) (string, error) {
	var mode string

	err := conn.QueryRowContext(ctx, "SELECT @@SESSION.sql_mode").Scan(&mode)

	return mode, err
}

// MySQLStrict reports whether a session of that sql_mode refuses a value that does
// not fit its column, rather than cutting or clamping it.
func MySQLStrict(mode string) bool {
	for name := range strings.SplitSeq(mode, ",") {
		switch strings.ToUpper(strings.TrimSpace(name)) {
		case "STRICT_TRANS_TABLES", "STRICT_ALL_TABLES", "TRADITIONAL":
			return true
		}
	}

	return false
}

// ProbeColumn creates two temporary tables in turn, one holding declared and one
// holding the live column as SHOW CREATE TABLE writes it, and compares what the
// engine reports for each. Both sides go through the same door on purpose: a
// temporary table is described from memory and a kept one from the data
// dictionary, and the two spell a default differently (an expression in one more
// pair of parentheses, a binary literal as text where the dictionary has hex, a
// four-byte character whole where the dictionary has "?"), so a probe compared
// with information_schema matched for types and differed for those defaults on
// every start.
func (d MySQLDialect) ProbeColumn(ctx context.Context, conn Executor, table string, inspected Column, declared ColumnSpec) (Spelling, bool, error) {
	definition, err := ColumnDefinitionSQL(d, probeSpec(declared))
	if err != nil {
		return Spelling{}, true, err
	}

	live, err := d.liveColumnDefinition(ctx, conn, table, inspected.Name)
	if err != nil {
		return Spelling{}, true, err
	}

	wanted, err := d.probeDefinition(ctx, conn, definition)
	if err != nil {
		return Spelling{}, true, err
	}

	held, err := d.probeLive(ctx, conn, live)
	if err != nil {
		return Spelling{}, true, err
	}

	return Spelling{
		Type:    wanted.columnType != "" && strings.EqualFold(wanted.columnType, held.columnType),
		Default: wanted.value == held.value && wanted.generated == held.generated,
	}, true, nil
}

// mysqlProbed is what SHOW COLUMNS reports for the one column of a probe table.
type mysqlProbed struct {
	columnType string
	value      sql.NullString
	generated  bool
}

// liveColumnDefinition is the definition of a column of table as SHOW CREATE TABLE
// writes it: one line, which the engine reads back as the column it describes.
func (d MySQLDialect) liveColumnDefinition(ctx context.Context, conn Executor, table, column string) (string, error) {
	var name, create string
	if err := conn.QueryRowContext(ctx, "SHOW CREATE TABLE "+d.QuoteIdent(table)).Scan(&name, &create); err != nil {
		return "", err
	}

	// Column names match without case. The server quotes one with backquotes, or
	// with double quotes under ANSI_QUOTES, and writes the quote in a name twice.
	quoted := strings.ToLower("`" + strings.ReplaceAll(column, "`", "``") + "` ")
	ansi := strings.ToLower(`"` + strings.ReplaceAll(column, `"`, `""`) + `" `)

	for line := range strings.SplitSeq(create, "\n") {
		line = strings.TrimSuffix(strings.TrimSpace(line), ",")
		if lower := strings.ToLower(line); strings.HasPrefix(lower, quoted) || strings.HasPrefix(lower, ansi) {
			return line, nil
		}
	}

	return "", fmt.Errorf("column %s is not in the definition of table %s", column, table)
}

// probeLive is probeDefinition of a line SHOW CREATE TABLE wrote. The server
// escapes a default there with backslashes whatever the mode ('ab\0\0' for the
// 'ab' of a BINARY(4)), and under NO_BACKSLASH_ESCAPES it would read its own line
// back as another value: the line is read with the mode off.
func (d MySQLDialect) probeLive(ctx context.Context, conn Executor, line string) (mysqlProbed, error) {
	mode, err := mysqlSessionMode(ctx, conn)
	if err != nil {
		return mysqlProbed{}, err
	}

	modes := strings.Split(mode, ",")

	escaping := slices.DeleteFunc(slices.Clone(modes), func(name string) bool { return strings.EqualFold(name, "NO_BACKSLASH_ESCAPES") })
	if len(escaping) == len(modes) {
		return d.probeDefinition(ctx, conn, line)
	}

	if _, err := conn.ExecContext(ctx, "SET SESSION sql_mode = "+quoteLiteral(strings.Join(escaping, ","))); err != nil {
		return mysqlProbed{}, err
	}

	probed, err := d.probeDefinition(ctx, conn, line)

	if _, restoreErr := conn.ExecContext(ctx, "SET SESSION sql_mode = "+quoteLiteral(mode)); err == nil {
		err = restoreErr
	}

	return probed, err
}

// probeDefinition creates a temporary table of the one column definition says and
// reports how the engine describes it. A temporary table is not in
// information_schema, so it is read with SHOW COLUMNS.
func (d MySQLDialect) probeDefinition(ctx context.Context, conn Executor, definition string) (mysqlProbed, error) {
	drop := "DROP TEMPORARY TABLE IF EXISTS " + d.QuoteIdent(probeTable)
	if _, err := conn.ExecContext(ctx, drop); err != nil {
		return mysqlProbed{}, err
	}

	if _, err := conn.ExecContext(ctx, fmt.Sprintf("CREATE TEMPORARY TABLE %s (%s)", d.QuoteIdent(probeTable), definition)); err != nil {
		return mysqlProbed{}, err
	}

	probed, err := d.showProbeColumn(ctx, conn)

	if _, dropErr := conn.ExecContext(ctx, drop); err == nil {
		err = dropErr
	}

	return probed, err
}

func (d MySQLDialect) showProbeColumn(ctx context.Context, conn Executor) (mysqlProbed, error) {
	rows, err := conn.QueryContext(ctx, "SHOW FULL COLUMNS FROM "+d.QuoteIdent(probeTable))
	if err != nil {
		return mysqlProbed{}, err
	}

	defer func() {
		_ = rows.Close()
	}()

	names, err := rows.Columns()
	if err != nil {
		return mysqlProbed{}, err
	}

	// SHOW FULL COLUMNS: Field, Type, Collation, Null, Key, Default, Extra, ...
	values := make([]sql.NullString, len(names))
	dest := make([]any, len(names))

	for i := range values {
		dest[i] = &values[i]
	}

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return mysqlProbed{}, err
		}

		return mysqlProbed{}, errors.New("the probe table has no column")
	}

	if err := rows.Scan(dest...); err != nil {
		return mysqlProbed{}, err
	}

	field := func(name string) sql.NullString {
		for i, column := range names {
			if strings.EqualFold(column, name) {
				return values[i]
			}
		}

		return sql.NullString{}
	}

	return mysqlProbed{
		columnType: strings.TrimSpace(field("Type").String),
		value:      field("Default"),
		generated:  strings.Contains(strings.ToUpper(field("Extra").String), "DEFAULT_GENERATED"),
	}, rows.Err()
}

// mysqlDefault reads a column default back as it was declared. An expression
// default (DEFAULT_GENERATED: every default of a TEXT or BLOB column, which MySQL
// accepts only as an expression) is reported as its stored text, with the string
// literals escaped and prefixed by their character set: DEFAULT ('USD') reads back
// as _utf8mb4\'USD\', which compared as a different default on every boot.
//
// A literal default of a character or time column is reported as its bare value,
// quotes gone: it is quoted again here, or '(none)' reads as an expression in
// parentheses, 'a::b' as a cast and '  x ' as x, each of them a different default
// on every boot too.
func mysqlDefault(value sql.NullString, extra, dataType string) string {
	if !value.Valid {
		return ""
	}

	if !strings.Contains(strings.ToUpper(extra), "DEFAULT_GENERATED") {
		switch strings.ToLower(dataType) {
		case "char", "varchar", "enum", "set":
			return quoteLiteral(value.String)
		case "date", "datetime", "timestamp", "time":
			// Servers before 8.0 do not mark CURRENT_TIMESTAMP as an expression.
			if !IsCurrentTime(value.String) {
				return quoteLiteral(value.String)
			}
		}

		return strings.TrimSpace(value.String)
	}

	def := strings.TrimSpace(value.String)
	if def == "" {
		return def
	}

	def = stripIntroducers(strings.NewReplacer(`\'`, "'", `\\`, `\`).Replace(def))

	for len(def) >= 2 && def[0] == '(' && def[len(def)-1] == ')' {
		def = strings.TrimSpace(def[1 : len(def)-1])
	}

	return def
}

// stripIntroducers removes the character set introducers (_utf8mb4 in
// _utf8mb4'USD') in front of string literals. Only outside a literal: 'en_US' is
// text, and a pattern over the whole default cut it to 'en'.
func stripIntroducers(def string) string {
	var b strings.Builder

	quoted := false

	for i := 0; i < len(def); i++ {
		c := def[i]

		if !quoted && c == '_' && (i == 0 || !isIdentByte(def[i-1])) {
			j := i + 1
			for j < len(def) && isIdentByte(def[j]) {
				j++
			}

			if j > i+1 && j < len(def) && def[j] == '\'' {
				i = j - 1

				continue
			}
		}

		if c == '\'' {
			quoted = !quoted
		}

		b.WriteByte(c)
	}

	return b.String()
}

func isIdentByte(c byte) bool {
	return c == '_' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

func (d MySQLDialect) ListIndexes(ctx context.Context, db Executor, table string) ([]Index, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT
			index_name,
			CASE WHEN MIN(non_unique) = 0 THEN 1 ELSE 0 END AS is_unique,
			GROUP_CONCAT(column_name ORDER BY seq_in_index SEPARATOR ',') AS columns_csv
		FROM information_schema.statistics
		WHERE table_schema = DATABASE() AND table_name = ?
		GROUP BY index_name
		ORDER BY index_name`,
		table,
	)
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = rows.Close()
	}()

	indexes := make([]Index, 0)

	for rows.Next() {
		var name string
		var unique int

		var columns sql.NullString
		if err := rows.Scan(&name, &unique, &columns); err != nil {
			return nil, err
		}

		indexes = append(indexes, Index{
			Name:       name,
			Table:      table,
			Unique:     unique == 1,
			Fields:     parseColumnsCSV(columns.String),
			PrimaryKey: name == "PRIMARY",
		})
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	keys, err := d.foreignKeyColumns(ctx, db, table)
	if err != nil {
		return nil, err
	}

	for i, index := range indexes {
		indexes[i].Constraint = slices.ContainsFunc(keys, func(columns []string) bool {
			return len(columns) <= len(index.Fields) && slices.Equal(index.Fields[:len(columns)], columns)
		})
	}

	return indexes, nil
}

// foreignKeyColumns lists the column lists of table that a foreign key needs an
// index on: its own foreign keys, and the columns other tables' foreign keys
// reference. MySQL refuses to drop an index a foreign key uses (error 1553), which
// is what Index.Constraint means here. It does not flag every unique index:
// information_schema lists each one as a UNIQUE constraint, TSQ's own included.
func (d MySQLDialect) foreignKeyColumns(ctx context.Context, db Executor, table string) ([][]string, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT GROUP_CONCAT(column_name ORDER BY ordinal_position SEPARATOR ',')
		FROM information_schema.key_column_usage
		WHERE table_schema = DATABASE() AND table_name = ? AND referenced_table_name IS NOT NULL
		GROUP BY constraint_name
		UNION ALL
		SELECT GROUP_CONCAT(referenced_column_name ORDER BY position_in_unique_constraint SEPARATOR ',')
		FROM information_schema.key_column_usage
		WHERE referenced_table_schema = DATABASE() AND referenced_table_name = ?
		GROUP BY table_name, constraint_name`,
		table, table,
	)
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = rows.Close()
	}()

	var keys [][]string

	for rows.Next() {
		var columns sql.NullString
		if err := rows.Scan(&columns); err != nil {
			return nil, err
		}

		keys = append(keys, parseColumnsCSV(columns.String))
	}

	return keys, rows.Err()
}

func (d MySQLDialect) EnsureIndex(ctx context.Context, db Executor, table, idx string, fields []string, unique bool) (string, error) {
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
		"ALTER TABLE %s ADD %sINDEX %s(%s)",
		quotedTable, uniqueClause, quotedIndex, strings.Join(quotedFields, ", "),
	)

	_, err = db.ExecContext(ctx, query)
	if err != nil {
		definition, found, inspectErr := d.InspectIndex(ctx, db, table, idx)
		if inspectErr == nil && found && ValidateIndex(table, unique, idx, fields, definition) == nil {
			return "", nil
		}

		return "", err
	}

	return query, nil
}

func (d MySQLDialect) InspectIndex(ctx context.Context, db Executor, table, idx string) (Index, bool, error) {
	type row struct {
		Table   string         `db:"table_name"`
		Unique  int            `db:"is_unique"`
		Columns sql.NullString `db:"columns_csv"`
	}

	var existing row

	err := db.QueryRowContext(ctx, `
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
			return Index{}, false, nil
		}

		return Index{}, false, err
	}

	return Index{
		Table:  existing.Table,
		Unique: existing.Unique == 1,
		Fields: parseColumnsCSV(existing.Columns.String),
	}, true, nil
}

func parseMySQLColumnType(dataType, columnType string, size sql.NullInt64) (ColumnType, error) {
	data := strings.ToLower(strings.TrimSpace(dataType))
	rawColumnType := strings.TrimSpace(columnType)
	colType := strings.ToLower(rawColumnType)
	unsigned := strings.Contains(colType, "unsigned")

	switch data {
	case "bool", "boolean":
		return ColumnType{Kind: KindBool}, nil
	case "tinyint":
		if strings.HasPrefix(colType, "tinyint(1)") {
			return ColumnType{Kind: KindBool}, nil
		}

		return ColumnType{Kind: KindInt, Bits: 8, Unsigned: unsigned}, nil
	case "smallint":
		return ColumnType{Kind: KindInt, Bits: 16, Unsigned: unsigned}, nil
	case "int", "integer":
		return ColumnType{Kind: KindInt, Bits: 32, Unsigned: unsigned}, nil
	case "bigint":
		return ColumnType{Kind: KindInt, Bits: 64, Unsigned: unsigned}, nil
	case "float":
		return ColumnType{Kind: KindFloat, Bits: 32}, nil
	case "double", "double precision":
		return ColumnType{Kind: KindFloat, Bits: 64}, nil
	case "blob":
		return ColumnType{Kind: KindBytes}, nil
	case "mediumblob":
		return ColumnType{Kind: KindBytes, Size: mysqlMaxBlobBytes + 1}, nil
	case "longblob":
		return ColumnType{Kind: KindBytes, Size: mysqlMaxMediumBlobBytes + 1}, nil
	case "tinyblob":
		return ColumnType{RawType: "TINYBLOB"}, nil
	case "varchar", "char":
		result := ColumnType{Kind: KindString}
		if size.Valid && size.Int64 > 0 {
			result.Size = int(size.Int64)
		}

		return result, nil
	case "text", "tinytext":
		// TSQ renders a string as VARCHAR, MEDIUMTEXT or LONGTEXT, never TEXT or
		// TINYTEXT, so these keep their raw type: read as MEDIUMTEXT, a TINYTEXT
		// holding 255 bytes matched a string declared for 100000 and was never
		// altered. A column declared type:TEXT still matches.
		return ColumnType{RawType: strings.ToUpper(data)}, nil
	case "mediumtext":
		return ColumnType{Kind: KindString, Size: mysqlMaxVarcharChars + 1}, nil
	case "longtext":
		return ColumnType{Kind: KindString, Size: mysqlMaxMediumTextChars + 1}, nil
	case "datetime", "timestamp", "date":
		// TSQ renders a time as DATETIME(6). Any other precision keeps its raw type,
		// so a DATETIME column, which rounds to the second, is widened by Reconcile
		// instead of silently disagreeing with the microseconds held in memory. A
		// column declared with the same type:X still matches.
		if colType == "datetime(6)" {
			return ColumnType{Kind: KindTime}, nil
		}

		return ColumnType{RawType: strings.ToUpper(rawColumnType)}, nil
	default:
		// Anything TSQ does not render keeps its raw type, so it matches only a
		// column declared with that type: a MEDIUMINT is narrower than the INT
		// TSQ declares, and a DECIMAL rounds where a DOUBLE does not.
		if rawColumnType == "" {
			rawColumnType = strings.TrimSpace(dataType)
		}

		return ColumnType{RawType: rawColumnType}, nil
	}
}

// The largest values of MySQL's BLOB and MEDIUMBLOB, in bytes.
const (
	mysqlMaxBlobBytes       = 1<<16 - 1
	mysqlMaxMediumBlobBytes = 1<<24 - 1
)

func (d MySQLDialect) ColumnTypeSQL(desc ColumnType) string {
	if desc.RawType != "" {
		return desc.RawType
	}

	switch desc.Kind {
	case KindBool:
		return "BOOLEAN"
	case KindBytes:
		// A BLOB holds 64 KiB; a larger declared size takes the type that holds it.
		switch {
		case desc.Size <= mysqlMaxBlobBytes:
			return "BLOB"
		case desc.Size <= mysqlMaxMediumBlobBytes:
			return "MEDIUMBLOB"
		default:
			return "LONGBLOB"
		}
	case KindFloat:
		if desc.Bits <= 32 {
			return "FLOAT"
		}

		return "DOUBLE"
	case KindInt:
		switch {
		case desc.Bits <= 8:
			if desc.Unsigned {
				return "TINYINT UNSIGNED"
			}

			return "TINYINT"
		case desc.Bits <= 16:
			if desc.Unsigned {
				return "SMALLINT UNSIGNED"
			}

			return "SMALLINT"
		case desc.Bits <= 32:
			if desc.Unsigned {
				return "INT UNSIGNED"
			}

			return "INT"
		default:
			if desc.Unsigned {
				return "BIGINT UNSIGNED"
			}

			return "BIGINT"
		}

	case KindString:
		switch {
		case desc.Size <= 0:
			return fmt.Sprintf("VARCHAR(%d)", defaultDDLStringSize)
		case desc.Size <= mysqlMaxVarcharChars:
			return fmt.Sprintf("VARCHAR(%d)", desc.Size)
		case desc.Size <= mysqlMaxMediumTextChars:
			return "MEDIUMTEXT"
		default:
			return "LONGTEXT"
		}
	case KindTime:
		return "DATETIME(6)"
	default:
		return "TEXT"
	}
}

func (d MySQLDialect) AutoIncrementColumnSQL(quotedColumn string, desc ColumnType) (string, error) {
	if desc.Kind != KindInt {
		return "", errors.New("auto-increment primary key requires an integer field")
	}

	return strings.Join([]string{
		quotedColumn,
		d.ColumnTypeSQL(desc),
		"PRIMARY KEY",
		"AUTO_INCREMENT",
	}, " "), nil
}

// FullTextIndexSQL renders MySQL's FULLTEXT index, which MATCH ... AGAINST needs.
func (d MySQLDialect) FullTextIndexSQL(table, idx string, quotedFields []string) string {
	return fmt.Sprintf(
		"ALTER TABLE %s ADD FULLTEXT INDEX %s(%s);",
		d.QuoteIdent(table), d.QuoteIdent(idx), strings.Join(quotedFields, ", "),
	)
}

// FullTextVectorSQL is empty: MySQL's predicate names the columns itself.
func (d MySQLDialect) FullTextVectorSQL(quotedFields []string) string { return "" }

func (d MySQLDialect) CreateIndexSQL(table, idx string, fields []string, unique bool) string {
	uniqueClause := ""
	if unique {
		uniqueClause = "UNIQUE "
	}

	return fmt.Sprintf(
		"ALTER TABLE %s ADD %sINDEX %s(%s)%s",
		d.QuoteIdent(table),
		uniqueClause,
		d.QuoteIdent(idx),
		strings.Join(fields, ", "),
		";",
	)
}

func (d MySQLDialect) DropIndexSQL(table, idx string) string {
	return fmt.Sprintf(
		"DROP INDEX %s ON %s;",
		d.QuoteIdent(idx),
		d.QuoteIdent(table),
	)
}

// InspectRebuild is refused: MySQL alters a column in place.
func (d MySQLDialect) InspectRebuild(context.Context, Executor, string) (Rebuild, error) {
	return Rebuild{}, errors.New("mysql alters columns in place and never rebuilds a table")
}

func (d MySQLDialect) AlterMode() AlterMode {
	return AlterInPlace
}

func (d MySQLDialect) AlterColumnSQL(table string, before Column, after ColumnSpec) []string {
	var statements []string

	// NOT NULL over rows holding NULL fails in strict mode and turns them into
	// zero values silently otherwise: fill them first, the same on every dialect.
	if fill := NullFill(d, before, after); fill != "" {
		statements = append(statements, fmt.Sprintf("UPDATE %s SET %s = %s WHERE %s IS NULL;",
			d.QuoteIdent(table), d.QuoteIdent(after.Name), fill, d.QuoteIdent(after.Name)))
	}

	// A number that becomes a BOOLEAN keeps its value, and a 2 in a TINYINT(1) is
	// read into no bool: every later read of the table failed. Anything but zero
	// is true, as PostgreSQL's USING c <> 0 says it.
	if before.Type.RawType == "" && after.Type.RawType == "" && after.Type.Kind == KindBool &&
		(before.Type.Kind == KindInt || before.Type.Kind == KindFloat) {
		statements = append(statements, fmt.Sprintf("UPDATE %s SET %s = 1 WHERE %s <> 0;",
			d.QuoteIdent(table), d.QuoteIdent(after.Name), d.QuoteIdent(after.Name)))
	}

	return append(statements, fmt.Sprintf(
		"ALTER TABLE %s MODIFY COLUMN %s;",
		d.QuoteIdent(table),
		d.renderModifyColumnDefinition(after, before),
	))
}

// renderModifyColumnDefinition renders a column definition for MODIFY COLUMN.
// It must not repeat PRIMARY KEY: MySQL rejects MODIFY COLUMN ... PRIMARY KEY
// on a column that already is the primary key (error 1068 "Multiple primary
// key defined"). AUTO_INCREMENT, however, must be restated or it gets dropped.
// So would the live column's own collation and its comment, which are not
// TSQ's: they are restated from the live column (kept) where the column stays
// a string.
func (d MySQLDialect) renderModifyColumnDefinition(column ColumnSpec, kept Column) string {
	parts := []string{d.QuoteIdent(column.Name), d.ColumnTypeSQL(column.Type)}

	if kept.Collation != "" && mysqlCollates(strings.ToUpper(d.ColumnTypeSQL(column.Type))) {
		parts = append(parts, "COLLATE "+kept.Collation)
	}

	if column.PrimaryKey || !column.Type.Nullable {
		parts = append(parts, "NOT NULL")
	}

	if column.AutoIncrement {
		parts = append(parts, "AUTO_INCREMENT")
	} else if column.Default != "" {
		parts = append(parts, "DEFAULT "+DefaultSQL(d, column))
	}

	if kept.Comment != "" {
		parts = append(parts, "COMMENT "+quoteLiteral(kept.Comment))
	}

	return strings.Join(parts, " ")
}

// mysqlCollates reports a spelled type that takes a COLLATE clause: the
// character types. Any other (a BLOB, a number, a JSON) refuses one.
func mysqlCollates(spelled string) bool {
	for _, t := range []string{"CHAR", "TEXT", "ENUM", "SET("} {
		if strings.Contains(spelled, t) && !strings.Contains(spelled, "BINARY") {
			return true
		}
	}

	return false
}
