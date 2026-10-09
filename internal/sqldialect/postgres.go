package sqldialect

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
)

type PostgresDialect struct{}

func (d PostgresDialect) Name() Name {
	return Postgres
}

func (d PostgresDialect) QuoteIdent(f string) string {
	return `"` + f + `"`
}

func (d PostgresDialect) Placeholder(i int) string {
	return "$" + strconv.Itoa(i+1)
}

func (d PostgresDialect) ReturningClause(col string) string {
	return " RETURNING " + d.QuoteIdent(col)
}

func (d PostgresDialect) Returning(cols ...string) string {
	quoted := make([]string, len(cols))
	for i, col := range cols {
		quoted[i] = d.QuoteIdent(col)
	}

	return " RETURNING " + strings.Join(quoted, ", ")
}

func (d PostgresDialect) ValidateIdentifier(identifier string) error {
	return validateDialectIdentifier(identifier, d.Name(), maxIdentifierLengthPostgreSQL)
}

func (d PostgresDialect) SupportsCapability(capability Capability) bool {
	return tsqdialect.Supports(d.Name(), capability)
}

func (d PostgresDialect) BatchInsertStartID(lastID, rowsAffected, step int64) (int64, bool) {
	return 0, false
}

func (d PostgresDialect) InsertIDStepQuery() string { return "" }

// KeySequenceAdvanceQuery sets the column's sequence to the greater of the key
// written and its last value, as the counters of MySQL and SQLite move on their
// own: a sequence does not follow a key a statement writes, so the next key it
// generated collided with a row inserted with its own key. Moving it takes UPDATE
// on the sequence, which the query checks instead of failing: inside a
// transaction a refused statement would abort the whole of it.
func (d PostgresDialect) KeySequenceAdvanceQuery(table, column string) (string, error) {
	quotedTable, err := quoteDialectIdentifier(d, table)
	if err != nil {
		return "", err
	}

	if err := d.ValidateIdentifier(column); err != nil {
		return "", err
	}

	return "SELECT seq IS NOT NULL, " +
		"seq IS NOT NULL AND has_sequence_privilege(seq::regclass, 'UPDATE'), " +
		"CASE WHEN seq IS NOT NULL AND has_sequence_privilege(seq::regclass, 'UPDATE') " +
		"THEN setval(seq::regclass, GREATEST($1::bigint, COALESCE(pg_sequence_last_value(seq::regclass), 0)), true) END " +
		"FROM pg_get_serial_sequence(" + quoteLiteral(quotedTable) + ", " + quoteLiteral(column) + ") AS seq", nil
}

func (d PostgresDialect) InspectColumns(ctx context.Context, db Executor, table string) ([]Column, bool, error) {
	return d.inspectColumns(ctx, db, "current_schema()", table)
}

// postgresSchemaLock is the key of the advisory lock schema changes are made
// under: the bytes of "tsqSCHMA". Advisory locks are kept per database.
const postgresSchemaLock int64 = 0x7473715343484D41

// LockSchema takes a session advisory lock.
func (d PostgresDialect) LockSchema(ctx context.Context, conn Executor) (func(context.Context) error, bool, error) {
	if _, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock($1)", postgresSchemaLock); err != nil {
		return nil, true, err
	}

	return func(ctx context.Context) error {
		_, err := conn.ExecContext(ctx, "SELECT pg_advisory_unlock($1)", postgresSchemaLock)

		return err
	}, true, nil
}

// postgresTempSchema names the schema of the session's temporary tables.
const postgresTempSchema = "(SELECT nspname FROM pg_namespace WHERE oid = pg_my_temp_schema())"

// ProbeColumn creates declared in a temporary table and reads it back with the
// query InspectColumns runs, pointed at the session's temporary schema: the two
// columns are then described by one reader.
func (d PostgresDialect) ProbeColumn(ctx context.Context, conn Executor, table string, inspected Column, declared ColumnSpec) (Spelling, bool, error) {
	definition, err := ColumnDefinitionSQL(d, probeSpec(declared))
	if err != nil {
		return Spelling{}, true, err
	}

	drop := "DROP TABLE IF EXISTS pg_temp." + d.QuoteIdent(probeTable)
	if _, err := conn.ExecContext(ctx, drop); err != nil {
		return Spelling{}, true, err
	}

	if _, err := conn.ExecContext(ctx, fmt.Sprintf("CREATE TEMPORARY TABLE %s (%s)", d.QuoteIdent(probeTable), definition)); err != nil {
		return Spelling{}, true, err
	}

	columns, _, err := d.inspectColumns(ctx, conn, postgresTempSchema, probeTable)

	if _, dropErr := conn.ExecContext(ctx, drop); err == nil {
		err = dropErr
	}

	if err != nil {
		return Spelling{}, true, err
	}

	if len(columns) != 1 {
		return Spelling{}, true, fmt.Errorf("the probe of column %s read %d columns", declared.Name, len(columns))
	}

	probe := columns[0]
	same := Spelling{
		Type:    probe.NativeType != "" && strings.EqualFold(strings.TrimSpace(probe.NativeType), strings.TrimSpace(inspected.NativeType)),
		Default: probe.Default == inspected.Default,
	}

	// A SERIAL column draws from a sequence named after its table and itself:
	// nextval('t_c_seq') for the live column and nextval('_tsq_probe_c_seq') for the
	// probe are one default, and both columns count as filled by the database.
	if probe.AutoIncrement && inspected.AutoIncrement && !inspected.PrimaryKey &&
		strings.ReplaceAll(probe.Default, probeTable, table) == inspected.Default {
		same.Default, same.Serial = true, true
	}

	return same, true, nil
}

// inspectColumns reads the columns of table in the schema the SQL expression
// schema names.
func (d PostgresDialect) inspectColumns(ctx context.Context, db Executor, schema, table string) ([]Column, bool, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT
			c.column_name,
			c.data_type,
			c.udt_name,
			(
				SELECT pg_catalog.format_type(a.atttypid, a.atttypmod)
				FROM pg_class t
				JOIN pg_namespace ns ON ns.oid = t.relnamespace
				JOIN pg_attribute a ON a.attrelid = t.oid
				WHERE ns.nspname = `+schema+`
					AND t.relname = c.table_name
					AND a.attname = c.column_name
					AND a.attnum > 0
					AND NOT a.attisdropped
				LIMIT 1
			) AS formatted_type,
			c.is_nullable,
			c.column_default,
			c.is_identity,
			c.character_maximum_length,
			COALESCE(c.collation_name, ''),
			EXISTS (
				SELECT 1
				FROM pg_index i
				JOIN pg_class t ON t.oid = i.indrelid
				JOIN pg_namespace ns ON ns.oid = t.relnamespace
				JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(i.indkey)
				WHERE ns.nspname = `+schema+`
					AND t.relname = c.table_name
					AND a.attname = c.column_name
					AND i.indisprimary
			) AS is_primary,
			(
				SELECT pg_catalog.pg_get_constraintdef(con.oid)
				FROM pg_constraint con
				JOIN pg_class t ON t.oid = con.conrelid
				JOIN pg_namespace ns ON ns.oid = t.relnamespace
				WHERE ns.nspname = `+schema+`
					AND t.relname = c.table_name
					AND con.contype = 'c'
					AND con.conname = 'ck_' || c.column_name
				LIMIT 1
			) AS range_check
		FROM information_schema.columns c
		WHERE c.table_schema = `+schema+` AND c.table_name = $1
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
		UDT       string
		Format    string
		Null      string
		Default   sql.NullString
		Identity  sql.NullString
		Size      sql.NullInt64
		Collation string
		Primary   bool
		Check     sql.NullString
	}

	columns := make([]Column, 0)

	for rows.Next() {
		var item row
		if err := rows.Scan(&item.Name, &item.Data, &item.UDT, &item.Format, &item.Null, &item.Default, &item.Identity, &item.Size, &item.Collation, &item.Primary, &item.Check); err != nil {
			return nil, false, err
		}

		desc, err := parsePostgresColumnType(item.Data, item.UDT, item.Format, item.Size)
		if err != nil {
			return nil, false, fmt.Errorf("inspect postgres column %s.%s: %w", table, item.Name, err)
		}

		defaultValue := normalizeDDLDefault(item.Default)
		// SERIAL columns surface as a nextval(...) default; identity columns
		// (GENERATED ... AS IDENTITY) have no default and must be detected via
		// is_identity. Both are database-managed auto-increment mechanisms.
		autoIncrement := strings.HasPrefix(defaultValue, "nextval(") ||
			strings.EqualFold(strings.TrimSpace(item.Identity.String), "YES")
		columns = append(columns, Column{
			Name:          item.Name,
			Type:          withDDLNullable(desc, strings.EqualFold(item.Null, "YES") && !item.Primary),
			PrimaryKey:    item.Primary,
			AutoIncrement: autoIncrement,
			Default:       defaultValue,
			NativeType:    strings.TrimSpace(item.Format),
			Check:         strings.TrimSpace(item.Check.String),
			Collation:     item.Collation,
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

func (d PostgresDialect) ListIndexes(ctx context.Context, db Executor, table string) ([]Index, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT
			idx.relname AS index_name,
			i.indisunique AS is_unique,
			i.indisprimary AS is_primary,
			COALESCE(c.oid IS NOT NULL, false) AS is_constraint,
			STRING_AGG(a.attname, ',' ORDER BY ord.ord) AS columns_csv
		FROM pg_class t
		JOIN pg_namespace ns ON ns.oid = t.relnamespace
		JOIN pg_index i ON i.indrelid = t.oid
		JOIN pg_class idx ON idx.oid = i.indexrelid
		JOIN UNNEST(i.indkey) WITH ORDINALITY AS ord(attnum, ord) ON TRUE
		-- LEFT JOIN, because an expression index has attnum 0 and no pg_attribute
		-- row: an inner join would hide the index instead of reporting it with no
		-- columns, and TSQ would try to create it again on every boot.
		LEFT JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ord.attnum
		LEFT JOIN pg_constraint c ON c.conindid = idx.oid
		WHERE ns.nspname = current_schema() AND t.relname = $1
		GROUP BY idx.relname, i.indisunique, i.indisprimary, c.oid
		ORDER BY idx.relname`,
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
		var item Index

		var columns sql.NullString
		if err := rows.Scan(&item.Name, &item.Unique, &item.PrimaryKey, &item.Constraint, &columns); err != nil {
			return nil, err
		}
		item.Table = table
		item.Fields = parseColumnsCSV(columns.String)
		indexes = append(indexes, item)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return indexes, nil
}

func (d PostgresDialect) EnsureIndex(ctx context.Context, db Executor, table, idx string, fields []string, unique bool) (string, error) {
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
		if inspectErr == nil && found && ValidateIndex(table, unique, idx, fields, definition) == nil {
			return "", nil
		}

		return "", err
	}

	return query, nil
}

func (d PostgresDialect) InspectIndex(ctx context.Context, db Executor, table, idx string) (Index, bool, error) {
	type row struct {
		Table   string         `db:"table_name"`
		Unique  bool           `db:"is_unique"`
		Columns sql.NullString `db:"columns_csv"`
	}

	var existing row

	err := db.QueryRowContext(ctx, `
		SELECT
			t.relname AS table_name,
			i.indisunique AS is_unique,
			STRING_AGG(a.attname, ',' ORDER BY ord.ord) AS columns_csv
		FROM pg_class idx
		JOIN pg_namespace ns ON ns.oid = idx.relnamespace
		JOIN pg_index i ON i.indexrelid = idx.oid
		JOIN pg_class t ON t.oid = i.indrelid
		JOIN UNNEST(i.indkey) WITH ORDINALITY AS ord(attnum, ord) ON TRUE
		-- LEFT JOIN, because an expression index has attnum 0 and no pg_attribute
		-- row: an inner join would hide the index instead of reporting it with no
		-- columns, and TSQ would try to create it again on every boot.
		LEFT JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ord.attnum
		WHERE ns.nspname = current_schema()
			AND idx.relname = $1
		GROUP BY t.relname, i.indisunique`,
		idx,
	).Scan(&existing.Table, &existing.Unique, &existing.Columns)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Index{}, false, nil
		}

		return Index{}, false, err
	}

	return Index{
		Table:  existing.Table,
		Unique: existing.Unique,
		Fields: parseColumnsCSV(existing.Columns.String),
	}, true, nil
}

func parsePostgresColumnType(dataType, udtName, formattedType string, size sql.NullInt64) (ColumnType, error) {
	data := strings.ToLower(strings.TrimSpace(dataType))
	udt := strings.ToLower(strings.TrimSpace(udtName))

	switch data {
	case "boolean":
		return ColumnType{Kind: KindBool}, nil
	case "smallint":
		return ColumnType{Kind: KindInt, Bits: 16}, nil
	case "integer":
		return ColumnType{Kind: KindInt, Bits: 32}, nil
	case "bigint":
		return ColumnType{Kind: KindInt, Bits: 64}, nil
	case "real":
		return ColumnType{Kind: KindFloat, Bits: 32}, nil
	case "double precision":
		return ColumnType{Kind: KindFloat, Bits: 64}, nil
	case "numeric":
		// NUMERIC(20) is what TSQ declares for uint64; any other NUMERIC keeps its
		// raw type, so a DECIMAL(10,2) no longer passes for a float column.
		if n := strings.ReplaceAll(strings.ToLower(formattedType), " ", ""); n == "numeric(20,0)" || n == "numeric(20)" {
			return ColumnType{Kind: KindInt, Bits: 64, Unsigned: true}, nil
		}

		return ColumnType{RawType: strings.ToUpper(strings.TrimSpace(formattedType))}, nil
	case "bytea":
		return ColumnType{Kind: KindBytes}, nil
	case "character varying", "character":
		desc := ColumnType{Kind: KindString}
		if size.Valid && size.Int64 > 0 {
			desc.Size = int(size.Int64)
		}

		return desc, nil
	case "text":
		// Keep TEXT as a raw type so it round-trips; mapping it to the string
		// kind would render as VARCHAR(n) and produce spurious ALTERs on every
		// reconcile of columns declared as TEXT.
		return ColumnType{RawType: "TEXT"}, nil
	case "timestamp without time zone", "timestamp with time zone":
		return ColumnType{Kind: KindTime}, nil
	case "date":
		// A DATE drops the time of day TSQ writes, so it is not a time column.
		return ColumnType{RawType: "DATE"}, nil
	}

	switch udt {
	case "bool":
		return ColumnType{Kind: KindBool}, nil
	case "bytea":
		return ColumnType{Kind: KindBytes}, nil
	case "int2":
		return ColumnType{Kind: KindInt, Bits: 16}, nil
	case "int4":
		return ColumnType{Kind: KindInt, Bits: 32}, nil
	case "int8":
		return ColumnType{Kind: KindInt, Bits: 64}, nil
	case "float4":
		return ColumnType{Kind: KindFloat, Bits: 32}, nil
	case "float8":
		return ColumnType{Kind: KindFloat, Bits: 64}, nil
	case "varchar":
		desc := ColumnType{Kind: KindString}
		if size.Valid && size.Int64 > 0 {
			desc.Size = int(size.Int64)
		}

		return desc, nil
	case "text":
		return ColumnType{RawType: "TEXT"}, nil
	case "timestamp", "timestamptz", "date":
		return ColumnType{Kind: KindTime}, nil
	default:
		rawType := strings.TrimSpace(formattedType)
		if rawType == "" {
			rawType = strings.TrimSpace(dataType)
		}

		return ColumnType{RawType: rawType}, nil
	}
}

func (d PostgresDialect) ColumnTypeSQL(desc ColumnType) string {
	if desc.RawType != "" {
		return desc.RawType
	}

	switch desc.Kind {
	case KindBool:
		return "BOOLEAN"
	case KindBytes:
		return "BYTEA"
	case KindFloat:
		if desc.Bits <= 32 {
			return "REAL"
		}

		return "DOUBLE PRECISION"
	case KindInt:
		// PostgreSQL has no unsigned integers: an unsigned type takes the next
		// wider one, so its upper half fits, and uint64 a NUMERIC(20).
		bits := desc.Bits
		if bits <= 0 {
			bits = 64
		}

		if desc.Unsigned {
			bits *= 2
		}

		switch {
		case bits <= 16:
			return "SMALLINT"
		case bits <= 32:
			return "INTEGER"
		case bits <= 64:
			return "BIGINT"
		default:
			return "NUMERIC(20)"
		}
	case KindString:
		switch {
		case desc.Size <= 0:
			return fmt.Sprintf("VARCHAR(%d)", defaultDDLStringSize)
		case desc.Size > postgresMaxVarcharChars:
			// A longer VARCHAR is refused; TEXT holds what MySQL's LONGTEXT does.
			return "TEXT"
		}

		return fmt.Sprintf("VARCHAR(%d)", desc.Size)
	case KindTime:
		return "TIMESTAMP"
	default:
		return "TEXT"
	}
}

func (d PostgresDialect) AutoIncrementColumnSQL(quotedColumn string, desc ColumnType) (string, error) {
	if desc.Kind != KindInt {
		return "", errors.New("auto-increment primary key requires an integer field")
	}

	// The key is as wide as the column the same field would be elsewhere, where
	// an unsigned type takes the next wider one; the widest sequence is a BIGINT's.
	bits := desc.Bits
	if bits <= 0 {
		bits = 64
	}

	if desc.Unsigned {
		bits *= 2
	}

	switch {
	case bits <= 16:
		return quotedColumn + " SMALLSERIAL PRIMARY KEY", nil
	case bits <= 32:
		return quotedColumn + " SERIAL PRIMARY KEY", nil
	default:
		return quotedColumn + " BIGSERIAL PRIMARY KEY", nil
	}
}

// FullTextIndexSQL indexes the same expression the predicate repeats, which is what
// lets PostgreSQL use the index.
func (d PostgresDialect) FullTextIndexSQL(table, idx string, quotedFields []string) string {
	return fmt.Sprintf(
		"CREATE INDEX %s ON %s USING GIN (%s);",
		d.QuoteIdent(idx), d.QuoteIdent(table), d.FullTextVectorSQL(quotedFields),
	)
}

// FullTextVectorSQL builds the tsvector of the fields. The 'simple' configuration
// only folds case, so the same term finds the same rows whatever the server's
// default_text_search_config is.
func (d PostgresDialect) FullTextVectorSQL(quotedFields []string) string {
	parts := make([]string, 0, len(quotedFields))
	for _, field := range quotedFields {
		parts = append(parts, fmt.Sprintf("coalesce(%s, '')", field))
	}

	return fmt.Sprintf("to_tsvector('simple', %s)", strings.Join(parts, " || ' ' || "))
}

func (d PostgresDialect) CreateIndexSQL(table, idx string, fields []string, unique bool) string {
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

func (d PostgresDialect) DropIndexSQL(table, idx string) string {
	return fmt.Sprintf("DROP INDEX %s;", d.QuoteIdent(idx))
}

// InspectRebuild is refused: PostgreSQL alters a column in place.
func (d PostgresDialect) InspectRebuild(context.Context, Executor, string) (Rebuild, error) {
	return Rebuild{}, errors.New("postgres alters columns in place and never rebuilds a table")
}

func (d PostgresDialect) AlterMode() AlterMode {
	return AlterInPlace
}

func (d PostgresDialect) AlterColumnSQL(table string, before Column, after ColumnSpec) []string {
	statements := make([]string, 0, 3)
	quotedTable := d.QuoteIdent(table)
	quotedColumn := d.QuoteIdent(after.Name)
	droppedDefault := false

	// Compare resolved types instead of raw struct equality: nullability lives
	// inside DDLColumnType, and a nullability-only drift must not trigger a
	// table-rewriting ALTER TYPE.
	// USING says how to convert where PostgreSQL has no assignment cast (BOOLEAN
	// to INTEGER, VARCHAR to BIGINT). Within one kind it is left out: an explicit
	// cast to VARCHAR(8) cuts a longer value to 8 characters without a word, while
	// the assignment cast refuses it, and a narrower integer fails on overflow
	// either way. Elsewhere postgresUsing writes it, never as a cast to a character
	// type's length. An auto-increment key keeps its SERIAL default; its sequence is
	// widened too, or it stops at the old type's maximum however wide the column is.
	wanted, _ := RangeCheck(d, after)
	droppedCheck := false

	if !SameColumnType(d, before, after) {
		spelled := d.ColumnTypeSQL(storageType(d, after))

		// ALTER COLUMN TYPE resets a collation of the column's own to the
		// default: the live one is restated where the column stays a character
		// type. It is not TSQ's to declare, so it is kept, not compared.
		collated := spelled
		if before.Collation != "" && postgresCollates(strings.ToUpper(spelled)) {
			collated = spelled + " COLLATE " + d.QuoteIdent(before.Collation)
		}

		// The range constraint of the old type goes first: its expression is read
		// against the new type, and "c >= 0" over a VARCHAR is "operator does not
		// exist". It comes back below where the new type still needs one.
		if before.Check != "" {
			droppedCheck = true

			statements = append(statements, fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s;", quotedTable, d.QuoteIdent(RangeCheckName(after.Name))))
		}

		if after.AutoIncrement {
			statements = append(statements, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s;", quotedTable, quotedColumn, collated))

			// A key that generates nothing yet has no sequence to widen: the
			// identity added below takes the column's type.
			if spelled == "BIGINT" && before.AutoIncrement {
				statements = append(statements, fmt.Sprintf(
					"DO $$ BEGIN EXECUTE format('ALTER SEQUENCE %%s AS BIGINT', pg_get_serial_sequence(%s, %s)); END $$;",
					quoteLiteral(quotedTable), quoteLiteral(after.Name)))
			}
		} else if before.Type.Kind == after.Type.Kind && before.Type.RawType == "" && after.Type.RawType == "" {
			statements = append(statements, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s;", quotedTable, quotedColumn, collated))
		} else {
			// The default of the old type goes first: the server casts it to the
			// new type on its own, not through USING, and refuses the change where
			// it cannot ("default for column cannot be cast automatically").
			if before.Default != "" {
				droppedDefault = true

				statements = append(statements, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT;", quotedTable, quotedColumn))
			}

			statements = append(statements, fmt.Sprintf(
				"ALTER TABLE %s ALTER COLUMN %s TYPE %s USING %s;",
				quotedTable, quotedColumn, collated, postgresUsing(quotedColumn, before.Type, after.Type, spelled),
			))
		}
	}

	// A primary key that generates nothing where the declaration generates its
	// keys gets an identity, as MySQL's gets AUTO_INCREMENT: a table another tool
	// created with a plain key. The identity starts past the rows present, as
	// MySQL's counter does. The other way round drops the generator, whichever
	// kind it is (an identity, or a SERIAL's default; the sequence stays, owned
	// by the column). A key that moves is a migration to write.
	if before.PrimaryKey && after.PrimaryKey && before.AutoIncrement != after.AutoIncrement && after.Type.Kind == tsqdialect.KindInt && after.Type.RawType == "" {
		if after.AutoIncrement {
			return append(statements,
				fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s ADD GENERATED BY DEFAULT AS IDENTITY;", quotedTable, quotedColumn),
				fmt.Sprintf("SELECT setval(pg_get_serial_sequence(%s, %s), COALESCE((SELECT MAX(%s) FROM %s), 0) + 1, false);",
					quoteLiteral(quotedTable), quoteLiteral(after.Name), quotedColumn, quotedTable),
			)
		}

		return append(statements,
			fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP IDENTITY IF EXISTS;", quotedTable, quotedColumn),
			fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT;", quotedTable, quotedColumn),
		)
	}

	if before.PrimaryKey != after.PrimaryKey || before.AutoIncrement != after.AutoIncrement {
		return nil
	}

	if before.Type.Nullable != after.Type.Nullable {
		action := "SET"
		if after.Type.Nullable {
			action = "DROP"
		}

		// SET NOT NULL fails on a row holding NULL: fill those first.
		if fill := NullFill(d, before, after); fill != "" {
			statements = append(statements, fmt.Sprintf("UPDATE %s SET %s = %s WHERE %s IS NULL;",
				quotedTable, quotedColumn, fill, quotedColumn))
		}

		statements = append(statements, fmt.Sprintf(
			"ALTER TABLE %s ALTER COLUMN %s %s NOT NULL;",
			quotedTable,
			quotedColumn,
			action,
		))
	}

	// Never touch defaults on auto-increment columns: the database-side
	// nextval('..._seq') default is an implementation detail of SERIAL and
	// dropping it would break inserts.
	// The default is written as CREATE TABLE writes it (a current time in UTC),
	// and compared by meaning: 'USD'::character varying is the declared 'USD'.
	if (droppedDefault || !SameDefault(before.Default, DefaultSQL(d, after))) && (!before.AutoIncrement || !after.AutoIncrement) {
		if after.Default == "" {
			if !droppedDefault {
				statements = append(statements, fmt.Sprintf(
					"ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT;",
					quotedTable,
					quotedColumn,
				))
			}
		} else {
			statements = append(statements, fmt.Sprintf(
				"ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s;",
				quotedTable,
				quotedColumn,
				DefaultSQL(d, after),
			))
		}
	}

	// The range constraint follows the field: a wider or narrower one, or none. A
	// narrower one is refused by the server where a row is outside it, which is
	// the refusal a type change gets too.
	if droppedCheck || !SameRangeCheck(after.Name, before.Check, wanted) {
		name := d.QuoteIdent(RangeCheckName(after.Name))

		if before.Check != "" && !droppedCheck {
			statements = append(statements, fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s;", quotedTable, name))
		}

		if wanted != "" {
			statements = append(statements, fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s);", quotedTable, name, wanted))
		}
	}

	return statements
}

// postgresMaxVarcharChars is the longest VARCHAR PostgreSQL declares.
const postgresMaxVarcharChars = 10485760

// postgresCollates reports a spelled type that takes a COLLATE clause: the
// character types.
func postgresCollates(spelled string) bool {
	return strings.Contains(spelled, "CHAR") || strings.Contains(spelled, "TEXT")
}

// postgresUsing is the USING expression of a type change between kinds, or to or
// from a raw type. A cast to the new type itself cuts a value to a character
// type's length without a word ('abcdef'::VARCHAR(3) is 'abc'), which changed
// stored values in a migration nobody was told about: a character target takes the
// value as TEXT and leaves the length to the assignment, which refuses a value
// that does not fit. BOOLEAN casts to INTEGER only, and no number casts to it.
//
// Bytes and text are not cast into one another at all: bytea::TEXT is the hex
// spelling of the bytes ('abc' becomes '\x616263') and text::BYTEA reads the text
// as an escape string ('tab\101x' becomes 'tabAx'), and both rewrote every stored
// value where MySQL and SQLite keep the bytes. convert_from and convert_to carry
// the bytes over, and refuse bytes that are not text.
func postgresUsing(column string, before, after ColumnType, spelled string) string {
	plain := before.RawType == "" && after.RawType == ""
	target := strings.ToUpper(strings.TrimSpace(spelled))
	source := strings.ToUpper(strings.TrimSpace(PostgresDialect{}.ColumnTypeSQL(before)))

	switch {
	case postgresCharacterType(target) && source == "BYTEA":
		return "convert_from(" + column + ", 'UTF8')"
	case target == "BYTEA" && postgresCharacterType(source):
		return "convert_to(" + column + ", 'UTF8')"
	case postgresCharacterType(target):
		return column + "::TEXT"
	case plain && before.Kind == KindBool && after.Kind == KindInt:
		return column + "::INTEGER"
	case plain && before.Kind == KindBool && after.Kind == KindFloat:
		// No cast from BOOLEAN to a floating-point type, where INTEGER has both.
		return column + "::INTEGER::" + spelled
	case plain && (before.Kind == KindInt || before.Kind == KindFloat) && after.Kind == KindBool:
		return column + " <> 0"
	default:
		return column + "::" + spelled
	}
}

// postgresCharacterType reports a type spelled in upper case that holds text.
func postgresCharacterType(spelled string) bool {
	if strings.Contains(spelled, "[") {
		return false
	}

	for _, name := range []string{"VARCHAR", "CHAR", "BPCHAR", "TEXT", "CITEXT"} {
		if strings.HasPrefix(spelled, name) {
			return true
		}
	}

	return false
}

// quoteLiteral writes s as a SQL string literal.
func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
