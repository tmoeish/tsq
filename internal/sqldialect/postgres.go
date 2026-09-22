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

func (d PostgresDialect) ValidateIdentifier(identifier string) error {
	return validateDialectIdentifier(identifier, d.Name(), maxIdentifierLengthPostgreSQL)
}

func (d PostgresDialect) SupportsCapability(capability Capability) bool {
	return tsqdialect.Supports(d.Name(), capability)
}

func (d PostgresDialect) BatchInsertStartID(lastID, rowsAffected int64) (int64, bool) {
	return 0, false
}

func (d PostgresDialect) InspectColumns(ctx context.Context, db Executor, table string) ([]Column, bool, error) {
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
				WHERE ns.nspname = current_schema()
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
			EXISTS (
				SELECT 1
				FROM pg_index i
				JOIN pg_class t ON t.oid = i.indrelid
				JOIN pg_namespace ns ON ns.oid = t.relnamespace
				JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(i.indkey)
				WHERE ns.nspname = current_schema()
					AND t.relname = c.table_name
					AND a.attname = c.column_name
					AND i.indisprimary
			) AS is_primary
		FROM information_schema.columns c
		WHERE c.table_schema = current_schema() AND c.table_name = $1
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
		Name     string
		Data     string
		UDT      string
		Format   string
		Null     string
		Default  sql.NullString
		Identity sql.NullString
		Size     sql.NullInt64
		Primary  bool
	}

	columns := make([]Column, 0)

	for rows.Next() {
		var item row
		if err := rows.Scan(&item.Name, &item.Data, &item.UDT, &item.Format, &item.Null, &item.Default, &item.Identity, &item.Size, &item.Primary); err != nil {
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
	case "double precision", "numeric":
		return ColumnType{Kind: KindFloat, Bits: 64}, nil
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
	case "timestamp without time zone", "timestamp with time zone", "date":
		return ColumnType{Kind: KindTime}, nil
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
		switch {
		case desc.Bits <= 16:
			return "SMALLINT"
		case desc.Bits <= 32:
			return "INTEGER"
		default:
			return "BIGINT"
		}
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

func (d PostgresDialect) AutoIncrementColumnSQL(quotedColumn string, desc ColumnType) (string, error) {
	if desc.Kind != KindInt {
		return "", errors.New("auto-increment primary key requires an integer field")
	}

	return quotedColumn + " " + ddlSerialType(desc), nil
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

	// Compare resolved types instead of raw struct equality: nullability lives
	// inside DDLColumnType, and a nullability-only drift must not trigger a
	// table-rewriting ALTER TYPE.
	if !SameColumnType(d, before, after) {
		statements = append(statements, fmt.Sprintf(
			"ALTER TABLE %s ALTER COLUMN %s TYPE %s;",
			quotedTable,
			quotedColumn,
			d.ColumnTypeSQL(after.Type),
		))
	}

	if before.PrimaryKey != after.PrimaryKey || before.AutoIncrement != after.AutoIncrement {
		return nil
	}

	if before.Type.Nullable != after.Type.Nullable {
		action := "SET"
		if after.Type.Nullable {
			action = "DROP"
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
	if before.Default != after.Default && (!before.AutoIncrement || !after.AutoIncrement) {
		if after.Default == "" {
			statements = append(statements, fmt.Sprintf(
				"ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT;",
				quotedTable,
				quotedColumn,
			))
		} else {
			statements = append(statements, fmt.Sprintf(
				"ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s;",
				quotedTable,
				quotedColumn,
				after.Default,
			))
		}
	}

	return statements
}
