package dialect

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type PostgresDialect struct{}

func (d PostgresDialect) Name() Name {
	return Postgres
}

func (d PostgresDialect) QuoteField(f string) string {
	return `"` + f + `"`
}

func (d PostgresDialect) BindVar(i int) string {
	return "$" + strconv.Itoa(i+1)
}

func (d PostgresDialect) CreateTableSuffix() string {
	return ";"
}

func (d PostgresDialect) CreateIndexSuffix() string {
	return ";"
}

func (d PostgresDialect) DropIndexSuffix() string {
	return ""
}

func (d PostgresDialect) TruncateClause() string {
	return "TRUNCATE TABLE"
}

func (d PostgresDialect) AutoIncrementClause() string {
	return ""
}

func (d PostgresDialect) AutoIncrementBindValue() string {
	return "DEFAULT"
}

func (d PostgresDialect) LastInsertIdReturningSuffix(table, col string) string {
	return " RETURNING " + d.QuoteField(col)
}

func (d PostgresDialect) AllTablesQuery() string {
	return "SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema()"
}

func (d PostgresDialect) ListTables(ctx context.Context, db Executor) ([]string, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = current_schema() AND table_type = 'BASE TABLE'
		ORDER BY table_name`)
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

func (d PostgresDialect) CreateTableIfNotExistsSuffix() string {
	return "IF NOT EXISTS"
}

func (d PostgresDialect) HasConstraintsQuery(table, column string) string {
	return `
		SELECT constraint_name
		FROM information_schema.constraint_column_usage
		WHERE table_schema = current_schema()
			AND table_name = $1
			AND column_name = $2`
}

func (d PostgresDialect) ValidateIdentifier(identifier string) error {
	return validateDialectIdentifier(identifier, d.Name(), maxIdentifierLengthPostgreSQL)
}

func (d PostgresDialect) SupportsCapability(capability Capability) bool {
	switch canonicalCapabilityName(string(capability)) {
	case CapabilityCTE,
		CapabilityExcept,
		CapabilityFullOuterJoin,
		CapabilityIntersect,
		CapabilitySelectForUpdate,
		CapabilitySelectForShare,
		CapabilitySelectForNoWait,
		CapabilitySelectForSkipLocked:
		return true
	default:
		return false
	}
}

func (d PostgresDialect) BatchInsertStartID(lastID, rowsAffected int64) (int64, bool) {
	return 0, false
}

func (d PostgresDialect) InspectTableColumns(ctx context.Context, db Executor, table string) ([]DDLColumnSpec, bool, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT
			c.column_name,
			c.data_type,
			c.udt_name,
			c.is_nullable,
			c.column_default,
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
		Name    string
		Data    string
		UDT     string
		Null    string
		Default sql.NullString
		Size    sql.NullInt64
		Primary bool
	}

	columns := make([]DDLColumnSpec, 0)

	for rows.Next() {
		var item row
		if err := rows.Scan(&item.Name, &item.Data, &item.UDT, &item.Null, &item.Default, &item.Size, &item.Primary); err != nil {
			return nil, false, err
		}

		desc, err := parsePostgresDDLColumnType(item.Data, item.UDT, item.Size)
		if err != nil {
			return nil, false, fmt.Errorf("inspect postgres column %s.%s: %w", table, item.Name, err)
		}

		defaultValue := normalizeDDLDefault(item.Default)
		columns = append(columns, DDLColumnSpec{
			Name:          item.Name,
			Type:          withDDLNullable(desc, strings.EqualFold(item.Null, "YES") && !item.Primary),
			PrimaryKey:    item.Primary,
			AutoIncrement: strings.HasPrefix(defaultValue, "nextval("),
			Default:       defaultValue,
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

func (d PostgresDialect) ListIndexes(ctx context.Context, db Executor, table string) ([]NamedIndexDefinition, error) {
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
		JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ord.attnum
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

	indexes := make([]NamedIndexDefinition, 0)

	for rows.Next() {
		var item NamedIndexDefinition

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

func (d PostgresDialect) EnsureIndex(ctx context.Context, db Executor, table string, unique bool, idx string, fields []string) (string, error) {
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

func (d PostgresDialect) InspectIndexDefinition(ctx context.Context, db Executor, table, idx string) (IndexDefinition, bool, error) {
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

func parsePostgresDDLColumnType(dataType, udtName string, size sql.NullInt64) (DDLColumnType, error) {
	data := strings.ToLower(strings.TrimSpace(dataType))
	udt := strings.ToLower(strings.TrimSpace(udtName))

	switch data {
	case "boolean":
		return DDLColumnType{Kind: DDLColumnKindBool}, nil
	case "smallint":
		return DDLColumnType{Kind: DDLColumnKindInt, Bits: 16}, nil
	case "integer":
		return DDLColumnType{Kind: DDLColumnKindInt, Bits: 32}, nil
	case "bigint":
		return DDLColumnType{Kind: DDLColumnKindInt, Bits: 64}, nil
	case "real":
		return DDLColumnType{Kind: DDLColumnKindFloat, Bits: 32}, nil
	case "double precision", "numeric":
		return DDLColumnType{Kind: DDLColumnKindFloat, Bits: 64}, nil
	case "bytea":
		return DDLColumnType{Kind: DDLColumnKindBytes}, nil
	case "character varying", "character":
		desc := DDLColumnType{Kind: DDLColumnKindString}
		if size.Valid && size.Int64 > 0 {
			desc.Size = int(size.Int64)
		}

		return desc, nil
	case "text":
		return DDLColumnType{Kind: DDLColumnKindString}, nil
	case "timestamp without time zone", "timestamp with time zone", "date":
		return DDLColumnType{Kind: DDLColumnKindTime}, nil
	}

	switch udt {
	case "bool":
		return DDLColumnType{Kind: DDLColumnKindBool}, nil
	case "bytea":
		return DDLColumnType{Kind: DDLColumnKindBytes}, nil
	case "int2":
		return DDLColumnType{Kind: DDLColumnKindInt, Bits: 16}, nil
	case "int4":
		return DDLColumnType{Kind: DDLColumnKindInt, Bits: 32}, nil
	case "int8":
		return DDLColumnType{Kind: DDLColumnKindInt, Bits: 64}, nil
	case "float4":
		return DDLColumnType{Kind: DDLColumnKindFloat, Bits: 32}, nil
	case "float8":
		return DDLColumnType{Kind: DDLColumnKindFloat, Bits: 64}, nil
	case "varchar":
		desc := DDLColumnType{Kind: DDLColumnKindString}
		if size.Valid && size.Int64 > 0 {
			desc.Size = int(size.Int64)
		}

		return desc, nil
	case "text":
		return DDLColumnType{Kind: DDLColumnKindString}, nil
	case "timestamp", "timestamptz", "date":
		return DDLColumnType{Kind: DDLColumnKindTime}, nil
	default:
		return DDLColumnType{}, fmt.Errorf("unsupported postgres column type %q (%q)", dataType, udtName)
	}
}

func (d PostgresDialect) DDLColumnType(desc DDLColumnType) string {
	switch desc.Kind {
	case DDLColumnKindBool:
		return "BOOLEAN"
	case DDLColumnKindBytes:
		return "BYTEA"
	case DDLColumnKindFloat:
		if desc.Bits <= 32 {
			return "REAL"
		}

		return "DOUBLE PRECISION"
	case DDLColumnKindInt:
		switch {
		case desc.Bits <= 16:
			return "SMALLINT"
		case desc.Bits <= 32:
			return "INTEGER"
		default:
			return "BIGINT"
		}
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

func (d PostgresDialect) DDLAutoIncrementPrimaryKey(quotedColumn string, desc DDLColumnType) (string, error) {
	if desc.Kind != DDLColumnKindInt {
		return "", errors.New("auto-increment primary key requires an integer field")
	}

	return quotedColumn + " " + ddlSerialType(desc), nil
}

func (d PostgresDialect) DDLCreateIndex(table, idx string, fields []string, unique bool) string {
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

func (d PostgresDialect) DDLDropIndex(table, idx string) string {
	return fmt.Sprintf("DROP INDEX %s;", d.QuoteField(idx))
}

func (d PostgresDialect) DDLAlterColumnMode() DDLAlterColumnMode {
	return DDLAlterColumnDirect
}

func (d PostgresDialect) DDLAlterColumnStatements(table string, before, after DDLColumnSpec) []string {
	statements := make([]string, 0, 3)
	quotedTable := d.QuoteField(table)
	quotedColumn := d.QuoteField(after.Name)

	if before.Type != after.Type {
		statements = append(statements, fmt.Sprintf(
			"ALTER TABLE %s ALTER COLUMN %s TYPE %s;",
			quotedTable,
			quotedColumn,
			d.DDLColumnType(after.Type),
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

	if before.Default != after.Default {
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
