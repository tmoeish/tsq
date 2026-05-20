package tsq

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// PostgresDialect is the PostgreSQL dialect implementation.
type PostgresDialect struct{}

// Name returns DialectPostgres.
func (d PostgresDialect) Name() DialectName {
	return DialectPostgres
}

// QuoteField quotes an identifier for PostgreSQL.
func (d PostgresDialect) QuoteField(f string) string {
	return `"` + f + `"`
}

// BindVar returns PostgreSQL's placeholder at position i.
func (d PostgresDialect) BindVar(i int) string {
	// PostgreSQL uses $1, $2, etc for bind variables (1-indexed)
	return "$" + strconv.Itoa(i+1)
}

// CreateTableSuffix returns the PostgreSQL CREATE TABLE suffix.
func (d PostgresDialect) CreateTableSuffix() string {
	return ";"
}

// CreateIndexSuffix returns the PostgreSQL CREATE INDEX suffix.
func (d PostgresDialect) CreateIndexSuffix() string {
	return ""
}

// DropIndexSuffix returns the PostgreSQL DROP INDEX suffix.
func (d PostgresDialect) DropIndexSuffix() string {
	return ""
}

// TruncateClause returns the PostgreSQL TRUNCATE clause.
func (d PostgresDialect) TruncateClause() string {
	return "TRUNCATE TABLE"
}

// AutoIncrementClause returns the PostgreSQL auto-increment clause.
func (d PostgresDialect) AutoIncrementClause() string {
	return ""
}

// AutoIncrementBindValue returns the PostgreSQL auto-increment bind value.
func (d PostgresDialect) AutoIncrementBindValue() string {
	return "DEFAULT"
}

// LastInsertIdReturningSuffix returns the PostgreSQL returning suffix for last insert id.
func (d PostgresDialect) LastInsertIdReturningSuffix(table, col string) string {
	return " RETURNING " + d.QuoteField(col)
}

// AllTablesQuery returns the PostgreSQL query used to list all tables.
func (d PostgresDialect) AllTablesQuery() string {
	return "SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema()"
}

// CreateTableIfNotExistsSuffix returns PostgreSQL's IF NOT EXISTS fragment.
func (d PostgresDialect) CreateTableIfNotExistsSuffix() string {
	return "IF NOT EXISTS"
}

// HasConstraintsQuery returns the PostgreSQL query used to inspect column constraints.
func (d PostgresDialect) HasConstraintsQuery(table, column string) string {
	return `
		SELECT constraint_name
		FROM information_schema.constraint_column_usage
		WHERE table_schema = current_schema()
			AND table_name = $1
			AND column_name = $2`
}

// ValidateIdentifier applies PostgreSQL's identifier validation rules.
func (d PostgresDialect) ValidateIdentifier(identifier string) error {
	return validateDialectIdentifier(identifier, d.Name(), maxIdentifierLengthPostgreSQL)
}

// SupportsCapability reports whether PostgreSQL supports capability.
func (d PostgresDialect) SupportsCapability(capability DialectCapability) bool {
	switch canonicalCapabilityName(string(capability)) {
	case DialectCapabilityCTE,
		DialectCapabilityExcept,
		DialectCapabilityFullOuterJoin,
		DialectCapabilityIntersect,
		DialectCapabilitySelectForUpdate,
		DialectCapabilitySelectForShare,
		DialectCapabilitySelectForNoWait,
		DialectCapabilitySelectForSkipLocked:
		return true
	default:
		return false
	}
}

// BatchInsertStartID reports PostgreSQL's inability to derive a batch insert start id.
func (d PostgresDialect) BatchInsertStartID(lastID, rowsAffected int64) (int64, bool) {
	return 0, false
}

// EnsureIndex creates or updates an index definition for PostgreSQL.
func (d PostgresDialect) EnsureIndex(ctx context.Context, db SQLExecutor, table string, unique bool, idx string, fields []string) (string, error) {
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

// InspectIndexDefinition reads back an existing PostgreSQL index definition.
func (d PostgresDialect) InspectIndexDefinition(ctx context.Context, db SQLExecutor, table, idx string) (IndexDefinition, bool, error) {
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

// DDLColumnType renders a PostgreSQL column type for desc.
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
		if desc.Size > 0 {
			return fmt.Sprintf("VARCHAR(%d)", desc.Size)
		}

		return "TEXT"
	case DDLColumnKindTime:
		return "TIMESTAMP"
	default:
		return "TEXT"
	}
}

// DDLAutoIncrementPrimaryKey renders a PostgreSQL auto-increment primary key column.
func (d PostgresDialect) DDLAutoIncrementPrimaryKey(quotedColumn string, desc DDLColumnType) (string, error) {
	if desc.Kind != DDLColumnKindInt {
		return "", errors.New("auto-increment primary key requires an integer field")
	}

	return quotedColumn + " " + ddlSerialType(desc), nil
}

// DDLCreateIndex renders a PostgreSQL CREATE INDEX statement.
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

// DDLDropIndex renders a PostgreSQL DROP INDEX statement.
func (d PostgresDialect) DDLDropIndex(table, idx string) string {
	return fmt.Sprintf("DROP INDEX %s;", d.QuoteField(idx))
}

// DDLAlterColumnMode reports that PostgreSQL alters columns in place.
func (d PostgresDialect) DDLAlterColumnMode() DDLAlterColumnMode {
	return DDLAlterColumnDirect
}

// DDLAlterColumnStatements returns PostgreSQL ALTER COLUMN statements for the change.
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
