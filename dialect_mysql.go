package tsq

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// MySQLDialect is the MySQL dialect implementation.
type MySQLDialect struct{}

// Name returns DialectMySQL.
func (d MySQLDialect) Name() DialectName {
	return DialectMySQL
}

// QuoteField quotes an identifier for MySQL.
func (d MySQLDialect) QuoteField(f string) string {
	return "`" + f + "`"
}

// BindVar returns MySQL's placeholder at position i.
func (d MySQLDialect) BindVar(i int) string {
	return "?"
}

// CreateTableSuffix returns the MySQL CREATE TABLE suffix.
func (d MySQLDialect) CreateTableSuffix() string {
	return ";"
}

// CreateIndexSuffix returns the MySQL CREATE INDEX suffix.
func (d MySQLDialect) CreateIndexSuffix() string {
	return ""
}

// DropIndexSuffix returns the MySQL DROP INDEX suffix.
func (d MySQLDialect) DropIndexSuffix() string {
	return ""
}

// TruncateClause returns the MySQL TRUNCATE clause.
func (d MySQLDialect) TruncateClause() string {
	return "TRUNCATE TABLE"
}

// AutoIncrementClause returns the MySQL AUTO_INCREMENT clause.
func (d MySQLDialect) AutoIncrementClause() string {
	return "AUTO_INCREMENT"
}

// AutoIncrementBindValue returns the MySQL AUTO_INCREMENT bind value.
func (d MySQLDialect) AutoIncrementBindValue() string {
	return "0"
}

// LastInsertIdReturningSuffix returns MySQL's empty returning suffix.
func (d MySQLDialect) LastInsertIdReturningSuffix(table, col string) string {
	return ""
}

// AllTablesQuery returns the MySQL query used to list all tables.
func (d MySQLDialect) AllTablesQuery() string {
	return "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE()"
}

// CreateTableIfNotExistsSuffix returns MySQL's IF NOT EXISTS fragment.
func (d MySQLDialect) CreateTableIfNotExistsSuffix() string {
	return "IF NOT EXISTS"
}

// HasConstraintsQuery returns the MySQL query used to inspect column constraints.
func (d MySQLDialect) HasConstraintsQuery(table, column string) string {
	return ""
}

// ValidateIdentifier applies MySQL's identifier validation rules.
func (d MySQLDialect) ValidateIdentifier(identifier string) error {
	return validateDialectIdentifier(identifier, d.Name(), MaxIdentifierLengthMySQL)
}

// SupportsCapability reports whether MySQL supports capability.
func (d MySQLDialect) SupportsCapability(capability DialectCapability) bool {
	switch canonicalCapabilityName(string(capability)) {
	case DialectCapabilityCTE, DialectCapabilityExcept, DialectCapabilityFullOuterJoin, DialectCapabilityIntersect:
		return false
	case DialectCapabilitySelectForUpdate,
		DialectCapabilitySelectForShare,
		DialectCapabilitySelectForNoWait,
		DialectCapabilitySelectForSkipLocked:
		return true
	default:
		return false
	}
}

// BatchInsertStartID derives the first id assigned by a MySQL batch insert when possible.
func (d MySQLDialect) BatchInsertStartID(lastID, rowsAffected int64) (int64, bool) {
	if rowsAffected <= 0 {
		return 0, false
	}

	return lastID, true
}

// EnsureIndex creates or updates an index definition for MySQL.
func (d MySQLDialect) EnsureIndex(ctx context.Context, db SQLExecutor, table string, unique bool, idx string, fields []string) (string, error) {
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
		// Check if it's already created and matches
		definition, found, inspectErr := d.InspectIndexDefinition(ctx, db, table, idx)
		if inspectErr == nil && found && validateIndexDefinition(table, unique, idx, fields, definition) == nil {
			return "", nil
		}

		return "", err
	}

	return query, nil
}

// InspectIndexDefinition reads back an existing MySQL index definition.
func (d MySQLDialect) InspectIndexDefinition(ctx context.Context, db SQLExecutor, table, idx string) (IndexDefinition, bool, error) {
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
			return IndexDefinition{}, false, nil
		}

		return IndexDefinition{}, false, err
	}

	return IndexDefinition{
		Table:  existing.Table,
		Unique: existing.Unique == 1,
		Fields: parseColumnsCSV(existing.Columns.String),
	}, true, nil
}

// DDLColumnType renders a MySQL column type for desc.
func (d MySQLDialect) DDLColumnType(desc DDLColumnType) string {
	switch desc.Kind {
	case DDLColumnKindBool:
		return "BOOLEAN"
	case DDLColumnKindBytes:
		return "BLOB"
	case DDLColumnKindFloat:
		if desc.Bits <= 32 {
			return "FLOAT"
		}

		return "DOUBLE"
	case DDLColumnKindInt:
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

	case DDLColumnKindString:
		if desc.Size > 0 {
			return fmt.Sprintf("VARCHAR(%d)", desc.Size)
		}

		return "TEXT"
	case DDLColumnKindTime:
		return "DATETIME"
	default:
		return "TEXT"
	}
}

// DDLAutoIncrementPrimaryKey renders a MySQL auto-increment primary key column.
func (d MySQLDialect) DDLAutoIncrementPrimaryKey(quotedColumn string, desc DDLColumnType) (string, error) {
	if desc.Kind != DDLColumnKindInt {
		return "", errors.New("auto-increment primary key requires an integer field")
	}

	return strings.Join([]string{
		quotedColumn,
		d.DDLColumnType(desc),
		"PRIMARY KEY",
		d.AutoIncrementClause(),
	}, " "), nil
}

// DDLCreateIndex renders a MySQL CREATE INDEX statement.
func (d MySQLDialect) DDLCreateIndex(table, idx string, fields []string, unique bool) string {
	uniqueClause := ""
	if unique {
		uniqueClause = "UNIQUE "
	}

	return fmt.Sprintf(
		"ALTER TABLE %s ADD %sINDEX %s(%s)%s",
		d.QuoteField(table),
		uniqueClause,
		d.QuoteField(idx),
		strings.Join(fields, ", "),
		d.CreateIndexSuffix(),
	)
}

// DDLDropIndex renders the MySQL statements needed to drop idx.
func (d MySQLDialect) DDLDropIndex(table, idx string) string {
	return fmt.Sprintf(
		"DROP INDEX %s ON %s;",
		d.QuoteField(idx),
		d.QuoteField(table),
	)
}

// DDLAlterColumnMode reports that MySQL alters columns in place.
func (d MySQLDialect) DDLAlterColumnMode() DDLAlterColumnMode {
	return DDLAlterColumnDirect
}

// DDLAlterColumnStatements returns MySQL ALTER COLUMN statements for the change.
func (d MySQLDialect) DDLAlterColumnStatements(table string, before, after DDLColumnSpec) []string {
	return []string{fmt.Sprintf(
		"ALTER TABLE %s MODIFY COLUMN %s;",
		d.QuoteField(table),
		renderDDLColumnDefinition(d, after),
	)}
}
