package dialect

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

type MySQLDialect struct{}

func (d MySQLDialect) Name() Name {
	return MySQL
}

func (d MySQLDialect) QuoteField(f string) string {
	return "`" + f + "`"
}

func (d MySQLDialect) BindVar(i int) string {
	return "?"
}

func (d MySQLDialect) CreateTableSuffix() string {
	return ";"
}

func (d MySQLDialect) CreateIndexSuffix() string {
	return ""
}

func (d MySQLDialect) DropIndexSuffix() string {
	return ""
}

func (d MySQLDialect) TruncateClause() string {
	return "TRUNCATE TABLE"
}

func (d MySQLDialect) AutoIncrementClause() string {
	return "AUTO_INCREMENT"
}

func (d MySQLDialect) AutoIncrementBindValue() string {
	return "0"
}

func (d MySQLDialect) LastInsertIdReturningSuffix(table, col string) string {
	return ""
}

func (d MySQLDialect) AllTablesQuery() string {
	return "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE()"
}

func (d MySQLDialect) CreateTableIfNotExistsSuffix() string {
	return "IF NOT EXISTS"
}

func (d MySQLDialect) HasConstraintsQuery(table, column string) string {
	return ""
}

func (d MySQLDialect) ValidateIdentifier(identifier string) error {
	return validateDialectIdentifier(identifier, d.Name(), maxIdentifierLengthMySQL)
}

func (d MySQLDialect) SupportsCapability(capability Capability) bool {
	switch canonicalCapabilityName(string(capability)) {
	case CapabilityCTE, CapabilityExcept, CapabilityFullOuterJoin, CapabilityIntersect:
		return false
	case CapabilitySelectForUpdate,
		CapabilitySelectForShare,
		CapabilitySelectForNoWait,
		CapabilitySelectForSkipLocked:
		return true
	default:
		return false
	}
}

func (d MySQLDialect) BatchInsertStartID(lastID, rowsAffected int64) (int64, bool) {
	if rowsAffected <= 0 {
		return 0, false
	}

	return lastID, true
}

func (d MySQLDialect) EnsureIndex(ctx context.Context, db Executor, table string, unique bool, idx string, fields []string) (string, error) {
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
		definition, found, inspectErr := d.InspectIndexDefinition(ctx, db, table, idx)
		if inspectErr == nil && found && validateIndexDefinition(table, unique, idx, fields, definition) == nil {
			return "", nil
		}

		return "", err
	}

	return query, nil
}

func (d MySQLDialect) InspectIndexDefinition(ctx context.Context, db Executor, table, idx string) (IndexDefinition, bool, error) {
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

func (d MySQLDialect) DDLDropIndex(table, idx string) string {
	return fmt.Sprintf(
		"DROP INDEX %s ON %s;",
		d.QuoteField(idx),
		d.QuoteField(table),
	)
}

func (d MySQLDialect) DDLAlterColumnMode() DDLAlterColumnMode {
	return DDLAlterColumnDirect
}

func (d MySQLDialect) DDLAlterColumnStatements(table string, before, after DDLColumnSpec) []string {
	return []string{fmt.Sprintf(
		"ALTER TABLE %s MODIFY COLUMN %s;",
		d.QuoteField(table),
		renderDDLColumnDefinition(d, after),
	)}
}
