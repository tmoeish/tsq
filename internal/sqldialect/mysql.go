package sqldialect

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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

func (d MySQLDialect) ValidateIdentifier(identifier string) error {
	return validateDialectIdentifier(identifier, d.Name(), maxIdentifierLengthMySQL)
}

func (d MySQLDialect) SupportsCapability(capability Capability) bool {
	return tsqdialect.Supports(d.Name(), capability)
}

func (d MySQLDialect) BatchInsertStartID(lastID, rowsAffected int64) (int64, bool) {
	if rowsAffected <= 0 {
		return 0, false
	}

	return lastID, true
}

func (d MySQLDialect) InspectColumns(ctx context.Context, db Executor, table string) ([]Column, bool, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT
			column_name,
			data_type,
			column_type,
			is_nullable,
			column_default,
			column_key,
			extra,
			character_maximum_length
		FROM information_schema.columns
		WHERE table_schema = DATABASE() AND table_name = ?
		ORDER BY ordinal_position`,
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
		Type    string
		Null    string
		Default sql.NullString
		Key     string
		Extra   string
		Size    sql.NullInt64
	}

	columns := make([]Column, 0)

	for rows.Next() {
		var item row
		if err := rows.Scan(&item.Name, &item.Data, &item.Type, &item.Null, &item.Default, &item.Key, &item.Extra, &item.Size); err != nil {
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
			Default:       normalizeDDLDefault(item.Default),
			NativeType:    strings.TrimSpace(item.Type),
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

	return indexes, nil
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
		if inspectErr == nil && found && validateIndex(table, unique, idx, fields, definition) == nil {
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
	case "int", "integer", "mediumint":
		return ColumnType{Kind: KindInt, Bits: 32, Unsigned: unsigned}, nil
	case "bigint":
		return ColumnType{Kind: KindInt, Bits: 64, Unsigned: unsigned}, nil
	case "float":
		return ColumnType{Kind: KindFloat, Bits: 32}, nil
	case "double", "double precision", "decimal", "numeric":
		return ColumnType{Kind: KindFloat, Bits: 64}, nil
	case "blob", "tinyblob", "mediumblob", "longblob":
		return ColumnType{Kind: KindBytes}, nil
	case "varchar", "char":
		result := ColumnType{Kind: KindString}
		if size.Valid && size.Int64 > 0 {
			result.Size = int(size.Int64)
		}

		return result, nil
	case "text", "tinytext":
		return ColumnType{Kind: KindString, Size: mysqlMaxVarcharChars + 1}, nil
	case "mediumtext":
		return ColumnType{Kind: KindString, Size: mysqlMaxVarcharChars + 1}, nil
	case "longtext":
		return ColumnType{Kind: KindString, Size: mysqlMaxMediumTextChars + 1}, nil
	case "datetime", "timestamp", "date":
		return ColumnType{Kind: KindTime}, nil
	default:
		if rawColumnType == "" {
			rawColumnType = strings.TrimSpace(dataType)
		}

		return ColumnType{RawType: rawColumnType}, nil
	}
}

func (d MySQLDialect) ColumnTypeSQL(desc ColumnType) string {
	if desc.RawType != "" {
		return desc.RawType
	}

	switch desc.Kind {
	case KindBool:
		return "BOOLEAN"
	case KindBytes:
		return "BLOB"
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
		return "DATETIME"
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
	return []string{fmt.Sprintf(
		"ALTER TABLE %s MODIFY COLUMN %s;",
		d.QuoteIdent(table),
		d.renderModifyColumnDefinition(after),
	)}
}

// renderModifyColumnDefinition renders a column definition for MODIFY COLUMN.
// It must not repeat PRIMARY KEY: MySQL rejects MODIFY COLUMN ... PRIMARY KEY
// on a column that already is the primary key (error 1068 "Multiple primary
// key defined"). AUTO_INCREMENT, however, must be restated or it gets dropped.
func (d MySQLDialect) renderModifyColumnDefinition(column ColumnSpec) string {
	parts := []string{d.QuoteIdent(column.Name), d.ColumnTypeSQL(column.Type)}

	if column.PrimaryKey || !column.Type.Nullable {
		parts = append(parts, "NOT NULL")
	}

	if column.AutoIncrement {
		parts = append(parts, "AUTO_INCREMENT")
	} else if column.Default != "" {
		parts = append(parts, "DEFAULT "+column.Default)
	}

	return strings.Join(parts, " ")
}
