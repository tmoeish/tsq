package tsq

import (
	"errors"
	"fmt"
	"strings"
)

// DialectName is the stable name used by tsq for a SQL dialect.
type DialectName string

const (
	// DialectMySQL identifies MySQL-compatible SQL rendering.
	DialectMySQL DialectName = "mysql"
	// DialectPostgres identifies PostgreSQL-compatible SQL rendering.
	DialectPostgres DialectName = "postgres"
	// DialectSQLite identifies SQLite-compatible SQL rendering.
	DialectSQLite DialectName = "sqlite"
	// DialectUnknown is used when no dialect can be determined.
	DialectUnknown DialectName = "unknown"
)

// DialectCapability identifies an optional SQL feature that may vary by dialect.
type DialectCapability string

const (
	// DialectCapabilityCTE represents WITH / common table expression support.
	DialectCapabilityCTE DialectCapability = "CTE"
	// DialectCapabilityExcept represents EXCEPT support.
	DialectCapabilityExcept DialectCapability = "EXCEPT"
	// DialectCapabilityFullOuterJoin represents FULL OUTER JOIN support.
	DialectCapabilityFullOuterJoin DialectCapability = "FULL_OUTER_JOIN"
	// DialectCapabilityIntersect represents INTERSECT support.
	DialectCapabilityIntersect DialectCapability = "INTERSECT"
)

// DDLAlterColumnMode describes how a dialect applies ALTER COLUMN changes.
type DDLAlterColumnMode string

const (
	// DDLAlterColumnDirect means the dialect can alter a column in place.
	DDLAlterColumnDirect DDLAlterColumnMode = "direct"
	// DDLAlterColumnRebuild means the dialect must rebuild a table to alter a column.
	DDLAlterColumnRebuild DDLAlterColumnMode = "rebuild"
)

// DDLColumnKind is the abstract tsq column family used for DDL rendering.
type DDLColumnKind string

const (
	// DDLColumnKindBool is a boolean-like column.
	DDLColumnKindBool DDLColumnKind = "bool"
	// DDLColumnKindBytes is a binary/blob column.
	DDLColumnKindBytes DDLColumnKind = "bytes"
	// DDLColumnKindFloat is a floating-point column.
	DDLColumnKindFloat DDLColumnKind = "float"
	// DDLColumnKindInt is an integer column.
	DDLColumnKindInt DDLColumnKind = "int"
	// DDLColumnKindString is a text-like column.
	DDLColumnKindString DDLColumnKind = "string"
	// DDLColumnKindTime is a timestamp/date-time column.
	DDLColumnKindTime DDLColumnKind = "time"
)

// DDLColumnType describes the SQL type shape for a generated column.
type DDLColumnType struct {
	Kind     DDLColumnKind
	Bits     int
	Unsigned bool
	Nullable bool
	Size     int
}

// DDLColumnSpec describes a full column definition for DDL rendering.
type DDLColumnSpec struct {
	Name          string
	Type          DDLColumnType
	PrimaryKey    bool
	AutoIncrement bool
	Default       string
}

// IndexDefinition is the normalized definition tsq reads back from an existing index.
type IndexDefinition struct {
	Table  string
	Unique bool
	Fields []string
}

// ErrUnsupportedOperation reports that a dialect cannot perform a requested capability.
type ErrUnsupportedOperation struct {
	operation DialectCapability
	dialect   DialectName
	reason    string
}

// NewErrUnsupportedOperation constructs an ErrUnsupportedOperation.
func NewErrUnsupportedOperation(operation DialectCapability, dialect DialectName, reason string) *ErrUnsupportedOperation {
	return &ErrUnsupportedOperation{
		operation: canonicalCapabilityName(string(operation)),
		dialect:   dialect,
		reason:    reason,
	}
}

// Error implements error.
func (e *ErrUnsupportedOperation) Error() string {
	if e.reason != "" {
		return fmt.Sprintf(
			"operation %s is not supported by %s dialect; %s",
			displayCapabilityName(e.operation),
			displayDialectName(e.dialect),
			e.reason,
		)
	}

	return fmt.Sprintf(
		"operation %s is not supported by %s dialect",
		displayCapabilityName(e.operation),
		displayDialectName(e.dialect),
	)
}

// ValidateOperationForDialect reports whether d supports operation.
func ValidateOperationForDialect(operation string, d Dialect) error {
	if d == nil {
		return nil
	}

	return validateDialectCapability(d, canonicalCapabilityName(operation))
}

// ValidateIdentifierForDialect validates identifier syntax and dialect-specific limits.
func ValidateIdentifierForDialect(identifier string, dialect Dialect) error {
	if identifier == "" {
		return errors.New("identifier cannot be empty")
	}

	if !builtInIdentifierPattern.MatchString(identifier) {
		return fmt.Errorf("invalid SQL identifier: %s (must match pattern [A-Za-z_][A-Za-z0-9_]*)", identifier)
	}

	return ValidateIdentifierLength(identifier, dialect)
}

// ValidateIdentifierLength validates only the dialect-specific identifier length rules.
func ValidateIdentifierLength(identifier string, dialect Dialect) error {
	if identifier == "" {
		return errors.New("identifier cannot be empty")
	}

	if dialect == nil {
		return nil
	}

	return dialect.ValidateIdentifier(identifier)
}

func validateDialectCapability(dialect Dialect, capability DialectCapability) error {
	if dialect == nil || dialect.SupportsCapability(capability) {
		return nil
	}

	return NewErrUnsupportedOperation(
		capability,
		dialect.Name(),
		unsupportedCapabilityHint(capability, dialect.Name()),
	)
}

func validateDialectIdentifier(identifier string, dialect DialectName, maxLen int) error {
	if identifier == "" {
		return errors.New("identifier cannot be empty")
	}

	if !builtInIdentifierPattern.MatchString(identifier) {
		return fmt.Errorf("invalid SQL identifier: %s (must match pattern [A-Za-z_][A-Za-z0-9_]*)", identifier)
	}

	if maxLen > 0 && len(identifier) > maxLen {
		return fmt.Errorf(
			"identifier %q exceeds %s maximum length of %d characters (got %d)",
			identifier,
			displayDialectName(dialect),
			maxLen,
			len(identifier),
		)
	}

	return nil
}

func canonicalCapabilityName(operation string) DialectCapability {
	value := strings.ToUpper(strings.TrimSpace(operation))

	switch value {
	case "FULL JOIN", "FULL OUTER JOIN":
		return DialectCapabilityFullOuterJoin
	case "CTE":
		return DialectCapabilityCTE
	case "INTERSECT":
		return DialectCapabilityIntersect
	case "EXCEPT", "MINUS":
		return DialectCapabilityExcept
	default:
		return DialectCapability(value)
	}
}

func displayCapabilityName(operation DialectCapability) string {
	switch canonicalCapabilityName(string(operation)) {
	case DialectCapabilityFullOuterJoin:
		return "FULL JOIN"
	default:
		return string(canonicalCapabilityName(string(operation)))
	}
}

func displayDialectName(dialect DialectName) string {
	if dialect == "" {
		return string(DialectUnknown)
	}

	return string(dialect)
}

func unsupportedCapabilityHint(operation DialectCapability, dialect DialectName) string {
	switch canonicalCapabilityName(string(operation)) {
	case DialectCapabilityCTE:
		return "use a subquery, split the query, or execute on sqlite/postgres"
	case DialectCapabilityFullOuterJoin:
		return "use LEFT/RIGHT JOIN with UNION, or execute on postgres"
	case DialectCapabilityIntersect:
		return "use IN/EXISTS filtering, or execute on sqlite/postgres"
	case DialectCapabilityExcept:
		return "use NOT EXISTS filtering, or execute on sqlite/postgres"
	default:
		return "use a simpler query shape or a dialect that supports this capability"
	}
}

func ddlSerialType(desc DDLColumnType) string {
	switch {
	case desc.Bits <= 16:
		return "SMALLSERIAL PRIMARY KEY"
	case desc.Bits <= 32:
		return "SERIAL PRIMARY KEY"
	default:
		return "BIGSERIAL PRIMARY KEY"
	}
}

// Name returns DialectSQLite.
func (d SQLiteDialect) Name() DialectName {
	return DialectSQLite
}

// ValidateIdentifier applies SQLite's identifier validation rules.
func (d SQLiteDialect) ValidateIdentifier(identifier string) error {
	return validateDialectIdentifier(identifier, d.Name(), 0)
}

// SupportsCapability reports whether SQLite supports capability.
func (d SQLiteDialect) SupportsCapability(capability DialectCapability) bool {
	switch canonicalCapabilityName(string(capability)) {
	case DialectCapabilityCTE, DialectCapabilityExcept, DialectCapabilityIntersect:
		return true
	case DialectCapabilityFullOuterJoin:
		return false
	default:
		return false
	}
}

// BatchInsertStartID derives the first id assigned by a SQLite batch insert when possible.
func (d SQLiteDialect) BatchInsertStartID(lastID, rowsAffected int64) (int64, bool) {
	if rowsAffected <= 0 {
		return 0, false
	}

	return lastID - rowsAffected + 1, true
}

// EnsureIndex creates or updates an index definition for SQLite.
func (d SQLiteDialect) EnsureIndex(db *Engine, table string, unique bool, idx string, fields []string) error {
	return ensureSQLiteIndex(db, table, unique, idx, fields)
}

// InspectIndexDefinition reads back an existing SQLite index definition.
func (d SQLiteDialect) InspectIndexDefinition(db *Engine, table, idx string) (IndexDefinition, bool, error) {
	return inspectSQLiteIndexDefinition(db, idx)
}

// DDLColumnType renders a SQLite column type for desc.
func (d SQLiteDialect) DDLColumnType(desc DDLColumnType) string {
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
		return "TEXT"
	case DDLColumnKindTime:
		return "TIMESTAMP"
	default:
		return "TEXT"
	}
}

// DDLAutoIncrementPrimaryKey renders a SQLite auto-increment primary key column.
func (d SQLiteDialect) DDLAutoIncrementPrimaryKey(quotedColumn string, desc DDLColumnType) (string, error) {
	if desc.Kind != DDLColumnKindInt {
		return "", errors.New("auto-increment primary key requires an integer field")
	}

	return quotedColumn + " INTEGER PRIMARY KEY " + d.AutoIncrementClause(), nil
}

// DDLCreateIndex renders a SQLite CREATE INDEX statement.
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

// DDLDropIndex renders a SQLite DROP INDEX statement.
func (d SQLiteDialect) DDLDropIndex(table, idx string) string {
	return fmt.Sprintf("DROP INDEX %s;", d.QuoteField(idx))
}

// DDLAlterColumnMode reports that SQLite applies column changes by table rebuild.
func (d SQLiteDialect) DDLAlterColumnMode() DDLAlterColumnMode {
	return DDLAlterColumnRebuild
}

// DDLAlterColumnStatements returns SQLite's direct alter-column statements, if any.
func (d SQLiteDialect) DDLAlterColumnStatements(table string, before, after DDLColumnSpec) []string {
	return nil
}

// Name returns DialectMySQL.
func (d MySQLDialect) Name() DialectName {
	return DialectMySQL
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
func (d MySQLDialect) EnsureIndex(db *Engine, table string, unique bool, idx string, fields []string) error {
	return ensureMySQLIndex(db, table, unique, idx, fields)
}

// InspectIndexDefinition reads back an existing MySQL index definition.
func (d MySQLDialect) InspectIndexDefinition(db *Engine, table, idx string) (IndexDefinition, bool, error) {
	return inspectMySQLIndexDefinition(db, table, idx)
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

// Name returns DialectPostgres.
func (d PostgresDialect) Name() DialectName {
	return DialectPostgres
}

// ValidateIdentifier applies PostgreSQL's identifier validation rules.
func (d PostgresDialect) ValidateIdentifier(identifier string) error {
	return validateDialectIdentifier(identifier, d.Name(), MaxIdentifierLengthPostgreSQL)
}

// SupportsCapability reports whether PostgreSQL supports capability.
func (d PostgresDialect) SupportsCapability(capability DialectCapability) bool {
	switch canonicalCapabilityName(string(capability)) {
	case DialectCapabilityCTE, DialectCapabilityExcept, DialectCapabilityFullOuterJoin, DialectCapabilityIntersect:
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
func (d PostgresDialect) EnsureIndex(db *Engine, table string, unique bool, idx string, fields []string) error {
	return ensurePostgresIndex(db, table, unique, idx, fields)
}

// InspectIndexDefinition reads back an existing PostgreSQL index definition.
func (d PostgresDialect) InspectIndexDefinition(db *Engine, table, idx string) (IndexDefinition, bool, error) {
	return inspectPostgresIndexDefinition(db, idx)
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

func renderDDLColumnDefinition(dialect Dialect, column DDLColumnSpec) string {
	quotedColumn := dialect.QuoteField(column.Name)
	if column.PrimaryKey && column.AutoIncrement {
		definition, err := dialect.DDLAutoIncrementPrimaryKey(quotedColumn, column.Type)
		if err == nil {
			return definition
		}
	}

	parts := []string{quotedColumn, dialect.DDLColumnType(column.Type)}
	if column.PrimaryKey {
		parts = append(parts, "PRIMARY KEY")
	} else if !column.Type.Nullable {
		parts = append(parts, "NOT NULL")
	}

	if column.Default != "" {
		parts = append(parts, "DEFAULT "+column.Default)
	}

	return strings.Join(parts, " ")
}
