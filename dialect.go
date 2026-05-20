package tsq

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// SQLExecutor defines the shared query execution surface implemented by
// database/sql entry points such as *sql.DB and *sql.Tx.
// The standard library does not provide this exact interface, so tsq defines
// the minimal Context-based method set it needs.
type SQLExecutor interface {
	// QueryContext runs a query that returns rows.
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	// QueryRowContext runs a query that is expected to return at most one row.
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	// ExecContext runs a statement that does not return rows.
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// Dialect defines the operations tsq needs from a SQL dialect.
type Dialect interface {
	// Name returns the stable tsq dialect name.
	Name() DialectName
	// QuoteField returns the dialect-specific quoted identifier
	QuoteField(field string) string
	// BindVar returns the dialect-specific bind variable placeholder
	BindVar(i int) string
	// CreateTableSuffix returns dialect-specific create table suffix
	CreateTableSuffix() string
	// CreateIndexSuffix returns dialect-specific create index suffix
	CreateIndexSuffix() string
	// DropIndexSuffix returns dialect-specific drop index suffix
	DropIndexSuffix() string
	// TruncateClause returns the dialect-specific truncate clause
	TruncateClause() string
	// AutoIncrementClause returns the dialect-specific auto-increment clause
	AutoIncrementClause() string
	// AutoIncrementBindValue returns the dialect-specific auto-increment bind value
	AutoIncrementBindValue() string
	// LastInsertIdReturningSuffix returns the dialect-specific returning suffix for last insert id
	LastInsertIdReturningSuffix(table, col string) string
	// AllTablesQuery returns the dialect-specific query to list all tables
	AllTablesQuery() string
	// CreateTableIfNotExistsSuffix returns the dialect-specific create if not exists suffix
	CreateTableIfNotExistsSuffix() string
	// HasConstraintsQuery returns the dialect-specific query to check constraints
	HasConstraintsQuery(string, string) string
	// ValidateIdentifier validates a SQL identifier for this dialect.
	ValidateIdentifier(identifier string) error
	// SupportsCapability reports whether this dialect supports the named SQL capability.
	SupportsCapability(capability DialectCapability) bool
	// BatchInsertStartID returns the first ID assigned by a multi-row insert when it can be derived.
	BatchInsertStartID(lastID, rowsAffected int64) (int64, bool)
	// EnsureIndex creates an index for this dialect. Returns the SQL query executed.
	EnsureIndex(ctx context.Context, db SQLExecutor, table string, unique bool, idx string, fields []string) (string, error)
	// InspectIndexDefinition returns the current definition of an existing index.
	InspectIndexDefinition(ctx context.Context, db SQLExecutor, table, idx string) (IndexDefinition, bool, error)
	// DDLColumnType returns the SQL type for a column descriptor.
	DDLColumnType(desc DDLColumnType) string
	// DDLAutoIncrementPrimaryKey renders an auto-increment primary key column definition.
	DDLAutoIncrementPrimaryKey(quotedColumn string, desc DDLColumnType) (string, error)
	// DDLCreateIndex renders the dialect-specific index creation statement.
	DDLCreateIndex(table, idx string, fields []string, unique bool) string
	// DDLDropIndex renders the dialect-specific index drop statement.
	DDLDropIndex(table, idx string) string
	// DDLAlterColumnMode reports how this dialect applies column changes.
	DDLAlterColumnMode() DDLAlterColumnMode
	// DDLAlterColumnStatements renders direct ALTER COLUMN statements for this dialect.
	DDLAlterColumnStatements(table string, before, after DDLColumnSpec) []string
}

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
	// DialectCapabilitySelectForUpdate represents SELECT ... FOR UPDATE support.
	DialectCapabilitySelectForUpdate DialectCapability = "SELECT_FOR_UPDATE"
	// DialectCapabilitySelectForShare represents SELECT ... FOR SHARE support.
	DialectCapabilitySelectForShare DialectCapability = "SELECT_FOR_SHARE"
	// DialectCapabilitySelectForNoWait represents NOWAIT row-lock modifier support.
	DialectCapabilitySelectForNoWait DialectCapability = "SELECT_FOR_NOWAIT"
	// DialectCapabilitySelectForSkipLocked represents SKIP LOCKED row-lock modifier support.
	DialectCapabilitySelectForSkipLocked DialectCapability = "SELECT_FOR_SKIP_LOCKED"
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
	Kind     DDLColumnKind // Kind selects the abstract column family to render.
	Bits     int           // Bits records the requested integer or floating-point width.
	Unsigned bool          // Unsigned reports whether integer output should omit the sign bit.
	Nullable bool          // Nullable reports whether the column allows NULL values.
	Size     int           // Size records the requested length for sized text or binary types.
}

// DDLColumnSpec describes a full column definition for DDL rendering.
type DDLColumnSpec struct {
	Name          string        // Name is the logical column name before dialect quoting.
	Type          DDLColumnType // Type describes the SQL type shape for the column.
	PrimaryKey    bool          // PrimaryKey marks the column as part of the table primary key.
	AutoIncrement bool          // AutoIncrement requests dialect-specific generated key behavior.
	Default       string        // Default is appended as the raw SQL DEFAULT expression when non-empty.
}

// IndexDefinition is the normalized definition tsq reads back from an existing index.
type IndexDefinition struct {
	Table  string   // Table is the physical table that owns the index.
	Unique bool     // Unique reports whether the index enforces uniqueness.
	Fields []string // Fields preserves the indexed column order returned by the dialect.
}

// ErrUnsupportedCapability reports that a dialect cannot perform a requested capability.
type ErrUnsupportedCapability struct {
	operation DialectCapability
	dialect   DialectName
	reason    string
}

// NewErrUnsupportedCapability constructs an ErrUnsupportedCapability.
func NewErrUnsupportedCapability(operation DialectCapability, dialect DialectName, reason string) *ErrUnsupportedCapability {
	return &ErrUnsupportedCapability{
		operation: canonicalCapabilityName(string(operation)),
		dialect:   dialect,
		reason:    reason,
	}
}

// Error implements error.
func (e *ErrUnsupportedCapability) Error() string {
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

	return NewErrUnsupportedCapability(
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
	case "FOR UPDATE":
		return DialectCapabilitySelectForUpdate
	case "FOR SHARE":
		return DialectCapabilitySelectForShare
	case "NOWAIT":
		return DialectCapabilitySelectForNoWait
	case "SKIP LOCKED":
		return DialectCapabilitySelectForSkipLocked
	default:
		return DialectCapability(value)
	}
}

func displayCapabilityName(operation DialectCapability) string {
	switch canonicalCapabilityName(string(operation)) {
	case DialectCapabilityFullOuterJoin:
		return "FULL JOIN"
	case DialectCapabilitySelectForUpdate:
		return "FOR UPDATE"
	case DialectCapabilitySelectForShare:
		return "FOR SHARE"
	case DialectCapabilitySelectForNoWait:
		return "NOWAIT"
	case DialectCapabilitySelectForSkipLocked:
		return "SKIP LOCKED"
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
	case DialectCapabilitySelectForUpdate, DialectCapabilitySelectForShare:
		return "execute on a dialect that supports row-locking reads"
	case DialectCapabilitySelectForNoWait, DialectCapabilitySelectForSkipLocked:
		return "execute on a dialect that supports row-lock wait modifiers"
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
