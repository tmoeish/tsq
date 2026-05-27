package dialect

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

var builtInIdentifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

const (
	maxIdentifierLengthMySQL      = 64
	maxIdentifierLengthPostgreSQL = 63
	defaultDDLStringSize          = 255
)

// Executor defines the minimal execution surface dialects need from *sql.DB or *sql.Tx.
type Executor interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// Dialect defines the operations tsq needs from a SQL dialect.
type Dialect interface {
	Name() Name
	QuoteField(field string) string
	BindVar(i int) string
	CreateTableSuffix() string
	CreateIndexSuffix() string
	DropIndexSuffix() string
	TruncateClause() string
	AutoIncrementClause() string
	AutoIncrementBindValue() string
	LastInsertIdReturningSuffix(table, col string) string
	AllTablesQuery() string
	CreateTableIfNotExistsSuffix() string
	HasConstraintsQuery(string, string) string
	ValidateIdentifier(identifier string) error
	SupportsCapability(capability Capability) bool
	BatchInsertStartID(lastID, rowsAffected int64) (int64, bool)
	ListTables(ctx context.Context, db Executor) ([]string, error)
	InspectTableColumns(ctx context.Context, db Executor, table string) ([]DDLColumnSpec, bool, error)
	ListIndexes(ctx context.Context, db Executor, table string) ([]NamedIndexDefinition, error)
	EnsureIndex(ctx context.Context, db Executor, table string, unique bool, idx string, fields []string) (string, error)
	InspectIndexDefinition(ctx context.Context, db Executor, table, idx string) (IndexDefinition, bool, error)
	DDLColumnType(desc DDLColumnType) string
	DDLAutoIncrementPrimaryKey(quotedColumn string, desc DDLColumnType) (string, error)
	DDLCreateIndex(table, idx string, fields []string, unique bool) string
	DDLDropIndex(table, idx string) string
	DDLAlterColumnMode() DDLAlterColumnMode
	DDLAlterColumnStatements(table string, before, after DDLColumnSpec) []string
}

type Name string

const (
	MySQL    Name = "mysql"
	Postgres Name = "postgres"
	SQLite   Name = "sqlite"
	Unknown  Name = "unknown"
)

type Capability string

const (
	CapabilityCTE                 Capability = "CTE"
	CapabilityExcept              Capability = "EXCEPT"
	CapabilityFullOuterJoin       Capability = "FULL_OUTER_JOIN"
	CapabilityIntersect           Capability = "INTERSECT"
	CapabilitySelectForUpdate     Capability = "SELECT_FOR_UPDATE"
	CapabilitySelectForShare      Capability = "SELECT_FOR_SHARE"
	CapabilitySelectForNoWait     Capability = "SELECT_FOR_NOWAIT"
	CapabilitySelectForSkipLocked Capability = "SELECT_FOR_SKIP_LOCKED"
)

type DDLAlterColumnMode string

const (
	DDLAlterColumnDirect  DDLAlterColumnMode = "direct"
	DDLAlterColumnRebuild DDLAlterColumnMode = "rebuild"
)

type DDLColumnKind string

const (
	DDLColumnKindBool   DDLColumnKind = "bool"
	DDLColumnKindBytes  DDLColumnKind = "bytes"
	DDLColumnKindFloat  DDLColumnKind = "float"
	DDLColumnKindInt    DDLColumnKind = "int"
	DDLColumnKindString DDLColumnKind = "string"
	DDLColumnKindTime   DDLColumnKind = "time"
)

type DDLColumnType struct {
	Kind     DDLColumnKind
	Bits     int
	Unsigned bool
	Nullable bool
	Size     int
}

type DDLColumnSpec struct {
	Name          string
	Type          DDLColumnType
	PrimaryKey    bool
	AutoIncrement bool
	Default       string
}

type IndexDefinition struct {
	Table  string
	Unique bool
	Fields []string
}

type NamedIndexDefinition struct {
	Name       string
	Table      string
	Unique     bool
	Fields     []string
	PrimaryKey bool
	Constraint bool
}

// ErrUnsupportedCapability reports that a dialect cannot perform a requested capability.
type ErrUnsupportedCapability struct {
	operation Capability
	dialect   Name
	reason    string
}

func newErrUnsupportedCapability(operation Capability, dialect Name, reason string) *ErrUnsupportedCapability {
	return &ErrUnsupportedCapability{
		operation: canonicalCapabilityName(string(operation)),
		dialect:   dialect,
		reason:    reason,
	}
}

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

// ValidateCapability reports whether dialect supports capability.
func ValidateCapability(dialect Dialect, capability Capability) error {
	if dialect == nil || dialect.SupportsCapability(capability) {
		return nil
	}

	return newErrUnsupportedCapability(
		capability,
		dialect.Name(),
		unsupportedCapabilityHint(capability, dialect.Name()),
	)
}

func ValidateIdentifierLength(identifier string, dialect Dialect) error {
	if identifier == "" {
		return errors.New("identifier cannot be empty")
	}

	if dialect == nil {
		return nil
	}

	return dialect.ValidateIdentifier(identifier)
}

func validateDialectIdentifier(identifier string, dialect Name, maxLen int) error {
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

func normalizeDDLDefault(value sql.NullString) string {
	if !value.Valid {
		return ""
	}

	return strings.TrimSpace(value.String)
}

func withDDLNullable(desc DDLColumnType, nullable bool) DDLColumnType {
	desc.Nullable = nullable
	return desc
}

func canonicalCapabilityName(operation string) Capability {
	value := strings.ToUpper(strings.TrimSpace(operation))

	switch value {
	case "FULL JOIN", "FULL OUTER JOIN":
		return CapabilityFullOuterJoin
	case "CTE":
		return CapabilityCTE
	case "INTERSECT":
		return CapabilityIntersect
	case "EXCEPT", "MINUS":
		return CapabilityExcept
	case "FOR UPDATE":
		return CapabilitySelectForUpdate
	case "FOR SHARE":
		return CapabilitySelectForShare
	case "NOWAIT":
		return CapabilitySelectForNoWait
	case "SKIP LOCKED":
		return CapabilitySelectForSkipLocked
	default:
		return Capability(value)
	}
}

func displayCapabilityName(operation Capability) string {
	switch canonicalCapabilityName(string(operation)) {
	case CapabilityFullOuterJoin:
		return "FULL JOIN"
	case CapabilitySelectForUpdate:
		return "FOR UPDATE"
	case CapabilitySelectForShare:
		return "FOR SHARE"
	case CapabilitySelectForNoWait:
		return "NOWAIT"
	case CapabilitySelectForSkipLocked:
		return "SKIP LOCKED"
	default:
		return string(canonicalCapabilityName(string(operation)))
	}
}

func displayDialectName(dialect Name) string {
	if dialect == "" {
		return string(Unknown)
	}

	return string(dialect)
}

func unsupportedCapabilityHint(operation Capability, dialect Name) string {
	switch canonicalCapabilityName(string(operation)) {
	case CapabilityCTE:
		return "use a subquery, split the query, or execute on sqlite/postgres"
	case CapabilityFullOuterJoin:
		return "use LEFT/RIGHT JOIN with UNION, or execute on postgres"
	case CapabilityIntersect:
		return "use IN/EXISTS filtering, or execute on sqlite/postgres"
	case CapabilityExcept:
		return "use NOT EXISTS filtering, or execute on sqlite/postgres"
	case CapabilitySelectForUpdate, CapabilitySelectForShare:
		return "execute on a dialect that supports row-locking reads"
	case CapabilitySelectForNoWait, CapabilitySelectForSkipLocked:
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

func validateBuiltInIdentifier(name string) error {
	if !builtInIdentifierPattern.MatchString(name) {
		return fmt.Errorf("invalid SQL identifier: %s", name)
	}

	return nil
}

func validateIndexDefinition(
	table string,
	unique bool,
	idx string,
	fields []string,
	existing IndexDefinition,
) error {
	if existing.Table != table {
		return fmt.Errorf(
			"index %s already exists on table %s, expected table %s",
			idx,
			existing.Table,
			table,
		)
	}

	if existing.Unique != unique || !sameOrderedFields(existing.Fields, fields) {
		return fmt.Errorf(
			"index %s on table %s has definition unique=%t fields=%v, expected unique=%t fields=%v",
			idx,
			table,
			existing.Unique,
			existing.Fields,
			unique,
			fields,
		)
	}

	return nil
}

func sameOrderedFields(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}

	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}

	return true
}

func parseColumnsCSV(csv string) []string {
	if csv == "" {
		return nil
	}

	return strings.Split(csv, ",")
}

func canonicalQuoteIdentifier(name string) string {
	return `"` + name + `"`
}

func quoteDialectIdentifier(dialect Dialect, name string) (string, error) {
	if err := validateBuiltInIdentifier(name); err != nil {
		return "", err
	}

	if dialect == nil {
		return canonicalQuoteIdentifier(name), nil
	}

	if err := dialect.ValidateIdentifier(name); err != nil {
		return "", err
	}

	return dialect.QuoteField(name), nil
}

func quoteDialectIdentifiers(dialect Dialect, names []string) ([]string, error) {
	quoted := make([]string, len(names))

	for i, name := range names {
		value, err := quoteDialectIdentifier(dialect, name)
		if err != nil {
			return nil, err
		}

		quoted[i] = value
	}

	return quoted, nil
}
