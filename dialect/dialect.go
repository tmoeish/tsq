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
)

// Executor defines the minimal execution surface dialects need from *sql.DB or *sql.Tx.
type Executor interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// Dialect defines the operations tsq needs from a SQL dialect.
type Dialect interface {
	Name() DialectName
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
	SupportsCapability(capability DialectCapability) bool
	BatchInsertStartID(lastID, rowsAffected int64) (int64, bool)
	EnsureIndex(ctx context.Context, db Executor, table string, unique bool, idx string, fields []string) (string, error)
	InspectIndexDefinition(ctx context.Context, db Executor, table, idx string) (IndexDefinition, bool, error)
	DDLColumnType(desc DDLColumnType) string
	DDLAutoIncrementPrimaryKey(quotedColumn string, desc DDLColumnType) (string, error)
	DDLCreateIndex(table, idx string, fields []string, unique bool) string
	DDLDropIndex(table, idx string) string
	DDLAlterColumnMode() DDLAlterColumnMode
	DDLAlterColumnStatements(table string, before, after DDLColumnSpec) []string
}

type DialectName string

const (
	DialectMySQL    DialectName = "mysql"
	DialectPostgres DialectName = "postgres"
	DialectSQLite   DialectName = "sqlite"
	DialectUnknown  DialectName = "unknown"
)

type DialectCapability string

const (
	DialectCapabilityCTE                 DialectCapability = "CTE"
	DialectCapabilityExcept              DialectCapability = "EXCEPT"
	DialectCapabilityFullOuterJoin       DialectCapability = "FULL_OUTER_JOIN"
	DialectCapabilityIntersect           DialectCapability = "INTERSECT"
	DialectCapabilitySelectForUpdate     DialectCapability = "SELECT_FOR_UPDATE"
	DialectCapabilitySelectForShare      DialectCapability = "SELECT_FOR_SHARE"
	DialectCapabilitySelectForNoWait     DialectCapability = "SELECT_FOR_NOWAIT"
	DialectCapabilitySelectForSkipLocked DialectCapability = "SELECT_FOR_SKIP_LOCKED"
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

// ErrUnsupportedCapability reports that a dialect cannot perform a requested capability.
type ErrUnsupportedCapability struct {
	operation DialectCapability
	dialect   DialectName
	reason    string
}

func newErrUnsupportedCapability(operation DialectCapability, dialect DialectName, reason string) *ErrUnsupportedCapability {
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
func ValidateCapability(dialect Dialect, capability DialectCapability) error {
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
