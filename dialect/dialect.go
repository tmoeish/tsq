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

// Dialect is everything TSQ needs to know about one SQL engine: how to spell
// identifiers and placeholders, which optional features exist, how to inspect a live
// schema, and how to render the DDL that changes it.
type Dialect interface {
	// Name identifies the dialect.
	Name() Name
	// QuoteIdent quotes an identifier the way the dialect expects.
	QuoteIdent(ident string) string
	// Placeholder renders the bind placeholder for the argument at zero-based index i.
	Placeholder(i int) string
	// ReturningClause renders the clause that returns the generated key column after an
	// INSERT, or "" when the key comes from LastInsertId instead.
	ReturningClause(col string) string
	// ValidateIdentifier reports whether identifier is valid and within the length limit.
	ValidateIdentifier(identifier string) error
	// SupportsCapability reports whether the dialect supports capability.
	SupportsCapability(capability Capability) bool
	// BatchInsertStartID derives the first generated key of a multi-row INSERT from
	// LastInsertId, reporting false when the engine does not make that possible.
	BatchInsertStartID(lastID, rowsAffected int64) (int64, bool)
	// InspectColumns reports the live columns of table, and false when it does not exist.
	InspectColumns(ctx context.Context, db Executor, table string) ([]ColumnSpec, bool, error)
	// ListIndexes reports the live indexes of table.
	ListIndexes(ctx context.Context, db Executor, table string) ([]Index, error)
	// EnsureIndex creates an index and returns the statement it ran. An existing index
	// with the same definition is not an error, and the returned statement is then "".
	EnsureIndex(ctx context.Context, db Executor, table, idx string, fields []string, unique bool) (string, error)
	// InspectIndex reports one live index, and false when it does not exist.
	InspectIndex(ctx context.Context, db Executor, table, idx string) (Index, bool, error)
	// ColumnTypeSQL renders a column type.
	ColumnTypeSQL(t ColumnType) string
	// AutoIncrementColumnSQL renders the definition of an auto-increment primary key.
	AutoIncrementColumnSQL(quotedColumn string, t ColumnType) (string, error)
	// CreateIndexSQL renders a CREATE INDEX statement over already-quoted fields.
	CreateIndexSQL(table, idx string, quotedFields []string, unique bool) string
	// DropIndexSQL renders a DROP INDEX statement.
	DropIndexSQL(table, idx string) string
	// AlterMode says whether a column type change is an ALTER or a table rebuild.
	AlterMode() AlterMode
	// AlterColumnSQL renders the statements that turn column before into after.
	AlterColumnSQL(table string, before, after ColumnSpec) []string
}

// Name identifies a dialect.
type Name string

const (
	MySQL    Name = "mysql"
	Postgres Name = "postgres"
	SQLite   Name = "sqlite"
	Unknown  Name = "unknown"
)

// Capability is an optional SQL feature a dialect may or may not support.
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

// AllCapabilities returns every capability this package defines, in declaration order.
//
// Every dialect must take an explicit position on each of them: a capability missing
// from a dialect's table would otherwise read as "unsupported" without anyone having
// decided that. TestDialectsCoverAllCapabilities enumerates this list against each
// dialect and fails when one is unaccounted for, so adding a constant here without
// updating mysql.go, postgres.go and sqlite.go breaks the build's tests rather than
// silently changing behavior.
func AllCapabilities() []Capability {
	return []Capability{
		CapabilityCTE,
		CapabilityExcept,
		CapabilityFullOuterJoin,
		CapabilityIntersect,
		CapabilitySelectForUpdate,
		CapabilitySelectForShare,
		CapabilitySelectForNoWait,
		CapabilitySelectForSkipLocked,
	}
}

// capabilitySupport looks a capability up in a dialect's declaration table. A
// capability absent from the table is reported as unsupported, which is what the
// exhaustiveness test exists to prevent from ever happening silently.
func capabilitySupport(table map[Capability]bool, capability Capability) bool {
	supported, declared := table[canonicalCapabilityName(string(capability))]

	return declared && supported
}

// maxBindParams is each dialect's ceiling on the number of bound parameters in one
// statement. Like the capability tables, every dialect states its own value instead of
// inheriting a default, so a dialect added later cannot silently pick up a limit that
// is wrong for it.
//
//   - MySQL and PostgreSQL both encode the parameter count as an unsigned 16-bit
//     integer in their wire protocols, capping a statement at 65535.
//   - SQLite's SQLITE_MAX_VARIABLE_NUMBER has defaulted to 32766 since 3.32 (2020),
//     and the bundled modernc.org/sqlite reports exactly that. Stating 65535 for every
//     dialect, as tsq used to, made wide-table batches fail on SQLite alone.
//
// The values follow the same version baselines as the capability tables: an engine
// older than the baseline reports a database error rather than an UnsupportedCapabilityError.
var maxBindParams = map[Name]int{
	MySQL:    65535,
	Postgres: 65535,
	SQLite:   32766,
}

// minBindParamsLimit is the tightest ceiling among the declared dialects. It is
// derived from the table rather than written down twice, so adding a dialect with a
// smaller limit also tightens the unknown-dialect fallback.
var minBindParamsLimit = func() int {
	smallest := 0
	for _, limit := range maxBindParams {
		if smallest == 0 || limit < smallest {
			smallest = limit
		}
	}

	return smallest
}()

// MaxBindParams reports how many bound parameters dialect accepts in a single
// statement. Batch helpers use it to size a chunk by placeholders rather than by rows.
//
// A nil or unrecognized dialect answers the tightest limit among the supported
// dialects: chunking smaller than necessary only costs round trips, while chunking
// larger than the database allows is an outright failure at execution time.
func MaxBindParams(dialect Dialect) int {
	if dialect == nil {
		return minBindParamsLimit
	}

	if limit, ok := maxBindParams[dialect.Name()]; ok {
		return limit
	}

	return minBindParamsLimit
}

// AlterMode says how a dialect changes an existing column's type: in place with
// ALTER TABLE, or by rebuilding the table (SQLite).
type AlterMode string

const (
	AlterInPlace AlterMode = "direct"
	AlterRebuild AlterMode = "rebuild"
)

// ColumnKind is the portable family of a column type; each dialect maps it, with
// ColumnType's size and sign details, to its own SQL type.
type ColumnKind string

const (
	KindBool   ColumnKind = "bool"
	KindBytes  ColumnKind = "bytes"
	KindFloat  ColumnKind = "float"
	KindInt    ColumnKind = "int"
	KindString ColumnKind = "string"
	KindTime   ColumnKind = "time"
)

// ColumnType describes a column type independently of any dialect. RawType, when
// set, is used verbatim instead of the mapping from Kind.
type ColumnType struct {
	Kind     ColumnKind
	Bits     int
	Unsigned bool
	Nullable bool
	Size     int
	RawType  string
}

// ColumnSpec describes one table column, either as declared by generated code or as
// reported by InspectColumns.
type ColumnSpec struct {
	Name          string
	Type          ColumnType
	PrimaryKey    bool
	AutoIncrement bool
	Default       string
	// NativeType is the column type exactly as reported by the database.
	// It is populated by InspectColumns and is empty on declared specs.
	NativeType string
}

// Index describes a table index, either as declared or as reported by the database.
// PrimaryKey and Constraint are only set on inspected indexes.
type Index struct {
	Name       string
	Table      string
	Unique     bool
	Fields     []string
	PrimaryKey bool
	Constraint bool
}

// UnsupportedCapabilityError reports that a dialect cannot perform a requested capability.
type UnsupportedCapabilityError struct {
	operation Capability
	dialect   Name
	reason    string
}

func newUnsupportedCapabilityError(operation Capability, dialect Name, reason string) *UnsupportedCapabilityError {
	return &UnsupportedCapabilityError{
		operation: canonicalCapabilityName(string(operation)),
		dialect:   dialect,
		reason:    reason,
	}
}

func (e *UnsupportedCapabilityError) Error() string {
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

	return newUnsupportedCapabilityError(
		capability,
		dialect.Name(),
		unsupportedCapabilityHint(capability, dialect.Name()),
	)
}

// ValidateIdentifier reports whether identifier is a plain SQL identifier that fits
// dialect's length limit. A nil dialect checks only that identifier is not empty.
func ValidateIdentifier(dialect Dialect, identifier string) error {
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

// ddlNativeTypeAliases maps database-reported type spellings to the canonical
// spellings commonly used in declared raw types (db:"...,type:X"), so the two
// representations can be compared textually.
var ddlNativeTypeAliases = map[string]string{
	"BOOLEAN":                     "BOOL",
	"CHARACTER VARYING":           "VARCHAR",
	"CHARACTER":                   "CHAR",
	"INTEGER":                     "INT",
	"NUMERIC":                     "DECIMAL",
	"TIMESTAMP WITHOUT TIME ZONE": "TIMESTAMP",
	"TIMESTAMP WITH TIME ZONE":    "TIMESTAMPTZ",
}

// SameColumnType reports whether two column specs resolve to the same
// database type under the dialect. Besides comparing rendered DDL types, it
// matches a declared raw type override against the type the database reported
// during inspection. Without that second check, types that inspection collapses
// into a canonical kind (TEXT, DECIMAL(n,m), CHAR(n), ...) would be flagged as
// drift on every reconcile and produce repeated, never-converging ALTERs.
func SameColumnType(dialect Dialect, left, right ColumnSpec) bool {
	if strings.EqualFold(
		strings.TrimSpace(dialect.ColumnTypeSQL(left.Type)),
		strings.TrimSpace(dialect.ColumnTypeSQL(right.Type)),
	) {
		return true
	}

	return nativeDDLTypeMatchesDeclared(left, right) || nativeDDLTypeMatchesDeclared(right, left)
}

func nativeDDLTypeMatchesDeclared(inspected, declared ColumnSpec) bool {
	if inspected.NativeType == "" || declared.Type.RawType == "" {
		return false
	}

	return normalizeDDLNativeTypeName(inspected.NativeType) == normalizeDDLNativeTypeName(declared.Type.RawType)
}

func normalizeDDLNativeTypeName(value string) string {
	value = strings.ToUpper(strings.TrimSpace(value))

	base, args, hasArgs := strings.Cut(value, "(")
	base = strings.Join(strings.Fields(base), " ")

	if alias, ok := ddlNativeTypeAliases[base]; ok {
		base = alias
	}

	if !hasArgs {
		return base
	}

	return base + "(" + strings.ReplaceAll(args, " ", "")
}

func normalizeDDLDefault(value sql.NullString) string {
	if !value.Valid {
		return ""
	}

	return strings.TrimSpace(value.String)
}

func withDDLNullable(desc ColumnType, nullable bool) ColumnType {
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
		return "use a subquery or split the query"
	case CapabilityFullOuterJoin:
		return "use LEFT/RIGHT JOIN with UNION, or execute on sqlite/postgres"
	case CapabilityIntersect:
		return "use IN/EXISTS filtering"
	case CapabilityExcept:
		return "use NOT EXISTS filtering"
	case CapabilitySelectForUpdate, CapabilitySelectForShare:
		return "execute on a dialect that supports row-locking reads"
	case CapabilitySelectForNoWait, CapabilitySelectForSkipLocked:
		return "execute on a dialect that supports row-lock wait modifiers"
	default:
		return "use a simpler query shape or a dialect that supports this capability"
	}
}

func ddlSerialType(desc ColumnType) string {
	switch {
	case desc.Bits <= 16:
		return "SMALLSERIAL PRIMARY KEY"
	case desc.Bits <= 32:
		return "SERIAL PRIMARY KEY"
	default:
		return "BIGSERIAL PRIMARY KEY"
	}
}

func validateBuiltInIdentifier(name string) error {
	if !builtInIdentifierPattern.MatchString(name) {
		return fmt.Errorf("invalid SQL identifier: %s", name)
	}

	return nil
}

func validateIndex(
	table string,
	unique bool,
	idx string,
	fields []string,
	existing Index,
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

	return dialect.QuoteIdent(name), nil
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
