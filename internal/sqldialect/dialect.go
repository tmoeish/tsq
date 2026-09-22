package sqldialect

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
)

// The names and schema types are the public dialect package's; these aliases let
// the implementation spell them unqualified.
type (
	Name       = tsqdialect.Name
	Capability = tsqdialect.Capability
	ColumnKind = tsqdialect.ColumnKind
	ColumnType = tsqdialect.ColumnType
	ColumnSpec = tsqdialect.ColumnSpec
	Fill       = tsqdialect.Fill
)

const (
	MySQL    = tsqdialect.MySQL
	Postgres = tsqdialect.Postgres
	SQLite   = tsqdialect.SQLite

	CapabilityCTE                 = tsqdialect.CapabilityCTE
	CapabilityExcept              = tsqdialect.CapabilityExcept
	CapabilityFullOuterJoin       = tsqdialect.CapabilityFullOuterJoin
	CapabilityIntersect           = tsqdialect.CapabilityIntersect
	CapabilitySelectForUpdate     = tsqdialect.CapabilitySelectForUpdate
	CapabilitySelectForShare      = tsqdialect.CapabilitySelectForShare
	CapabilitySelectForNoWait     = tsqdialect.CapabilitySelectForNoWait
	CapabilitySelectForSkipLocked = tsqdialect.CapabilitySelectForSkipLocked
	CapabilityFullTextSearch      = tsqdialect.CapabilityFullTextSearch

	KindBool   = tsqdialect.KindBool
	KindBytes  = tsqdialect.KindBytes
	KindFloat  = tsqdialect.KindFloat
	KindInt    = tsqdialect.KindInt
	KindString = tsqdialect.KindString
	KindTime   = tsqdialect.KindTime

	FillCaller    = tsqdialect.FillCaller
	FillDefault   = tsqdialect.FillDefault
	FillGenerated = tsqdialect.FillGenerated
)

// Column is a live column as InspectColumns reads it back: its spec, plus the
// type exactly as the database reports it, which a declared RawType is compared
// with.
type Column struct {
	ColumnSpec
	NativeType string
}

// For returns the implementation of engine.
func For(engine Name) (Dialect, error) {
	switch engine {
	case MySQL:
		return MySQLDialect{}, nil
	case Postgres:
		return PostgresDialect{}, nil
	case SQLite:
		return SQLiteDialect{}, nil
	default:
		return nil, fmt.Errorf("unsupported dialect %q: TSQ supports mysql, postgres and sqlite", engine)
	}
}

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
//
// **It is not an extension point.** TSQ supports MySQL, PostgreSQL and SQLite, and
// the constructs the three spell differently — date parts, ROUND, NULL ordering,
// full-text search — are chosen inside the library by dialect name, not through this
// interface. A type implementing Dialect for a fourth engine would render most
// queries and then fail on those with "not supported on <name>". The boundary is
// deliberate: TSQ supports the engines its tests run against, and an untested
// fourth would be a promise nothing keeps.
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
	InspectColumns(ctx context.Context, db Executor, table string) ([]Column, bool, error)
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
	// FullTextIndexSQL renders the statement that creates a full-text index over the
	// already-quoted fields, and "" where the dialect has no full-text index.
	FullTextIndexSQL(table, idx string, quotedFields []string) string
	// FullTextVectorSQL is the indexed expression a full-text predicate repeats over
	// the already-quoted fields, and "" where the predicate names the columns
	// directly (MySQL) or matches substrings instead (SQLite).
	FullTextVectorSQL(quotedFields []string) string
	// DropIndexSQL renders a DROP INDEX statement.
	DropIndexSQL(table, idx string) string
	// AlterMode says whether a column type change is an ALTER or a table rebuild.
	AlterMode() AlterMode
	// AlterColumnSQL renders the statements that turn column before into after.
	AlterColumnSQL(table string, before Column, after ColumnSpec) []string
	// InspectRebuild reports what rebuilding table must carry over besides its
	// columns. Only a dialect whose AlterMode is AlterRebuild answers; the others
	// never rebuild a table and return an error.
	InspectRebuild(ctx context.Context, db Executor, table string) (Rebuild, error)
}

// Rebuild is what rebuilding a table in place must know besides its columns.
type Rebuild struct {
	// Blockers name what the rebuild cannot carry over, such as a constraint the
	// declared columns do not describe; a rebuild with any is refused.
	Blockers []string
	// Objects are the table's own indexes and triggers, to create again as they
	// were.
	Objects []RebuildObject
}

// RebuildObject is an index or trigger of a table, with the statement that created
// it.
type RebuildObject struct {
	Name string
	SQL  string
	// Columns are the plain columns an index covers, and nil for a trigger. An
	// index over a column the rebuild drops goes with the column.
	Columns []string
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

// ColumnDefinitionSQL renders the column as a CREATE TABLE or ADD COLUMN clause.
// The library and the generator share it, so a declared column reads the same in a
// generated .sql file and in the DDL a runtime applies.
func ColumnDefinitionSQL(dialect Dialect, column ColumnSpec) (string, error) {
	quoted := dialect.QuoteIdent(column.Name)

	if column.PrimaryKey && column.AutoIncrement {
		return dialect.AutoIncrementColumnSQL(quoted, column.Type)
	}

	// A generated column takes neither NOT NULL nor DEFAULT, and STORED is the one
	// form MySQL 5.7+, PostgreSQL 12+ and SQLite 3.31+ all accept.
	if column.Generated != "" {
		return fmt.Sprintf("%s %s GENERATED ALWAYS AS (%s) STORED", quoted, dialect.ColumnTypeSQL(column.Type), column.Generated), nil
	}

	parts := []string{quoted, dialect.ColumnTypeSQL(column.Type)}

	switch {
	case column.PrimaryKey:
		parts = append(parts, "PRIMARY KEY")
	case !column.Type.Nullable:
		parts = append(parts, "NOT NULL")
	}

	if column.Default != "" {
		parts = append(parts, "DEFAULT "+column.Default)
	}

	return strings.Join(parts, " "), nil
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

// ValidateCapability reports whether dialect supports capability.
func ValidateCapability(dialect Dialect, capability Capability) error {
	if dialect == nil {
		return nil
	}

	return tsqdialect.Check(dialect.Name(), capability)
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
			dialect,
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
func SameColumnType(dialect Dialect, inspected Column, declared ColumnSpec) bool {
	if strings.EqualFold(
		strings.TrimSpace(dialect.ColumnTypeSQL(inspected.Type)),
		strings.TrimSpace(dialect.ColumnTypeSQL(declared.Type)),
	) {
		return true
	}

	return nativeDDLTypeMatchesDeclared(inspected, declared)
}

func nativeDDLTypeMatchesDeclared(inspected Column, declared ColumnSpec) bool {
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
