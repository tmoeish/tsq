package tsq

import (
	"context"
	"fmt"
	"log/slog"

	tsqdialect "github.com/tmoeish/tsq/v4/dialect"
)

// RegistrationErrorType identifies a table-registration failure category.
type RegistrationErrorType string

const (
	// RegistrationErrorNilTable means RegisterTable received a nil table.
	RegistrationErrorNilTable RegistrationErrorType = "nil_table"
	// RegistrationErrorInvalidIndex means RegisterTable received invalid index metadata.
	RegistrationErrorInvalidIndex RegistrationErrorType = "invalid_index"
	// RegistrationErrorDuplicate means the same table key was registered twice.
	RegistrationErrorDuplicate RegistrationErrorType = "duplicate"
)

// SchemaPolicy controls how TSQ manages declared schema objects during runtime bootstrap.
type SchemaPolicy string

const (
	// SchemaPolicyManual leaves declared objects untouched and only logs a reminder.
	SchemaPolicyManual SchemaPolicy = "manual"
	// SchemaPolicyValidate fails when a declared object is missing or mismatched.
	SchemaPolicyValidate SchemaPolicy = "validate"
	// SchemaPolicyCreateMissing creates missing declared objects but still fails on mismatches.
	SchemaPolicyCreateMissing SchemaPolicy = "create_missing"
	// SchemaPolicyReconcile creates missing declared objects and reconciles mismatches.
	SchemaPolicyReconcile SchemaPolicy = "reconcile"
	// SchemaPolicyManaged reconciles declared objects and removes TSQ-managed extras.
	SchemaPolicyManaged SchemaPolicy = "managed"
)

// IndexInitMode is kept as a deprecated alias for SchemaPolicy during the policy rename.
type IndexInitMode = SchemaPolicy

const (
	// IndexInitSkip is deprecated; use SchemaPolicyManual.
	IndexInitSkip = SchemaPolicyManual
	// IndexInitValidate is deprecated; use SchemaPolicyValidate.
	IndexInitValidate = SchemaPolicyValidate
	// IndexInitUpsert is deprecated; use SchemaPolicyCreateMissing.
	IndexInitUpsert = SchemaPolicyCreateMissing
)

// TableIndex declares one physical index owned by a registered table.
type TableIndex struct {
	Name   string   // Name is the stable physical index name.
	Fields []string // Fields preserves the indexed column order.
	Unique bool     // Unique reports whether the index enforces uniqueness.
}

// TableRegistration describes one table plus its declared indexes for runtime bootstrap.
type TableRegistration struct {
	Table   Table                      // Table is the physical table metadata.
	Columns []tsqdialect.DDLColumnSpec // Columns declares the physical column schema owned by Table.
	Indexes []TableIndex               // Indexes declares the indexes owned by Table.
}

// ErrIndexMissing reports that an expected index was not found.
type ErrIndexMissing struct {
	Table  string   // Table is the table that should contain the index.
	Name   string   // Name is the expected index name.
	Fields []string // Fields is the expected indexed column order.
	Unique bool     // Unique reports whether the missing index should be unique.
}

// Error implements error.
func (e *ErrIndexMissing) Error() string {
	if e == nil {
		return ""
	}

	return fmt.Sprintf(
		"index %s on table %s is missing; expected fields %v; use RuntimeOptions{IndexPolicy: SchemaPolicyCreateMissing} or create the index in your migration",
		e.Name,
		e.Table,
		e.Fields,
	)
}

// ErrTableMissing reports that an expected table was not found.
type ErrTableMissing struct {
	Name string // Name is the expected physical table name.
}

// Error implements error.
func (e *ErrTableMissing) Error() string {
	if e == nil {
		return ""
	}

	return fmt.Sprintf(
		"table %s is missing; use RuntimeOptions{TablePolicy: SchemaPolicyCreateMissing} or create the table in your migration",
		e.Name,
	)
}

// RegistrationError reports a table-registration failure.
type RegistrationError struct {
	Type      RegistrationErrorType // Type classifies the registration failure.
	TableName string                // TableName identifies the conflicting or invalid table entry.
	Message   string                // Message contains the user-facing error text.
}

// Error implements error.
func (e *RegistrationError) Error() string {
	return e.Message
}

// RuntimeOptions controls runtime initialization behavior.
type RuntimeOptions struct {
	TablePolicy SchemaPolicy // TablePolicy chooses how TSQ manages declared tables and columns during NewRuntime.
	IndexPolicy SchemaPolicy // IndexPolicy chooses how TSQ manages declared indexes during NewRuntime.
	Tracers     []Tracer     // Tracers configures the runtime's tracer chain during NewRuntime.
	Logger      Logger       // Logger receives schema bootstrap decisions and executed DDL.
	// IdentifierValidationMode controls how to handle identifier length violations:
	// "strict" = fail if any identifier exceeds dialect limits (default for most dialects)
	// "warn"   = log warnings but allow (for permissive databases)
	// "skip"   = no validation (useful for dynamic schemas)
	IdentifierValidationMode string
}

// Logger is the subset of slog.Logger used by runtime bootstrap.
type Logger interface {
	Enabled(ctx context.Context, level slog.Level) bool
	LogAttrs(ctx context.Context, level slog.Level, msg string, attrs ...slog.Attr)
}

// Table defines a physical SQL table source.
// Unlike Result, a Table is both a scan owner and a mutation target, and it
// exposes stable column and primary-key metadata for metadata-driven execution.
type Table interface {
	Owner
	Cols() []SQLColumn             // Cols returns the physical columns exposed by the table.
	Table() string                 // Table returns the SQL identifier used in rendered queries.
	SearchColumns() []SearchColumn // SearchColumns returns columns eligible for keyword-search helpers.
	PrimaryKeys() []string         // PrimaryKeys returns the primary-key column names in declaration order.
	AutoIncrement() bool           // AutoIncrement reports whether inserts rely on generated primary keys.
	VersionColumn() string         // VersionColumn returns the optimistic-lock column name, if any.
}
