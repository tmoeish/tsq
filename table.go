package tsq

import "fmt"

// RegistrationErrorType identifies a table-registration failure category.
type RegistrationErrorType string

const (
	// RegistrationErrorNilTable means RegisterTable received a nil table.
	RegistrationErrorNilTable RegistrationErrorType = "nil_table"
	// RegistrationErrorNilInitFunc means RegisterTable received a nil init hook.
	RegistrationErrorNilInitFunc RegistrationErrorType = "nil_init_func"
	// RegistrationErrorDuplicate means the same table key was registered twice.
	RegistrationErrorDuplicate RegistrationErrorType = "duplicate"
	// RegistrationErrorNilRuntime means a method was called on a nil runtime.
	RegistrationErrorNilRuntime RegistrationErrorType = "nil_runtime"
)

// IndexInitMode controls how tsq handles declared indexes during Init.
type IndexInitMode string

const (
	// IndexInitSkip leaves declared indexes untouched.
	IndexInitSkip IndexInitMode = "skip"
	// IndexInitUpsert creates missing declared indexes when possible.
	IndexInitUpsert IndexInitMode = "upsert"
	// IndexInitValidate fails when a declared index is missing or mismatched.
	IndexInitValidate IndexInitMode = "validate"
)

// SchemaEventKind classifies emitted schema events.
type SchemaEventKind string

const (
	// SchemaEventCreateTable reports table creation.
	SchemaEventCreateTable SchemaEventKind = "create_table"
	// SchemaEventCreateIndex reports index creation.
	SchemaEventCreateIndex SchemaEventKind = "create_index"
	// SchemaEventValidateIndex reports successful index validation.
	SchemaEventValidateIndex SchemaEventKind = "validate_index"
	// SchemaEventSkipIndex reports that index work was skipped.
	SchemaEventSkipIndex SchemaEventKind = "skip_index"
)

// SchemaEvent reports a schema action performed or skipped during Init.
type SchemaEvent struct {
	Kind  SchemaEventKind // Kind identifies which schema action tsq took.
	Table string          // Table names the table associated with the action.
	Name  string          // Name names the affected schema object, such as an index.
	SQL   string          // SQL contains the statement tsq executed when one was emitted.
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
		"index %s on table %s is missing; expected fields %v; enable IndexInitUpsert or create the index in your migration",
		e.Name,
		e.Table,
		e.Fields,
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

// InitOptions controls runtime initialization behavior.
type InitOptions struct {
	UpsertIndexes      bool              // UpsertIndexes keeps the legacy "create missing indexes" behavior when IndexMode is unset.
	IndexMode          IndexInitMode     // IndexMode chooses whether Init skips, upserts, or validates declared indexes.
	Tracers            []Tracer          // Tracers are appended to the runtime before initialization work begins.
	SchemaEventHandler func(SchemaEvent) // SchemaEventHandler receives emitted schema actions such as created or validated indexes.
	// IdentifierValidationMode controls how to handle identifier length violations:
	// "strict" = fail if any identifier exceeds dialect limits (default for most dialects)
	// "warn"   = log warnings but allow (for permissive databases)
	// "skip"   = no validation (useful for dynamic schemas)
	IdentifierValidationMode string
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
