package tsq

import (
	"context"
	"fmt"
	"log/slog"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
)

// RegistrationErrorKind identifies a table-registration failure category.
type RegistrationErrorKind string

const (
	// RegistrationErrorNilTable means a TableRegistration carried a nil table.
	RegistrationErrorNilTable RegistrationErrorKind = "nil_table"
	// RegistrationErrorInvalidIndex means a TableRegistration carried invalid index metadata.
	RegistrationErrorInvalidIndex RegistrationErrorKind = "invalid_index"
	// RegistrationErrorDuplicate means the same table key was registered twice.
	RegistrationErrorDuplicate RegistrationErrorKind = "duplicate"
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
)

// DefaultMaxPageSize caps PageRequest.Size when WithMaxPageSize is zero.
const DefaultMaxPageSize = 1000

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

// MissingIndexError reports that an expected index was not found.
type MissingIndexError struct {
	Table  string   // Table is the table that should contain the index.
	Name   string   // Name is the expected index name.
	Fields []string // Fields is the expected indexed column order.
	Unique bool     // Unique reports whether the missing index should be unique.
}

// Error implements error.
func (e *MissingIndexError) Error() string {
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

// MissingTableError reports that an expected table was not found.
type MissingTableError struct {
	Name string // Name is the expected physical table name.
}

// Error implements error.
func (e *MissingTableError) Error() string {
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
	Kind    RegistrationErrorKind // Type classifies the registration failure.
	Table   string                // TableName identifies the conflicting or invalid table entry.
	Message string                // Message contains the user-facing error text.
}

// Error implements error.
func (e *RegistrationError) Error() string {
	return e.Message
}

// Logger is the subset of slog.Logger used by runtime bootstrap.
type Logger interface {
	Enabled(ctx context.Context, level slog.Level) bool
	LogAttrs(ctx context.Context, level slog.Level, msg string, attrs ...slog.Attr)
}

// ManagedColumns names the columns TSQ maintains on the caller's behalf. An
// empty name means the table does not declare that role.
//
// It is a struct rather than one interface method per role so that a future
// managed column is a new field here instead of a new method every hand-written
// Table implementation has to grow.
type ManagedColumns struct {
	Version   string // Version is the optimistic-lock column.
	CreatedAt string // CreatedAt is set once, when the row is inserted.
	UpdatedAt string // UpdatedAt is refreshed by every update, including soft deletes.
	DeletedAt string // DeletedAt carries the soft-delete tombstone; when set, Delete is a soft delete.
}

// Table defines a physical SQL table source.
// Unlike Result, a Table is both a scan owner and a mutation target, and it
// exposes stable column and primary-key metadata for metadata-driven execution.
type Table interface {
	Owner
	Cols() []SQLColumn              // Cols returns the physical columns exposed by the table.
	TableName() string              // Table returns the SQL identifier used in rendered queries.
	SearchColumns() []SearchColumn  // SearchColumns returns columns eligible for keyword-search helpers.
	PrimaryKey() string             // PrimaryKey returns the primary-key column name.
	AutoIncrement() bool            // AutoIncrement reports whether inserts rely on generated primary keys.
	ManagedColumns() ManagedColumns // ManagedColumns returns the columns TSQ maintains automatically.
}

// DeclareTable returns table unchanged. The cols argument is never read: it
// exists so that a package-level table variable records a visible dependency on
// the column slice that the table's Cols method returns.
//
// Go orders package-level variable initialization by the references that appear
// in initialization expressions. Cols reaches its column slice through an
// interface method call, which that analysis cannot see, so a package-level
// query variable that selects individual columns never mentions the slice and
// may be initialized before it. The slice is not empty at that point but fully
// sized with nil elements, because the compiler emits the slice header as static
// data and fills the elements in later, so column validation finds no match and
// reports that the column does not belong to the table. Whether it happens at
// all depends on file name order within the package.
//
// Generated code uses this helper; declare hand-written tables the same way:
//
//	var TableUser tsq.Table = tsq.DeclareTable(User{}, User__Cols)
func DeclareTable[O Table](table O, cols []BoundColumn[O]) Table {
	_ = cols

	return table
}
