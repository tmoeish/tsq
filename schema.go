package tsq

import (
	"context"
	"fmt"
	"log/slog"
	"slices"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
)

// SchemaPolicy controls what a Runtime does to declared tables and indexes when it
// starts. TSQ only ever adds: it never drops a table or an index it does not see
// declared, because a runtime cannot tell an obsolete object from another
// service's.
type SchemaPolicy string

const (
	// SchemaPolicyManual changes nothing; the schema belongs to migrations. It is the
	// default and the production setting.
	SchemaPolicyManual SchemaPolicy = "manual"
	// SchemaPolicyValidate fails startup when a declared object is missing or differs.
	SchemaPolicyValidate SchemaPolicy = "validate"
	// SchemaPolicyCreateMissing creates missing tables, columns and indexes, and fails
	// on anything that differs.
	SchemaPolicyCreateMissing SchemaPolicy = "create_missing"
	// SchemaPolicyReconcile also changes differing columns back to their
	// declaration: the development setting, where editing a struct and restarting is
	// enough.
	SchemaPolicyReconcile SchemaPolicy = "reconcile"
)

// DefaultMaxPageSize caps PageRequest.Size unless WithMaxPageSize says otherwise.
const DefaultMaxPageSize = 1000

// MissingIndexError reports a declared index the database does not have.
type MissingIndexError struct {
	Table   string
	Name    string
	Columns []string
	Unique  bool
}

func (e *MissingIndexError) Error() string {
	return fmt.Sprintf("index %s on table %s (%v) is missing; create it in a migration or use SchemaPolicyCreateMissing",
		e.Name, e.Table, e.Columns)
}

// MissingTableError reports a declared table the database does not have.
type MissingTableError struct {
	Name string
}

func (e *MissingTableError) Error() string {
	return fmt.Sprintf("table %s is missing; create it in a migration or use SchemaPolicyCreateMissing", e.Name)
}

// Logger is the subset of *slog.Logger the runtime writes to.
type Logger interface {
	Enabled(ctx context.Context, level slog.Level) bool
	LogAttrs(ctx context.Context, level slog.Level, msg string, attrs ...slog.Attr)
}

// registeredTable is a table as the schema policies see it.
type registeredTable struct {
	name    string
	columns []*columnCore
	Columns []tsqdialect.ColumnSpec
	Indexes []TableIndex
}

func registerTables(tables []Table) ([]*registeredTable, error) {
	seen := make(map[string]bool, len(tables))
	result := make([]*registeredTable, 0, len(tables))

	for i, table := range tables {
		if isNilValue(table) {
			return nil, fmt.Errorf("table %d is nil", i)
		}

		def := table.definition()
		if def == nil {
			return nil, fmt.Errorf("%s is not a declared table; register tables, not CTEs", table.TableName())
		}

		if table.TableName() != def.name {
			return nil, fmt.Errorf("register table %s itself, not its alias %s", def.name, table.TableName())
		}

		if err := tableErr(table); err != nil {
			return nil, err
		}

		if seen[def.name] {
			return nil, fmt.Errorf("table %s is registered twice", def.name)
		}

		seen[def.name] = true
		result = append(result, &registeredTable{
			name:    def.name,
			columns: def.columns,
			Columns: slices.Clone(def.schema),
			Indexes: slices.Clone(def.indexes),
		})
	}

	slices.SortFunc(result, func(a, b *registeredTable) int {
		switch {
		case a.name < b.name:
			return -1
		case a.name > b.name:
			return 1
		default:
			return 0
		}
	})

	return result, nil
}
