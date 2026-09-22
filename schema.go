package tsq

import (
	"fmt"
	"slices"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
)

// SchemaPolicy controls what a Runtime does to declared tables and indexes when it
// starts. No policy drops a table or an index it does not see declared, because a
// runtime cannot tell an obsolete object from another service's.
// SchemaPolicyReconcile does drop a column its table no longer declares.
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
	// declaration and drops the columns a table no longer declares, with their data:
	// the development setting, where the database follows the code and editing a
	// struct and restarting is enough. Production keeps SchemaPolicyManual.
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
			// Each index is copied with its column list, so the registration shares no
			// slice with the table definition.
			Indexes: cloneTableIndexes(def.indexes),
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

func resolveSchemaPolicy(policy SchemaPolicy) SchemaPolicy {
	if policy == "" {
		return SchemaPolicyManual
	}

	return policy
}

func validateSchemaPolicy(policy SchemaPolicy) error {
	switch policy {
	case SchemaPolicyManual, SchemaPolicyValidate, SchemaPolicyCreateMissing, SchemaPolicyReconcile:
		return nil
	default:
		return fmt.Errorf("invalid schema policy %q", policy)
	}
}
