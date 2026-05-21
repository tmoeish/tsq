package tsq

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/tmoeish/tsq/v4/dialect"
)

type registry struct {
	mu     sync.RWMutex
	tables map[string]*registeredTable
}

func newRegistry() *registry {
	return &registry{
		tables: make(map[string]*registeredTable),
	}
}

// RegisterTable registers a table in the global registry.
// Returns an error if registration fails.
func RegisterTable(
	table Table,
	indexes ...TableIndex,
) error {
	return defaultRuntime.RegisterTable(table, indexes...)
}

// Register adds a table/index declaration to the runtime registry keyed by table name.
func (r *registry) Register(
	table Table,
	indexes ...TableIndex,
) error {
	if isNilValue(table) {
		return &RegistrationError{
			Type:    RegistrationErrorNilTable,
			Message: "registered table cannot be nil",
		}
	}

	if err := validateRegisteredIndexes(table, indexes); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	key := registeredTableKey(table)
	if _, exists := r.tables[key]; exists {
		return &RegistrationError{
			Type:      RegistrationErrorDuplicate,
			TableName: key,
			Message:   fmt.Sprintf("table %s is already registered", key),
		}
	}

	r.tables[key] = &registeredTable{
		Table:   table,
		Indexes: cloneTableIndexes(indexes),
	}

	return nil
}

type registeredTable struct {
	Table
	Indexes []TableIndex
}

// Init initializes indexes and tracers for the default runtime with optional explicit options.
func Init(db *sql.DB, sqlDialect dialect.Dialect, options ...*InitOptions) error {
	return defaultRuntime.Init(db, sqlDialect, options...)
}

// DefaultRuntime returns the package-level runtime.
func DefaultRuntime() *Runtime {
	return defaultRuntime
}

func registeredTableKey(table Table) string {
	if table == nil {
		return ""
	}

	if schemaTable, ok := table.(schemaTabler); ok && strings.TrimSpace(schemaTable.Schema()) != "" {
		return schemaTable.Schema() + "." + table.Table()
	}

	return table.Table()
}

func snapshotRegisteredTables() []*registeredTable {
	return defaultRuntime.snapshotRegisteredTables()
}

// Snapshot returns the registered tables in deterministic key order.
func (r *registry) Snapshot() []*registeredTable {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.tables))
	for name := range r.tables {
		names = append(names, name)
	}

	sort.Strings(names)

	result := make([]*registeredTable, 0, len(names))
	for _, name := range names {
		result = append(result, r.tables[name])
	}

	return result
}

func validateRegisteredIndexes(table Table, indexes []TableIndex) error {
	if len(indexes) == 0 {
		return nil
	}

	tableName := physicalTableName(table)

	availableColumns := make(map[string]struct{}, len(table.Cols()))
	for _, col := range table.Cols() {
		if col == nil {
			continue
		}

		availableColumns[col.OutputName()] = struct{}{}
	}

	for _, index := range indexes {
		if err := validateBuiltInIdentifier(index.Name); err != nil {
			return &RegistrationError{
				Type:      RegistrationErrorInvalidIndex,
				TableName: tableName,
				Message:   fmt.Sprintf("invalid index %q on table %s: %v", index.Name, tableName, err),
			}
		}

		if len(index.Fields) == 0 {
			return &RegistrationError{
				Type:      RegistrationErrorInvalidIndex,
				TableName: tableName,
				Message:   fmt.Sprintf("index %q on table %s must declare at least one field", index.Name, tableName),
			}
		}

		for _, field := range index.Fields {
			if err := validateBuiltInIdentifier(field); err != nil {
				return &RegistrationError{
					Type:      RegistrationErrorInvalidIndex,
					TableName: tableName,
					Message:   fmt.Sprintf("invalid field %q in index %q on table %s: %v", field, index.Name, tableName, err),
				}
			}

			if _, ok := availableColumns[field]; !ok {
				return &RegistrationError{
					Type:      RegistrationErrorInvalidIndex,
					TableName: tableName,
					Message:   fmt.Sprintf("index %q on table %s references unknown field %q", index.Name, tableName, field),
				}
			}
		}
	}

	return nil
}

func cloneTableIndexes(indexes []TableIndex) []TableIndex {
	if len(indexes) == 0 {
		return nil
	}

	result := make([]TableIndex, 0, len(indexes))
	for _, index := range indexes {
		fields := append([]string(nil), index.Fields...)
		result = append(result, TableIndex{
			Name:   index.Name,
			Fields: fields,
			Unique: index.Unique,
		})
	}

	return result
}
