package tsq

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"sync"
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
	initFunc func(db *Engine) error,
) error {
	return defaultRuntime.RegisterTable(table, initFunc)
}

// Register adds a table/init pair to the runtime registry keyed by table name.
func (r *registry) Register(
	table Table,
	initFunc func(db *Engine) error,
) error {
	if isNilValue(table) {
		return &RegistrationError{
			Type:    RegistrationErrorNilTable,
			Message: "registered table cannot be nil",
		}
	}

	if initFunc == nil {
		return &RegistrationError{
			Type:      RegistrationErrorNilInitFunc,
			TableName: fmt.Sprintf("%v", table),
			Message:   "init function cannot be nil",
		}
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
		Table:    table,
		InitFunc: initFunc,
	}

	return nil
}

type registeredTable struct {
	Table
	InitFunc func(db *Engine) error
}

// Init initializes indexes and tracers for the default runtime with optional explicit options.
func Init(db *sql.DB, dialect Dialect, options ...*InitOptions) error {
	return defaultRuntime.Init(db, dialect, options...)
}

// DefaultEngine returns the Engine of the default runtime.
func DefaultEngine() *Engine {
	return defaultRuntime.Engine()
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
