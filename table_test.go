package tsq

import (
	"testing"

	"gopkg.in/gorp.v2"
)

func TestRegisterTable(t *testing.T) {
	// Clear tables before test
	tables = make(map[string]Table)

	table := newMockTable("test_table")
	RegisterTable(table)

	if len(tables) != 1 {
		t.Errorf("Expected 1 registered table, got %d", len(tables))
	}

	if _, exists := tables["test_table"]; !exists {
		t.Error("Expected 'test_table' to be registered")
	}
}

func TestGetRegisteredTable(t *testing.T) {
	// Clear tables before test
	tables = make(map[string]Table)

	table := newMockTable("users")
	RegisterTable(table)

	// Test existing table
	retrieved, exists := GetRegisteredTable("users")
	if !exists {
		t.Error("Expected table 'users' to exist")
	}

	if retrieved.Table() != "users" {
		t.Errorf("Expected table name 'users', got '%s'", retrieved.Table())
	}

	// Test non-existing table
	_, exists = GetRegisteredTable("non_existing")
	if exists {
		t.Error("Expected table 'non_existing' to not exist")
	}
}

func TestGetAllRegisteredTables(t *testing.T) {
	// Clear tables before test
	tables = make(map[string]Table)

	// Test empty tables
	allTables := GetAllRegisteredTables()
	if len(allTables) != 0 {
		t.Errorf("Expected 0 tables, got %d", len(allTables))
	}

	// Register some tables
	table1 := newMockTable("users")
	table2 := newMockTable("orders")

	RegisterTable(table1)
	RegisterTable(table2)

	allTables = GetAllRegisteredTables()
	if len(allTables) != 2 {
		t.Errorf("Expected 2 tables, got %d", len(allTables))
	}

	// Check that returned map is a copy
	allTables["test"] = newMockTable("test")

	if len(tables) != 2 {
		t.Error("GetAllRegisteredTables() should return a copy, not the original map")
	}
}

func TestMockTable_Interface(t *testing.T) {
	table := newMockTable("test_table")

	// Test Table interface compliance
	var _ Table = table

	// Test all interface methods
	if table.Table() != "test_table" {
		t.Errorf("Expected table name 'test_table', got '%s'", table.Table())
	}

	if table.KwList() != nil {
		t.Errorf("Expected nil KwList, got %v", table.KwList())
	}
}

func TestTableRegistry_MultipleRegistrations(t *testing.T) {
	// Clear tables before test
	tables = make(map[string]Table)

	// Register multiple tables
	tableNames := []string{"users", "orders", "products", "categories"}
	for _, name := range tableNames {
		RegisterTable(newMockTable(name))
	}

	// Verify all tables are registered
	if len(tables) != len(tableNames) {
		t.Errorf("Expected %d tables, got %d", len(tableNames), len(tables))
	}

	for _, name := range tableNames {
		if _, exists := tables[name]; !exists {
			t.Errorf("Expected table '%s' to be registered", name)
		}
	}
}

func TestTableRegistry_OverwriteRegistration(t *testing.T) {
	// Clear tables before test
	tables = make(map[string]Table)

	// Register a table
	table1 := newMockTable("users")
	RegisterTable(table1)

	// Register another table with the same name
	table2 := newMockTable("users")
	RegisterTable(table2)

	// Should have only one table registered
	if len(tables) != 1 {
		t.Errorf("Expected 1 table, got %d", len(tables))
	}

	// The second registration should overwrite the first
	retrieved, exists := GetRegisteredTable("users")
	if !exists {
		t.Error("Expected table 'users' to exist")
	}

	// We can't directly compare the tables, but we can verify it's the right type
	if retrieved.Table() != "users" {
		t.Errorf("Expected table name 'users', got '%s'", retrieved.Table())
	}
}

func TestTableRegistry_EmptyName(t *testing.T) {
	// Clear tables before test
	tables = make(map[string]Table)

	// Register a table with empty name
	table := newMockTable("")
	RegisterTable(table)

	// Should be registered with empty key
	if len(tables) != 1 {
		t.Errorf("Expected 1 table, got %d", len(tables))
	}

	retrieved, exists := GetRegisteredTable("")
	if !exists {
		t.Error("Expected table with empty name to exist")
	}

	if retrieved.Table() != "" {
		t.Errorf("Expected empty table name, got '%s'", retrieved.Table())
	}
}

// Extended mock table for testing with more features
type extendedMockTable struct {
	tableName    string
	customID     bool
	idField      string
	versionField string
	cols         []Column
	kwList       []Column
	uxMap        map[string][]string
	idxMap       map[string][]string
}

func (e extendedMockTable) Table() string                                  { return e.tableName }
func (e extendedMockTable) KwList() []Column                               { return e.kwList }
func (e extendedMockTable) Init(db *gorp.DbMap, upsertIndexies bool) error { return nil }

func TestExtendedMockTable(t *testing.T) {
	table := extendedMockTable{
		tableName:    "extended_table",
		customID:     true,
		idField:      "custom_id",
		versionField: "version",
		cols:         []Column{},
		kwList:       []Column{},
		uxMap:        map[string][]string{"unique_name": {"name"}},
		idxMap:       map[string][]string{"idx_status": {"status"}},
	}

	if table.Table() != "extended_table" {
		t.Errorf("Expected table name 'extended_table', got '%s'", table.Table())
	}

	if table.KwList() == nil {
		t.Error("Expected non-nil KwList")
	}

	if err := table.Init(nil, false); err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}
