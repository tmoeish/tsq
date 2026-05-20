package tsq

import (
	"strings"
	"testing"
)

type mockTable struct{ tableName string }
type queryBuilderCaseRow struct{ Label string }

func (queryBuilderCaseRow) TSQOwner() {
}
func (m mockTable) Init(db *Engine) error {
	return nil
}
func (m mockTable) TSQOwner() {
}
func (m mockTable) Table() string {
	return m.tableName
}
func (m mockTable) Cols() []SQLColumn {
	return nil
}
func (m mockTable) SearchColumns() []SearchColumn {
	return nil
}
func (m mockTable) PrimaryKeys() []string {
	return nil
}
func (m mockTable) AutoIncrement() bool {
	return false
}
func (m mockTable) VersionColumn() string {
	return ""
}
func newMockTable(name string) Table {
	return mockTable{tableName: name}
}
func newMockColumn(table Table, name string) columnImpl[Table, string] {
	return newColForTable[Table, string](table, name, name, nil)
}
func TestSelect(t *testing.T) {
	table1 := newMockTable("users")
	col1 := newMockColumn(table1, "id")
	col2 := newMockColumn(table1, "name")
	qb := Select(col1, col2)
	if qb == nil {
		t.Fatal("Select() returned nil")
	}
	if len(qb.spec.Selects) != 2 {
		t.Errorf("Expected 2 select columns, got %d", len(qb.spec.Selects))
	}
	selectTables := qb.spec.selectTables()
	if len(selectTables) != 1 {
		t.Errorf("Expected 1 select table, got %d", len(selectTables))
	}
	if _, exists := selectTables["users"]; !exists {
		t.Error("Expected 'users' table to be in selectTables")
	}
}
func TestFromCreatesBuilderAndSelectSetsColumns(t *testing.T) {
	table := newMockTable("users")
	id := newMockColumn(table, "id")
	name := newMockColumn(table, "name")
	qb := From[Table](table).Select(id, name)
	if qb == nil {
		t.Fatal("From().Select() returned nil")
	}
	if qb.spec.From != table {
		t.Fatalf("expected from table %v, got %v", table, qb.spec.From)
	}
	if len(qb.spec.Selects) != 2 {
		t.Fatalf("expected 2 selected columns, got %d", len(qb.spec.Selects))
	}
}
func TestSelect_NilColumnDefersToBuildError(t *testing.T) {
	table := newMockTable("users")
	var col BoundColumn[Table]
	_, err := Select(col).From(table).Build()
	if err == nil {
		t.Fatal("expected nil select column to return an error")
	}
	if !strings.Contains(err.Error(), "column cannot be nil") {
		t.Fatalf("unexpected error: %v", err)
	}
}
func TestSelect_ZeroValueColumnDefersToBuildError(t *testing.T) {
	table := newMockTable("users")
	var col columnImpl[Table, int]
	_, err := Select(col).From(table).Build()
	if err == nil {
		t.Fatal("expected zero-value select column to return an error")
	}
	if !strings.Contains(err.Error(), "must reference at least one table") {
		t.Fatalf("unexpected error: %v", err)
	}
}
func TestJoinTypeConstants(t *testing.T) {
	tests := []struct {
		joinType joinType
		expected string
	}{{leftJoinType, "LEFT JOIN"}, {innerJoinType, "INNER JOIN"}, {rightJoinType, "RIGHT JOIN"}, {fullJoinType, "FULL JOIN"}, {crossJoinType, "CROSS JOIN"}}
	for _, tt := range tests {
		t.Run(string(tt.joinType), func(t *testing.T) {
			if string(tt.joinType) != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, string(tt.joinType))
			}
		})
	}
}

type mockCondition struct {
	clause string
	tables map[string]Table
}

func (m *mockCondition) Clause() string {
	return m.clause
}
func (m *mockCondition) Tables() map[string]Table {
	return m.tables
}
func (m *mockCondition) Args() []any {
	return nil
}
