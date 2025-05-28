package tsq

import (
	"testing"

	"gopkg.in/gorp.v2"
)

// Mock table and column for testing
type mockTable struct {
	tableName string
}

func (m mockTable) Init(db *gorp.DbMap, upsertIndexies bool) error { return nil }
func (m mockTable) Table() string                                  { return m.tableName }
func (m mockTable) Cols() []Column                                 { return nil }
func (m mockTable) KwList() []Column                               { return nil }

func newMockTable(name string) Table {
	return mockTable{tableName: name}
}

func newMockColumn(table Table, name string) Column {
	return NewCol[string](table, name, name, nil)
}

func TestSelect(t *testing.T) {
	table1 := newMockTable("users")
	col1 := newMockColumn(table1, "id")
	col2 := newMockColumn(table1, "name")

	qb := Select(col1, col2)

	if qb == nil {
		t.Fatal("Select() returned nil")
	}

	if len(qb.selectCols) != 2 {
		t.Errorf("Expected 2 select columns, got %d", len(qb.selectCols))
	}

	if len(qb.selectTables) != 1 {
		t.Errorf("Expected 1 select table, got %d", len(qb.selectTables))
	}

	if _, exists := qb.selectTables["users"]; !exists {
		t.Error("Expected 'users' table to be in selectTables")
	}
}

func TestQueryBuilder_LeftJoin(t *testing.T) {
	table1 := newMockTable("users")
	table2 := newMockTable("orders")
	col1 := newMockColumn(table1, "id")
	col2 := newMockColumn(table2, "user_id")

	qb := Select(col1).LeftJoin(col1, col2)

	if len(qb.joins) != 1 {
		t.Errorf("Expected 1 join, got %d", len(qb.joins))
	}

	join := qb.joins[0]
	if join.joinType != LeftJoinType {
		t.Errorf("Expected LEFT JOIN, got %s", join.joinType)
	}

	if join.left.Name() != "id" {
		t.Errorf("Expected left column 'id', got '%s'", join.left.Name())
	}

	if join.right.Name() != "user_id" {
		t.Errorf("Expected right column 'user_id', got '%s'", join.right.Name())
	}

	// Check that both tables are added to selectTables
	if len(qb.selectTables) != 2 {
		t.Errorf("Expected 2 tables in selectTables, got %d", len(qb.selectTables))
	}
}

func TestQueryBuilder_InnerJoin(t *testing.T) {
	table1 := newMockTable("users")
	table2 := newMockTable("orders")
	col1 := newMockColumn(table1, "id")
	col2 := newMockColumn(table2, "user_id")

	qb := Select(col1).InnerJoin(col1, col2)

	if len(qb.joins) != 1 {
		t.Errorf("Expected 1 join, got %d", len(qb.joins))
	}

	if qb.joins[0].joinType != InnerJoinType {
		t.Errorf("Expected INNER JOIN, got %s", qb.joins[0].joinType)
	}
}

func TestQueryBuilder_RightJoin(t *testing.T) {
	table1 := newMockTable("users")
	table2 := newMockTable("orders")
	col1 := newMockColumn(table1, "id")
	col2 := newMockColumn(table2, "user_id")

	qb := Select(col1).RightJoin(col1, col2)

	if len(qb.joins) != 1 {
		t.Errorf("Expected 1 join, got %d", len(qb.joins))
	}

	if qb.joins[0].joinType != RightJoinType {
		t.Errorf("Expected RIGHT JOIN, got %s", qb.joins[0].joinType)
	}
}

func TestQueryBuilder_FullJoin(t *testing.T) {
	table1 := newMockTable("users")
	table2 := newMockTable("orders")
	col1 := newMockColumn(table1, "id")
	col2 := newMockColumn(table2, "user_id")

	qb := Select(col1).FullJoin(col1, col2)

	if len(qb.joins) != 1 {
		t.Errorf("Expected 1 join, got %d", len(qb.joins))
	}

	if qb.joins[0].joinType != FullJoinType {
		t.Errorf("Expected FULL JOIN, got %s", qb.joins[0].joinType)
	}
}

func TestQueryBuilder_CrossJoin(t *testing.T) {
	table1 := newMockTable("users")
	table2 := newMockTable("orders")
	col1 := newMockColumn(table1, "id")

	qb := Select(col1).CrossJoin(table2)

	if len(qb.joins) != 1 {
		t.Errorf("Expected 1 join, got %d", len(qb.joins))
	}

	join := qb.joins[0]
	if join.joinType != CrossJoinType {
		t.Errorf("Expected CROSS JOIN, got %s", join.joinType)
	}

	if join.table.Table() != "orders" {
		t.Errorf("Expected table 'orders', got '%s'", join.table.Table())
	}
}

func TestQueryBuilder_GroupBy(t *testing.T) {
	table1 := newMockTable("users")
	col1 := newMockColumn(table1, "department")
	col2 := newMockColumn(table1, "status")

	qb := Select(col1).GroupBy(col1, col2)

	if len(qb.groupByCols) != 2 {
		t.Errorf("Expected 2 GROUP BY columns, got %d", len(qb.groupByCols))
	}

	if qb.groupByCols[0].Name() != "department" {
		t.Errorf("Expected first GROUP BY column 'department', got '%s'", qb.groupByCols[0].Name())
	}

	if qb.groupByCols[1].Name() != "status" {
		t.Errorf("Expected second GROUP BY column 'status', got '%s'", qb.groupByCols[1].Name())
	}
}

func TestQueryBuilder_Having(t *testing.T) {
	table1 := newMockTable("users")
	col1 := newMockColumn(table1, "count")

	// Create a mock condition
	mockCond := &mockCondition{
		clause: "COUNT(*) > 5",
		tables: map[string]Table{"users": table1},
	}

	qb := Select(col1).Having(mockCond)

	if len(qb.havingConditions) != 1 {
		t.Errorf("Expected 1 HAVING condition, got %d", len(qb.havingConditions))
	}
}

func TestQueryBuilder_Where(t *testing.T) {
	table1 := newMockTable("users")
	col1 := newMockColumn(table1, "id")

	// Create a mock condition
	mockCond := &mockCondition{
		clause: "`users`.`id` = 1",
		tables: map[string]Table{"users": table1},
	}

	qb := Select(col1).Where(mockCond)

	if len(qb.conditions) != 1 {
		t.Errorf("Expected 1 WHERE condition, got %d", len(qb.conditions))
	}

	if len(qb.conditionClauses) != 1 {
		t.Errorf("Expected 1 condition clause, got %d", len(qb.conditionClauses))
	}

	if qb.conditionClauses[0] != "`users`.`id` = 1" {
		t.Errorf("Expected condition clause '`users`.`id` = 1', got '%s'", qb.conditionClauses[0])
	}
}

func TestQueryBuilder_And(t *testing.T) {
	table1 := newMockTable("users")
	col1 := newMockColumn(table1, "id")

	mockCond1 := &mockCondition{
		clause: "`users`.`id` = 1",
		tables: map[string]Table{"users": table1},
	}

	mockCond2 := &mockCondition{
		clause: "`users`.`name` = 'test'",
		tables: map[string]Table{"users": table1},
	}

	qb := Select(col1).Where(mockCond1).And(mockCond2)

	if len(qb.conditions) != 2 {
		t.Errorf("Expected 2 conditions, got %d", len(qb.conditions))
	}

	if len(qb.conditionClauses) != 2 {
		t.Errorf("Expected 2 condition clauses, got %d", len(qb.conditionClauses))
	}
}

func TestQueryBuilder_AndIf(t *testing.T) {
	table1 := newMockTable("users")
	col1 := newMockColumn(table1, "id")

	mockCond1 := &mockCondition{
		clause: "`users`.`id` = 1",
		tables: map[string]Table{"users": table1},
	}

	mockCond2 := &mockCondition{
		clause: "`users`.`name` = 'test'",
		tables: map[string]Table{"users": table1},
	}

	// Test with true condition
	qb1 := Select(col1).Where(mockCond1).AndIf(true, mockCond2)
	if len(qb1.conditions) != 2 {
		t.Errorf("Expected 2 conditions when condition is true, got %d", len(qb1.conditions))
	}

	// Test with false condition
	qb2 := Select(col1).Where(mockCond1).AndIf(false, mockCond2)
	if len(qb2.conditions) != 1 {
		t.Errorf("Expected 1 condition when condition is false, got %d", len(qb2.conditions))
	}
}

func TestQueryBuilder_KwSearch(t *testing.T) {
	table1 := newMockTable("users")
	col1 := newMockColumn(table1, "name")
	col2 := newMockColumn(table1, "email")

	qb := Select(col1).KwSearch(col1, col2)

	if len(qb.kwCols) != 2 {
		t.Errorf("Expected 2 keyword search columns, got %d", len(qb.kwCols))
	}

	if len(qb.kwTables) != 1 {
		t.Errorf("Expected 1 keyword search table, got %d", len(qb.kwTables))
	}

	if _, exists := qb.kwTables["users"]; !exists {
		t.Error("Expected 'users' table to be in kwTables")
	}
}

func TestJoinType_Constants(t *testing.T) {
	tests := []struct {
		joinType JoinType
		expected string
	}{
		{LeftJoinType, "LEFT JOIN"},
		{InnerJoinType, "INNER JOIN"},
		{RightJoinType, "RIGHT JOIN"},
		{FullJoinType, "FULL JOIN"},
		{CrossJoinType, "CROSS JOIN"},
	}

	for _, tt := range tests {
		t.Run(string(tt.joinType), func(t *testing.T) {
			if string(tt.joinType) != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, string(tt.joinType))
			}
		})
	}
}

func TestQueryBuilder_ChainedOperations(t *testing.T) {
	table1 := newMockTable("users")
	table2 := newMockTable("orders")
	col1 := newMockColumn(table1, "id")
	col2 := newMockColumn(table1, "name")
	col3 := newMockColumn(table2, "user_id")

	mockCond := &mockCondition{
		clause: "`users`.`id` > 0",
		tables: map[string]Table{"users": table1},
	}

	qb := Select(col1, col2).
		LeftJoin(col1, col3).
		Where(mockCond).
		GroupBy(col2).
		Having(mockCond).
		KwSearch(col2)

	// Verify all operations were applied
	if len(qb.selectCols) != 2 {
		t.Errorf("Expected 2 select columns, got %d", len(qb.selectCols))
	}

	if len(qb.joins) != 1 {
		t.Errorf("Expected 1 join, got %d", len(qb.joins))
	}

	if len(qb.conditions) != 1 {
		t.Errorf("Expected 1 WHERE condition, got %d", len(qb.conditions))
	}

	if len(qb.groupByCols) != 1 {
		t.Errorf("Expected 1 GROUP BY column, got %d", len(qb.groupByCols))
	}

	if len(qb.havingConditions) != 1 {
		t.Errorf("Expected 1 HAVING condition, got %d", len(qb.havingConditions))
	}

	if len(qb.kwCols) != 1 {
		t.Errorf("Expected 1 keyword search column, got %d", len(qb.kwCols))
	}

	// Verify tables are properly tracked
	if len(qb.selectTables) != 2 {
		t.Errorf("Expected 2 tables in selectTables, got %d", len(qb.selectTables))
	}
}

// Mock condition for testing
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
