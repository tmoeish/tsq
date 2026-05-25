package tsq

import (
	"strings"
	"testing"
)

func TestQueryBuilder_LeftJoin(t *testing.T) {
	table1 := newMockTable("users")
	table2 := newMockTable("orders")
	col1 := newColForTable[Table, string](table1, "id", "id", nil)
	col2 := newColForTable[Table, string](table2, "user_id", "user_id", nil)
	qb := Select(col1).From(col1.Table()).LeftJoin(table2, col1.EQ(col2))
	core := mustBuilderCore[Table](t, qb)
	if len(core.spec.Joins) != 1 {
		t.Errorf("Expected 1 join, got %d", len(core.spec.Joins))
	}
	join := core.spec.Joins[0]
	if join.joinType != leftJoinType {
		t.Errorf("Expected LEFT JOIN, got %s", join.joinType)
	}
	if join.table.Table() != "orders" {
		t.Errorf("Expected join table 'orders', got '%s'", join.table.Table())
	}
	if len(join.on) != 1 {
		t.Errorf("Expected 1 join condition, got %d", len(join.on))
	}
	if len(core.spec.pageQueryTables()) != 2 {
		t.Errorf("Expected 2 tables in join graph, got %d", len(core.spec.pageQueryTables()))
	}
}

func TestQueryBuilder_InnerJoin(t *testing.T) {
	table1 := newMockTable("users")
	table2 := newMockTable("orders")
	col1 := newColForTable[Table, string](table1, "id", "id", nil)
	col2 := newColForTable[Table, string](table2, "user_id", "user_id", nil)
	qb := Select(col1).From(col1.Table()).InnerJoin(table2, col1.EQ(col2))
	core := mustBuilderCore[Table](t, qb)
	if len(core.spec.Joins) != 1 {
		t.Errorf("Expected 1 join, got %d", len(core.spec.Joins))
	}
	if core.spec.Joins[0].joinType != innerJoinType {
		t.Errorf("Expected INNER JOIN, got %s", core.spec.Joins[0].joinType)
	}
}

func TestQueryBuilder_RightJoin(t *testing.T) {
	table1 := newMockTable("users")
	table2 := newMockTable("orders")
	col1 := newColForTable[Table, string](table1, "id", "id", nil)
	col2 := newColForTable[Table, string](table2, "user_id", "user_id", nil)
	qb := Select(col1).From(col1.Table()).RightJoin(table2, col1.EQ(col2))
	core := mustBuilderCore[Table](t, qb)
	if len(core.spec.Joins) != 1 {
		t.Errorf("Expected 1 join, got %d", len(core.spec.Joins))
	}
	if core.spec.Joins[0].joinType != rightJoinType {
		t.Errorf("Expected RIGHT JOIN, got %s", core.spec.Joins[0].joinType)
	}
}

func TestQueryBuilder_FullJoin(t *testing.T) {
	table1 := newMockTable("users")
	table2 := newMockTable("orders")
	col1 := newColForTable[Table, string](table1, "id", "id", nil)
	col2 := newColForTable[Table, string](table2, "user_id", "user_id", nil)
	qb := Select(col1).From(col1.Table()).FullJoin(table2, col1.EQ(col2))
	core := mustBuilderCore[Table](t, qb)
	if len(core.spec.Joins) != 1 {
		t.Errorf("Expected 1 join, got %d", len(core.spec.Joins))
	}
	if core.spec.Joins[0].joinType != fullJoinType {
		t.Errorf("Expected FULL JOIN, got %s", core.spec.Joins[0].joinType)
	}
}

func TestQueryBuilder_CrossJoin(t *testing.T) {
	table1 := newMockTable("users")
	table2 := newMockTable("orders")
	col1 := newMockColumn(table1, "id")
	qb := Select(col1).From(col1.Table()).CrossJoin(table2)
	core := mustBuilderCore[Table](t, qb)
	if len(core.spec.Joins) != 1 {
		t.Errorf("Expected 1 join, got %d", len(core.spec.Joins))
	}
	join := core.spec.Joins[0]
	if join.joinType != crossJoinType {
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
	qb := Select(col1).From(col1.Table()).GroupBy(col1, col2)
	core := mustBuilderCore[Table](t, qb)
	if len(core.spec.GroupBy) != 2 {
		t.Errorf("Expected 2 GROUP BY columns, got %d", len(core.spec.GroupBy))
	}
	if core.spec.GroupBy[0].Name() != "department" {
		t.Errorf("Expected first GROUP BY column 'department', got '%s'", core.spec.GroupBy[0].Name())
	}
	if core.spec.GroupBy[1].Name() != "status" {
		t.Errorf("Expected second GROUP BY column 'status', got '%s'", core.spec.GroupBy[1].Name())
	}
}

func TestQueryBuilder_Having(t *testing.T) {
	table1 := newMockTable("users")
	col1 := newMockColumn(table1, "count")
	mockCond := &mockCondition{clause: "COUNT(*) > 5", tables: map[string]Table{"users": table1}}
	qb := Select(col1).From(col1.Table()).GroupBy(col1).Having(mockCond)
	core := mustBuilderCore[Table](t, qb)
	if len(core.spec.Having) != 1 {
		t.Errorf("Expected 1 HAVING condition, got %d", len(core.spec.Having))
	}
}

func TestQueryBuilder_HavingRejectsEmptyConditionClause(t *testing.T) {
	table := newMockTable("users")
	col := newMockColumn(table, "count")
	_, err := Select(col).From(col.Table()).GroupBy(col).Having(conditionImpl{}).Build()
	if err == nil {
		t.Fatal("expected empty HAVING condition clause to return an error")
	}
	if !strings.Contains(err.Error(), "condition clause cannot be empty") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestQueryBuilder_Where(t *testing.T) {
	table1 := newMockTable("users")
	col1 := newMockColumn(table1, "id")
	mockCond := &mockCondition{clause: "`users`.`id` = 1", tables: map[string]Table{"users": table1}}
	qb := Select(col1).From(col1.Table()).Where(mockCond)
	core := mustBuilderCore[Table](t, qb)
	if len(core.spec.Filters) != 1 {
		t.Errorf("Expected 1 WHERE condition, got %d", len(core.spec.Filters))
	}
	if len(core.spec.Filters) != 1 {
		t.Errorf("Expected 1 condition clause, got %d", len(core.spec.Filters))
	}
	if conditionClause(core.spec.Filters[0]) != "`users`.`id` = 1" {
		t.Errorf("Expected condition clause '`users`.`id` = 1', got '%s'", conditionClause(core.spec.Filters[0]))
	}
}

func TestQueryBuilder_WhereRejectsNilCondition(t *testing.T) {
	table := newMockTable("users")
	col := newMockColumn(table, "id")
	var cond Condition
	_, err := Select(col).From(col.Table()).Where(cond).Build()
	if err == nil {
		t.Fatal("expected nil WHERE condition to return an error")
	}
	if !strings.Contains(err.Error(), "condition cannot be nil") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestQueryBuilder_WhereAcceptsMultipleConditions(t *testing.T) {
	table1 := newMockTable("users")
	col1 := newMockColumn(table1, "id")
	mockCond1 := &mockCondition{clause: "`users`.`id` = 1", tables: map[string]Table{"users": table1}}
	mockCond2 := &mockCondition{clause: "`users`.`name` = 'test'", tables: map[string]Table{"users": table1}}
	qb := Select(col1).From(col1.Table()).Where(mockCond1, mockCond2)
	core := mustBuilderCore[Table](t, qb)
	if len(core.spec.Filters) != 2 {
		t.Errorf("Expected 2 conditions, got %d", len(core.spec.Filters))
	}
	if len(core.spec.Filters) != 2 {
		t.Errorf("Expected 2 condition clauses, got %d", len(core.spec.Filters))
	}
}

func TestQueryBuilder_KwSearch(t *testing.T) {
	table1 := newMockTable("users")
	col1 := newMockColumn(table1, "name")
	col2 := newMockColumn(table1, "email")
	qb := Select(col1).From(col1.Table()).Search(col1, col2)
	core := mustBuilderCore[Table](t, qb)
	if len(core.spec.KeywordSearch) != 2 {
		t.Errorf("Expected 2 keyword search columns, got %d", len(core.spec.KeywordSearch))
	}
	kwTables := core.spec.keywordTables()
	if len(kwTables) != 1 {
		t.Errorf("Expected 1 keyword search table, got %d", len(kwTables))
	}
	if _, exists := kwTables["users"]; !exists {
		t.Error("Expected 'users' table to be in kwTables")
	}
}

func TestQueryBuilder_ChainedOperations(t *testing.T) {
	table1 := newMockTable("users")
	table2 := newMockTable("orders")
	col1 := newColForTable[Table, string](table1, "id", "id", nil)
	col2 := newColForTable[Table, string](table1, "name", "name", nil)
	col3 := newColForTable[Table, string](table2, "user_id", "user_id", nil)
	mockCond := &mockCondition{clause: "`users`.`id` > 0", tables: map[string]Table{"users": table1}}
	qb := Select(col1, col2).From(col1.Table()).LeftJoin(table2, col1.EQ(col3)).Search(col2).Where(mockCond).GroupBy(col2).Having(mockCond)
	core := mustBuilderCore[Table](t, qb)
	if len(core.spec.Selects) != 2 {
		t.Errorf("Expected 2 select columns, got %d", len(core.spec.Selects))
	}
	if len(core.spec.Joins) != 1 {
		t.Errorf("Expected 1 join, got %d", len(core.spec.Joins))
	}
	if len(core.spec.Filters) != 1 {
		t.Errorf("Expected 1 WHERE condition, got %d", len(core.spec.Filters))
	}
	if len(core.spec.GroupBy) != 1 {
		t.Errorf("Expected 1 GROUP BY column, got %d", len(core.spec.GroupBy))
	}
	if len(core.spec.Having) != 1 {
		t.Errorf("Expected 1 HAVING condition, got %d", len(core.spec.Having))
	}
	if len(core.spec.KeywordSearch) != 1 {
		t.Errorf("Expected 1 keyword search column, got %d", len(core.spec.KeywordSearch))
	}
	if len(core.spec.pageQueryTables()) != 2 {
		t.Errorf("Expected 2 tables in planned query, got %d", len(core.spec.pageQueryTables()))
	}
}
