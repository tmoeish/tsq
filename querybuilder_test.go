package tsq

import (
	"strings"
	"testing"
)

// Mock table and column for testing
type mockTable struct {
	tableName string
}

func (m mockTable) Init(db *DbMap, upsertIndexies bool) error { return nil }
func (m mockTable) Table() string                             { return m.tableName }
func (m mockTable) Cols() []Column                            { return nil }
func (m mockTable) KwList() []Column                          { return nil }

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

func TestSelect_NilColumnDefersToBuildError(t *testing.T) {
	table := newMockTable("users")
	var col Column

	_, err := Select(col).
		From(table).Build()
	if err == nil {
		t.Fatal("expected nil select column to return an error")
	}

	if !strings.Contains(err.Error(), "column cannot be nil") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSelect_ZeroValueColumnDefersToBuildError(t *testing.T) {
	table := newMockTable("users")
	var col Col[int]

	_, err := Select(col).
		From(table).Build()
	if err == nil {
		t.Fatal("expected zero-value select column to return an error")
	}

	if !strings.Contains(err.Error(), "table cannot be nil") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestQueryBuilder_LeftJoin(t *testing.T) {
	table1 := newMockTable("users")
	table2 := newMockTable("orders")
	col1 := NewCol[string](table1, "id", "id", nil)
	col2 := NewCol[string](table2, "user_id", "user_id", nil)

	qb := Select(col1).
		From(col1.Table()).LeftJoin(table2, col1.EQCol(col2))

	if len(qb.spec.Joins) != 1 {
		t.Errorf("Expected 1 join, got %d", len(qb.spec.Joins))
	}

	join := qb.spec.Joins[0]
	if join.joinType != LeftJoinType {
		t.Errorf("Expected LEFT JOIN, got %s", join.joinType)
	}

	if join.table.Table() != "orders" {
		t.Errorf("Expected join table 'orders', got '%s'", join.table.Table())
	}

	if len(join.on) != 1 {
		t.Errorf("Expected 1 join condition, got %d", len(join.on))
	}

	// Check that both tables are added to selectTables
	if len(qb.spec.pageQueryTables()) != 2 {
		t.Errorf("Expected 2 tables in join graph, got %d", len(qb.spec.pageQueryTables()))
	}
}

func TestQueryBuilder_InnerJoin(t *testing.T) {
	table1 := newMockTable("users")
	table2 := newMockTable("orders")
	col1 := NewCol[string](table1, "id", "id", nil)
	col2 := NewCol[string](table2, "user_id", "user_id", nil)

	qb := Select(col1).
		From(col1.Table()).InnerJoin(table2, col1.EQCol(col2))

	if len(qb.spec.Joins) != 1 {
		t.Errorf("Expected 1 join, got %d", len(qb.spec.Joins))
	}

	if qb.spec.Joins[0].joinType != InnerJoinType {
		t.Errorf("Expected INNER JOIN, got %s", qb.spec.Joins[0].joinType)
	}
}

func TestQueryBuilder_RightJoin(t *testing.T) {
	table1 := newMockTable("users")
	table2 := newMockTable("orders")
	col1 := NewCol[string](table1, "id", "id", nil)
	col2 := NewCol[string](table2, "user_id", "user_id", nil)

	qb := Select(col1).
		From(col1.Table()).RightJoin(table2, col1.EQCol(col2))

	if len(qb.spec.Joins) != 1 {
		t.Errorf("Expected 1 join, got %d", len(qb.spec.Joins))
	}

	if qb.spec.Joins[0].joinType != RightJoinType {
		t.Errorf("Expected RIGHT JOIN, got %s", qb.spec.Joins[0].joinType)
	}
}

func TestQueryBuilder_FullJoin(t *testing.T) {
	table1 := newMockTable("users")
	table2 := newMockTable("orders")
	col1 := NewCol[string](table1, "id", "id", nil)
	col2 := NewCol[string](table2, "user_id", "user_id", nil)

	qb := Select(col1).
		From(col1.Table()).FullJoin(table2, col1.EQCol(col2))

	if len(qb.spec.Joins) != 1 {
		t.Errorf("Expected 1 join, got %d", len(qb.spec.Joins))
	}

	if qb.spec.Joins[0].joinType != FullJoinType {
		t.Errorf("Expected FULL JOIN, got %s", qb.spec.Joins[0].joinType)
	}
}

func TestQueryBuilder_CrossJoin(t *testing.T) {
	table1 := newMockTable("users")
	table2 := newMockTable("orders")
	col1 := newMockColumn(table1, "id")

	qb := Select(col1).
		From(col1.Table()).CrossJoin(table2)

	if len(qb.spec.Joins) != 1 {
		t.Errorf("Expected 1 join, got %d", len(qb.spec.Joins))
	}

	join := qb.spec.Joins[0]
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

	qb := Select(col1).
		From(col1.Table()).GroupBy(col1, col2)

	if len(qb.spec.GroupBy) != 2 {
		t.Errorf("Expected 2 GROUP BY columns, got %d", len(qb.spec.GroupBy))
	}

	if qb.spec.GroupBy[0].Name() != "department" {
		t.Errorf("Expected first GROUP BY column 'department', got '%s'", qb.spec.GroupBy[0].Name())
	}

	if qb.spec.GroupBy[1].Name() != "status" {
		t.Errorf("Expected second GROUP BY column 'status', got '%s'", qb.spec.GroupBy[1].Name())
	}
}

func TestQueryBuilder_Union(t *testing.T) {
	users := newMockTable("users")
	orders := newMockTable("orders")
	userID := newMockColumn(users, "id")
	orderUserID := newMockColumn(orders, "user_id")

	qb := Select(userID).
		From(userID.Table()).
		Union(Select(orderUserID).From(orderUserID.Table()))

	if len(qb.spec.SetOps) != 1 {
		t.Fatalf("expected 1 set operation, got %d", len(qb.spec.SetOps))
	}

	if qb.spec.SetOps[0].op != UnionType {
		t.Fatalf("expected UNION operation, got %s", qb.spec.SetOps[0].op)
	}
}

func TestQueryBuilder_SetOperationRejectsMismatchedSelectCounts(t *testing.T) {
	users := newMockTable("users")
	id := newMockColumn(users, "id")
	name := newMockColumn(users, "name")

	_, err := Select(id).
		From(id.Table()).
		Union(Select(id, name).From(id.Table())).
		Build()
	if err == nil {
		t.Fatal("expected mismatched select counts to fail")
	}

	if !strings.Contains(err.Error(), "matching select column counts") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestQueryBuilder_SetOperationRejectsKeywordSearch(t *testing.T) {
	users := newMockTable("users")
	id := newMockColumn(users, "id")

	_, err := Select(id).
		From(id.Table()).
		KwSearch(id).
		Union(Select(id).From(id.Table())).
		Build()
	if err == nil {
		t.Fatal("expected keyword search with set operations to fail")
	}

	if !strings.Contains(err.Error(), "do not support keyword search") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestQueryBuilder_SetOperationBuildsWrappedCountSQL(t *testing.T) {
	users := newMockTable("users")
	orders := newMockTable("orders")
	userID := newMockColumn(users, "id")
	orderUserID := newMockColumn(orders, "user_id")

	query := mustBuild(Select(userID).
		From(userID.Table()).
		UnionAll(Select(orderUserID).From(orderUserID.Table())))

	wantList := `SELECT "users"."id" FROM "users" UNION ALL SELECT "orders"."user_id" FROM "orders"`
	if query.ListSQL() != wantList {
		t.Fatalf("expected list SQL %q, got %q", wantList, query.ListSQL())
	}

	wantCount := `SELECT COUNT(1) FROM (` + wantList + `) AS _tsq_cnt`
	if query.CntSQL() != wantCount {
		t.Fatalf("expected count SQL %q, got %q", wantCount, query.CntSQL())
	}
}

func TestQueryBuilder_CTEBuildsWithClause(t *testing.T) {
	users := newMockTable("users")
	id := NewCol[int](users, "id", "id", nil)
	name := NewCol[string](users, "name", "name", nil)

	activeUsers := CTE("active_users", Select(id, name).
		From(id.Table()).Where(id.GT(10)))
	activeUserID := RebindColumn(id, activeUsers)
	activeUserName := RebindColumn(name, activeUsers)

	query := mustBuild(Select(activeUserID, activeUserName).From(activeUsers))

	wantList := `WITH "active_users" AS (SELECT "users"."id", "users"."name" FROM "users" WHERE "users"."id" > ?) SELECT "active_users"."id", "active_users"."name" FROM "active_users"`
	if query.ListSQL() != wantList {
		t.Fatalf("expected CTE list SQL %q, got %q", wantList, query.ListSQL())
	}

	wantCount := `WITH "active_users" AS (SELECT "users"."id", "users"."name" FROM "users" WHERE "users"."id" > ?) SELECT COUNT(1) FROM "active_users"`
	if query.CntSQL() != wantCount {
		t.Fatalf("expected CTE count SQL %q, got %q", wantCount, query.CntSQL())
	}
}

func TestQueryBuilder_CTECollectsNestedDependencies(t *testing.T) {
	users := newMockTable("users")
	id := NewCol[int](users, "id", "id", nil)

	baseUsers := CTE("base_users", Select(id).
		From(id.Table()).Where(id.GT(1)))
	baseUserID := RebindColumn(id, baseUsers)
	filteredUsers := CTE("filtered_users", Select(baseUserID).From(baseUserID.Table()))
	filteredUserID := RebindColumn(id, filteredUsers)

	query := mustBuild(Select(filteredUserID).From(filteredUsers))

	want := `WITH "base_users" AS (SELECT "users"."id" FROM "users" WHERE "users"."id" > ?), "filtered_users" AS (SELECT "base_users"."id" FROM "base_users") SELECT "filtered_users"."id" FROM "filtered_users"`
	if query.ListSQL() != want {
		t.Fatalf("expected nested CTE SQL %q, got %q", want, query.ListSQL())
	}
}

func TestQueryBuilder_CTERejectsKeywordSearchInDefinition(t *testing.T) {
	users := newMockTable("users")
	id := NewCol[int](users, "id", "id", nil)
	name := NewCol[string](users, "name", "name", nil)

	searchUsers := CTE("search_users", Select(id, name).
		From(id.Table()).KwSearch(name))
	searchUserID := RebindColumn(id, searchUsers)

	_, err := Select(searchUserID).
		From(searchUserID.Table()).Build()
	if err == nil {
		t.Fatal("expected CTE keyword search definition to fail")
	}

	if !strings.Contains(err.Error(), "does not support keyword search") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestQueryBuilder_CaseExpressionTracksConditionTables(t *testing.T) {
	users := newMockTable("users")
	orgs := newMockTable("orgs")
	userID := NewCol[int](users, "id", "id", nil)
	orgID := NewCol[int](orgs, "id", "id", nil)
	orgName := NewCol[string](orgs, "name", "name", nil)

	label := Case[string]().
		When(orgID.EQ(1), orgName).
		Else("unknown").
		End()

	_, err := Select(userID, label).
		From(userID.Table()).Build()
	if err == nil {
		t.Fatal("expected CASE expression to surface orgs table into join validation")
	}

	if !strings.Contains(err.Error(), "use CrossJoin") {
		t.Fatalf("unexpected error: %v", err)
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

	qb := Select(col1).
		From(col1.Table()).Having(mockCond)

	if len(qb.spec.Having) != 1 {
		t.Errorf("Expected 1 HAVING condition, got %d", len(qb.spec.Having))
	}
}

func TestQueryBuilder_HavingRejectsEmptyConditionClause(t *testing.T) {
	table := newMockTable("users")
	col := newMockColumn(table, "count")

	_, err := Select(col).
		From(col.Table()).Having(Cond{}).Build()
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

	// Create a mock condition
	mockCond := &mockCondition{
		clause: "`users`.`id` = 1",
		tables: map[string]Table{"users": table1},
	}

	qb := Select(col1).
		From(col1.Table()).Where(mockCond)

	if len(qb.spec.Filters) != 1 {
		t.Errorf("Expected 1 WHERE condition, got %d", len(qb.spec.Filters))
	}

	if len(qb.spec.Filters) != 1 {
		t.Errorf("Expected 1 condition clause, got %d", len(qb.spec.Filters))
	}

	if conditionClause(qb.spec.Filters[0]) != "`users`.`id` = 1" {
		t.Errorf("Expected condition clause '`users`.`id` = 1', got '%s'", conditionClause(qb.spec.Filters[0]))
	}
}

func TestQueryBuilder_SetWhereMatchesWhereOverwriteBehavior(t *testing.T) {
	table1 := newMockTable("users")
	col1 := newMockColumn(table1, "id")

	mockCond1 := &mockCondition{
		clause: "`users`.`id` = 1",
		tables: map[string]Table{"users": table1},
	}
	mockCond2 := &mockCondition{
		clause: "`users`.`id` = 2",
		tables: map[string]Table{"users": table1},
	}

	qb := Select(col1).
		From(col1.Table()).Where(mockCond1).SetWhere(mockCond2)
	if len(qb.spec.Filters) != 1 {
		t.Fatalf("expected SetWhere to overwrite filters, got %d", len(qb.spec.Filters))
	}
	if conditionClause(qb.spec.Filters[0]) != "`users`.`id` = 2" {
		t.Fatalf("expected SetWhere to keep latest condition, got %q", conditionClause(qb.spec.Filters[0]))
	}
}

func TestQueryBuilder_WhereRejectsNilCondition(t *testing.T) {
	table := newMockTable("users")
	col := newMockColumn(table, "id")

	var cond Condition

	_, err := Select(col).
		From(col.Table()).Where(cond).Build()
	if err == nil {
		t.Fatal("expected nil WHERE condition to return an error")
	}

	if !strings.Contains(err.Error(), "condition cannot be nil") {
		t.Fatalf("unexpected error: %v", err)
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

	qb := Select(col1).
		From(col1.Table()).Where(mockCond1).And(mockCond2)

	if len(qb.spec.Filters) != 2 {
		t.Errorf("Expected 2 conditions, got %d", len(qb.spec.Filters))
	}

	if len(qb.spec.Filters) != 2 {
		t.Errorf("Expected 2 condition clauses, got %d", len(qb.spec.Filters))
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
	qb1 := Select(col1).
		From(col1.Table()).Where(mockCond1).AndIf(true, mockCond2)
	if len(qb1.spec.Filters) != 2 {
		t.Errorf("Expected 2 conditions when condition is true, got %d", len(qb1.spec.Filters))
	}

	// Test with false condition
	qb2 := Select(col1).
		From(col1.Table()).Where(mockCond1).AndIf(false, mockCond2)
	if len(qb2.spec.Filters) != 1 {
		t.Errorf("Expected 1 condition when condition is false, got %d", len(qb2.spec.Filters))
	}
}

func TestQueryBuilder_KwSearch(t *testing.T) {
	table1 := newMockTable("users")
	col1 := newMockColumn(table1, "name")
	col2 := newMockColumn(table1, "email")

	qb := Select(col1).
		From(col1.Table()).KwSearch(col1, col2)

	if len(qb.spec.KeywordSearch) != 2 {
		t.Errorf("Expected 2 keyword search columns, got %d", len(qb.spec.KeywordSearch))
	}

	kwTables := qb.spec.keywordTables()
	if len(kwTables) != 1 {
		t.Errorf("Expected 1 keyword search table, got %d", len(kwTables))
	}

	if _, exists := kwTables["users"]; !exists {
		t.Error("Expected 'users' table to be in kwTables")
	}
}

func TestQueryBuilder_SetKwSearchMatchesKwSearchOverwriteBehavior(t *testing.T) {
	table1 := newMockTable("users")
	col1 := newMockColumn(table1, "name")
	col2 := newMockColumn(table1, "email")

	qb := Select(col1).
		From(col1.Table()).KwSearch(col1).SetKwSearch(col2)
	if len(qb.spec.KeywordSearch) != 1 {
		t.Fatalf("expected SetKwSearch to overwrite keyword search columns, got %d", len(qb.spec.KeywordSearch))
	}
	if qb.spec.KeywordSearch[0].Name() != "email" {
		t.Fatalf("expected SetKwSearch to keep latest keyword search column, got %q", qb.spec.KeywordSearch[0].Name())
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
	col1 := NewCol[string](table1, "id", "id", nil)
	col2 := NewCol[string](table1, "name", "name", nil)
	col3 := NewCol[string](table2, "user_id", "user_id", nil)

	mockCond := &mockCondition{
		clause: "`users`.`id` > 0",
		tables: map[string]Table{"users": table1},
	}

	qb := Select(col1, col2).
		From(col1.Table()).
		LeftJoin(table2, col1.EQCol(col3)).
		Where(mockCond).
		GroupBy(col2).
		Having(mockCond).
		KwSearch(col2)

	// Verify all operations were applied
	if len(qb.spec.Selects) != 2 {
		t.Errorf("Expected 2 select columns, got %d", len(qb.spec.Selects))
	}

	if len(qb.spec.Joins) != 1 {
		t.Errorf("Expected 1 join, got %d", len(qb.spec.Joins))
	}

	if len(qb.spec.Filters) != 1 {
		t.Errorf("Expected 1 WHERE condition, got %d", len(qb.spec.Filters))
	}

	if len(qb.spec.GroupBy) != 1 {
		t.Errorf("Expected 1 GROUP BY column, got %d", len(qb.spec.GroupBy))
	}

	if len(qb.spec.Having) != 1 {
		t.Errorf("Expected 1 HAVING condition, got %d", len(qb.spec.Having))
	}

	if len(qb.spec.KeywordSearch) != 1 {
		t.Errorf("Expected 1 keyword search column, got %d", len(qb.spec.KeywordSearch))
	}

	if len(qb.spec.pageQueryTables()) != 2 {
		t.Errorf("Expected 2 tables in planned query, got %d", len(qb.spec.pageQueryTables()))
	}
}

func TestQueryBuilder_GroupedCountUsesWrappedSubquery(t *testing.T) {
	table := newMockTable("users")
	department := newMockColumn(table, "department")
	having := &mockCondition{
		clause: "COUNT(*) > 1",
		tables: map[string]Table{"users": table},
	}

	query := mustBuild(Select(department).
		From(department.Table()).
		GroupBy(department).
		Having(having))

	wantList := `SELECT "users"."department" FROM "users" GROUP BY "users"."department" HAVING COUNT(*) > 1`
	if query.ListSQL() != wantList {
		t.Fatalf("expected list SQL %q, got %q", wantList, query.ListSQL())
	}

	wantCount := "SELECT COUNT(1) FROM (" + wantList + ") AS _tsq_cnt"
	if query.CntSQL() != wantCount {
		t.Fatalf("expected count SQL %q, got %q", wantCount, query.CntSQL())
	}
}

func TestQueryBuilder_DistinctCountUsesWrappedSubquery(t *testing.T) {
	table := newMockTable("users")
	name := NewCol[string](table, "name", "name", nil)

	query := mustBuild(Select(name.Distinct()).From(name.Table()))

	wantList := `SELECT DISTINCT("users"."name") FROM "users"`
	if query.ListSQL() != wantList {
		t.Fatalf("expected list SQL %q, got %q", wantList, query.ListSQL())
	}

	wantCount := "SELECT COUNT(1) FROM (" + wantList + ") AS _tsq_cnt"
	if query.CntSQL() != wantCount {
		t.Fatalf("expected count SQL %q, got %q", wantCount, query.CntSQL())
	}
}

func TestQueryBuilder_AggregateCountUsesWrappedSubquery(t *testing.T) {
	table := newMockTable("users")
	id := NewCol[int](table, "id", "id", nil)

	query := mustBuild(Select(id.Count()).From(id.Table()))

	wantList := `SELECT COUNT("users"."id") FROM "users"`
	if query.ListSQL() != wantList {
		t.Fatalf("expected list SQL %q, got %q", wantList, query.ListSQL())
	}

	wantCount := "SELECT COUNT(1) FROM (" + wantList + ") AS _tsq_cnt"
	if query.CntSQL() != wantCount {
		t.Fatalf("expected count SQL %q, got %q", wantCount, query.CntSQL())
	}
}

func TestQueryBuilder_HavingKeepsRawClauseForDialectRendering(t *testing.T) {
	users := newMockTable("users")
	id := NewCol[int](users, "id", "id", nil)

	q, err := Select(id).
		From(id.Table()).GroupBy(id).Having(id.GT(1)).Build()
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}

	rendered := renderSQLForDialect(q.listSQL, MySQLDialect{})
	if !strings.Contains(rendered, "HAVING `users`.`id` > ?") {
		t.Fatalf("expected HAVING clause to use dialect identifiers, got %s", rendered)
	}

	if strings.Contains(rendered, `"users"."id"`) {
		t.Fatalf("expected HAVING clause not to leak canonical identifiers into dialect SQL, got %s", rendered)
	}
}

func TestQueryBuilder_CrossJoinKeepsSelectedBaseTable(t *testing.T) {
	users := newMockTable("users")
	orders := newMockTable("orders")
	userID := newMockColumn(users, "id")

	query := mustBuild(Select(userID).
		From(userID.Table()).CrossJoin(orders))
	want := `SELECT "users"."id" FROM "users" CROSS JOIN "orders"`

	if query.ListSQL() != want {
		t.Fatalf("expected cross join SQL %q, got %q", want, query.ListSQL())
	}
}

func TestQueryBuilder_Build_RejectsTablesReferencedOutsideJoinGraph(t *testing.T) {
	users := newMockTable("users")
	orgs := newMockTable("orgs")
	items := newMockTable("items")
	userID := NewCol[int](users, "id", "id", nil)
	userOrgID := NewCol[int](users, "org_id", "org_id", nil)
	orgID := NewCol[int](orgs, "id", "id", nil)
	itemID := NewCol[int](items, "id", "id", nil)

	_, err := Select(userID).
		From(userID.Table()).
		LeftJoin(orgs, userOrgID.EQCol(orgID)).
		Where(itemID.EQVar()).
		Build()
	if err == nil {
		t.Fatal("expected unjoined table reference to return an error")
	}

	if !strings.Contains(err.Error(), "use CrossJoin") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestQueryBuilder_WhereReplacesConditionTables(t *testing.T) {
	users := newMockTable("users")
	orders := newMockTable("orders")
	userID := NewCol[int](users, "id", "id", nil)
	orderID := NewCol[int](orders, "id", "id", nil)

	query := mustBuild(Select(userID).
		From(userID.Table()).
		Where(orderID.EQVar()).
		Where(userID.EQVar()))

	want := `SELECT "users"."id" FROM "users" WHERE "users"."id" = ?`
	if query.ListSQL() != want {
		t.Fatalf("expected repeated Where to replace condition tables, got %q", query.ListSQL())
	}
}

func TestQueryBuilder_Build_RejectsDisconnectedJoinGraph(t *testing.T) {
	users := newMockTable("users")
	orgs := newMockTable("orgs")
	orders := newMockTable("orders")
	items := newMockTable("items")

	userID := NewCol[int](users, "id", "id", nil)
	userOrgID := NewCol[int](users, "org_id", "org_id", nil)
	orgID := NewCol[int](orgs, "id", "id", nil)
	orderItemID := NewCol[int](orders, "item_id", "item_id", nil)
	itemID := NewCol[int](items, "id", "id", nil)

	_, err := Select(userID).
		From(userID.Table()).
		LeftJoin(orgs, userOrgID.EQCol(orgID)).
		LeftJoin(items, orderItemID.EQCol(itemID)).
		Build()
	if err == nil {
		t.Fatal("expected disconnected join graph to return an error")
	}

	if !strings.Contains(err.Error(), "not connected") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestQueryBuilder_Build_RejectsRepeatedJoinTableWithoutAliases(t *testing.T) {
	users := newMockTable("users")
	orgs := newMockTable("orgs")

	userID := NewCol[int](users, "id", "id", nil)
	userOrgID := NewCol[int](users, "org_id", "org_id", nil)
	orgID := NewCol[int](orgs, "id", "id", nil)
	orgParentID := NewCol[int](orgs, "parent_id", "parent_id", nil)

	_, err := Select(userID).
		From(userID.Table()).
		LeftJoin(orgs, userOrgID.EQCol(orgID)).
		LeftJoin(orgs, orgParentID.EQCol(orgID)).
		Build()
	if err == nil {
		t.Fatal("expected repeated join table to return an error")
	}

	errMsg := err.Error()
	if !strings.Contains(errMsg, "aliases are required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestQueryBuilder_Build_AllowsRepeatedJoinTableWithAliases(t *testing.T) {
	users := newMockTable("users")
	orgs := newMockTable("orgs")

	userID := NewCol[int](users, "id", "id", nil)
	userOrgID := NewCol[int](users, "org_id", "org_id", nil)
	orgID := NewCol[int](orgs, "id", "id", nil)
	parentOrgID := orgID.As("parent_orgs")

	query, err := Select(userID, parentOrgID).
		From(userID.Table()).
		LeftJoin(orgs, userOrgID.EQCol(orgID)).
		LeftJoin(parentOrgID.Table(), NewCol[int](orgs, "parent_id", "parent_id", nil).EQCol(parentOrgID)).
		Build()
	if err != nil {
		t.Fatalf("expected aliased repeated join to build, got %v", err)
	}

	want := `SELECT "users"."id", "parent_orgs"."id" FROM "users" LEFT JOIN "orgs" ON "users"."org_id" = "orgs"."id" LEFT JOIN "orgs" AS "parent_orgs" ON "orgs"."parent_id" = "parent_orgs"."id"`
	if got := query.ListSQL(); got != want {
		t.Fatalf("expected aliased join SQL %q, got %q", want, got)
	}
}

func TestQueryBuilder_Build_RejectsNilReceiver(t *testing.T) {
	var qb *QueryBuilder

	_, err := qb.Build()
	if err == nil {
		t.Fatal("expected nil query builder to return an error")
	}

	if !strings.Contains(err.Error(), "query builder cannot be nil") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestQueryBuilder_MethodsHandleNilReceiverWithoutPanicking(t *testing.T) {
	users := newMockTable("users")
	userID := NewCol[int](users, "id", "id", nil)

	var qb *QueryBuilder

	_, err := qb.
		Where(userID.EQVar()).
		GroupBy(userID).
		KwSearch(userID).
		Build()
	if err == nil {
		t.Fatal("expected nil receiver chain to return an error")
	}

	if !strings.Contains(err.Error(), "query builder cannot be nil") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestQueryBuilder_MethodsInitializeZeroValueBuilder(t *testing.T) {
	users := newMockTable("users")
	userID := NewCol[int](users, "id", "id", nil)
	qb := &QueryBuilder{}

	got := qb.
		GroupBy(userID).
		Where(userID.EQVar()).
		KwSearch(userID)
	if got != qb {
		t.Fatal("expected zero-value builder methods to reuse the same builder")
	}

	if len(qb.spec.GroupBy) != 1 {
		t.Fatalf("expected group by column to be recorded, got %d", len(qb.spec.GroupBy))
	}

	if len(qb.spec.Filters) != 1 {
		t.Fatalf("expected where clause to be recorded, got %d", len(qb.spec.Filters))
	}

	if len(qb.spec.KeywordSearch) != 1 {
		t.Fatalf("expected keyword search column to be recorded, got %d", len(qb.spec.KeywordSearch))
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

func (m *mockCondition) Args() []any {
	return nil
}
