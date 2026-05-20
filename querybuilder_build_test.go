package tsq

import (
	"strings"
	"testing"
)

func TestQueryBuilder_GroupedCountUsesWrappedSubquery(t *testing.T) {
	table := newMockTable("users")
	department := newMockColumn(table, "department")
	having := &mockCondition{clause: "COUNT(*) > 1", tables: map[string]Table{"users": table}}
	query := mustBuild(Select(department).From(department.Table()).GroupBy(department).Having(having))
	wantList := `SELECT "users"."department" FROM "users" GROUP BY "users"."department" HAVING COUNT(*) > 1`
	if query.ListSQL() != wantList {
		t.Fatalf("expected list SQL %q, got %q", wantList, query.ListSQL())
	}
	wantCount := "SELECT COUNT(1) FROM (" + wantList + ") AS _tsq_cnt"
	if query.CountSQL() != wantCount {
		t.Fatalf("expected count SQL %q, got %q", wantCount, query.CountSQL())
	}
}
func TestQueryBuilder_DistinctCountUsesWrappedSubquery(t *testing.T) {
	table := newMockTable("users")
	name := newColForTable[Table, string](table, "name", "name", nil)
	query := mustBuild(Select(name.Distinct()).From(name.Table()))
	wantList := `SELECT DISTINCT("users"."name") FROM "users"`
	if query.ListSQL() != wantList {
		t.Fatalf("expected list SQL %q, got %q", wantList, query.ListSQL())
	}
	wantCount := "SELECT COUNT(1) FROM (" + wantList + ") AS _tsq_cnt"
	if query.CountSQL() != wantCount {
		t.Fatalf("expected count SQL %q, got %q", wantCount, query.CountSQL())
	}
}
func TestQueryBuilder_AggregateCountUsesWrappedSubquery(t *testing.T) {
	table := newMockTable("users")
	id := newColForTable[Table, int](table, "id", "id", nil)
	query := mustBuild(Select(id.Count()).From(id.Table()))
	wantList := `SELECT COUNT("users"."id") FROM "users"`
	if query.ListSQL() != wantList {
		t.Fatalf("expected list SQL %q, got %q", wantList, query.ListSQL())
	}
	wantCount := "SELECT COUNT(1) FROM (" + wantList + ") AS _tsq_cnt"
	if query.CountSQL() != wantCount {
		t.Fatalf("expected count SQL %q, got %q", wantCount, query.CountSQL())
	}
}
func TestQueryBuilder_HavingKeepsRawClauseForDialectRendering(t *testing.T) {
	users := newMockTable("users")
	id := newColForTable[Table, int](users, "id", "id", nil)
	q, err := Select(id).From(id.Table()).GroupBy(id).Having(id.GT(1)).Build()
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
	query := mustBuild(Select(userID).From(userID.Table()).CrossJoin(orders))
	want := `SELECT "users"."id" FROM "users" CROSS JOIN "orders"`
	if query.ListSQL() != want {
		t.Fatalf("expected cross join SQL %q, got %q", want, query.ListSQL())
	}
}
func TestQueryBuilder_Build_RejectsTablesReferencedOutsideJoinGraph(t *testing.T) {
	users := newMockTable("users")
	orgs := newMockTable("orgs")
	items := newMockTable("items")
	userID := newColForTable[Table, int](users, "id", "id", nil)
	userOrgID := newColForTable[Table, int](users, "org_id", "org_id", nil)
	orgID := newColForTable[Table, int](orgs, "id", "id", nil)
	itemID := newColForTable[Table, int](items, "id", "id", nil)
	_, err := Select(userID).From(userID.Table()).LeftJoin(orgs, userOrgID.EQCol(orgID)).Where(itemID.EQVar()).Build()
	if err == nil {
		t.Fatal("expected unjoined table reference to return an error")
	}
	if !strings.Contains(err.Error(), "use CrossJoin") {
		t.Fatalf("unexpected error: %v", err)
	}
}
func TestQueryBuilder_WhereTracksAllConditionTables(t *testing.T) {
	users := newMockTable("users")
	userID := newColForTable[Table, int](users, "id", "id", nil)
	query := mustBuild(Select(userID).From(userID.Table()).Where(userID.EQVar(), userID.EQVar()))
	want := `SELECT "users"."id" FROM "users" WHERE ("users"."id" = ? AND "users"."id" = ?)`
	if query.ListSQL() != want {
		t.Fatalf("expected Where to keep all condition tables, got %q", query.ListSQL())
	}
}
func TestQueryBuilder_Build_RejectsDisconnectedJoinGraph(t *testing.T) {
	users := newMockTable("users")
	orgs := newMockTable("orgs")
	orders := newMockTable("orders")
	items := newMockTable("items")
	userID := newColForTable[Table, int](users, "id", "id", nil)
	userOrgID := newColForTable[Table, int](users, "org_id", "org_id", nil)
	orgID := newColForTable[Table, int](orgs, "id", "id", nil)
	orderItemID := newColForTable[Table, int](orders, "item_id", "item_id", nil)
	itemID := newColForTable[Table, int](items, "id", "id", nil)
	_, err := Select(userID).From(userID.Table()).LeftJoin(orgs, userOrgID.EQCol(orgID)).LeftJoin(items, orderItemID.EQCol(itemID)).Build()
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
	userID := newColForTable[Table, int](users, "id", "id", nil)
	userOrgID := newColForTable[Table, int](users, "org_id", "org_id", nil)
	orgID := newColForTable[Table, int](orgs, "id", "id", nil)
	orgParentID := newColForTable[Table, int](orgs, "parent_id", "parent_id", nil)
	_, err := Select(userID).From(userID.Table()).LeftJoin(orgs, userOrgID.EQCol(orgID)).LeftJoin(orgs, orgParentID.EQCol(orgID)).Build()
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
	userID := newColForTable[Table, int](users, "id", "id", nil)
	userOrgID := newColForTable[Table, int](users, "org_id", "org_id", nil)
	orgID := newColForTable[Table, int](orgs, "id", "id", nil)
	parentOrgID := orgID.As("parent_orgs")
	query, err := Select(userID, parentOrgID).From(userID.Table()).LeftJoin(orgs, userOrgID.EQCol(orgID)).LeftJoin(parentOrgID.Table(), newColForTable[Table, int](orgs, "parent_id", "parent_id", nil).EQCol(parentOrgID)).Build()
	if err != nil {
		t.Fatalf("expected aliased repeated join to build, got %v", err)
	}
	want := `SELECT "users"."id", "parent_orgs"."id" FROM "users" LEFT JOIN "orgs" ON "users"."org_id" = "orgs"."id" LEFT JOIN "orgs" AS "parent_orgs" ON "orgs"."parent_id" = "parent_orgs"."id"`
	if got := query.ListSQL(); got != want {
		t.Fatalf("expected aliased join SQL %q, got %q", want, got)
	}
}
func TestQueryBuilder_Build_RejectsNilReceiver(t *testing.T) {
	var qb *QueryBuilder[Table]
	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("expected nil query builder to panic")
		}
		err, ok := recovered.(error)
		if !ok || !strings.Contains(err.Error(), "query builder cannot be nil") {
			t.Fatalf("unexpected panic: %#v", recovered)
		}
	}()
	qb.Build()
}
func TestQueryBuilder_Build_PreservesOwnerType(t *testing.T) {
	users := newMockTable("users")
	userID := newColForTable[Table, int](users, "id", "id", nil)
	qb := Select(userID).From(users)
	var build func() (*Query[Table], error) = qb.Build
	query, err := build()
	if err != nil {
		t.Fatalf("expected typed build to succeed, got %v", err)
	}
	if query == nil {
		t.Fatal("expected typed build to return a query")
	}
}
func TestQueryBuilder_MethodsHandleNilReceiverWithoutPanicking(t *testing.T) {
	users := newMockTable("users")
	userID := newColForTable[Table, int](users, "id", "id", nil)
	var qb *QueryBuilder[Table]
	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("expected nil receiver chain to panic")
		}
		err, ok := recovered.(error)
		if !ok || !strings.Contains(err.Error(), "query builder cannot be nil") {
			t.Fatalf("unexpected panic: %#v", recovered)
		}
	}()
	qb.Search(userID).GroupBy(userID).Build()
}
func TestQueryBuilder_BranchingDoesNotShareMutableState(t *testing.T) {
	users := newMockTable("users")
	userID := newColForTable[Table, int](users, "id", "id", nil)
	base := Select(userID).From(users)
	left, err := base.Where(userID.EQ(1)).Build()
	if err != nil {
		t.Fatalf("expected left branch to build, got %v", err)
	}
	right, err := base.Where(userID.EQ(2)).Build()
	if err != nil {
		t.Fatalf("expected right branch to build, got %v", err)
	}
	if len(left.listArgs) != 1 || left.listArgs[0] != 1 {
		t.Fatalf("expected left branch args [1], got %#v", left.listArgs)
	}
	if len(right.listArgs) != 1 || right.listArgs[0] != 2 {
		t.Fatalf("expected right branch args [2], got %#v", right.listArgs)
	}
	if len(base.queryBuilderCore.spec.Filters) != 0 {
		t.Fatalf("expected base builder to remain unfiltered, got %#v", base.queryBuilderCore.spec.Filters)
	}
}
