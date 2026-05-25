package tsq

import (
	"strings"
	"testing"
)

func TestQueryBuilder_CTEBuildsWithClause(t *testing.T) {
	users := newMockTable("users")
	id := newColForTable[Table, int](users, "id", "id", nil)
	name := newColForTable[Table, string](users, "name", "name", nil)
	activeUsers := CTE("active_users", Select(id, name).From(id.Table()).Where(id.GTVal(10)))
	activeUserID := id.WithTable(activeUsers)
	activeUserName := name.WithTable(activeUsers)
	query := mustBuild(Select(activeUserID, activeUserName).From(activeUsers))
	wantList := `WITH "active_users" AS (SELECT "users"."id", "users"."name" FROM "users" WHERE "users"."id" > ?) SELECT "active_users"."id", "active_users"."name" FROM "active_users"`
	if query.ListSQL() != wantList {
		t.Fatalf("expected CTE list SQL %q, got %q", wantList, query.ListSQL())
	}
	wantCount := `WITH "active_users" AS (SELECT "users"."id", "users"."name" FROM "users" WHERE "users"."id" > ?) SELECT COUNT(1) FROM "active_users"`
	if query.CountSQL() != wantCount {
		t.Fatalf("expected CTE count SQL %q, got %q", wantCount, query.CountSQL())
	}
}

func TestQueryBuilder_CTECollectsNestedDependencies(t *testing.T) {
	users := newMockTable("users")
	id := newColForTable[Table, int](users, "id", "id", nil)
	baseUsers := CTE("base_users", Select(id).From(id.Table()).Where(id.GTVal(1)))
	baseUserID := id.WithTable(baseUsers)
	filteredUsers := CTE("filtered_users", Select(baseUserID).From(baseUserID.Table()))
	filteredUserID := id.WithTable(filteredUsers)
	query := mustBuild(Select(filteredUserID).From(filteredUsers))
	want := `WITH "base_users" AS (SELECT "users"."id" FROM "users" WHERE "users"."id" > ?), "filtered_users" AS (SELECT "base_users"."id" FROM "base_users") SELECT "filtered_users"."id" FROM "filtered_users"`
	if query.ListSQL() != want {
		t.Fatalf("expected nested CTE SQL %q, got %q", want, query.ListSQL())
	}
}

func TestQueryBuilder_CTERejectsKeywordSearchInDefinition(t *testing.T) {
	users := newMockTable("users")
	id := newColForTable[Table, int](users, "id", "id", nil)
	name := newColForTable[Table, string](users, "name", "name", nil)
	searchUsers := CTE("search_users", Select(id, name).From(id.Table()).Search(name))
	searchUserID := id.WithTable(searchUsers)
	_, err := Select(searchUserID).From(searchUserID.Table()).Build()
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
	userID := newColForTable[Table, int](users, "id", "id", nil)
	orgID := newColForTable[Table, int](orgs, "id", "id", nil)
	orgName := newColForTable[Table, string](orgs, "name", "name", nil)
	label := MapInto[queryBuilderCaseRow](Case[string]().When(orgID.EQVal(1), orgName).Else("unknown").End(), func(holder *queryBuilderCaseRow) *string {
		return &holder.Label
	}, "label")
	_, err := Select[queryBuilderCaseRow](label).From(userID.Table()).Build()
	if err == nil {
		t.Fatal("expected CASE expression to surface orgs table into join validation")
	}
	if !strings.Contains(err.Error(), "use CrossJoin") {
		t.Fatalf("unexpected error: %v", err)
	}
}
