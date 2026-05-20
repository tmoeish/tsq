package tsq

import (
	"strings"
	"testing"
)

func TestQueryBuilder_Union(t *testing.T) {
	users := newMockTable("users")
	orders := newMockTable("orders")
	userID := newMockColumn(users, "id")
	orderUserID := newMockColumn(orders, "user_id")
	qb := Select(userID).From(userID.Table()).Union(Select(orderUserID).From(orderUserID.Table()))
	core := mustBuilderCore[Table](t, qb)
	if len(core.spec.SetOps) != 1 {
		t.Fatalf("expected 1 set operation, got %d", len(core.spec.SetOps))
	}
	if core.spec.SetOps[0].op != unionType {
		t.Fatalf("expected UNION operation, got %s", core.spec.SetOps[0].op)
	}
}

func TestQueryBuilder_SetOperationRejectsMismatchedSelectCounts(t *testing.T) {
	users := newMockTable("users")
	id := newMockColumn(users, "id")
	name := newMockColumn(users, "name")
	_, err := Select(id).From(id.Table()).Union(Select(id, name).From(id.Table())).Build()
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
	_, err := Select(id).From(id.Table()).Search(id).Union(Select(id).From(id.Table())).Build()
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
	query := mustBuild(Select(userID).From(userID.Table()).UnionAll(Select(orderUserID).From(orderUserID.Table())))
	wantList := `SELECT "users"."id" FROM "users" UNION ALL SELECT "orders"."user_id" FROM "orders"`
	if query.ListSQL() != wantList {
		t.Fatalf("expected list SQL %q, got %q", wantList, query.ListSQL())
	}
	wantCount := `SELECT COUNT(1) FROM (` + wantList + `) AS _tsq_cnt`
	if query.CountSQL() != wantCount {
		t.Fatalf("expected count SQL %q, got %q", wantCount, query.CountSQL())
	}
}
