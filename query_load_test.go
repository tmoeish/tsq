package tsq

import (
	"context"
	"errors"
	_ "github.com/mattn/go-sqlite3"
	"strings"
	"testing"
)

func TestQuery_buildPageSQLsNormalizesNilRequest(t *testing.T) {
	query := &Query[queryOwner]{cntSQL: "SELECT COUNT(*) FROM users", listSQL: "SELECT * FROM users", kwCntSQL: "SELECT COUNT(*) FROM users WHERE name LIKE ?", kwListSQL: "SELECT * FROM users WHERE name LIKE ?"}
	cntSQL, listSQL, err := query.buildPageSQLs(nil)
	if err != nil {
		t.Fatalf("expected nil page request to be normalized, got error %v", err)
	}
	if cntSQL != "SELECT COUNT(*) FROM users" {
		t.Fatalf("unexpected count SQL: %q", cntSQL)
	}
	if listSQL != "SELECT * FROM users\nLIMIT ? OFFSET ?" {
		t.Fatalf("unexpected list SQL: %q", listSQL)
	}
}
func TestQuery_buildPageSQLsRejectsNilQuery(t *testing.T) {
	var query *Query[queryOwner]
	_, _, err := query.buildPageSQLs(nil)
	if err == nil {
		t.Fatal("expected nil query to return an error")
	}
	if !strings.Contains(err.Error(), "query cannot be nil") {
		t.Fatalf("unexpected error: %v", err)
	}
}
func TestQuery_buildPageSQLsRejectsUnbuiltQuery(t *testing.T) {
	query := &Query[queryOwner]{}
	_, _, err := query.buildPageSQLs(nil)
	if err == nil {
		t.Fatal("expected unbuilt query to return an error")
	}
	if !strings.Contains(err.Error(), "query is not built") {
		t.Fatalf("unexpected error: %v", err)
	}
}
func TestQuery_buildPageSQLsRejectsAmbiguousSortField(t *testing.T) {
	users := newMockTable("users")
	orders := newMockTable("orders")
	userID := newMockColumn(users, "id")
	orderID := newMockColumn(orders, "id")
	query := mustBuild(Select(userID, orderID).From(userID.Table()).CrossJoin(orderID.Table()))
	_, _, err := query.buildPageSQLs(&PageRequest{OrderBy: "id", Order: "ASC"})
	if err == nil {
		t.Fatal("expected ambiguous sort field to return an error")
	}
	var ambiguousErr *ErrAmbiguousSortField
	if !errors.As(err, &ambiguousErr) {
		t.Fatalf("expected ErrAmbiguousSortField, got %v", err)
	}
}
func TestQuery_buildPageSQLsIgnoresHiddenJSONSortAlias(t *testing.T) {
	users := newMockTable("users")
	hidden := newColForTable[Table, string](users, "secret", "-", nil)
	query := mustBuild(Select(hidden).From(hidden.Table()))
	_, _, err := query.buildPageSQLs(&PageRequest{OrderBy: "-", Order: "ASC"})
	if err == nil {
		t.Fatal("expected json:- sort alias to be rejected")
	}
	var unknownErr *ErrUnknownSortField
	if !errors.As(err, &unknownErr) {
		t.Fatalf("expected ErrUnknownSortField, got %v", err)
	}
}
func TestQuery_buildPageSQLsDefaultsMissingOrderToASC(t *testing.T) {
	users := newMockTable("users")
	userID := newMockColumn(users, "id")
	userName := newMockColumn(users, "name")
	query := mustBuild(Select(userID, userName).From(userID.Table()))
	_, listSQL, err := query.buildPageSQLs(&PageRequest{OrderBy: "name,id"})
	if err != nil {
		t.Fatalf("expected missing order to default to ASC, got %v", err)
	}
	want := `SELECT "users"."id", "users"."name" FROM "users"` + "\nORDER BY " + `"users"."name" ASC, "users"."id" ASC` + "\nLIMIT ? OFFSET ?"
	if got := renderCanonicalSQL(listSQL); got != want {
		t.Fatalf("expected list SQL %q, got %q", want, got)
	}
}
func TestQuery_buildPageSQLsRejectsExplicitOrderCountMismatch(t *testing.T) {
	users := newMockTable("users")
	userID := newMockColumn(users, "id")
	userName := newMockColumn(users, "name")
	query := mustBuild(Select(userID, userName).From(userID.Table()))
	_, _, err := query.buildPageSQLs(&PageRequest{OrderBy: "name,id", Order: "DESC"})
	if err == nil {
		t.Fatal("expected explicit order count mismatch to return an error")
	}
	var mismatchErr *ErrOrderCountMismatch
	if !errors.As(err, &mismatchErr) {
		t.Fatalf("expected ErrOrderCountMismatch, got %v", err)
	}
}
func TestPageFnRejectsNilQuery(t *testing.T) {
	_, err := pageFn[queryOwner](context.Background(), nil, nil, nil)
	if err == nil {
		t.Fatal("expected nil query to return an error")
	}
	if !strings.Contains(err.Error(), "query cannot be nil") {
		t.Fatalf("unexpected error: %v", err)
	}
}
