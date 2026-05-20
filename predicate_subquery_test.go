package tsq

import (
	"strings"
	"testing"
)

func TestCondition_ExistsSubIsStandalonePredicate(t *testing.T) {
	col := newColForTable[Table, int](newMockTable("users"), "id", "id", nil)
	orderID := newColForTable[Table, int](newMockTable("orders"), "id", "id", nil)
	subquery := mustBuild(Select(orderID).From(orderID.Table()))
	got := renderCanonicalSQL(col.ExistsSub(subquery).Clause())
	want := `EXISTS (SELECT "orders"."id" FROM "orders")`
	if got != want {
		t.Fatalf("expected exists clause %q, got %q", want, got)
	}
}

func TestCondition_UnbuiltSubqueryFailsFast(t *testing.T) {
	col := newColForTable[Table, int](newMockTable("users"), "id", "id", nil)
	if _, _, _, err := validateConditionInput(col.InSub(&Query[queryOwner]{})); err == nil {
		t.Fatal("expected unbuilt subquery to be captured as a build error")
	} else if !strings.Contains(err.Error(), "subquery is not built") {
		t.Fatalf("expected unbuilt subquery error, got %v", err)
	}
}

func TestCondition_ScalarSubqueryRejectsMultipleColumns(t *testing.T) {
	users := newMockTable("users")
	orders := newMockTable("orders")
	userID := newColForTable[Table, int](users, "id", "id", nil)
	orderID := newColForTable[Table, int](orders, "id", "id", nil)
	orderUserID := newColForTable[Table, int](orders, "user_id", "user_id", nil)
	subquery := mustBuild(Select(orderID, orderUserID).From(orders))
	if _, _, _, err := validateConditionInput(userID.EQSub(subquery)); err == nil {
		t.Fatal("expected scalar subquery to reject multiple columns")
	} else if !strings.Contains(err.Error(), "scalar subquery must select exactly one column") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCondition_InSubRejectsMultipleColumns(t *testing.T) {
	users := newMockTable("users")
	orders := newMockTable("orders")
	userID := newColForTable[Table, int](users, "id", "id", nil)
	orderID := newColForTable[Table, int](orders, "id", "id", nil)
	orderUserID := newColForTable[Table, int](orders, "user_id", "user_id", nil)
	subquery := mustBuild(Select(orderID, orderUserID).From(orders))
	if _, _, _, err := validateConditionInput(userID.InSub(subquery)); err == nil {
		t.Fatal("expected IN subquery to reject multiple columns")
	} else if !strings.Contains(err.Error(), "in subquery must select exactly one column") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCondition_ExistsSubAllowsMultipleColumnsAndKeepsArgs(t *testing.T) {
	users := newMockTable("users")
	orders := newMockTable("orders")
	userID := newColForTable[Table, int](users, "id", "id", nil)
	orderID := newColForTable[Table, int](orders, "id", "id", nil)
	orderUserID := newColForTable[Table, int](orders, "user_id", "user_id", nil)
	subquery := mustBuild(Select(orderID, orderUserID).From(orders).Where(orderUserID.EQ(1)))
	clause, _, args, err := validateConditionInput(userID.ExistsSub(subquery))
	if err != nil {
		t.Fatalf("expected EXISTS subquery to allow multiple columns, got %v", err)
	}
	wantClause := `EXISTS (SELECT "orders"."id", "orders"."user_id" FROM "orders" WHERE "orders"."user_id" = ?)`
	if got := renderCanonicalSQL(clause); got != wantClause {
		t.Fatalf("expected exists clause %q, got %q", wantClause, got)
	}
	if len(args) != 1 || args[0] != 1 {
		t.Fatalf("expected EXISTS subquery args [1], got %#v", args)
	}
}

func TestCondition_UniqueSubqueryPredicatesFailFast(t *testing.T) {
	col := newColForTable[Table, int](newMockTable("users"), "id", "id", nil)
	subquery := &Query[queryOwner]{listSQL: "SELECT 1"}
	if _, _, _, err := validateConditionInput(col.Unique(subquery)); err == nil {
		t.Fatal("expected Unique to return a build error for unsupported predicate")
	}
}

func TestUnsupportedSubqueryPredicatesDeferred(t *testing.T) {
	col := newColForTable[Table, int](newMockTable("users"), "id", "id", nil)
	query := &Query[queryOwner]{listSQL: "SELECT 1"}
	tests := []struct // TestUnsupportedSubqueryPredicatesDeferred tests that unsupported subquery predicates
	// return deferred errors at Build() time, not immediate panics.
	{
		name string
		cond Condition
	}{{"Unique", col.Unique(query)}, {"NUnique", col.NUnique(query)}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, _, err := validateConditionInput(tt.cond)
			if err == nil {
				t.Fatalf("expected %s to have deferred error", tt.name)
			}
			if !strings.Contains(err.Error(), "subquery") {
				t.Fatalf("expected error to mention subquery, got: %v", err)
			}
		})
	}
}
