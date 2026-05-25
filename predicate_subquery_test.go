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

func TestAsSubquery_UnbuiltQueryFailsFast(t *testing.T) {
	col := newColForTable[Table, int](newMockTable("users"), "id", "id", nil)
	if _, err := AsSubquery(&Query[Table]{}, col); err == nil {
		t.Fatal("expected unbuilt query to be rejected")
	} else if !strings.Contains(err.Error(), "subquery is not built") {
		t.Fatalf("expected unbuilt subquery error, got %v", err)
	}
}

func TestAsSubquery_RejectsMultipleColumns(t *testing.T) {
	users := newMockTable("users")
	orders := newMockTable("orders")
	userID := newColForTable[Table, int](users, "id", "id", nil)
	orderID := newColForTable[Table, int](orders, "id", "id", nil)
	orderUserID := newColForTable[Table, int](orders, "user_id", "user_id", nil)
	query := mustBuild(Select(orderID, orderUserID).From(orders))
	if _, err := AsSubquery(query, userID); err == nil {
		t.Fatal("expected typed subquery creation to reject multiple columns")
	} else if !strings.Contains(err.Error(), "subquery must select exactly one column") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCondition_ExistsSubAllowsMultipleColumnsAndKeepsArgs(t *testing.T) {
	users := newMockTable("users")
	orders := newMockTable("orders")
	userID := newColForTable[Table, int](users, "id", "id", nil)
	orderID := newColForTable[Table, int](orders, "id", "id", nil)
	orderUserID := newColForTable[Table, int](orders, "user_id", "user_id", nil)
	subquery := mustBuild(Select(orderID, orderUserID).From(orders).Where(orderUserID.EQVal(1)))
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

func TestAsSubquery_RejectsMismatchedSelectedColumn(t *testing.T) {
	orders := newMockTable("orders")
	orderID := newColForTable[Table, int](orders, "id", "id", nil)
	orderUserID := newColForTable[Table, int](orders, "user_id", "user_id", nil)
	query := mustBuild(Select(orderID).From(orders))
	if _, err := AsSubquery(query, orderUserID); err == nil {
		t.Fatal("expected mismatched selected column to be rejected")
	} else if !strings.Contains(err.Error(), `subquery selected "orders"."id"`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCondition_TypedSubqueryBuildsScalarPredicate(t *testing.T) {
	users := newMockTable("users")
	orders := newMockTable("orders")
	userID := newColForTable[Table, int](users, "id", "id", nil)
	orderUserID := newColForTable[Table, int](orders, "user_id", "user_id", nil)
	subquery, err := BuildSubquery(
		Select(orderUserID).From(orders).Where(orderUserID.EQVal(1)),
		orderUserID,
	)
	if err != nil {
		t.Fatalf("BuildSubquery() error = %v", err)
	}
	clause, _, args, err := validateConditionInput(userID.EQ(subquery))
	if err != nil {
		t.Fatalf("expected typed scalar subquery to validate, got %v", err)
	}
	wantClause := `"` + users.Table() + `"."id" = (SELECT "orders"."user_id" FROM "orders" WHERE "orders"."user_id" = ?)`
	if got := renderCanonicalSQL(clause); got != wantClause {
		t.Fatalf("expected scalar clause %q, got %q", wantClause, got)
	}
	if len(args) != 1 || args[0] != 1 {
		t.Fatalf("expected scalar subquery args [1], got %#v", args)
	}
}

func TestCondition_TypedSubqueryBuildsBetweenPredicate(t *testing.T) {
	users := newMockTable("users")
	orders := newMockTable("orders")
	userID := newColForTable[Table, int](users, "id", "id", nil)
	orderMinID := newColForTable[Table, int](orders, "min_id", "min_id", nil)
	orderMaxID := newColForTable[Table, int](orders, "max_id", "max_id", nil)
	minSubquery, err := BuildSubquery(Select(orderMinID).From(orders).Where(orderMinID.GTVal(0)), orderMinID)
	if err != nil {
		t.Fatalf("BuildSubquery(min) error = %v", err)
	}
	maxSubquery, err := BuildSubquery(Select(orderMaxID).From(orders).Where(orderMaxID.GTVal(10)), orderMaxID)
	if err != nil {
		t.Fatalf("BuildSubquery(max) error = %v", err)
	}
	clause, _, args, err := validateConditionInput(userID.Between(minSubquery, maxSubquery))
	if err != nil {
		t.Fatalf("expected typed BETWEEN subqueries to validate, got %v", err)
	}
	wantClause := `"users"."id" BETWEEN (SELECT "orders"."min_id" FROM "orders" WHERE "orders"."min_id" > ?) AND (SELECT "orders"."max_id" FROM "orders" WHERE "orders"."max_id" > ?)`
	if got := renderCanonicalSQL(clause); got != wantClause {
		t.Fatalf("expected BETWEEN clause %q, got %q", wantClause, got)
	}
	if len(args) != 2 || args[0] != 0 || args[1] != 10 {
		t.Fatalf("expected BETWEEN subquery args [0 10], got %#v", args)
	}
}

func TestCondition_TypedSubqueryBuildsLikePredicate(t *testing.T) {
	users := newMockTable("users")
	patterns := newMockTable("patterns")
	userName := newColForTable[Table, string](users, "name", "name", nil)
	patternValue := newColForTable[Table, string](patterns, "pattern", "pattern", nil)
	subquery, err := BuildSubquery(
		Select(patternValue).From(patterns).Where(patternValue.LikeVal("%alice%")),
		patternValue,
	)
	if err != nil {
		t.Fatalf("BuildSubquery() error = %v", err)
	}
	clause, _, args, err := validateConditionInput(userName.Like(subquery))
	if err != nil {
		t.Fatalf("expected typed LIKE subquery to validate, got %v", err)
	}
	wantClause := `"users"."name" LIKE (SELECT "patterns"."pattern" FROM "patterns" WHERE "patterns"."pattern" LIKE ?)`
	if got := renderCanonicalSQL(clause); got != wantClause {
		t.Fatalf("expected LIKE clause %q, got %q", wantClause, got)
	}
	if len(args) != 1 || args[0] != "%alice%" {
		t.Fatalf("expected LIKE subquery args [%%alice%%], got %#v", args)
	}
}

func TestCondition_TypedSubqueryBuildsMembershipPredicate(t *testing.T) {
	users := newMockTable("users")
	orders := newMockTable("orders")
	userID := newColForTable[Table, int](users, "id", "id", nil)
	orderUserID := newColForTable[Table, int](orders, "user_id", "user_id", nil)
	subquery, err := BuildSubquery(
		Select(orderUserID).From(orders).Where(orderUserID.GTVal(5)),
		orderUserID,
	)
	if err != nil {
		t.Fatalf("BuildSubquery() error = %v", err)
	}
	clause, _, args, err := validateConditionInput(userID.In(subquery))
	if err != nil {
		t.Fatalf("expected typed IN subquery to validate, got %v", err)
	}
	wantClause := `"users"."id" IN (SELECT "orders"."user_id" FROM "orders" WHERE "orders"."user_id" > ?)`
	if got := renderCanonicalSQL(clause); got != wantClause {
		t.Fatalf("expected IN clause %q, got %q", wantClause, got)
	}
	if len(args) != 1 || args[0] != 5 {
		t.Fatalf("expected IN subquery args [5], got %#v", args)
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
