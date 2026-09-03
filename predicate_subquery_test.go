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
	if _, err := (&Query[Table]{}).AsSubquery(col); err == nil {
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
	if _, err := query.AsSubquery(userID); err == nil {
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
	if _, err := query.AsSubquery(orderUserID); err == nil {
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

// An outer-table reference that was not declared is still rejected, and the
// message must not send the reader to a join: joining the outer table in makes
// it shadow the outer one, so the predicate silently stops being correlated and
// the query still runs.
func TestSubquery_UndeclaredCorrelatedReferenceIsRejected(t *testing.T) {
	users := newMockTable("users")
	orders := newMockTable("orders")
	userID := newColForTable[Table, int](users, "id", "id", nil)
	orderID := newColForTable[Table, int](orders, "id", "id", nil)
	orderUserID := newColForTable[Table, int](orders, "user_id", "user_id", nil)

	_, err := Select(orderID).From(orders).Where(orderUserID.EQ(userID)).Build()
	if err == nil {
		t.Fatal("expected an undeclared correlated reference to be rejected")
	}

	if !strings.Contains(err.Error(), "declare it with Correlate(users)") {
		t.Fatalf("unexpected error: %v", err)
	}

	if strings.Contains(err.Error(), "CrossJoin") {
		t.Fatalf("error must not recommend CrossJoin for a correlated reference: %v", err)
	}
}

func TestSubquery_CorrelateRendersOuterReferenceWithoutJoiningIt(t *testing.T) {
	users := newMockTable("users")
	orders := newMockTable("orders")
	userID := newColForTable[Table, int](users, "id", "id", nil)
	orderID := newColForTable[Table, int](orders, "id", "id", nil)
	orderUserID := newColForTable[Table, int](orders, "user_id", "user_id", nil)

	sub, err := BuildSubquery(
		Select(orderID).From(orders).Correlate(users).Where(orderUserID.EQ(userID)),
		orderID,
	)
	if err != nil {
		t.Fatalf("expected a correlated subquery to build, got %v", err)
	}

	outer := mustBuild(Select(userID).From(users).Where(userID.NExistsSub(sub)))

	got := renderCanonicalSQL(outer.subquerySQL())
	want := `SELECT "users"."id" FROM "users" WHERE NOT EXISTS ` +
		`(SELECT "orders"."id" FROM "orders" WHERE "orders"."user_id" = "users"."id")`

	if got != want {
		t.Fatalf("expected correlated subquery SQL\n  %s\ngot\n  %s", want, got)
	}
}

// Declaring a table as correlated and also joining it is the exact mistake the
// old CrossJoin advice produced, so it has to be a build error rather than a
// query that runs and answers a different question.
func TestSubquery_CorrelateRejectsATableThisQueryAlsoJoins(t *testing.T) {
	users := newMockTable("users")
	orders := newMockTable("orders")
	userID := newColForTable[Table, int](users, "id", "id", nil)
	orderID := newColForTable[Table, int](orders, "id", "id", nil)
	orderUserID := newColForTable[Table, int](orders, "user_id", "user_id", nil)

	_, err := Select(orderID).From(orders).
		Correlate(users).
		CrossJoin(users).
		Where(orderUserID.EQ(userID)).
		Build()
	if err == nil {
		t.Fatal("expected a table that is both correlated and joined to be rejected")
	}

	if !strings.Contains(err.Error(), "would shadow the outer one") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSubquery_CorrelateRejectsDuplicateAndMissingTables(t *testing.T) {
	users := newMockTable("users")
	orders := newMockTable("orders")
	orderID := newColForTable[Table, int](orders, "id", "id", nil)

	if _, err := Select(orderID).From(orders).Correlate().Build(); err == nil ||
		!strings.Contains(err.Error(), "at least one outer table") {
		t.Fatalf("expected Correlate with no table to be rejected, got %v", err)
	}

	if _, err := Select(orderID).From(orders).Correlate(users, users).Build(); err == nil ||
		!strings.Contains(err.Error(), "already declared") {
		t.Fatalf("expected a repeated correlated table to be rejected, got %v", err)
	}
}

// A correlated query is not a runnable statement on its own. Refusing it in TSQ
// names the cause; letting it through produces a database error about a table
// the reader can plainly see in the SQL.
func TestSubquery_CorrelatedQueryRefusesStandaloneExecution(t *testing.T) {
	users := newMockTable("users")
	orders := newMockTable("orders")
	userID := newColForTable[Table, int](users, "id", "id", nil)
	orderID := newColForTable[Table, int](orders, "id", "id", nil)
	orderUserID := newColForTable[Table, int](orders, "user_id", "user_id", nil)

	query := mustBuild(Select(orderID).From(orders).Correlate(users).Where(orderUserID.EQ(userID)))

	_, err := query.List(t.Context(), nil)
	if err == nil {
		t.Fatal("expected a correlated query to refuse standalone execution")
	}

	if !strings.Contains(err.Error(), "can only be used as a subquery") {
		t.Fatalf("unexpected error: %v", err)
	}
}
