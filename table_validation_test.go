package tsq

import (
	"strings"
	"testing"
)

type strictMockTable struct {
	name string
	cols []Column
}

func (t *strictMockTable) Table() string    { return t.name }
func (t *strictMockTable) KwList() []Column { return nil }
func (t *strictMockTable) Cols() []Column   { return t.cols }

func newStrictMockTable(name string, colNames ...string) (*strictMockTable, []Col[any, int]) {
	table := &strictMockTable{name: name}
	cols := make([]Column, 0, len(colNames))
	typed := make([]Col[any, int], 0, len(colNames))

	for _, name := range colNames {
		col := NewCol[any, int](table, name, name, nil)
		cols = append(cols, col)
		typed = append(typed, col)
	}

	table.cols = cols

	return table, typed
}

func TestColumnValidation_RejectsColumnOutsideKnownTableSchema(t *testing.T) {
	users, cols := newStrictMockTable("users", "id")
	id := cols[0]
	missing := NewCol[any, int](users, "missing", "missing", nil)

	_, err := Select(id, missing).
		From(users).
		Build()
	if err == nil {
		t.Fatal("expected unknown column to fail schema validation")
	}

	if !strings.Contains(err.Error(), "column missing does not belong to table users") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestColumnValidation_AllowsDerivedColumnsFromKnownTableColumns(t *testing.T) {
	users, cols := newStrictMockTable("users", "id")
	id := cols[0]
	resultID := id.Into(func(holder any) any { return holder }, "result_id")

	_, err := Select(id.Count(), resultID).
		From(users).
		Where(id.EQVar()).
		Build()
	if err != nil {
		t.Fatalf("expected aggregate and result-bound source column to build, got: %v", err)
	}
}

func TestColumnValidation_RejectsAggregateFromUnknownTableColumn(t *testing.T) {
	users, _ := newStrictMockTable("users", "id")
	missing := NewCol[any, int](users, "missing", "missing", nil)

	_, err := Select(missing.Count()).
		From(users).
		Build()
	if err == nil {
		t.Fatal("expected aggregate over unknown column to fail schema validation")
	}

	if !strings.Contains(err.Error(), "column missing does not belong to table users") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestColumnValidation_AllowsAliasedKnownTableColumns(t *testing.T) {
	users, cols := newStrictMockTable("users", "id")
	id := cols[0]
	parentUsers := AliasTable(users, "parent_users")
	parentID := id.As("parent_users")

	_, err := Select(id, parentID).
		From(users).
		LeftJoin(parentUsers, id.EQCol(parentID)).
		Build()
	if err != nil {
		t.Fatalf("expected aliased known column to build, got: %v", err)
	}
}

func TestJoinValidation_DifferentTablesSucceeds(t *testing.T) {
	users := newMockTable("users")
	orders := newMockTable("orders")

	userID := NewCol[any, int](users, "id", "id", nil)
	orderUserID := NewCol[any, int](orders, "user_id", "user_id", nil)

	_, err := Select(userID).
		From(userID.Table()).
		LeftJoin(orders, userID.EQCol(orderUserID)).
		Build()
	if err != nil {
		t.Fatalf("expected no error for different tables, got: %v", err)
	}
}

func TestJoinValidation_SameTableWithoutAliasRejectsJoin(t *testing.T) {
	users := newMockTable("users")

	userID := NewCol[any, int](users, "id", "id", nil)
	parentID := NewCol[any, int](users, "parent_id", "parent_id", nil)

	_, err := Select(userID).
		From(userID.Table()).
		LeftJoin(users, userID.EQCol(parentID)).
		Build()
	if err == nil {
		t.Fatal("expected error for repeated table without alias, got nil")
	}

	if !strings.Contains(err.Error(), "aliases are required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestJoinValidation_NilTableRejectsJoin(t *testing.T) {
	users := newMockTable("users")
	userID := NewCol[any, int](users, "id", "id", nil)

	_, err := Select(userID).
		From(userID.Table()).
		LeftJoin(nil, userID.EQ(1)).
		Build()
	if err == nil {
		t.Fatal("expected error for nil join table, got nil")
	}

	if !strings.Contains(err.Error(), "join table cannot be nil") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestJoinValidation_NilConditionRejectsJoin(t *testing.T) {
	users := newMockTable("users")
	orders := newMockTable("orders")
	userID := NewCol[any, int](users, "id", "id", nil)

	var cond Condition
	_, err := Select(userID).
		From(userID.Table()).
		LeftJoin(orders, cond).
		Build()
	if err == nil {
		t.Fatal("expected error for nil join condition, got nil")
	}

	if !strings.Contains(err.Error(), "condition cannot be nil") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestJoinValidation_RejectsJoinWithoutCondition(t *testing.T) {
	users := newMockTable("users")
	orders := newMockTable("orders")
	userID := NewCol[any, int](users, "id", "id", nil)

	_, err := Select(userID).
		From(users).
		LeftJoin(orders).
		Build()
	if err == nil {
		t.Fatal("expected error for join without ON condition, got nil")
	}

	if !strings.Contains(err.Error(), "requires at least one ON condition") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestJoinValidation_RejectsJoinConditionWithoutJoinedTable(t *testing.T) {
	users := newMockTable("users")
	orders := newMockTable("orders")
	userID := NewCol[any, int](users, "id", "id", nil)

	_, err := Select(userID).
		From(users).
		LeftJoin(orders, userID.EQ(1)).
		Build()
	if err == nil {
		t.Fatal("expected error when ON condition omits joined table, got nil")
	}

	if !strings.Contains(err.Error(), "must reference joined table orders") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestJoinValidation_RejectsJoinConditionWithoutIntroducedTable(t *testing.T) {
	users := newMockTable("users")
	orders := newMockTable("orders")
	userID := NewCol[any, int](users, "id", "id", nil)
	orderStatus := NewCol[any, int](orders, "status", "status", nil)

	_, err := Select(userID).
		From(users).
		LeftJoin(orders, orderStatus.EQ(1)).
		Build()
	if err == nil {
		t.Fatal("expected error when ON condition omits introduced table, got nil")
	}

	if !strings.Contains(err.Error(), "must reference at least one table already in the FROM/JOIN graph") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestJoinValidation_RejectsJoinConditionReferencingFutureTable(t *testing.T) {
	users := newMockTable("users")
	orders := newMockTable("orders")
	items := newMockTable("items")

	userID := NewCol[any, int](users, "id", "id", nil)
	orderUserID := NewCol[any, int](orders, "user_id", "user_id", nil)
	itemOrderID := NewCol[any, int](items, "order_id", "order_id", nil)

	_, err := Select(userID).
		From(users).
		LeftJoin(orders, userID.EQCol(orderUserID), itemOrderID.EQ(1)).
		Build()
	if err == nil {
		t.Fatal("expected error for ON condition referencing future table, got nil")
	}

	if !strings.Contains(err.Error(), "join condition table items is not connected") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestJoinValidation_SelfJoinWithAliasSucceeds(t *testing.T) {
	users := newMockTable("users")
	parentUsers := AliasTable(users, "parent_users")

	userID := NewCol[any, int](users, "id", "id", nil)
	userParentID := NewCol[any, int](users, "parent_id", "parent_id", nil)
	parentID := NewCol[any, int](parentUsers, "id", "id", nil)

	query, err := Select(userID, parentID).
		From(userID.Table()).
		LeftJoin(parentUsers, userParentID.EQCol(parentID)).
		Build()
	if err != nil {
		t.Fatalf("expected no error for self-join with alias, got: %v", err)
	}

	want := `SELECT "users"."id", "parent_users"."id" FROM "users" LEFT JOIN "users" AS "parent_users" ON "users"."parent_id" = "parent_users"."id"`
	if got := query.ListSQL(); got != want {
		t.Fatalf("expected aliased self-join SQL %q, got %q", want, got)
	}
}

func TestJoinValidation_AllowsAdditionalJoinedTablePredicates(t *testing.T) {
	users := newMockTable("users")
	orders := newMockTable("orders")

	userID := NewCol[any, int](users, "id", "id", nil)
	orderUserID := NewCol[any, int](orders, "user_id", "user_id", nil)
	orderStatus := NewCol[any, int](orders, "status", "status", nil)

	query, err := Select(userID).
		From(users).
		LeftJoin(orders, userID.EQCol(orderUserID), orderStatus.EQ(1)).
		Build()
	if err != nil {
		t.Fatalf("expected joined-table predicate to build, got: %v", err)
	}

	want := `SELECT "users"."id" FROM "users" LEFT JOIN "orders" ON ("users"."id" = "orders"."user_id" AND "orders"."status" = ?)`
	if got := query.ListSQL(); got != want {
		t.Fatalf("expected join SQL %q, got %q", want, got)
	}
}

func TestJoinValidation_NonEqualityConditionSucceeds(t *testing.T) {
	users := newMockTable("users")
	orders := newMockTable("orders")

	userScore := NewCol[any, int](users, "score", "score", nil)
	orderMinimum := NewCol[any, int](orders, "minimum_score", "minimum_score", nil)

	query, err := Select(userScore).
		From(userScore.Table()).
		InnerJoin(orders, userScore.GTECol(orderMinimum)).
		Build()
	if err != nil {
		t.Fatalf("expected no error for non-equality join condition, got: %v", err)
	}

	want := `SELECT "users"."score" FROM "users" INNER JOIN "orders" ON "users"."score" >= "orders"."minimum_score"`
	if got := query.ListSQL(); got != want {
		t.Fatalf("expected non-equality join SQL %q, got %q", want, got)
	}
}
