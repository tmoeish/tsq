package tsq

import "testing"

type (
	typedUserOwner  struct{}
	typedOrderOwner struct{}
)

func TestOnCreatesTypedJoinCondition(t *testing.T) {
	users := newMockTable("users")
	orders := newMockTable("orders")
	userID := NewCol[typedUserOwner, int](users, "id", "id", nil)
	orderUserID := NewCol[typedOrderOwner, int](orders, "user_id", "user_id", nil)

	var cond Condition = On(userID, orderUserID)
	if cond.Clause() != `"users"."id" = "orders"."user_id"` {
		t.Fatalf("unexpected ON clause: %s", cond.Clause())
	}

	tables := cond.Tables()
	if _, ok := tables["users"]; !ok {
		t.Fatalf("expected users table in typed ON condition: %#v", tables)
	}
	if _, ok := tables["orders"]; !ok {
		t.Fatalf("expected orders table in typed ON condition: %#v", tables)
	}
}

func TestOnSupportsNonEqualityJoinEdges(t *testing.T) {
	users := newMockTable("users")
	orders := newMockTable("orders")
	userScore := NewCol[typedUserOwner, int](users, "score", "score", nil)
	orderMinimum := NewCol[typedOrderOwner, int](orders, "minimum_score", "minimum_score", nil)

	var cond Condition = OnGTE(userScore, orderMinimum)
	if cond.Clause() != `"users"."score" >= "orders"."minimum_score"` {
		t.Fatalf("unexpected ON clause: %s", cond.Clause())
	}
}

func TestJoinCondWrapsLeftRightPredicatesAndExtraEdges(t *testing.T) {
	users := newMockTable("users")
	orders := newMockTable("orders")
	userID := NewCol[typedUserOwner, int](users, "id", "id", nil)
	userStatus := NewCol[typedUserOwner, int](users, "status", "status", nil)
	orderUserID := NewCol[typedOrderOwner, int](orders, "user_id", "user_id", nil)
	orderStatus := NewCol[typedOrderOwner, int](orders, "status", "status", nil)

	conds := []Condition{
		OnExtra(On(userID, orderUserID)),
		OnLeft[typedUserOwner, typedOrderOwner](userStatus.EQ(1)),
		OnRight[typedUserOwner, typedOrderOwner](orderStatus.EQ(2)),
	}

	want := []string{
		`"users"."id" = "orders"."user_id"`,
		`"users"."status" = ?`,
		`"orders"."status" = ?`,
	}
	for i, cond := range conds {
		if cond.Clause() != want[i] {
			t.Fatalf("condition %d clause = %q, want %q", i, cond.Clause(), want[i])
		}
	}
}

func TestOwnedColumnsConvertsTypedColumns(t *testing.T) {
	users := newMockTable("users")
	userID := NewCol[typedUserOwner, int](users, "id", "id", nil)
	userName := NewCol[typedUserOwner, string](users, "name", "name", nil)

	cols := OwnedColumns[typedUserOwner](userID, userName)
	if len(cols) != 2 {
		t.Fatalf("expected 2 owned columns, got %d", len(cols))
	}
	if cols[0].QualifiedName() != `"users"."id"` {
		t.Fatalf("unexpected first column: %s", cols[0].QualifiedName())
	}
	if cols[1].QualifiedName() != `"users"."name"` {
		t.Fatalf("unexpected second column: %s", cols[1].QualifiedName())
	}
}
