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
