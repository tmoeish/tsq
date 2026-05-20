package tsq

import "testing"

func TestBuildQueryPlanClonesArgumentSlices(t *testing.T) {
	users := newMockTable("users")
	id := newColForTable[Table, int](users, "id", "id", nil)
	plan, err := buildQueryPlan(querySpec[Table]{From: users, Selects: []BoundColumn[Table]{id}, Filters: []Condition{id.EQ(1)}})
	if err != nil {
		t.Fatalf("expected query plan build to succeed, got %v", err)
	}
	if len(plan.cntArgs) != 1 || len(plan.listArgs) != 1 {
		t.Fatalf("expected both count and list args to contain one bound value, got cnt=%#v list=%#v", plan.cntArgs, plan.listArgs)
	}
	plan.listArgs[0] = 99
	if got := plan.cntArgs[0]; got != 1 {
		t.Fatalf("expected count args to be cloned independently, got %#v", plan.cntArgs)
	}
}
