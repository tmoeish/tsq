package tsq

import (
	"database/sql/driver"
	"testing"
)

type customValuer struct {
	value string
	valid bool
}

func (c customValuer) Value() (driver.Value, error) {
	if !c.valid {
		return nil, nil
	}
	return c.value, nil
}

type pointerValuer struct{ value string }

func (p *pointerValuer) Value() (driver.Value, error) {
	if p == nil {
		return nil, nil
	}
	return p.value, nil
}

func stringPtr(s string) *string {
	return &s
}

func intPtr(i int) *int {
	return &i
}

func TestConditionClauseRendersCanonicalSQL(t *testing.T) {
	col := newColForTable[Table, int](newMockTable("users"), "id", "id", nil)
	cond := col.EQ(1)
	if got := cond.Clause(); got != `"users"."id" = ?` {
		t.Fatalf("expected public condition clause to render canonical SQL, got %q", got)
	}
	if got := cond.Args(); len(got) != 1 || got[0] != 1 {
		t.Fatalf("expected parameterized predicate args [1], got %#v", got)
	}
}

func TestCondition_EmptyAndOrShortCircuit(t *testing.T) {
	if got := And().Clause(); got != "1 = 1" {
		t.Fatalf("expected empty And to short-circuit to true predicate, got %q", got)
	}
	if got := Or().Clause(); got != "1 = 0" {
		t.Fatalf("expected empty Or to short-circuit to false predicate, got %q", got)
	}
}

func TestCondition_TablesReturnsDefensiveCopy(t *testing.T) {
	users := newMockTable("users")
	cond := newColForTable[Table, int](users, "id", "id", nil).EQ(1)
	tables := cond.Tables()
	delete(tables, "users")
	tables["other"] = newMockTable("other")
	fresh := cond.Tables()
	if len(fresh) != 1 || fresh["users"] == nil {
		t.Fatalf("expected condition tables to stay intact, got %#v", fresh)
	}
}

func TestCondition_NilCompositeInputsFailFast(t *testing.T) {
	var nilCond Condition
	for _, cond := range []Condition{And(nilCond), Or(nilCond)} {
		if _, _, _, err := validateConditionInput(cond); err == nil {
			t.Fatal("expected nil condition to be captured as a build error")
		}
	}
}

func TestCondition_EmptyClauseFailsFast(t *testing.T) {
	if _, _, _, err := validateConditionInput(And(conditionImpl{})); err == nil {
		t.Fatal("expected empty condition clause to be captured as a build error")
	}
}
