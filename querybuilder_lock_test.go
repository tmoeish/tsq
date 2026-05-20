package tsq

import (
	"strings"
	"testing"
)

func TestQueryBuilder_ForUpdate(t *testing.T) {
	table := newMockTable("users")
	id := newMockColumn(table, "id")
	qb := Select(id).From(table).ForUpdate()
	if qb.spec.Lock.strength != queryLockStrengthUpdate {
		t.Fatalf("expected FOR UPDATE lock, got %q", qb.spec.Lock.strength)
	}
	if qb.spec.Lock.waitMode != "" {
		t.Fatalf("expected empty wait mode, got %q", qb.spec.Lock.waitMode)
	}
}
func TestQueryBuilder_ForShareSkipLocked(t *testing.T) {
	table := newMockTable("users")
	id := newMockColumn(table, "id")
	qb := Select(id).From(table).ForShare().SkipLocked()
	if qb.spec.Lock.strength != queryLockStrengthShare {
		t.Fatalf("expected FOR SHARE lock, got %q", qb.spec.Lock.strength)
	}
	if qb.spec.Lock.waitMode != queryLockWaitSkipLocked {
		t.Fatalf("expected SKIP LOCKED wait mode, got %q", qb.spec.Lock.waitMode)
	}
}
func TestQueryBuilder_LockWaitModeCannotBeSetTwice(t *testing.T) {
	table := newMockTable("users")
	id := newMockColumn(table, "id")
	_, err := Select(id).From(table).ForUpdate().NoWait().SkipLocked().Build()
	if err == nil {
		t.Fatal("expected duplicate lock wait mode to fail")
	}
	if !strings.Contains(err.Error(), "lock wait mode is already set") {
		t.Fatalf("unexpected error: %v", err)
	}
}
