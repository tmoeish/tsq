package tsq

import (
	"strings"
	"testing"
)

func TestQueryBuilder_ForUpdate(t *testing.T) {
	table := newMockTable("users")
	id := newMockColumn(table, "id")
	qb := Select(id).From(table).ForUpdate()
	core := mustBuilderCore[Table](t, qb)
	if core.spec.Lock.strength != queryLockStrengthUpdate {
		t.Fatalf("expected FOR UPDATE lock, got %q", core.spec.Lock.strength)
	}
	if core.spec.Lock.waitMode != "" {
		t.Fatalf("expected empty wait mode, got %q", core.spec.Lock.waitMode)
	}
}

func TestQueryBuilder_ForShareSkipLocked(t *testing.T) {
	table := newMockTable("users")
	id := newMockColumn(table, "id")
	qb := Select(id).From(table).ForShare().SkipLocked()
	core := mustBuilderCore[Table](t, qb)
	if core.spec.Lock.strength != queryLockStrengthShare {
		t.Fatalf("expected FOR SHARE lock, got %q", core.spec.Lock.strength)
	}
	if core.spec.Lock.waitMode != queryLockWaitSkipLocked {
		t.Fatalf("expected SKIP LOCKED wait mode, got %q", core.spec.Lock.waitMode)
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
