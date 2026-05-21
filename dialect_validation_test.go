package tsq

import (
	"strings"
	"testing"

	tsqdialect "github.com/tmoeish/tsq/v4/dialect"
)

func TestDialectNames(t *testing.T) {
	dialects := []Dialect{
		SQLiteDialect{},
		MySQLDialect{},
		PostgresDialect{},
	}

	for _, dialect := range dialects {
		name := string(dialect.Name())
		if name == "" {
			t.Fatalf("dialect name should not be empty")
		}

		for _, ch := range name {
			if ch >= 'A' && ch <= 'Z' {
				t.Fatalf("dialect name %q should be lowercase", name)
			}
		}
	}
}

func TestDialectCapabilities(t *testing.T) {
	tests := []struct {
		name       string
		dialect    Dialect
		capability DialectCapability
		want       bool
	}{
		{name: "sqlite lacks full join", dialect: SQLiteDialect{}, capability: DialectCapabilityFullOuterJoin, want: false},
		{name: "sqlite lacks for update", dialect: SQLiteDialect{}, capability: DialectCapabilitySelectForUpdate, want: false},
		{name: "sqlite supports cte", dialect: SQLiteDialect{}, capability: DialectCapabilityCTE, want: true},
		{name: "mysql lacks cte", dialect: MySQLDialect{}, capability: DialectCapabilityCTE, want: false},
		{name: "mysql supports skip locked", dialect: MySQLDialect{}, capability: DialectCapabilitySelectForSkipLocked, want: true},
		{name: "postgres supports full join", dialect: PostgresDialect{}, capability: DialectCapabilityFullOuterJoin, want: true},
		{name: "postgres supports for share", dialect: PostgresDialect{}, capability: DialectCapabilitySelectForShare, want: true},
		{name: "postgres supports except", dialect: PostgresDialect{}, capability: DialectCapabilityExcept, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.dialect.SupportsCapability(tt.capability); got != tt.want {
				t.Fatalf("SupportsCapability(%q) = %t, want %t", tt.capability, got, tt.want)
			}
		})
	}
}

func TestErrUnsupportedOperation(t *testing.T) {
	err := tsqdialect.ValidateCapability(MySQLDialect{}, DialectCapabilityFullOuterJoin)
	if err == nil {
		t.Fatal("expected unsupported capability error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "FULL JOIN") {
		t.Fatalf("expected FULL JOIN in error, got %q", msg)
	}
	if !strings.Contains(msg, "mysql") {
		t.Fatalf("expected mysql in error, got %q", msg)
	}
	if !strings.Contains(msg, "UNION") {
		t.Fatalf("expected actionable hint in error, got %q", msg)
	}
}

func TestValidateOperationForDialect(t *testing.T) {
	if err := validateOperationForDialect("FULL OUTER JOIN", nil); err != nil {
		t.Fatalf("nil dialect should skip capability validation: %v", err)
	}

	if err := validateOperationForDialect("FULL OUTER JOIN", PostgresDialect{}); err != nil {
		t.Fatalf("postgres should allow FULL OUTER JOIN: %v", err)
	}

	err := validateOperationForDialect("FULL OUTER JOIN", MySQLDialect{})
	if err == nil {
		t.Fatal("mysql should reject FULL OUTER JOIN")
	}
	if !strings.Contains(err.Error(), "FULL JOIN") {
		t.Fatalf("expected FULL JOIN in error, got %q", err.Error())
	}
}

func TestValidateOperationForDialectForUpdate(t *testing.T) {
	if err := validateOperationForDialect("FOR UPDATE", PostgresDialect{}); err != nil {
		t.Fatalf("postgres should allow FOR UPDATE: %v", err)
	}

	err := validateOperationForDialect("FOR UPDATE", SQLiteDialect{})
	if err == nil {
		t.Fatal("sqlite should reject FOR UPDATE")
	}
	if !strings.Contains(err.Error(), "FOR UPDATE") {
		t.Fatalf("expected FOR UPDATE in error, got %q", err.Error())
	}
}

func TestBatchInsertStartID(t *testing.T) {
	start, ok := SQLiteDialect{}.BatchInsertStartID(7, 3)
	if !ok || start != 5 {
		t.Fatalf("sqlite BatchInsertStartID = (%d, %t), want (5, true)", start, ok)
	}

	start, ok = MySQLDialect{}.BatchInsertStartID(7, 3)
	if !ok || start != 7 {
		t.Fatalf("mysql BatchInsertStartID = (%d, %t), want (7, true)", start, ok)
	}

	if _, ok = (PostgresDialect{}).BatchInsertStartID(7, 3); ok {
		t.Fatal("postgres should not derive multi-row insert IDs from LastInsertId")
	}
}
