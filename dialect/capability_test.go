package dialect

import (
	"strings"
	"testing"
)

// dialectTables pairs each dialect with the table backing its SupportsCapability.
// A new dialect belongs here the moment it exists, which is what makes the
// exhaustiveness check below cover the whole package rather than a chosen subset.
var dialectTables = map[Name]map[Capability]bool{
	MySQL:    mysqlCapabilities,
	Postgres: postgresCapabilities,
	SQLite:   sqliteCapabilities,
}

// TestDialectsCoverAllCapabilities is the gate behind the rule that every dialect
// must take an explicit position on every capability.
//
// Before this test existed each dialect answered from a switch with a `default:
// return false` arm, so a capability added to AllCapabilities without touching
// mysql.go, postgres.go and sqlite.go was reported as unsupported everywhere with
// nothing failing: not the compiler, not the linter, not the tests. That is the kind
// of mistake that only surfaces against a production database.
func TestDialectsCoverAllCapabilities(t *testing.T) {
	for name, table := range dialectTables {
		for _, capability := range AllCapabilities() {
			if _, declared := table[capability]; !declared {
				t.Errorf("dialect %s does not declare capability %s; add an explicit true/false entry to its capability table", name, capability)
			}
		}

		if len(table) != len(AllCapabilities()) {
			t.Errorf("dialect %s declares %d capabilities, want %d; it lists a capability that is not in AllCapabilities()", name, len(table), len(AllCapabilities()))
		}
	}
}

// TestAllCapabilitiesHasNoDuplicates guards the list itself: a duplicated entry would
// weaken the length comparison above into something a missing capability could pass.
func TestAllCapabilitiesHasNoDuplicates(t *testing.T) {
	seen := make(map[Capability]struct{}, len(AllCapabilities()))
	for _, capability := range AllCapabilities() {
		if _, dup := seen[capability]; dup {
			t.Errorf("capability %s appears twice in AllCapabilities()", capability)
		}

		seen[capability] = struct{}{}
	}
}

// TestDialectCapabilityBaselines pins the version baselines documented in
// architecture.md. Changing one of these is a user-visible change, not a refactor.
func TestDialectCapabilityBaselines(t *testing.T) {
	tests := []struct {
		name       string
		dialect    Dialect
		capability Capability
		want       bool
	}{
		{name: "sqlite supports full join since 3.39", dialect: SQLiteDialect{}, capability: CapabilityFullOuterJoin, want: true},
		{name: "sqlite supports cte", dialect: SQLiteDialect{}, capability: CapabilityCTE, want: true},
		{name: "sqlite lacks row locking", dialect: SQLiteDialect{}, capability: CapabilitySelectForUpdate, want: false},
		{name: "mysql supports cte since 8.0", dialect: MySQLDialect{}, capability: CapabilityCTE, want: true},
		{name: "mysql supports intersect since 8.0.31", dialect: MySQLDialect{}, capability: CapabilityIntersect, want: true},
		{name: "mysql supports except since 8.0.31", dialect: MySQLDialect{}, capability: CapabilityExcept, want: true},
		{name: "mysql still lacks full join", dialect: MySQLDialect{}, capability: CapabilityFullOuterJoin, want: false},
		{name: "mysql supports skip locked", dialect: MySQLDialect{}, capability: CapabilitySelectForSkipLocked, want: true},
		{name: "postgres supports full join", dialect: PostgresDialect{}, capability: CapabilityFullOuterJoin, want: true},
		{name: "postgres supports for share", dialect: PostgresDialect{}, capability: CapabilitySelectForShare, want: true},
		{name: "postgres supports except", dialect: PostgresDialect{}, capability: CapabilityExcept, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.dialect.SupportsCapability(tt.capability); got != tt.want {
				t.Fatalf("SupportsCapability(%q) = %t, want %t", tt.capability, got, tt.want)
			}
		})
	}
}

// TestSupportsCapabilityAcceptsAliases covers the spelling variants
// canonicalCapabilityName maps, which are what users see in error text.
func TestSupportsCapabilityAcceptsAliases(t *testing.T) {
	if !(PostgresDialect{}).SupportsCapability(Capability("FULL JOIN")) {
		t.Fatal(`postgres should recognize the "FULL JOIN" spelling`)
	}

	if (MySQLDialect{}).SupportsCapability(Capability("full outer join")) {
		t.Fatal(`mysql should reject the "full outer join" spelling too, not fail to recognize it`)
	}
}

// TestUnknownCapabilityIsUnsupported documents that an unrecognized capability name
// is reported as unsupported rather than accidentally allowed.
func TestUnknownCapabilityIsUnsupported(t *testing.T) {
	for name, dialect := range map[Name]Dialect{
		MySQL:    MySQLDialect{},
		Postgres: PostgresDialect{},
		SQLite:   SQLiteDialect{},
	} {
		if dialect.SupportsCapability(Capability("LATERAL_JOIN")) {
			t.Errorf("dialect %s reported an undeclared capability as supported", name)
		}
	}
}

// TestDialectNamesAreLowercase keeps error text stable: the dialect name is half of
// every ErrUnsupportedCapability message.
func TestDialectNamesAreLowercase(t *testing.T) {
	for _, dialect := range []Dialect{SQLiteDialect{}, MySQLDialect{}, PostgresDialect{}} {
		name := string(dialect.Name())
		if name == "" {
			t.Fatal("dialect name should not be empty")
		}

		for _, ch := range name {
			if ch >= 'A' && ch <= 'Z' {
				t.Fatalf("dialect name %q should be lowercase", name)
			}
		}
	}
}

// TestErrUnsupportedCapabilityNamesBothSides keeps the two facts a caller needs in the
// error text: which capability, and which dialect refused it. The actionable hint is
// checked too, because "unsupported" without a way forward costs a round of triage.
func TestErrUnsupportedCapabilityNamesBothSides(t *testing.T) {
	err := ValidateCapability(MySQLDialect{}, CapabilityFullOuterJoin)
	if err == nil {
		t.Fatal("expected unsupported capability error")
	}

	msg := err.Error()
	for _, want := range []string{"FULL JOIN", "mysql", "UNION"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("expected %q in error, got %q", want, msg)
		}
	}
}
