package dialect

import (
	"errors"
	"strings"
	"testing"
)

// TestEnginesCoverAllCapabilities is the gate behind the rule that every engine must
// take an explicit position on every capability. A capability missing from a table
// would read as unsupported with nothing failing: not the compiler, not the linter.
func TestEnginesCoverAllCapabilities(t *testing.T) {
	for _, engine := range []Name{MySQL, Postgres, SQLite} {
		table, ok := capabilities[engine]
		if !ok {
			t.Fatalf("engine %s has no capability table", engine)
		}

		for _, capability := range allCapabilities {
			if _, declared := table[capability]; !declared {
				t.Errorf("engine %s does not declare capability %s; add an explicit true/false entry", engine, capability)
			}
		}

		if len(table) != len(allCapabilities) {
			t.Errorf("engine %s declares %d capabilities, want %d", engine, len(table), len(allCapabilities))
		}
	}

	if len(capabilities) != 3 {
		t.Errorf("capability tables cover %d engines, want 3", len(capabilities))
	}
}

// TestAllCapabilitiesHasNoDuplicates keeps the length comparison above meaningful.
func TestAllCapabilitiesHasNoDuplicates(t *testing.T) {
	seen := make(map[Capability]bool, len(allCapabilities))
	for _, capability := range allCapabilities {
		if seen[capability] {
			t.Errorf("capability %s appears twice", capability)
		}

		seen[capability] = true
	}
}

// TestCapabilityBaselines pins the version baselines documented in architecture.md.
// Changing one of these is a user-visible change, not a refactor.
func TestCapabilityBaselines(t *testing.T) {
	tests := []struct {
		engine     Name
		capability Capability
		want       bool
	}{
		{SQLite, CapabilityFullOuterJoin, true},
		{SQLite, CapabilityCTE, true},
		{SQLite, CapabilitySelectForUpdate, false},
		{SQLite, CapabilityFullTextSearch, false},
		{MySQL, CapabilityCTE, true},
		{MySQL, CapabilityIntersect, true},
		{MySQL, CapabilityExcept, true},
		{MySQL, CapabilityFullOuterJoin, false},
		{MySQL, CapabilitySelectForSkipLocked, true},
		{Postgres, CapabilityFullOuterJoin, true},
		{Postgres, CapabilitySelectForShare, true},
		{Postgres, CapabilityExcept, true},
	}

	for _, tt := range tests {
		if got := Supports(tt.engine, tt.capability); got != tt.want {
			t.Errorf("Supports(%s, %s) = %t, want %t", tt.engine, tt.capability, got, tt.want)
		}
	}
}

// TestSupportsAcceptsSQLSpellings covers the spellings canonicalCapability maps.
func TestSupportsAcceptsSQLSpellings(t *testing.T) {
	if !Supports(Postgres, "FULL JOIN") {
		t.Fatal(`postgres should recognize the "FULL JOIN" spelling`)
	}

	if Supports(MySQL, "full outer join") {
		t.Fatal(`mysql should reject the "full outer join" spelling, not fail to recognize it`)
	}
}

// TestUnknownIsUnsupported documents that an unrecognized engine or capability is
// reported as unsupported rather than accidentally allowed.
func TestUnknownIsUnsupported(t *testing.T) {
	if Supports(MySQL, "LATERAL_JOIN") {
		t.Error("an undeclared capability was reported as supported")
	}

	if Supports("oracle", CapabilityCTE) {
		t.Error("an unknown engine was reported as supporting a capability")
	}
}

// TestUnsupportedCapabilityErrorNamesBothSides keeps the two facts a caller needs,
// and the actionable hint, in the error.
func TestUnsupportedCapabilityErrorNamesBothSides(t *testing.T) {
	err := Check(MySQL, CapabilityFullOuterJoin)

	target, ok := errors.AsType[*UnsupportedCapabilityError](err)
	if !ok {
		t.Fatalf("Check = %v, want *UnsupportedCapabilityError", err)
	}

	if target.Capability != CapabilityFullOuterJoin || target.Dialect != MySQL {
		t.Fatalf("error fields = %s/%s", target.Capability, target.Dialect)
	}

	for _, want := range []string{"FULL JOIN", "mysql", "UNION"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("expected %q in error, got %q", want, err)
		}
	}

	if err := Check(Postgres, CapabilityFullOuterJoin); err != nil {
		t.Fatalf("Check(postgres, full join) = %v, want nil", err)
	}
}
