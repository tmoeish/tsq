package sqldialect

import "testing"

// TestColumnDefaultsCompareByMeaning covers the default comparison of schema
// reconcile. It cut a value at its first "::", so PostgreSQL's 'a::b'::text never
// matched a declared 'a::b' and every boot set the default again, and it lowered
// everything, so 'Active' matched 'active' and a changed default was never seen.
func TestColumnDefaultsCompareByMeaning(t *testing.T) {
	for _, tt := range []struct {
		left, right string
		same        bool
	}{
		{"'USD'", "'USD'::character varying", true},
		{"'a::b'", "'a::b'::text", true},
		{"'it''s'", "'it''s'::text", true},
		{"CURRENT_TIMESTAMP", "current_timestamp", true},
		{"CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP(6)", true}, // MySQL on a DATETIME(6) column
		{"'USD'", "USD", true},                              // MySQL reads a string default back unquoted
		{"'Active'", "'active'::text", false},
		{"'a'", "'b'", false},
		{"true", "1", true}, // MySQL reads a boolean default back as a number
		{"FALSE", "0", true},
		{"0", "0.00", true}, // and a decimal one with its scale
		{"1.5", "1.50", true},
		{"1", "0", false},
		{"0", "false", true},
		{"('not_set')", "'not_set'", true}, // MySQL takes a TEXT default as an expression
		{"(UTC_TIMESTAMP(6))", "utc_timestamp(6)", true},
		{"(CURRENT_TIMESTAMP AT TIME ZONE 'UTC')", "timezone('UTC'::text, CURRENT_TIMESTAMP)", true},
		// A local current time where TSQ declares UTC, as a table created before it did.
		{"(UTC_TIMESTAMP(6))", "CURRENT_TIMESTAMP(6)", false},
		{"(CURRENT_TIMESTAMP AT TIME ZONE 'UTC')", "CURRENT_TIMESTAMP", false},
		{"(a) + (b)", "a) + (b", false},
	} {
		if got := SameDefault(tt.left, tt.right); got != tt.same {
			t.Errorf("SameDefault(%q, %q) = %v, want %v", tt.left, tt.right, got, tt.same)
		}
	}
}
