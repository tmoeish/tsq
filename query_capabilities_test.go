package tsq

import (
	"slices"
	"testing"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
)

// TestDetectSQLCapabilitiesIgnoresStringLiterals is the regression gate for a
// query that could not run at all.
//
// The check exists to turn "this dialect cannot do that" into a clear error
// instead of a database failure. Matching the keyword inside a string literal
// inverted it: an ordinary query whose data happened to contain the words was
// refused before it ever reached the server.
func TestDetectSQLCapabilitiesIgnoresStringLiterals(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []tsqdialect.Capability
	}{
		{
			name: "row lock words inside a literal",
			sql:  `SELECT "u"."name" FROM "u" WHERE "u"."note" = ' FOR UPDATE '`,
			want: nil,
		},
		{
			name: "set operator inside a literal",
			sql:  `SELECT "u"."name" FROM "u" WHERE "u"."note" = 'a EXCEPT b'`,
			want: nil,
		},
		{
			name: "join words inside a literal",
			sql:  `SELECT "u"."name" FROM "u" WHERE "u"."note" = 'x FULL JOIN y'`,
			want: nil,
		},
		{
			name: "keyword inside a line comment",
			sql:  "SELECT \"u\".\"name\" FROM \"u\" -- FOR UPDATE\n",
			want: nil,
		},
		{
			name: "a real row lock is still detected",
			sql:  `SELECT "u"."name" FROM "u" FOR UPDATE`,
			want: []tsqdialect.Capability{tsqdialect.CapabilitySelectForUpdate},
		},
		{
			name: "a real lock with a wait mode is detected",
			sql:  `SELECT "u"."name" FROM "u" FOR UPDATE SKIP LOCKED`,
			want: []tsqdialect.Capability{tsqdialect.CapabilitySelectForUpdate, tsqdialect.CapabilitySelectForSkipLocked},
		},
		{
			name: "a real set operation is detected",
			sql:  `SELECT "a" FROM "t" EXCEPT SELECT "a" FROM "u"`,
			want: []tsqdialect.Capability{tsqdialect.CapabilityExcept},
		},
		{
			name: "a real full join is detected",
			sql:  `SELECT "a" FROM "t" FULL JOIN "u" ON "t"."id" = "u"."id"`,
			want: []tsqdialect.Capability{tsqdialect.CapabilityFullOuterJoin},
		},
		{
			name: "a CTE prefix is detected",
			sql:  `WITH "c" AS (SELECT 1) SELECT "a" FROM "c"`,
			want: []tsqdialect.Capability{tsqdialect.CapabilityCTE},
		},
		{
			name: "a literal next to a real clause does not hide it",
			sql:  `SELECT "u"."name" FROM "u" WHERE "u"."note" = ' EXCEPT ' FOR UPDATE`,
			want: []tsqdialect.Capability{tsqdialect.CapabilitySelectForUpdate},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := detectSQLCapabilities(test.sql)
			if !slices.Equal(got, test.want) {
				t.Fatalf("expected %v, got %v", test.want, got)
			}
		})
	}
}

// TestExecutorRejectsOnlyRealCapabilityUse ties the detection to the behavior it
// drives: SQLite has no row locks, so a query that really takes one is refused
// and a query that merely mentions the words is not.
func TestExecutorRejectsOnlyRealCapabilityUse(t *testing.T) {
	_, dsn := newSQLiteIndexTestEngine(t)

	runtime, err := Open(t.Context(), "sqlite", dsn, nil)
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}

	t.Cleanup(func() { _ = runtime.Close() })

	literal := `SELECT 1 WHERE 'x' = ' FOR UPDATE '`
	if err := validateOperationalExecutorForSQL(runtime, literal); err != nil {
		t.Fatalf("expected a literal mentioning a row lock to be accepted, got %v", err)
	}

	locking := `SELECT 1 FOR UPDATE`
	if err := validateOperationalExecutorForSQL(runtime, locking); err == nil {
		t.Fatal("expected SQLite to refuse a real row lock")
	}
}
