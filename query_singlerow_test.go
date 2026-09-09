package tsq

import "testing"

// The clause order matters: every supported dialect wants LIMIT before the
// row-lock clause, and a query that already carries its own LIMIT must be left
// alone rather than given a second one.
func TestLimitToSingleRowPlacesTheClauseCorrectly(t *testing.T) {
	tests := []struct {
		name     string
		sqlText  string
		hasLimit bool
		want     string
	}{
		{
			name:    "plain select",
			sqlText: "SELECT a FROM t WHERE b = ?",
			want:    "SELECT a FROM t WHERE b = ? LIMIT 1",
		},
		{
			name:    "ordered select",
			sqlText: "SELECT a FROM t ORDER BY a DESC",
			want:    "SELECT a FROM t ORDER BY a DESC LIMIT 1",
		},
		{
			name:    "row lock keeps the trailing position",
			sqlText: "SELECT a FROM t WHERE b = ? FOR UPDATE",
			want:    "SELECT a FROM t WHERE b = ? LIMIT 1 FOR UPDATE",
		},
		{
			name:    "row lock with wait mode",
			sqlText: "SELECT a FROM t FOR UPDATE SKIP LOCKED",
			want:    "SELECT a FROM t LIMIT 1 FOR UPDATE SKIP LOCKED",
		},
		{
			name:     "builder already limited",
			sqlText:  "SELECT a FROM t LIMIT 5 OFFSET 10",
			hasLimit: true,
			want:     "SELECT a FROM t LIMIT 5 OFFSET 10",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := limitToSingleRow(test.sqlText, test.hasLimit); got != test.want {
				t.Fatalf("expected %q, got %q", test.want, got)
			}
		})
	}
}

func TestGetBoundsTheStatement(t *testing.T) {
	table := newMockTable("users")
	id := newColForTable[Table, int](table, "id", "id", nil)

	query := mustBuild(Select(id).From(table).Where(id.EQVar()))

	// Get and Find share one statement, so checking the rendered list SQL after
	// the single-row bound is applied covers both.
	got := renderCanonicalSQL(limitToSingleRow(query.listSQL, query.hasLimit))

	want := `SELECT "users"."id" FROM "users" WHERE "users"."id" = ? LIMIT 1`
	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}
