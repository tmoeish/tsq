package tsq

import (
	"context"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

// TestOrderByRendersAfterTheBodyAndBeforeTheLock pins the clause order. ORDER BY has to
// land outside the query body (a set-operation operand or CTE body would otherwise bind
// it to the operand) but still before the row-lock clause, which SQL puts last.
func TestOrderByRendersAfterTheBodyAndBeforeTheLock(t *testing.T) {
	table := newMockTable("users")
	id := newMockColumn(table, "id")
	name := newMockColumn(table, "name")

	query := Select[Table](id, name).From(table).
		OrderBy(name.Asc(), id.Desc()).
		Limit(10).
		Offset(20).
		ForUpdate().
		MustBuild()

	got := query.ListSQL()
	want := `SELECT "users"."id", "users"."name" FROM "users" ` +
		`ORDER BY "users"."name" ASC, "users"."id" DESC LIMIT ? OFFSET ? FOR UPDATE`

	if got != want {
		t.Fatalf("ListSQL()\n got: %s\nwant: %s", got, want)
	}

	// The count query answers "how many rows match", which a LIMIT does not change.
	if strings.Contains(query.CountSQL(), "LIMIT") || strings.Contains(query.CountSQL(), "ORDER BY") {
		t.Fatalf("count query must not carry the paging tail, got %s", query.CountSQL())
	}
}

// TestOrderByIsReachableFromEveryCompleteStage keeps the stage machine honest: a clause
// SQL allows after filtering, grouping and set operations must be reachable from each of
// those states, or callers have to restructure the chain to sort.
func TestOrderByIsReachableFromEveryCompleteStage(t *testing.T) {
	table := newMockTable("users")
	id := newMockColumn(table, "id")
	other := newMockTable("archived_users")
	otherID := newMockColumn(other, "id")

	stages := map[string]PagedStage[Table]{
		"base":     Select[Table](id).From(table).OrderBy(id.Asc()),
		"where":    Select[Table](id).From(table).Where(id.EQVar()).OrderBy(id.Asc()),
		"search":   Select[Table](id).From(table).Search(id).OrderBy(id.Asc()),
		"filtered": Select[Table](id).From(table).Where(id.EQVar()).Search(id).OrderBy(id.Asc()),
		"grouped":  Select[Table](id).From(table).GroupBy(id).OrderBy(id.Asc()),
		"having":   Select[Table](id).From(table).GroupBy(id).Having(id.EQVar()).OrderBy(id.Asc()),
		"compound": Select[Table](id).From(table).Union(Select[Table](otherID).From(other)).OrderBy(id.Asc()),
	}

	for name, stage := range stages {
		t.Run(name, func(t *testing.T) {
			query, err := stage.Build()
			if err != nil {
				t.Fatalf("OrderBy from the %s stage: %v", name, err)
			}

			if !strings.Contains(query.ListSQL(), "ORDER BY") {
				t.Fatalf("expected ORDER BY in %s", query.ListSQL())
			}
		})
	}
}

// TestOffsetWithoutLimitIsRejectedAtBuild covers a dialect asymmetry that would
// otherwise surface as a database error on two dialects out of three: PostgreSQL
// accepts a bare OFFSET, MySQL and SQLite reject it.
func TestOffsetWithoutLimitIsRejectedAtBuild(t *testing.T) {
	table := newMockTable("users")
	id := newMockColumn(table, "id")

	_, err := Select[Table](id).From(table).Offset(10).Build()
	if err == nil {
		t.Fatal("expected a bare OFFSET to be rejected at Build")
	}

	if !strings.Contains(err.Error(), "Offset requires Limit") {
		t.Fatalf("expected the error to name the constraint, got %v", err)
	}
}

// TestPagingRejectsInvalidValues keeps negative bounds out of the rendered SQL.
func TestPagingRejectsInvalidValues(t *testing.T) {
	table := newMockTable("users")
	id := newMockColumn(table, "id")

	if _, err := Select[Table](id).From(table).Limit(-1).Build(); err == nil {
		t.Fatal("expected a negative limit to be rejected")
	}

	if _, err := Select[Table](id).From(table).Limit(1).Offset(-1).Build(); err == nil {
		t.Fatal("expected a negative offset to be rejected")
	}

	if _, err := Select[Table](id).From(table).OrderBy().Build(); err == nil {
		t.Fatal("expected an empty OrderBy to be rejected")
	}
}

// TestOrderByColumnMustBeInTheJoinGraph reuses the existing structural rule: sorting by
// a column from a table the query never joins is the same mistake as selecting one.
func TestOrderByColumnMustBeInTheJoinGraph(t *testing.T) {
	table := newMockTable("users")
	id := newMockColumn(table, "id")
	stranger := newMockColumn(newMockTable("unrelated"), "id")

	_, err := Select[Table](id).From(table).OrderBy(stranger.Asc()).Build()
	if err == nil {
		t.Fatal("expected ORDER BY on an unjoined table to be rejected")
	}
}

// TestPageRefusesToFightBuilderPaging documents the interaction rule. Page appends its
// own paging clauses, so a builder-level one would be emitted a second time rather than
// replaced; that is invalid SQL on every dialect, and guessing which one the caller
// meant would be worse than saying so.
func TestPageRefusesToFightBuilderPaging(t *testing.T) {
	table := newMockTable("users")
	id := newMockColumn(table, "id")

	limited := Select[Table](id).From(table).Limit(10).MustBuild()
	if _, _, err := limited.buildPageSQLs(&PageRequest{Page: 1, Size: 10}); err == nil {
		t.Fatal("expected Page to reject a builder-level Limit")
	}

	ordered := Select[Table](id).From(table).OrderBy(id.Asc()).MustBuild()
	if _, _, err := ordered.buildPageSQLs(&PageRequest{Page: 1, Size: 10, OrderBy: "id"}); err == nil {
		t.Fatal("expected Page to reject a competing OrderBy")
	}

	// With no PageRequest.OrderBy there is no competition: the builder's ORDER BY
	// stands and Page only appends LIMIT/OFFSET after it.
	_, listSQL, err := ordered.buildPageSQLs(&PageRequest{Page: 1, Size: 10})
	if err != nil {
		t.Fatalf("expected a builder ORDER BY to survive paging, got %v", err)
	}

	if strings.Count(renderCanonicalSQL(listSQL), "ORDER BY") != 1 {
		t.Fatalf("expected exactly one ORDER BY, got %s", renderCanonicalSQL(listSQL))
	}
}

// TestOrderByLimitOffsetExecuteOnSQLite runs the rendered SQL. Golden-string tests prove
// the text is what we meant to write; only the database proves it is valid SQL.
func TestOrderByLimitOffsetExecuteOnSQLite(t *testing.T) {
	runtime := newBatchMutationEngine(t)
	exec := requireInitializedRuntime(t, runtime)

	for _, name := range []string{"alice", "bob", "carol", "dave"} {
		if _, err := runtime.DB().ExecContext(context.Background(),
			`INSERT INTO users (name, email) VALUES (?, ?)`, name, name+"@example.com"); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
	}

	rows, err := Select(batchMutationUserColumns()...).From(batchMutationUser{}).
		OrderBy(orderByName(t, DESC)).
		Limit(2).
		Offset(1).
		Build()
	if err != nil {
		t.Fatalf("build ordered query: %v", err)
	}

	list, err := rows.List(context.Background(), exec)
	if err != nil {
		t.Fatalf("execute ordered query: %v", err)
	}

	got := make([]string, 0, len(list))
	for _, row := range list {
		got = append(got, row.Name)
	}

	// Descending by name is dave, carol, bob, alice; skipping one and taking two.
	if strings.Join(got, ",") != "carol,bob" {
		t.Fatalf("expected carol,bob, got %v", got)
	}
}

func orderByName(t *testing.T, order Order) OrderBy {
	t.Helper()

	col, ok := batchMutationUserColumns()[1].(Column[batchMutationUser, string])
	if !ok {
		t.Fatal("expected the name column to expose the typed Column API")
	}

	if order == DESC {
		return col.Desc()
	}

	return col.Asc()
}
