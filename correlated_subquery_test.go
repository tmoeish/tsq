package tsq

import (
	"context"
	"database/sql"
	"testing"

	_ "modernc.org/sqlite"

	tsqdialect "github.com/tmoeish/tsq/v4/dialect"
)

type correlatedUser struct {
	ID int64
}

func (correlatedUser) TSQOwner()                     {}
func (correlatedUser) Table() string                 { return "users" }
func (correlatedUser) Cols() []SQLColumn             { return SQLColumns(correlatedUserCols()...) }
func (correlatedUser) SearchColumns() []SearchColumn { return nil }
func (correlatedUser) PrimaryKeys() []string         { return []string{"id"} }
func (correlatedUser) AutoIncrement() bool           { return true }
func (correlatedUser) VersionColumn() string         { return "" }

func correlatedUserCols() []BoundColumn[correlatedUser] {
	return []BoundColumn[correlatedUser]{correlatedUserID}
}

type correlatedOrder struct {
	ID     int64
	UserID int64
}

func (correlatedOrder) TSQOwner()                     {}
func (correlatedOrder) Table() string                 { return "orders" }
func (correlatedOrder) Cols() []SQLColumn             { return SQLColumns(correlatedOrderCols()...) }
func (correlatedOrder) SearchColumns() []SearchColumn { return nil }
func (correlatedOrder) PrimaryKeys() []string         { return []string{"id"} }
func (correlatedOrder) AutoIncrement() bool           { return true }
func (correlatedOrder) VersionColumn() string         { return "" }

func correlatedOrderCols() []BoundColumn[correlatedOrder] {
	return []BoundColumn[correlatedOrder]{correlatedOrderID, correlatedOrderUserID}
}

var (
	correlatedUserID = NewCol[correlatedUser, int64](
		"id", "id", func(t *correlatedUser) *int64 { return &t.ID },
	)
	correlatedOrderID = NewCol[correlatedOrder, int64](
		"id", "id", func(t *correlatedOrder) *int64 { return &t.ID },
	)
	correlatedOrderUserID = NewCol[correlatedOrder, int64](
		"user_id", "user_id", func(t *correlatedOrder) *int64 { return &t.UserID },
	)
	tableCorrelatedUser  Table = TableWithCols(correlatedUser{}, correlatedUserCols())
	tableCorrelatedOrder Table = TableWithCols(correlatedOrder{}, correlatedOrderCols())
)

func newCorrelatedRuntime(t *testing.T, seed ...string) *Runtime {
	t.Helper()

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}

	t.Cleanup(func() { _ = db.Close() })

	stmts := append([]string{
		`CREATE TABLE users (id INTEGER PRIMARY KEY)`,
		`CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER)`,
		`INSERT INTO users (id) VALUES (1), (2), (3)`,
		`INSERT INTO orders (id, user_id) VALUES (10, 1)`,
	}, seed...)
	for _, stmt := range stmts {
		if _, err := db.ExecContext(context.Background(), stmt); err != nil {
			t.Fatalf("seed %q: %v", stmt, err)
		}
	}

	return newRuntimeWithDB(db, tsqdialect.SQLiteDialect{})
}

func correlatedOrdersOfOuterUser(t *testing.T) Subquery[int64] {
	t.Helper()

	sub, err := BuildSubquery(
		Select(correlatedOrderID).
			From(tableCorrelatedOrder).
			Correlate(tableCorrelatedUser).
			Where(correlatedOrderUserID.EQ(correlatedUserID)),
		correlatedOrderID,
	)
	if err != nil {
		t.Fatalf("build correlated subquery: %v", err)
	}

	return sub
}

func listUserIDs(t *testing.T, runtime *Runtime, query *Query[correlatedUser]) []int64 {
	t.Helper()

	rows, err := query.List(context.Background(), requireInitializedRuntime(t, runtime))
	if err != nil {
		t.Fatalf("list users: %v", err)
	}

	ids := make([]int64, 0, len(rows))
	for _, row := range rows {
		ids = append(ids, row.ID)
	}

	return ids
}

// Only user 1 has an order, so a correlated NOT EXISTS must return exactly the
// other two. String comparison cannot prove this: the shape that answers the
// wrong question is also valid SQL, so only a real database settles it.
func TestCorrelatedSubquery_NotExistsSelectsRowsWithoutMatches(t *testing.T) {
	runtime := newCorrelatedRuntime(t)

	query := mustBuild(
		Select(correlatedUserID).
			From(tableCorrelatedUser).
			Where(correlatedUserID.NExistsSub(correlatedOrdersOfOuterUser(t))),
	)

	if got := listUserIDs(t, runtime, query); len(got) != 2 || got[0] != 2 || got[1] != 3 {
		t.Fatalf("expected users [2 3] to have no orders, got %v", got)
	}
}

// The shape the removed CrossJoin advice produced, run against a real database:
// the joined users table shadows the outer one, the predicate is no longer
// correlated, and NOT EXISTS answers "does any (order, user) pair exist at all"
// for every row. This is why Correlate rejects a table the query also joins.
func TestCorrelatedSubquery_JoiningTheOuterTableAnswersADifferentQuestion(t *testing.T) {
	runtime := newCorrelatedRuntime(t)
	exec := requireInitializedRuntime(t, runtime)

	rows, err := exec.QueryContext(context.Background(),
		`SELECT "users"."id" FROM "users" WHERE NOT EXISTS `+
			`(SELECT "orders"."id" FROM "orders" CROSS JOIN "users" `+
			`WHERE "orders"."user_id" = "users"."id")`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer func() { _ = rows.Close() }()

	count := 0
	for rows.Next() {
		count++
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("scan: %v", err)
	}

	if count != 0 {
		t.Fatalf("expected the uncorrelated shape to return no rows, got %d", count)
	}
}

// The NOT IN rewrite users reached for before Correlate existed is only
// equivalent when the subquery column cannot be NULL. Documented in README and
// skills/tsq; pinned here so the claim stays true.
func TestCorrelatedSubquery_NotInRewriteBreaksOnNull(t *testing.T) {
	runtime := newCorrelatedRuntime(t, `INSERT INTO orders (id, user_id) VALUES (11, NULL)`)

	correlatedQuery := mustBuild(
		Select(correlatedUserID).
			From(tableCorrelatedUser).
			Where(correlatedUserID.NExistsSub(correlatedOrdersOfOuterUser(t))),
	)
	if got := listUserIDs(t, runtime, correlatedQuery); len(got) != 2 {
		t.Fatalf("expected NOT EXISTS to stay correct with a NULL row, got %v", got)
	}

	membership, err := BuildSubquery(
		Select(correlatedOrderUserID).From(tableCorrelatedOrder),
		correlatedOrderUserID,
	)
	if err != nil {
		t.Fatalf("build membership subquery: %v", err)
	}

	notInQuery := mustBuild(
		Select(correlatedUserID).
			From(tableCorrelatedUser).
			Where(correlatedUserID.NIn(membership)),
	)
	if got := listUserIDs(t, runtime, notInQuery); len(got) != 0 {
		t.Fatalf("expected NOT IN over a NULL-containing set to return no rows, got %v", got)
	}
}
