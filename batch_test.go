package tsq

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"testing"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
	sqld "github.com/tmoeish/tsq/v5/internal/sqldialect"
)

// wideColumns makes the bind limit reachable with few rows: a batch UPDATE costs
// rows² × columns to evaluate, so a wider table keeps the test fast.
const wideColumns = 200

type wide struct {
	ID     int64
	Values [wideColumns - 1]int64
}

// bindLimitTable is a table wide enough that a batch at the default size would exceed
// SQLite's bind parameter limit if statements were sized by rows.
var bindLimitTable = func() *TableOf[wide, int64] {
	h := NewTable[wide, int64]("wide_rows")
	id := NewColumn(h, "id", "id", func(r *wide) *int64 { return &r.ID })
	cols := []BoundColumn[wide]{id}
	schema := []tsqdialect.ColumnSpec{{Name: "id", Type: tsqdialect.ColumnType{Kind: tsqdialect.ColumnKindInt, Bits: 64}, PrimaryKey: true, AutoIncrement: true}}

	for i := range wideColumns - 1 {
		name := fmt.Sprintf("c%d", i)
		cols = append(cols, NewColumn(h, name, name, func(r *wide) *int64 { return &r.Values[i] }))
		schema = append(schema, tsqdialect.ColumnSpec{Name: name, Type: tsqdialect.ColumnType{Kind: tsqdialect.ColumnKindInt, Bits: 64}})
	}

	return h.Define(TableSpec[wide, int64]{Columns: cols, PrimaryKey: id, AutoIncrement: true, ColumnSpecs: schema})
}()

func newWideRuntime(t *testing.T) *Runtime {
	t.Helper()

	rt, err := Open(context.Background(), "sqlite", t.TempDir()+"/wide.db", []Table{bindLimitTable}, WithSchemaPolicy(SchemaPolicyCreateMissing))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = rt.Close() })

	return rt
}

// TestBatchWritesOnAWideTableStayUnderTheBindLimit is the end-to-end gate for sizing
// statements by placeholders: SQLite accepts 32766, and a batch UPDATE binds about
// two per column per row.
func TestBatchWritesOnAWideTableStayUnderTheBindLimit(t *testing.T) {
	if raceEnabled {
		t.Skip("single-goroutine and slow under the race detector; runs in the plain test pass")
	}

	ctx := context.Background()
	rt := newWideRuntime(t)

	rows := make([]*wide, 400)
	for i := range rows {
		rows[i] = &wide{}
		for j := range rows[i].Values {
			rows[i].Values[j] = int64(i*wideColumns + j)
		}
	}

	if err := bindLimitTable.BatchInsert(ctx, rt, rows); err != nil {
		t.Fatalf("BatchInsert() error = %v", err)
	}

	if rows[len(rows)-1].ID != int64(len(rows)) {
		t.Fatalf("generated keys were not assigned across statements: last = %d", rows[len(rows)-1].ID)
	}

	// 90 rows bind about 36k parameters in one UPDATE, past the limit, so they must
	// be split; sized by the INSERT estimate they would not be.
	update := rows[:90]
	for i, row := range update {
		row.Values[0] = -int64(i + 1)
	}

	if err := bindLimitTable.BatchUpdate(ctx, rt, update); err != nil {
		t.Fatalf("BatchUpdate() error = %v", err)
	}

	n, err := Select(bindLimitTable.Columns()[1]).From(bindLimitTable).MustBuild().Count(ctx, rt)
	if err != nil || n != int64(len(rows)) {
		t.Fatalf("Count() = %d, %v", n, err)
	}

	var updated int
	if err := rt.QueryRowContext(ctx, `SELECT COUNT(*) FROM wide_rows WHERE c0 < 0`).Scan(&updated); err != nil || updated != len(update) {
		t.Fatalf("updated rows = %d, %v", updated, err)
	}
}

func TestSQLiteRejectsMoreBoundParametersThanItsCeiling(t *testing.T) {
	if raceEnabled {
		t.Skip("single-goroutine and slow under the race detector; runs in the plain test pass")
	}

	limit := sqld.MaxBindParams(sqld.SQLiteDialect{})

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = db.Close() })

	if _, err := db.ExecContext(context.Background(), `CREATE TABLE probe (a INT)`); err != nil {
		t.Fatal(err)
	}

	bind := func(count int) error {
		placeholders := make([]string, count)
		args := make([]any, count)

		for i := range placeholders {
			placeholders[i] = "(?)"
			args[i] = i
		}

		_, err := db.ExecContext(context.Background(), "INSERT INTO probe VALUES "+strings.Join(placeholders, ","), args...)

		return err
	}

	if err := bind(limit); err != nil {
		t.Fatalf("sqlite rejected %d parameters: %v", limit, err)
	}

	if err := bind(limit + 1); err == nil {
		t.Fatalf("sqlite accepted %d parameters; the declared limit %d is too low", limit+1, limit)
	}
}

func TestEffectiveChunkSize(t *testing.T) {
	limit := sqld.MaxBindParams(sqld.SQLiteDialect{})

	tests := []struct {
		chunk, perRow, max, want int
	}{
		{1000, 5, limit, 1000},
		{1000, 70, limit, limit / 70},
		{1000, limit + 1, limit, 1},
		{1000, 0, limit, 1000},
		{10, 1, limit, 10},
		{1000, 5, 3000, 600},
	}

	for _, tt := range tests {
		if got := effectiveChunkSize(tt.chunk, tt.perRow, tt.max); got != tt.want {
			t.Errorf("effectiveChunkSize(%d, %d, %d) = %d, want %d", tt.chunk, tt.perRow, tt.max, got, tt.want)
		}
	}
}

// TestBatchWritesFitTheDefaultBatchOnSQLite writes 1000 rows of a table with a
// version column under the default batch size. Matching them by key and version
// used to render one OR per row, and SQLite refuses an expression deeper than
// 1000, so every one of these failed from 998 rows on.
func TestBatchWritesFitTheDefaultBatchOnSQLite(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)

	rows := make([]*user, 1000)
	for i := range rows {
		rows[i] = &user{Name: "u", Email: fmt.Sprintf("u%d@example.test", i)}
	}

	if err := Users.BatchInsert(ctx, rt, rows); err != nil {
		t.Fatal(err)
	}

	for _, row := range rows {
		row.Name = "v"
	}

	if err := Users.BatchUpdate(ctx, rt, rows); err != nil {
		t.Fatalf("BatchUpdate: %v", err)
	}

	if err := Users.BatchDelete(ctx, rt, rows); err != nil {
		t.Fatalf("BatchDelete: %v", err)
	}

	if err := Users.BatchHardDelete(ctx, rt, rows); err != nil {
		t.Fatalf("BatchHardDelete: %v", err)
	}
}

// TestBatchUpdateJoinsTheRowsOnEveryDialect pins the statement a batch update of
// several rows is: the table joined to the list of the rows' values, where it was
// a CASE over the keys for every column, which a database evaluates arm by arm
// for each row (the default batch of a thousand cost a thousand times a thousand).
// A list of parameters has no types, and each dialect is told them its own way:
// PostgreSQL by a first row of NULLs of the columns' types, MySQL by a first
// branch that selects the columns over no rows, and SQLite needs none.
func TestBatchUpdateJoinsTheRowsOnEveryDialect(t *testing.T) {
	def := Users.def
	cols := []*columnCore{def.column("name")}
	rows := []*user{{ID: 1, Name: "a", Version: 3}, {ID: 2, Name: "b", Version: 4}}

	for name, want := range map[tsqdialect.Name]string{
		tsqdialect.SQLite: `WITH "tsq_v"("tsq_k", "tsq_n", "tsq_0") AS (VALUES (?, ?, ?), (?, ?, ?)) ` +
			`UPDATE "users" SET "name" = "tsq_v"."tsq_0", "version" = "users"."version" + 1 FROM "tsq_v" ` +
			`WHERE "users"."id" = "tsq_v"."tsq_k" AND "users"."version" = "tsq_v"."tsq_n"`,
		tsqdialect.Postgres: `UPDATE "users" SET "name" = "tsq_v"."tsq_0", "version" = "users"."version" + 1 ` +
			`FROM (VALUES ((SELECT "id" FROM "users" WHERE FALSE), (SELECT "version" FROM "users" WHERE FALSE), (SELECT "name" FROM "users" WHERE FALSE)), ` +
			`($1, $2, $3), ($4, $5, $6)) AS "tsq_v"("tsq_k", "tsq_n", "tsq_0") ` +
			`WHERE "users"."id" = "tsq_v"."tsq_k" AND "users"."version" = "tsq_v"."tsq_n"`,
		tsqdialect.MySQL: "UPDATE `users` JOIN (SELECT `id` AS `tsq_k`, `version` AS `tsq_n`, `name` AS `tsq_0` FROM `users` WHERE FALSE " +
			"UNION ALL SELECT ?, ?, ? UNION ALL SELECT ?, ?, ?) AS `tsq_v` " +
			"ON `users`.`id` = `tsq_v`.`tsq_k` AND `users`.`version` = `tsq_v`.`tsq_n` " +
			"SET `users`.`name` = `tsq_v`.`tsq_0`, `users`.`version` = `users`.`version` + 1 WHERE TRUE",
	} {
		d, err := sqld.For(name)
		if err != nil {
			t.Fatal(err)
		}

		w := &writeStmt{d: d}
		writeJoinedUpdate(w, def, cols, def.column("version"), rows)

		if got := w.sql.String(); got != want {
			t.Errorf("%s:\n got %s\nwant %s", name, got, want)
		}

		if want := []any{int64(1), int64(3), "a", int64(2), int64(4), "b"}; !slices.Equal(w.args, want) {
			t.Errorf("%s binds %v, want %v", name, w.args, want)
		}
	}

	// Without a version the rows are matched by key alone.
	w := &writeStmt{d: sqld.SQLiteDialect{}}
	writeJoinedUpdate(w, Orders.def, []*columnCore{Orders.def.column("note")}, nil, []*order{{ID: 1}, {ID: 2}})

	if got := w.sql.String(); strings.Contains(got, "tsq_n") || !strings.HasSuffix(got, `WHERE "orders"."id" = "tsq_v"."tsq_k"`) {
		t.Errorf("without a version: %s", got)
	}
}
