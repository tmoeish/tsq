package tsq

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
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
var bindLimitTable = func() *TableOf[wide] {
	h := NewTable[wide]("wide_rows")
	id := NewColumn(h, "id", "id", func(r *wide) *int64 { return &r.ID })
	cols := []BoundColumn[wide]{id}
	schema := []tsqdialect.ColumnSpec{{Name: "id", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64}, PrimaryKey: true, AutoIncrement: true}}

	for i := range wideColumns - 1 {
		name := fmt.Sprintf("c%d", i)
		cols = append(cols, NewColumn(h, name, name, func(r *wide) *int64 { return &r.Values[i] }))
		schema = append(schema, tsqdialect.ColumnSpec{Name: name, Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64}})
	}

	return h.Define(TableSpec[wide]{Columns: cols, PrimaryKey: id, AutoIncrement: true, Schema: schema})
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

	limit := tsqdialect.MaxBindParams(tsqdialect.SQLiteDialect{})

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
	limit := tsqdialect.MaxBindParams(tsqdialect.SQLiteDialect{})

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
