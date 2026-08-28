package tsq

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	_ "modernc.org/sqlite"

	tsqdialect "github.com/tmoeish/tsq/v4/dialect"
)

// wideRowColumns is chosen so that the default 1000-row chunk exceeds SQLite's
// 32766-parameter ceiling (1000 * 40 = 40000) while staying under the 65535 that tsq
// used to assume was the tightest limit. That gap is exactly the bug: sized against
// 65535, the chunk was left at 1000 rows and SQLite rejected the statement.
const wideRowColumns = 40

type wideRow struct {
	ID     int64
	Values [wideRowColumns - 1]int64
}

func (wideRow) TSQOwner() {}

func (wideRow) Table() string { return "wide_rows" }

func (wideRow) SearchColumns() []SearchColumn { return nil }

func (wideRow) PrimaryKeys() []string { return []string{"id"} }

func (wideRow) AutoIncrement() bool { return true }

func (wideRow) VersionColumn() string { return "" }

func (wideRow) Cols() []SQLColumn { return SQLColumns(wideRowColumnList...) }

var wideRowColumnList = buildWideRowColumns()

func buildWideRowColumns() []BoundColumn[wideRow] {
	cols := make([]BoundColumn[wideRow], 0, wideRowColumns)
	cols = append(cols, NewCol[wideRow, int64]("id", "id", func(t *wideRow) *int64 { return &t.ID }))

	for i := range wideRowColumns - 1 {
		name := fmt.Sprintf("c%d", i)
		cols = append(cols, NewCol[wideRow, int64](name, name, func(t *wideRow) *int64 { return &t.Values[i] }))
	}

	return cols
}

func newWideRowRuntime(t *testing.T) *Runtime {
	t.Helper()

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}

	t.Cleanup(func() { _ = db.Close() })

	columns := make([]string, 0, wideRowColumns)
	columns = append(columns, `"id" INTEGER PRIMARY KEY AUTOINCREMENT`)

	for i := range wideRowColumns - 1 {
		columns = append(columns, fmt.Sprintf(`"c%d" INTEGER NOT NULL`, i))
	}

	if _, err := db.ExecContext(context.Background(),
		"CREATE TABLE wide_rows (\n"+strings.Join(columns, ",\n")+"\n)"); err != nil {
		t.Fatalf("create wide_rows: %v", err)
	}

	return newRuntimeWithDB(db, tsqdialect.SQLiteDialect{})
}

// TestChunkedInsertOnWideTableStaysUnderSQLiteVariableLimit is the end-to-end gate for
// the per-dialect bind parameter ceiling. SQLite's SQLITE_MAX_VARIABLE_NUMBER is 32766,
// not the 65535 tsq assumed, so a wide-table batch at the default chunk size used to
// fail with "too many SQL variables" on the one database the unit suite runs against.
func TestChunkedInsertOnWideTableStaysUnderSQLiteVariableLimit(t *testing.T) {
	runtime := newWideRowRuntime(t)
	exec := requireInitializedRuntime(t, runtime)

	const rows = 1200

	items := make([]*wideRow, 0, rows)
	for i := range rows {
		row := &wideRow{}
		for j := range row.Values {
			row.Values[j] = int64(i*wideRowColumns + j)
		}

		items = append(items, row)
	}

	if err := ChunkedInsert(context.Background(), exec, items); err != nil {
		t.Fatalf("chunked insert of a wide table: %v", err)
	}

	var count int
	if err := runtime.DB().QueryRowContext(context.Background(), `SELECT COUNT(*) FROM wide_rows`).Scan(&count); err != nil {
		t.Fatalf("count wide rows: %v", err)
	}

	if count != rows {
		t.Fatalf("expected %d inserted rows, got %d", rows, count)
	}
}

// TestChunkedUpdateOnWideTableStaysUnderSQLiteVariableLimit covers the other half: a
// batch UPDATE binds about two placeholders per column per row, so sizing its chunk
// with the INSERT estimate overshoots the ceiling even when the ceiling is right.
func TestChunkedUpdateOnWideTableStaysUnderSQLiteVariableLimit(t *testing.T) {
	runtime := newWideRowRuntime(t)
	exec := requireInitializedRuntime(t, runtime)

	const rows = 1200

	items := make([]*wideRow, 0, rows)
	for range rows {
		items = append(items, &wideRow{})
	}

	if err := ChunkedInsert(context.Background(), exec, items); err != nil {
		t.Fatalf("seed wide rows: %v", err)
	}

	for i, row := range items {
		row.Values[0] = int64(i + 1)
	}

	if err := ChunkedUpdate(context.Background(), exec, items); err != nil {
		t.Fatalf("chunked update of a wide table: %v", err)
	}

	var updated int
	if err := runtime.DB().QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM wide_rows WHERE "c0" > 0`).Scan(&updated); err != nil {
		t.Fatalf("count updated rows: %v", err)
	}

	if updated != rows {
		t.Fatalf("expected %d updated rows, got %d", rows, updated)
	}
}

// TestChunkSizeForExecutorUsesTheExecutorDialect pins that the ceiling follows the
// executor rather than a single compiled-in number: the same rows chunk differently on
// SQLite than on PostgreSQL.
func TestChunkSizeForExecutorUsesTheExecutorDialect(t *testing.T) {
	items := []*wideRow{{}}

	sqliteRuntime := newRuntimeWithDB(nil, tsqdialect.SQLiteDialect{})
	postgresRuntime := newRuntimeWithDB(nil, tsqdialect.PostgresDialect{})

	sqliteSize := chunkSizeForExecutor(sqliteRuntime, 1000, insertBindParamsPerRow(items))
	postgresSize := chunkSizeForExecutor(postgresRuntime, 1000, insertBindParamsPerRow(items))

	if sqliteSize*wideRowColumns > tsqdialect.MaxBindParams(tsqdialect.SQLiteDialect{}) {
		t.Fatalf("sqlite chunk of %d rows binds %d parameters, over its limit",
			sqliteSize, sqliteSize*wideRowColumns)
	}

	if sqliteSize >= postgresSize {
		t.Fatalf("expected sqlite (%d) to chunk smaller than postgres (%d)", sqliteSize, postgresSize)
	}

	// An executor with no dialect must not be assumed to be the permissive one.
	if got := chunkSizeForExecutor(nil, 1000, insertBindParamsPerRow(items)); got != sqliteSize {
		t.Fatalf("dialect-less executor chunked %d rows, want the tightest limit's %d", got, sqliteSize)
	}
}
