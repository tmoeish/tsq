package tsq

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	_ "modernc.org/sqlite"

	tsqdialect "github.com/tmoeish/tsq/v4/dialect"
)

type pointerPKUser struct {
	ID *int64
}

func (pointerPKUser) TSQOwner() {}

func (pointerPKUser) Table() string { return "pointer_users" }

func (pointerPKUser) Cols() []SQLColumn {
	return SQLColumns(NewCol[pointerPKUser, *int64]("id", "id", func(t *pointerPKUser) **int64 {
		return &t.ID
	}))
}

func (pointerPKUser) SearchColumns() []SearchColumn { return nil }

func (pointerPKUser) PrimaryKeys() []string { return []string{"id"} }

func (pointerPKUser) AutoIncrement() bool { return false }

func (pointerPKUser) VersionColumn() string { return "" }

func TestDefaultChunkedInsertOptions(t *testing.T) {
	opts := DefaultChunkedInsertOptions()
	if opts == nil {
		t.Fatal("Expected non-nil options")
	}
	if opts.ChunkSize != 1000 {
		t.Errorf("Expected ChunkSize 1000, got %d", opts.ChunkSize)
	}
	if opts.IgnoreErrors != false {
		t.Errorf("Expected IgnoreErrors false, got %v", opts.IgnoreErrors)
	}
}

func TestDefaultChunkedOptions(t *testing.T) {
	opts := DefaultChunkedOptions()
	if opts == nil {
		t.Fatal("expected non-nil options")
	}
	if opts.ChunkSize != 1000 {
		t.Fatalf("expected chunk size 1000, got %d", opts.ChunkSize)
	}
}

func TestChunkedInsertOptions_Modification(t *testing.T) {
	opts := DefaultChunkedInsertOptions()
	opts.ChunkSize = 500
	opts.IgnoreErrors = true
	if opts.ChunkSize != 500 {
		t.Errorf("Expected ChunkSize 500, got %d", opts.ChunkSize)
	}
	if opts.IgnoreErrors != true {
		t.Errorf("Expected IgnoreErrors true, got %v", opts.IgnoreErrors)
	}
}

func TestBuildDeleteByIDsSQL(t *testing.T) {
	sqlStr, err := buildDeleteByPKsSQL("users", "id", 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := `DELETE FROM "users" WHERE "id" IN (?,?)`
	if got := renderCanonicalSQL(sqlStr); got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}

func TestBuildDeleteByIDsSQLRejectsInvalidIdentifiers(t *testing.T) {
	_, err := buildDeleteByPKsSQL("users; DROP TABLE users", "id", 1)
	if err == nil {
		t.Fatal("expected invalid table name to return an error")
	}
	_, err = buildDeleteByPKsSQL("users", "id)` OR 1=1 --", 1)
	if err == nil {
		t.Fatal("expected invalid column name to return an error")
	}
}

func TestNormalizeChunkedInsertOptionsValidatesInputs(t *testing.T) {
	if _, err := normalizeChunkedInsertOptions(&ChunkedInsertOptions{ChunkSize: 0}); err == nil {
		t.Fatal("expected zero chunk size to return an error")
	}
}

func TestNormalizeChunkedInsertOptionsRejectsMultipleValues(t *testing.T) {
	_, err := normalizeChunkedInsertOptions(&ChunkedInsertOptions{}, &ChunkedInsertOptions{})
	if err == nil {
		t.Fatal("expected multiple option values to return an error")
	}
	if !strings.Contains(err.Error(), "at most one") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNormalizeChunkedOptionsValidatesInputs(t *testing.T) {
	if _, err := normalizeChunkedOptions(&ChunkedOptions{ChunkSize: 0}); err == nil {
		t.Fatal("expected zero chunk size to return an error")
	}
}

func TestNormalizeChunkedOptionsRejectsMultipleValues(t *testing.T) {
	_, err := normalizeChunkedOptions(&ChunkedOptions{}, &ChunkedOptions{})
	if err == nil {
		t.Fatal("expected multiple option values to return an error")
	}
	if !strings.Contains(err.Error(), "at most one") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestChunkedUpdateChunkRejectsNilItems(t *testing.T) {
	err := chunkedUpdateChunk[*mockTable](context.Background(), nil, []*mockTable{nil})
	if err == nil {
		t.Fatal("expected nil batch update item to return an error")
	}
	if !strings.Contains(err.Error(), "item at index 0 is nil") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestChunkedDeleteChunkRejectsNilItems(t *testing.T) {
	err := chunkedDeleteChunk[*mockTable](context.Background(), nil, []*mockTable{nil})
	if err == nil {
		t.Fatal("expected nil batch delete item to return an error")
	}
	if !strings.Contains(err.Error(), "item at index 0 is nil") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestInsertRejectsTypedNilExecutor(t *testing.T) {
	var db *sql.DB
	row := mockTable{tableName: "users"}
	err := Insert(context.Background(), db, &row)
	if err == nil {
		t.Fatal("expected typed-nil executor to return an error")
	}
	if !strings.Contains(err.Error(), "sql executor cannot be nil") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestInsertRejectsNilItem(t *testing.T) {
	db := &sql.DB{}
	var value *mockTable
	err := Insert(context.Background(), db, value)
	if err == nil {
		t.Fatal("expected nil item to return an error")
	}
	if !strings.Contains(err.Error(), "mutation item cannot be nil") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestUpdateRejectsNilItem(t *testing.T) {
	db := &sql.DB{}
	var value *mockTable
	err := Update(context.Background(), db, value)
	if err == nil {
		t.Fatal("expected nil item to return an error")
	}
	if !strings.Contains(err.Error(), "mutation item cannot be nil") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDeleteRejectsNilItem(t *testing.T) {
	db := &sql.DB{}
	var value *mockTable
	err := Delete(context.Background(), db, value)
	if err == nil {
		t.Fatal("expected nil item to return an error")
	}
	if !strings.Contains(err.Error(), "mutation item cannot be nil") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestChunkedInsertRejectsTypedNilExecutor(t *testing.T) {
	var db *sql.DB
	row := mockTable{tableName: "users"}
	err := ChunkedInsert(context.Background(), db, []*mockTable{&row})
	if err == nil {
		t.Fatal("expected typed-nil executor to return an error")
	}
	if !strings.Contains(err.Error(), "sql executor cannot be nil") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestChunkedDeleteByIDsRejectsExecutorWithoutDialectForRenderedSQL(t *testing.T) {
	db := newEngineWithoutDialect(t)
	pkField := batchMutationUserColumns()[0].(TypedColumn[batchMutationUser, int64])
	err := ChunkedDeleteByPKs(context.Background(), db, pkField, []int64{1})
	if err == nil {
		t.Fatal("expected executor without dialect to return an error")
	}
	if !strings.Contains(err.Error(), "dialect cannot be determined") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestChunkedDeleteByIDsRejectsNilIDs(t *testing.T) {
	db := WrapExecutor(&sql.DB{}, tsqdialect.SQLiteDialect{})
	err := ChunkedDeleteByPKs(
		context.Background(),
		db,
		NewCol[pointerPKUser, *int64]("id", "id", func(t *pointerPKUser) **int64 { return &t.ID }),
		[]*int64{new(int64(1)), nil},
	)
	if err == nil {
		t.Fatal("expected nil ids to return an error")
	}
	if !strings.Contains(err.Error(), "cannot be nil") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestChunkedDeleteByPKsRejectsNonPKField(t *testing.T) {
	db := WrapExecutor(&sql.DB{}, tsqdialect.SQLiteDialect{})
	nameField := batchMutationUserColumns()[1].(TypedColumn[batchMutationUser, string])
	err := ChunkedDeleteByPKs(context.Background(), db, nameField, []string{"alice"})
	if err == nil {
		t.Fatal("expected non-primary-key field to return an error")
	}
	if !strings.Contains(err.Error(), "is not the primary key") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestEffectiveChunkSizeStaysWithinBindParamLimit covers the reason chunking counts
// placeholders rather than rows. A 1000-row chunk of a wide table binds far more
// parameters than any of the supported databases accepts, so the row count alone is
// not a safe unit; the caller's chunk size is an upper bound, never a floor.
func TestEffectiveChunkSizeStaysWithinBindParamLimit(t *testing.T) {
	const limit = 65535

	tests := []struct {
		name             string
		chunkSize        int
		bindParamsPerRow int
		maxBindParams    int
		want             int
	}{
		{name: "narrow table keeps the requested size", chunkSize: 1000, bindParamsPerRow: 5, maxBindParams: limit, want: 1000},
		{name: "wide table shrinks", chunkSize: 1000, bindParamsPerRow: 70, maxBindParams: limit, want: limit / 70},
		{name: "very wide table still sends one row", chunkSize: 1000, bindParamsPerRow: limit + 1, maxBindParams: limit, want: 1},
		{name: "unknown column count leaves the size alone", chunkSize: 1000, bindParamsPerRow: 0, maxBindParams: limit, want: 1000},
		{name: "never raises the requested size", chunkSize: 10, bindParamsPerRow: 1, maxBindParams: limit, want: 10},
		{name: "a tighter dialect limit shrinks further", chunkSize: 1000, bindParamsPerRow: 5, maxBindParams: 3000, want: 600},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := effectiveChunkSize(tt.chunkSize, tt.bindParamsPerRow, tt.maxBindParams)
			if got != tt.want {
				t.Fatalf("effectiveChunkSize(%d, %d, %d) = %d, want %d",
					tt.chunkSize, tt.bindParamsPerRow, tt.maxBindParams, got, tt.want)
			}

			if tt.bindParamsPerRow > 0 && got*tt.bindParamsPerRow > tt.maxBindParams && got > 1 {
				t.Fatalf("chunk of %d rows binds %d parameters, over the %d limit",
					got, got*tt.bindParamsPerRow, tt.maxBindParams)
			}
		})
	}
}

// TestUpdateBindsTwoPlaceholdersPerColumnPerRow pins the estimate an UPDATE chunk is
// sized with. The batch UPDATE renders `col = CASE pk WHEN ? THEN ? ... END`, so it
// binds about twice what an INSERT of the same rows binds; sizing it with the INSERT
// estimate undercounts by roughly half.
func TestUpdateBindsTwoPlaceholdersPerColumnPerRow(t *testing.T) {
	items := []*batchMutationUser{{Name: "alice"}}
	columns := len(batchMutationUser{}.Cols())

	if got := insertBindParamsPerRow(items); got != columns {
		t.Fatalf("insertBindParamsPerRow() = %d, want %d", got, columns)
	}

	if got := updateBindParamsPerRow(items); got != 2*columns {
		t.Fatalf("updateBindParamsPerRow() = %d, want %d", got, 2*columns)
	}
}

// TestColumnsPerRowSkipsNilItems keeps the sampling safe: the per-item nil check belongs
// to the chunk functions, which run after the chunk size has already been decided.
func TestColumnsPerRowSkipsNilItems(t *testing.T) {
	items := []*batchMutationUser{nil, nil, {Name: "alice"}}

	got := columnsPerRow(items)
	if got != len(batchMutationUser{}.Cols()) {
		t.Fatalf("columnsPerRow() = %d, want %d", got, len(batchMutationUser{}.Cols()))
	}

	if columnsPerRow([]*batchMutationUser{nil, nil}) != 0 {
		t.Fatal("an all-nil slice has no sampleable column count")
	}
}

// TestIsTransactionalExecutorSeesThroughWrappers pins the detection that decides
// whether the ignore-duplicates path brackets each row in a savepoint. WithTx hands the
// callback a dialect wrapper rather than the *sql.Tx itself, so a check that only looks
// at the outermost value answers "not a transaction" for the exact case that needs
// savepoints.
func TestIsTransactionalExecutorSeesThroughWrappers(t *testing.T) {
	runtime := newBatchMutationEngine(t)

	if isTransactionalExecutor(runtime) {
		t.Fatal("a runtime is not a transaction")
	}

	if isTransactionalExecutor(nil) {
		t.Fatal("a nil executor is not a transaction")
	}

	tx, err := runtime.DB().BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}

	defer func() { _ = tx.Rollback() }()

	if !isTransactionalExecutor(tx) {
		t.Fatal("a *sql.Tx is a transaction")
	}

	if !isTransactionalExecutor(wrapExecutor(tx, runtime.SQLDialect(), runtime)) {
		t.Fatal("a wrapped *sql.Tx is still a transaction")
	}
}

// TestChunkedInsertIgnoreErrorsInsideTransaction covers the in-transaction path end to
// end: the duplicate is skipped, the rows around it land, and the transaction is still
// usable afterwards. On PostgreSQL the last part is the whole point, and the integration
// suite runs this same shape against a real server.
func TestChunkedInsertIgnoreErrorsInsideTransaction(t *testing.T) {
	runtime := newBatchMutationEngine(t)
	exec := requireInitializedRuntime(t, runtime)

	items := []*batchMutationUser{
		{Name: "alice", Email: "alice@example.com"},
		{Name: "alice again", Email: "alice@example.com"},
		{Name: "bob", Email: "bob@example.com"},
	}

	err := exec.WithTx(context.Background(), nil, func(ctx context.Context, txExec SQLExecutor) error {
		if err := ChunkedInsert(ctx, txExec, items, &ChunkedInsertOptions{ChunkSize: 10, IgnoreErrors: true}); err != nil {
			return err
		}

		// The transaction has to still be usable after an ignored duplicate.
		var count int

		return txExec.QueryRowContext(ctx, `SELECT COUNT(*) FROM users`).Scan(&count)
	})
	if err != nil {
		t.Fatalf("chunked insert with IgnoreErrors inside a transaction: %v", err)
	}

	var count int
	if err := runtime.DB().QueryRowContext(context.Background(), `SELECT COUNT(*) FROM users`).Scan(&count); err != nil {
		t.Fatalf("count committed rows: %v", err)
	}

	if count != 2 {
		t.Fatalf("expected the duplicate to be skipped and 2 rows committed, got %d", count)
	}
}

// TestChunkedInsertIgnoreErrorsStillPropagatesOtherFailures keeps IgnoreErrors narrow:
// it skips duplicate keys, not everything.
func TestChunkedInsertIgnoreErrorsStillPropagatesOtherFailures(t *testing.T) {
	runtime := newBatchMutationEngine(t)
	exec := requireInitializedRuntime(t, runtime)

	if _, err := runtime.DB().ExecContext(context.Background(), `DROP TABLE users`); err != nil {
		t.Fatalf("drop users: %v", err)
	}

	items := []*batchMutationUser{{Name: "alice", Email: "alice@example.com"}}

	err := ChunkedInsert(context.Background(), exec, items, &ChunkedInsertOptions{ChunkSize: 10, IgnoreErrors: true})
	if err == nil {
		t.Fatal("expected a missing table to fail even with IgnoreErrors")
	}
}
