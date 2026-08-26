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
// placeholders rather than rows. A 1000-row chunk of a wide table binds far more than
// 65535 parameters, which PostgreSQL rejects outright, so the row count alone is not a
// safe unit; the caller's chunk size is an upper bound, never a floor.
func TestEffectiveChunkSizeStaysWithinBindParamLimit(t *testing.T) {
	tests := []struct {
		name         string
		chunkSize    int
		paramsPerRow int
		want         int
	}{
		{name: "narrow table keeps the requested size", chunkSize: 1000, paramsPerRow: 5, want: 1000},
		{name: "wide table shrinks", chunkSize: 1000, paramsPerRow: 70, want: maxBindParamsPerStatement / 70},
		{name: "very wide table still sends one row", chunkSize: 1000, paramsPerRow: maxBindParamsPerStatement + 1, want: 1},
		{name: "unknown column count leaves the size alone", chunkSize: 1000, paramsPerRow: 0, want: 1000},
		{name: "never raises the requested size", chunkSize: 10, paramsPerRow: 1, want: 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := effectiveChunkSize(tt.chunkSize, tt.paramsPerRow)
			if got != tt.want {
				t.Fatalf("effectiveChunkSize(%d, %d) = %d, want %d", tt.chunkSize, tt.paramsPerRow, got, tt.want)
			}

			if tt.paramsPerRow > 0 && got*tt.paramsPerRow > maxBindParamsPerStatement && got > 1 {
				t.Fatalf("chunk of %d rows binds %d parameters, over the %d limit", got, got*tt.paramsPerRow, maxBindParamsPerStatement)
			}
		})
	}
}

// TestParamsPerRowSkipsNilItems keeps the sampling safe: the per-item nil check belongs
// to the chunk functions, which run after the chunk size has already been decided.
func TestParamsPerRowSkipsNilItems(t *testing.T) {
	items := []*batchMutationUser{nil, nil, {Name: "alice"}}

	got := paramsPerRow(items)
	if got != len(batchMutationUser{}.Cols()) {
		t.Fatalf("paramsPerRow() = %d, want %d", got, len(batchMutationUser{}.Cols()))
	}

	if paramsPerRow([]*batchMutationUser{nil, nil}) != 0 {
		t.Fatal("an all-nil slice has no sampleable column count")
	}
}
