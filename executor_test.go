package tsq

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

type batchMutationUser struct {
	ID    int64
	Name  string
	Email string
}

func (batchMutationUser) TSQOwner()                     {}
func (batchMutationUser) Table() string                 { return "users" }
func (batchMutationUser) Cols() []SQLColumn             { return SQLColumns(batchMutationUserColumns()...) }
func (batchMutationUser) SearchColumns() []SearchColumn { return nil }
func (batchMutationUser) PrimaryKeys() []string         { return []string{"id"} }
func (batchMutationUser) AutoIncrement() bool           { return true }
func (batchMutationUser) VersionColumn() string         { return "" }

func batchMutationUserColumns() []BoundColumn[batchMutationUser] {
	return []BoundColumn[batchMutationUser]{
		NewCol[batchMutationUser, int64]("id", "id", func(t *batchMutationUser) *int64 { return &t.ID }),
		NewCol[batchMutationUser, string]("name", "name", func(t *batchMutationUser) *string { return &t.Name }),
		NewCol[batchMutationUser, string]("email", "email", func(t *batchMutationUser) *string { return &t.Email }),
	}
}

func newBatchMutationEngine(t *testing.T) *Engine {
	t.Helper()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}

	t.Cleanup(func() {
		_ = db.Close()
	})

	if _, err := db.ExecContext(context.Background(), `CREATE TABLE users (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL,
		email TEXT NOT NULL UNIQUE
	)`); err != nil {
		t.Fatalf("create users table: %v", err)
	}

	return &Engine{DB: db, Dialect: SQLiteDialect{}}
}

func TestEngineInsertBatchesRows(t *testing.T) {
	db := newBatchMutationEngine(t)

	u1 := &batchMutationUser{Name: "alice", Email: "alice@example.com"}
	u2 := &batchMutationUser{Name: "bob", Email: "bob@example.com"}

	if err := db.Insert(context.Background(), u1, u2); err != nil {
		t.Fatalf("batch insert failed: %v", err)
	}

	if u1.ID != 1 || u2.ID != 2 {
		t.Fatalf("expected contiguous IDs to be assigned, got %d and %d", u1.ID, u2.ID)
	}

	var count int
	if err := db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM users`).Scan(&count); err != nil {
		t.Fatalf("count inserted rows: %v", err)
	}

	if count != 2 {
		t.Fatalf("expected 2 inserted rows, got %d", count)
	}
}

func TestEngineUpdateBatchesRows(t *testing.T) {
	db := newBatchMutationEngine(t)

	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO users (id, name, email) VALUES
		(1, 'alice', 'alice@example.com'),
		(2, 'bob', 'bob@example.com')
	`); err != nil {
		t.Fatalf("seed rows: %v", err)
	}

	u1 := &batchMutationUser{ID: 1, Name: "alice-updated", Email: "alice+updated@example.com"}
	u2 := &batchMutationUser{ID: 2, Name: "bob-updated", Email: "bob+updated@example.com"}

	affected, err := db.Update(context.Background(), u1, u2)
	if err != nil {
		t.Fatalf("batch update failed: %v", err)
	}

	if affected != 2 {
		t.Fatalf("expected 2 updated rows, got %d", affected)
	}

	rows, err := db.QueryContext(context.Background(), `SELECT id, name, email FROM users ORDER BY id`)
	if err != nil {
		t.Fatalf("query updated rows: %v", err)
	}
	defer rows.Close()

	var got []batchMutationUser
	for rows.Next() {
		var user batchMutationUser
		if err := rows.Scan(&user.ID, &user.Name, &user.Email); err != nil {
			t.Fatalf("scan updated row: %v", err)
		}

		got = append(got, user)
	}

	if len(got) != 2 || got[0].Name != "alice-updated" || got[1].Name != "bob-updated" {
		t.Fatalf("unexpected updated rows: %#v", got)
	}
}

func TestEngineDeleteBatchesRows(t *testing.T) {
	db := newBatchMutationEngine(t)

	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO users (id, name, email) VALUES
		(1, 'alice', 'alice@example.com'),
		(2, 'bob', 'bob@example.com'),
		(3, 'carol', 'carol@example.com')
	`); err != nil {
		t.Fatalf("seed rows: %v", err)
	}

	affected, err := db.Delete(
		context.Background(),
		&batchMutationUser{ID: 1},
		&batchMutationUser{ID: 3},
	)
	if err != nil {
		t.Fatalf("batch delete failed: %v", err)
	}

	if affected != 2 {
		t.Fatalf("expected 2 deleted rows, got %d", affected)
	}

	var count int
	if err := db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM users`).Scan(&count); err != nil {
		t.Fatalf("count remaining rows: %v", err)
	}

	if count != 1 {
		t.Fatalf("expected 1 remaining row, got %d", count)
	}
}

type countingMutationExecutor struct {
	insertBatchSizes []int
	updateBatchSizes []int
	deleteBatchSizes []int
}

func (c *countingMutationExecutor) Insert(_ context.Context, dst ...Table) error {
	c.insertBatchSizes = append(c.insertBatchSizes, len(dst))
	return nil
}

func (c *countingMutationExecutor) Update(_ context.Context, dst ...Table) (int64, error) {
	c.updateBatchSizes = append(c.updateBatchSizes, len(dst))
	return int64(len(dst)), nil
}

func (c *countingMutationExecutor) Delete(_ context.Context, dst ...Table) (int64, error) {
	c.deleteBatchSizes = append(c.deleteBatchSizes, len(dst))
	return int64(len(dst)), nil
}

func TestChunkedInsertChunkUsesBatchInsert(t *testing.T) {
	exec := &countingMutationExecutor{}
	items := []*batchMutationUser{
		{Name: "alice", Email: "alice@example.com"},
		{Name: "bob", Email: "bob@example.com"},
	}

	if err := chunkedInsertChunk(context.Background(), exec, items, &ChunkedInsertOptions{}); err != nil {
		t.Fatalf("chunked insert chunk failed: %v", err)
	}

	if len(exec.insertBatchSizes) != 1 || exec.insertBatchSizes[0] != 2 {
		t.Fatalf("expected one batched insert call of size 2, got %#v", exec.insertBatchSizes)
	}
}

func TestChunkedUpdateChunkUsesBatchUpdate(t *testing.T) {
	exec := &countingMutationExecutor{}
	items := []*batchMutationUser{
		{ID: 1, Name: "alice", Email: "alice@example.com"},
		{ID: 2, Name: "bob", Email: "bob@example.com"},
	}

	if err := chunkedUpdateChunk(context.Background(), exec, items); err != nil {
		t.Fatalf("chunked update chunk failed: %v", err)
	}

	if len(exec.updateBatchSizes) != 1 || exec.updateBatchSizes[0] != 2 {
		t.Fatalf("expected one batched update call of size 2, got %#v", exec.updateBatchSizes)
	}
}

func TestChunkedDeleteChunkUsesBatchDelete(t *testing.T) {
	exec := &countingMutationExecutor{}
	items := []*batchMutationUser{
		{ID: 1},
		{ID: 2},
	}

	if err := chunkedDeleteChunk(context.Background(), exec, items); err != nil {
		t.Fatalf("chunked delete chunk failed: %v", err)
	}

	if len(exec.deleteBatchSizes) != 1 || exec.deleteBatchSizes[0] != 2 {
		t.Fatalf("expected one batched delete call of size 2, got %#v", exec.deleteBatchSizes)
	}
}

func TestEngineQueryUsesContext(t *testing.T) {
	db := newBatchMutationEngine(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := db.QueryContext(ctx, `SELECT id FROM users`)
	if err == nil {
		t.Fatal("expected canceled context to fail query")
	}
	if !strings.Contains(err.Error(), context.Canceled.Error()) {
		t.Fatalf("expected query to surface context cancellation, got %v", err)
	}
}

func TestEngineExecUsesContext(t *testing.T) {
	db := newBatchMutationEngine(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := db.ExecContext(ctx, `INSERT INTO users (name, email) VALUES ('alice', 'alice@example.com')`)
	if err == nil {
		t.Fatal("expected canceled context to fail exec")
	}
	if !strings.Contains(err.Error(), context.Canceled.Error()) {
		t.Fatalf("expected exec to surface context cancellation, got %v", err)
	}
}

func TestEngineQueryContextRejectsMissingDatabase(t *testing.T) {
	db := &Engine{}

	_, err := db.QueryContext(context.Background(), `SELECT 1`)
	if !errors.Is(err, errEngineDatabaseNil) {
		t.Fatalf("expected errEngineDatabaseNil, got %v", err)
	}
}

func TestEngineExecContextRejectsMissingDatabase(t *testing.T) {
	db := &Engine{}

	_, err := db.ExecContext(context.Background(), `SELECT 1`)
	if !errors.Is(err, errEngineDatabaseNil) {
		t.Fatalf("expected errEngineDatabaseNil, got %v", err)
	}
}

func TestEngineQueryRowContextRejectsMissingDatabase(t *testing.T) {
	db := &Engine{}

	var count int
	err := db.QueryRowContext(context.Background(), `SELECT 1`).Scan(&count)
	if !errors.Is(err, errEngineDatabaseNil) {
		t.Fatalf("expected errEngineDatabaseNil, got %v", err)
	}
}

func TestChunkedInsertIgnoreErrorsSkipsSQLiteUniqueViolations(t *testing.T) {
	db := newBatchMutationEngine(t)

	if err := db.Insert(context.Background(), &batchMutationUser{Name: "seed", Email: "alice@example.com"}); err != nil {
		t.Fatalf("seed insert failed: %v", err)
	}

	items := []*batchMutationUser{
		{Name: "duplicate", Email: "alice@example.com"},
		{Name: "fresh", Email: "bob@example.com"},
	}

	if err := ChunkedInsert(context.Background(), db, items, &ChunkedInsertOptions{
		ChunkSize:    2,
		IgnoreErrors: true,
	}); err != nil {
		t.Fatalf("chunked insert with ignore errors failed: %v", err)
	}

	var count int
	if err := db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM users`).Scan(&count); err != nil {
		t.Fatalf("count rows after ignored duplicate: %v", err)
	}

	if count != 2 {
		t.Fatalf("expected 2 rows after ignoring duplicate, got %d", count)
	}
}
