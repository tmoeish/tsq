package tsq

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

type batchMutationUser struct {
	ID    int64
	Name  string
	Email string
}

func (batchMutationUser) TSQOwner()              {}
func (batchMutationUser) Table() string          { return "users" }
func (batchMutationUser) Cols() []SQLColumn      { return SQLColumns(batchMutationUserColumns()...) }
func (batchMutationUser) KwList() []SearchColumn { return nil }
func (batchMutationUser) PrimaryKeys() []string  { return []string{"id"} }
func (batchMutationUser) AutoIncrement() bool    { return true }
func (batchMutationUser) VersionColumn() string  { return "" }

func batchMutationUserColumns() []BoundColumn[batchMutationUser] {
	return []BoundColumn[batchMutationUser]{
		NewCol[batchMutationUser, int64]("id", "id", func(t *batchMutationUser) *int64 { return &t.ID }),
		NewCol[batchMutationUser, string]("name", "name", func(t *batchMutationUser) *string { return &t.Name }),
		NewCol[batchMutationUser, string]("email", "email", func(t *batchMutationUser) *string { return &t.Email }),
	}
}

func newBatchMutationDBMap(t *testing.T) *DbMap {
	t.Helper()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}

	t.Cleanup(func() {
		_ = db.Close()
	})

	if _, err := db.Exec(`CREATE TABLE users (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL,
		email TEXT NOT NULL
	)`); err != nil {
		t.Fatalf("create users table: %v", err)
	}

	return &DbMap{Db: db, Dialect: SqliteDialect{}}
}

func TestDbMapInsertBatchesRows(t *testing.T) {
	db := newBatchMutationDBMap(t)

	u1 := &batchMutationUser{Name: "alice", Email: "alice@example.com"}
	u2 := &batchMutationUser{Name: "bob", Email: "bob@example.com"}

	if err := db.Insert(context.Background(), u1, u2); err != nil {
		t.Fatalf("batch insert failed: %v", err)
	}

	if u1.ID != 1 || u2.ID != 2 {
		t.Fatalf("expected contiguous IDs to be assigned, got %d and %d", u1.ID, u2.ID)
	}

	var count int
	if err := db.QueryRow(context.Background(), `SELECT COUNT(*) FROM users`).Scan(&count); err != nil {
		t.Fatalf("count inserted rows: %v", err)
	}

	if count != 2 {
		t.Fatalf("expected 2 inserted rows, got %d", count)
	}
}

func TestDbMapUpdateBatchesRows(t *testing.T) {
	db := newBatchMutationDBMap(t)

	if _, err := db.Exec(context.Background(), `
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

	rows, err := db.Query(context.Background(), `SELECT id, name, email FROM users ORDER BY id`)
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

func TestDbMapDeleteBatchesRows(t *testing.T) {
	db := newBatchMutationDBMap(t)

	if _, err := db.Exec(context.Background(), `
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
	if err := db.QueryRow(context.Background(), `SELECT COUNT(*) FROM users`).Scan(&count); err != nil {
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

func (c *countingMutationExecutor) Query(_ context.Context, _ string, _ ...any) (*sql.Rows, error) {
	return nil, nil
}

func (c *countingMutationExecutor) QueryRow(_ context.Context, _ string, _ ...any) *sql.Row {
	return &sql.Row{}
}

func (c *countingMutationExecutor) Exec(_ context.Context, _ string, _ ...any) (sql.Result, error) {
	return nil, nil
}

func (c *countingMutationExecutor) SelectInt(_ context.Context, _ string, _ ...any) (int64, error) {
	return 0, nil
}

func (c *countingMutationExecutor) SelectNullInt(_ context.Context, _ string, _ ...any) (sql.NullInt64, error) {
	return sql.NullInt64{}, nil
}

func (c *countingMutationExecutor) SelectFloat(_ context.Context, _ string, _ ...any) (float64, error) {
	return 0, nil
}

func (c *countingMutationExecutor) SelectNullFloat(_ context.Context, _ string, _ ...any) (sql.NullFloat64, error) {
	return sql.NullFloat64{}, nil
}

func (c *countingMutationExecutor) SelectStr(_ context.Context, _ string, _ ...any) (string, error) {
	return "", nil
}

func (c *countingMutationExecutor) SelectNullStr(_ context.Context, _ string, _ ...any) (sql.NullString, error) {
	return sql.NullString{}, nil
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

func TestDbMapQueryUsesContext(t *testing.T) {
	db := newBatchMutationDBMap(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := db.Query(ctx, `SELECT id FROM users`)
	if err == nil {
		t.Fatal("expected canceled context to fail query")
	}
	if !strings.Contains(err.Error(), context.Canceled.Error()) {
		t.Fatalf("expected query to surface context cancellation, got %v", err)
	}
}

func TestDbMapExecUsesContext(t *testing.T) {
	db := newBatchMutationDBMap(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := db.Exec(ctx, `INSERT INTO users (name, email) VALUES ('alice', 'alice@example.com')`)
	if err == nil {
		t.Fatal("expected canceled context to fail exec")
	}
	if !strings.Contains(err.Error(), context.Canceled.Error()) {
		t.Fatalf("expected exec to surface context cancellation, got %v", err)
	}
}
