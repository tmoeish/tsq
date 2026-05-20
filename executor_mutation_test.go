package tsq

import (
	"context"
	"errors"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

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
	affected, err := db.Delete(context.Background(), &batchMutationUser{ID: 1}, &batchMutationUser{ID: 3})
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

func TestEngineUpdateUsesOptimisticLockVersion(t *testing.T) {
	db := newOptimisticMutationEngine(t)
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO users (id, name, email, version) VALUES
		(1, 'alice', 'alice@example.com', 3),
		(2, 'bob', 'bob@example.com', 7)
	`); err != nil {
		t.Fatalf("seed rows: %v", err)
	}
	u1 := &optimisticMutationUser{ID: 1, Name: "alice-updated", Email: "alice+updated@example.com", Version: 3}
	u2 := &optimisticMutationUser{ID: 2, Name: "bob-updated", Email: "bob+updated@example.com", Version: 7}
	affected, err := db.Update(context.Background(), u1, u2)
	if err != nil {
		t.Fatalf("optimistic batch update failed: %v", err)
	}
	if affected != 2 {
		t.Fatalf("expected 2 updated rows, got %d", affected)
	}
	if u1.Version != 4 || u2.Version != 8 {
		t.Fatalf("expected in-memory versions to increment, got %d and %d", u1.Version, u2.Version)
	}
	rows, err := db.QueryContext(context.Background(), `SELECT id, version FROM users ORDER BY id`)
	if err != nil {
		t.Fatalf("query versions: %v", err)
	}
	defer rows.Close()
	var got []optimisticMutationUser
	for rows.Next() {
		var user optimisticMutationUser
		if err := rows.Scan(&user.ID, &user.Version); err != nil {
			t.Fatalf("scan version row: %v", err)
		}
		got = append(got, user)
	}
	if len(got) != 2 || got[0].Version != 4 || got[1].Version != 8 {
		t.Fatalf("unexpected stored versions: %#v", got)
	}
}

func TestEngineUpdateOptimisticLockConflict(t *testing.T) {
	db := newOptimisticMutationEngine(t)
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO users (id, name, email, version) VALUES
		(1, 'alice', 'alice@example.com', 3)
	`); err != nil {
		t.Fatalf("seed row: %v", err)
	}
	user := &optimisticMutationUser{ID: 1, Name: "alice-stale", Email: "alice+stale@example.com", Version: 2}
	affected, err := db.Update(context.Background(), user)
	if err == nil {
		t.Fatal("expected optimistic lock conflict")
	}
	if affected != 0 {
		t.Fatalf("expected 0 updated rows, got %d", affected)
	}
	if !errors.Is(err, &ErrOptimisticLockConflict{}) {
		t.Fatalf("expected optimistic lock conflict error, got %v", err)
	}
	if user.Version != 2 {
		t.Fatalf("expected in-memory version to stay unchanged, got %d", user.Version)
	}
}

func TestEngineDeleteUsesOptimisticLockVersion(t *testing.T) {
	db := newOptimisticMutationEngine(t)
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO users (id, name, email, version) VALUES
		(1, 'alice', 'alice@example.com', 3),
		(2, 'bob', 'bob@example.com', 5)
	`); err != nil {
		t.Fatalf("seed rows: %v", err)
	}
	affected, err := db.Delete(context.Background(), &optimisticMutationUser{ID: 1, Version: 3}, &optimisticMutationUser{ID: 2, Version: 5})
	if err != nil {
		t.Fatalf("optimistic delete failed: %v", err)
	}
	if affected != 2 {
		t.Fatalf("expected 2 deleted rows, got %d", affected)
	}
}

func TestEngineDeleteOptimisticLockConflict(t *testing.T) {
	db := newOptimisticMutationEngine(t)
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO users (id, name, email, version) VALUES
		(1, 'alice', 'alice@example.com', 3)
	`); err != nil {
		t.Fatalf("seed row: %v", err)
	}
	affected, err := db.Delete(context.Background(), &optimisticMutationUser{ID: 1, Version: 2})
	if err == nil {
		t.Fatal("expected optimistic lock conflict")
	}
	if affected != 0 {
		t.Fatalf("expected 0 deleted rows, got %d", affected)
	}
	if !errors.Is(err, &ErrOptimisticLockConflict{}) {
		t.Fatalf("expected optimistic lock conflict error, got %v", err)
	}
}

func TestChunkedInsertChunkUsesBatchInsert(t *testing.T) {
	exec := &countingMutationExecutor{}
	items := []*batchMutationUser{{Name: "alice", Email: "alice@example.com"}, {Name: "bob", Email: "bob@example.com"}}
	if err := chunkedInsertChunk(context.Background(), exec, items, &ChunkedInsertOptions{}); err != nil {
		t.Fatalf("chunked insert chunk failed: %v", err)
	}
	if len(exec.insertBatchSizes) != 1 || exec.insertBatchSizes[0] != 2 {
		t.Fatalf("expected one batched insert call of size 2, got %#v", exec.insertBatchSizes)
	}
}

func TestChunkedUpdateChunkUsesBatchUpdate(t *testing.T) {
	exec := &countingMutationExecutor{}
	items := []*batchMutationUser{{ID: 1, Name: "alice", Email: "alice@example.com"}, {ID: 2, Name: "bob", Email: "bob@example.com"}}
	if err := chunkedUpdateChunk(context.Background(), exec, items); err != nil {
		t.Fatalf("chunked update chunk failed: %v", err)
	}
	if len(exec.updateBatchSizes) != 1 || exec.updateBatchSizes[0] != 2 {
		t.Fatalf("expected one batched update call of size 2, got %#v", exec.updateBatchSizes)
	}
}

func TestChunkedDeleteChunkUsesBatchDelete(t *testing.T) {
	exec := &countingMutationExecutor{}
	items := []*batchMutationUser{{ID: 1}, {ID: 2}}
	if err := chunkedDeleteChunk(context.Background(), exec, items); err != nil {
		t.Fatalf("chunked delete chunk failed: %v", err)
	}
	if len(exec.deleteBatchSizes) != 1 || exec.deleteBatchSizes[0] != 2 {
		t.Fatalf("expected one batched delete call of size 2, got %#v", exec.deleteBatchSizes)
	}
}

func TestChunkedInsertIgnoreErrorsSkipsSQLiteUniqueViolations(t *testing.T) {
	db := newBatchMutationEngine(t)
	if err := db.Insert(context.Background(), &batchMutationUser{Name: "seed", Email: "alice@example.com"}); err != nil {
		t.Fatalf("seed insert failed: %v", err)
	}
	items := []*batchMutationUser{{Name: "duplicate", Email: "alice@example.com"}, {Name: "fresh", Email: "bob@example.com"}}
	if err := ChunkedInsert(context.Background(), db, items, &ChunkedInsertOptions{ChunkSize: 2, IgnoreErrors: true}); err != nil {
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
