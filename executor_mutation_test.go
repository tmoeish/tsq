package tsq

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"

	_ "modernc.org/sqlite"

	tsqdialect "github.com/tmoeish/tsq/v4/dialect"
)

func TestEngineInsertBatchesRows(t *testing.T) {
	db := newBatchMutationEngine(t)
	exec := requireInitializedRuntime(t, db)
	u1 := &batchMutationUser{Name: "alice", Email: "alice@example.com"}
	u2 := &batchMutationUser{Name: "bob", Email: "bob@example.com"}
	if err := insertTables(context.Background(), exec, u1, u2); err != nil {
		t.Fatalf("batch insert failed: %v", err)
	}
	if u1.ID != 1 || u2.ID != 2 {
		t.Fatalf("expected contiguous IDs to be assigned, got %d and %d", u1.ID, u2.ID)
	}
	var count int
	if err := db.DB().QueryRowContext(context.Background(), `SELECT COUNT(*) FROM users`).Scan(&count); err != nil {
		t.Fatalf("count inserted rows: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 inserted rows, got %d", count)
	}
}

func TestEngineUpdateBatchesRows(t *testing.T) {
	db := newBatchMutationEngine(t)
	exec := requireInitializedRuntime(t, db)
	if _, err := db.DB().ExecContext(context.Background(), `
		INSERT INTO users (id, name, email) VALUES
		(1, 'alice', 'alice@example.com'),
		(2, 'bob', 'bob@example.com')
	`); err != nil {
		t.Fatalf("seed rows: %v", err)
	}
	u1 := &batchMutationUser{ID: 1, Name: "alice-updated", Email: "alice+updated@example.com"}
	u2 := &batchMutationUser{ID: 2, Name: "bob-updated", Email: "bob+updated@example.com"}
	affected, err := updateTables(context.Background(), exec, u1, u2)
	if err != nil {
		t.Fatalf("batch update failed: %v", err)
	}
	if affected != 2 {
		t.Fatalf("expected 2 updated rows, got %d", affected)
	}
	rows, err := db.DB().QueryContext(context.Background(), `SELECT id, name, email FROM users ORDER BY id`)
	if err != nil {
		t.Fatalf("query updated rows: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var got []batchMutationUser
	for rows.Next() {
		var user batchMutationUser
		if err := rows.Scan(&user.ID, &user.Name, &user.Email); err != nil {
			t.Fatalf("scan updated row: %v", err)
		}
		got = append(got, user)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate updated rows: %v", err)
	}
	if len(got) != 2 || got[0].Name != "alice-updated" || got[1].Name != "bob-updated" {
		t.Fatalf("unexpected updated rows: %#v", got)
	}
}

func TestEngineDeleteBatchesRows(t *testing.T) {
	db := newBatchMutationEngine(t)
	exec := requireInitializedRuntime(t, db)
	if _, err := db.DB().ExecContext(context.Background(), `
		INSERT INTO users (id, name, email) VALUES
		(1, 'alice', 'alice@example.com'),
		(2, 'bob', 'bob@example.com'),
		(3, 'carol', 'carol@example.com')
	`); err != nil {
		t.Fatalf("seed rows: %v", err)
	}
	affected, err := deleteTables(context.Background(), exec, &batchMutationUser{ID: 1}, &batchMutationUser{ID: 3})
	if err != nil {
		t.Fatalf("batch delete failed: %v", err)
	}
	if affected != 2 {
		t.Fatalf("expected 2 deleted rows, got %d", affected)
	}
	var count int
	if err := db.DB().QueryRowContext(context.Background(), `SELECT COUNT(*) FROM users`).Scan(&count); err != nil {
		t.Fatalf("count remaining rows: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 remaining row, got %d", count)
	}
}

func TestEngineUpdateUsesOptimisticLockVersion(t *testing.T) {
	db := newOptimisticMutationEngine(t)
	exec := requireInitializedRuntime(t, db)
	if _, err := db.DB().ExecContext(context.Background(), `
		INSERT INTO users (id, name, email, version) VALUES
		(1, 'alice', 'alice@example.com', 3),
		(2, 'bob', 'bob@example.com', 7)
	`); err != nil {
		t.Fatalf("seed rows: %v", err)
	}
	u1 := &optimisticMutationUser{ID: 1, Name: "alice-updated", Email: "alice+updated@example.com", Version: 3}
	u2 := &optimisticMutationUser{ID: 2, Name: "bob-updated", Email: "bob+updated@example.com", Version: 7}
	affected, err := updateTables(context.Background(), exec, u1, u2)
	if err != nil {
		t.Fatalf("optimistic batch update failed: %v", err)
	}
	if affected != 2 {
		t.Fatalf("expected 2 updated rows, got %d", affected)
	}
	if u1.Version != 4 || u2.Version != 8 {
		t.Fatalf("expected in-memory versions to increment, got %d and %d", u1.Version, u2.Version)
	}
	rows, err := db.DB().QueryContext(context.Background(), `SELECT id, version FROM users ORDER BY id`)
	if err != nil {
		t.Fatalf("query versions: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var got []optimisticMutationUser
	for rows.Next() {
		var user optimisticMutationUser
		if err := rows.Scan(&user.ID, &user.Version); err != nil {
			t.Fatalf("scan version row: %v", err)
		}
		got = append(got, user)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate version rows: %v", err)
	}
	if len(got) != 2 || got[0].Version != 4 || got[1].Version != 8 {
		t.Fatalf("unexpected stored versions: %#v", got)
	}
}

func TestEngineUpdateOptimisticLockConflict(t *testing.T) {
	db := newOptimisticMutationEngine(t)
	exec := requireInitializedRuntime(t, db)
	if _, err := db.DB().ExecContext(context.Background(), `
		INSERT INTO users (id, name, email, version) VALUES
		(1, 'alice', 'alice@example.com', 3)
	`); err != nil {
		t.Fatalf("seed row: %v", err)
	}
	user := &optimisticMutationUser{ID: 1, Name: "alice-stale", Email: "alice+stale@example.com", Version: 2}
	affected, err := updateTables(context.Background(), exec, user)
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
	exec := requireInitializedRuntime(t, db)
	if _, err := db.DB().ExecContext(context.Background(), `
		INSERT INTO users (id, name, email, version) VALUES
		(1, 'alice', 'alice@example.com', 3),
		(2, 'bob', 'bob@example.com', 5)
	`); err != nil {
		t.Fatalf("seed rows: %v", err)
	}
	affected, err := deleteTables(context.Background(), exec, &optimisticMutationUser{ID: 1, Version: 3}, &optimisticMutationUser{ID: 2, Version: 5})
	if err != nil {
		t.Fatalf("optimistic delete failed: %v", err)
	}
	if affected != 2 {
		t.Fatalf("expected 2 deleted rows, got %d", affected)
	}
}

func TestEngineDeleteOptimisticLockConflict(t *testing.T) {
	db := newOptimisticMutationEngine(t)
	exec := requireInitializedRuntime(t, db)
	if _, err := db.DB().ExecContext(context.Background(), `
		INSERT INTO users (id, name, email, version) VALUES
		(1, 'alice', 'alice@example.com', 3)
	`); err != nil {
		t.Fatalf("seed row: %v", err)
	}
	affected, err := deleteTables(context.Background(), exec, &optimisticMutationUser{ID: 1, Version: 2})
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
	runtime := newBatchMutationEngine(t)
	exec := requireInitializedRuntime(t, runtime)
	items := []*batchMutationUser{{Name: "alice", Email: "alice@example.com"}, {Name: "bob", Email: "bob@example.com"}}
	if err := chunkedInsertChunk(context.Background(), exec, items, &ChunkedInsertOptions{}); err != nil {
		t.Fatalf("chunked insert chunk failed: %v", err)
	}
	var count int
	if err := runtime.DB().QueryRowContext(context.Background(), `SELECT COUNT(*) FROM users`).Scan(&count); err != nil {
		t.Fatalf("count inserted rows: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 inserted rows, got %d", count)
	}
}

func TestChunkedUpdateChunkUsesBatchUpdate(t *testing.T) {
	runtime := newBatchMutationEngine(t)
	exec := requireInitializedRuntime(t, runtime)
	if _, err := runtime.DB().ExecContext(context.Background(), `
		INSERT INTO users (id, name, email) VALUES
		(1, 'alice', 'alice@example.com'),
		(2, 'bob', 'bob@example.com')
	`); err != nil {
		t.Fatalf("seed rows: %v", err)
	}
	items := []*batchMutationUser{
		{ID: 1, Name: "alice-updated", Email: "alice+updated@example.com"},
		{ID: 2, Name: "bob-updated", Email: "bob+updated@example.com"},
	}
	if err := chunkedUpdateChunk(context.Background(), exec, items); err != nil {
		t.Fatalf("chunked update chunk failed: %v", err)
	}
	var count int
	if err := runtime.DB().QueryRowContext(context.Background(), `SELECT COUNT(*) FROM users WHERE name IN ('alice-updated', 'bob-updated')`).Scan(&count); err != nil {
		t.Fatalf("count updated rows: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 updated rows, got %d", count)
	}
}

func TestChunkedDeleteChunkUsesBatchDelete(t *testing.T) {
	runtime := newBatchMutationEngine(t)
	exec := requireInitializedRuntime(t, runtime)
	if _, err := runtime.DB().ExecContext(context.Background(), `
		INSERT INTO users (id, name, email) VALUES
		(1, 'alice', 'alice@example.com'),
		(2, 'bob', 'bob@example.com')
	`); err != nil {
		t.Fatalf("seed rows: %v", err)
	}
	items := []*batchMutationUser{{ID: 1}, {ID: 2}}
	if err := chunkedDeleteChunk(context.Background(), exec, items); err != nil {
		t.Fatalf("chunked delete chunk failed: %v", err)
	}
	var count int
	if err := runtime.DB().QueryRowContext(context.Background(), `SELECT COUNT(*) FROM users`).Scan(&count); err != nil {
		t.Fatalf("count remaining rows: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected all rows to be deleted, got %d remaining", count)
	}
}

func TestChunkedInsertIgnoreErrorsSkipsSQLiteUniqueViolations(t *testing.T) {
	db := newBatchMutationEngine(t)
	exec := requireInitializedRuntime(t, db)
	if err := Insert(context.Background(), exec, &batchMutationUser{Name: "seed", Email: "alice@example.com"}); err != nil {
		t.Fatalf("seed insert failed: %v", err)
	}
	items := []*batchMutationUser{{Name: "duplicate", Email: "alice@example.com"}, {Name: "fresh", Email: "bob@example.com"}}
	if err := ChunkedInsert(context.Background(), exec, items, &ChunkedInsertOptions{ChunkSize: 2, IgnoreErrors: true}); err != nil {
		t.Fatalf("chunked insert with ignore errors failed: %v", err)
	}
	var count int
	if err := db.DB().QueryRowContext(context.Background(), `SELECT COUNT(*) FROM users`).Scan(&count); err != nil {
		t.Fatalf("count rows after ignored duplicate: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 rows after ignoring duplicate, got %d", count)
	}
}

// returningDialect mimics PostgreSQL's key-return contract on top of SQLite
// (which also understands RETURNING): no LastInsertId, keys come back as rows.
type returningDialect struct {
	tsqdialect.SQLiteDialect
}

func (returningDialect) LastInsertIdReturningSuffix(_, col string) string {
	return " RETURNING " + tsqdialect.SQLiteDialect{}.QuoteField(col)
}

func (returningDialect) BatchInsertStartID(int64, int64) (int64, bool) {
	return 0, false
}

func TestEngineInsertAssignsIDsThroughReturningClause(t *testing.T) {
	db := newBatchMutationEngine(t)
	db.dialect = returningDialect{}
	exec := requireInitializedRuntime(t, db)

	users := []*batchMutationUser{
		{Name: "carol", Email: "carol@example.com"},
		{Name: "dave", Email: "dave@example.com"},
	}

	if err := insertTables(context.Background(), exec, users[0], users[1]); err != nil {
		t.Fatalf("insert via RETURNING: %v", err)
	}

	if users[0].ID <= 0 || users[1].ID != users[0].ID+1 {
		t.Fatalf("expected consecutive generated IDs from RETURNING, got %d and %d", users[0].ID, users[1].ID)
	}
}

// TestChunkedUpdateWithAStaleRowKeepsTheWrittenRowsCurrent covers a chunk in
// which one row is stale. The statement writes the others, but they kept their
// old version in memory and the error could not say which row was stale, so
// retrying the written row failed forever.
func TestChunkedUpdateWithAStaleRowKeepsTheWrittenRowsCurrent(t *testing.T) {
	db := newOptimisticMutationEngine(t)
	exec := requireInitializedRuntime(t, db)
	ctx := context.Background()

	if _, err := db.DB().ExecContext(ctx, `INSERT INTO users (id,name,email,version) VALUES (1,'a','a@x',3),(2,'b','b@x',7)`); err != nil {
		t.Fatal(err)
	}

	fresh := &optimisticMutationUser{ID: 1, Name: "a2", Email: "a2@x", Version: 3}
	stale := &optimisticMutationUser{ID: 2, Name: "b2", Email: "b2@x", Version: 6}

	err := ChunkedUpdate(ctx, exec, []*optimisticMutationUser{fresh, stale})
	if !errors.Is(err, &ErrOptimisticLockConflict{}) || !strings.Contains(err.Error(), "stale keys [2]") {
		t.Fatalf("ChunkedUpdate = %v; want a conflict naming key 2", err)
	}

	if fresh.Version != 4 || stale.Version != 6 {
		t.Fatalf("versions = %d, %d; want the written row advanced and the stale one untouched", fresh.Version, stale.Version)
	}

	fresh.Name = "a3"
	if err := Update(ctx, exec, fresh); err != nil {
		t.Fatalf("retrying the written row = %v", err)
	}
}

// TestUpdateWithoutAVersionReportsAMissingRow covers a table without a version
// column, where an update that matched nothing reported success.
func TestUpdateWithoutAVersionReportsAMissingRow(t *testing.T) {
	db := newBatchMutationEngine(t)
	exec := requireInitializedRuntime(t, db)
	ctx := context.Background()

	err := Update(ctx, exec, &batchMutationUser{ID: 42, Name: "ghost", Email: "g@x"})
	if !errors.Is(err, sql.ErrNoRows) || !strings.Contains(err.Error(), "42") {
		t.Fatalf("Update of a missing row = %v; want sql.ErrNoRows naming it", err)
	}

	if _, err := db.DB().ExecContext(ctx, `INSERT INTO users (id,name,email) VALUES (1,'a','a@x')`); err != nil {
		t.Fatal(err)
	}

	// Writing the values a row already holds is not a missing row, although
	// MySQL reports no row affected.
	if err := Update(ctx, exec, &batchMutationUser{ID: 1, Name: "a", Email: "a@x"}); err != nil {
		t.Fatalf("Update with unchanged values = %v", err)
	}
}

// TestChunkedUpdateRefusesOneKeyTwice covers two rows with one primary key: the
// CASE statement wrote the first and dropped the second without a word.
func TestChunkedUpdateRefusesOneKeyTwice(t *testing.T) {
	db := newBatchMutationEngine(t)
	exec := requireInitializedRuntime(t, db)

	err := ChunkedUpdate(context.Background(), exec, []*batchMutationUser{{ID: 1, Name: "first", Email: "f@x"}, {ID: 1, Name: "second", Email: "s@x"}})
	if err == nil || !strings.Contains(err.Error(), "same primary key") {
		t.Fatalf("ChunkedUpdate = %v; want the duplicate key refused", err)
	}
}

type failingInsertResult struct{}

func (failingInsertResult) LastInsertId() (int64, error) { return 0, errors.New("no id") }
func (failingInsertResult) RowsAffected() (int64, error) { return 1, nil }

// TestInsertReportsAKeyItCannotRead covers a driver that cannot report the
// generated key: the row silently kept a zero key.
func TestInsertReportsAKeyItCannotRead(t *testing.T) {
	record := mutationRecord{pkField: mutationField{column: "id", value: reflect.ValueOf(new(int64)).Elem()}}

	err := assignBatchInsertIDs(context.Background(), nil, []mutationRecord{record}, failingInsertResult{}, true)
	if err == nil || !strings.Contains(err.Error(), "generated key") {
		t.Fatalf("assignBatchInsertIDs = %v; want the missing key reported", err)
	}
}
