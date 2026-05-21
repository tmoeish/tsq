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
type optimisticMutationUser struct {
	ID      int64
	Name    string
	Email   string
	Version int64
}

func (batchMutationUser) TSQOwner() {
}

func (batchMutationUser) Table() string {
	return "users"
}

func (batchMutationUser) Cols() []SQLColumn {
	return SQLColumns(batchMutationUserColumns()...)
}

func (batchMutationUser) SearchColumns() []SearchColumn {
	return nil
}

func (batchMutationUser) PrimaryKeys() []string {
	return []string{"id"}
}

func (batchMutationUser) AutoIncrement() bool {
	return true
}

func (batchMutationUser) VersionColumn() string {
	return ""
}

func batchMutationUserColumns() []BoundColumn[batchMutationUser] {
	return []BoundColumn[batchMutationUser]{NewCol[batchMutationUser, int64]("id", "id", func(t *batchMutationUser) *int64 {
		return &t.ID
	}), NewCol[batchMutationUser, string]("name", "name", func(t *batchMutationUser) *string {
		return &t.Name
	}), NewCol[batchMutationUser, string]("email", "email", func(t *batchMutationUser) *string {
		return &t.Email
	})}
}

func (optimisticMutationUser) TSQOwner() {
}

func (optimisticMutationUser) Table() string {
	return "users"
}

func (optimisticMutationUser) Cols() []SQLColumn {
	return SQLColumns(optimisticMutationUserColumns()...)
}

func (optimisticMutationUser) SearchColumns() []SearchColumn {
	return nil
}

func (optimisticMutationUser) PrimaryKeys() []string {
	return []string{"id"}
}

func (optimisticMutationUser) AutoIncrement() bool {
	return true
}

func (optimisticMutationUser) VersionColumn() string {
	return "version"
}

func optimisticMutationUserColumns() []BoundColumn[optimisticMutationUser] {
	return []BoundColumn[optimisticMutationUser]{NewCol[optimisticMutationUser, int64]("id", "id", func(t *optimisticMutationUser) *int64 {
		return &t.ID
	}), NewCol[optimisticMutationUser, string]("name", "name", func(t *optimisticMutationUser) *string {
		return &t.Name
	}), NewCol[optimisticMutationUser, string]("email", "email", func(t *optimisticMutationUser) *string {
		return &t.Email
	}), NewCol[optimisticMutationUser, int64]("version", "version", func(t *optimisticMutationUser) *int64 {
		return &t.Version
	})}
}

func newRuntimeWithDB(db *sql.DB, dialect Dialect) *Runtime {
	runtime := NewRuntime()
	runtime.engine = newEngine(db, dialect)
	return runtime
}

func requireRuntimeExecutor(t *testing.T, runtime *Runtime) SQLExecutor {
	t.Helper()

	exec := runtime.Executor()
	if exec == nil {
		t.Fatal("expected runtime executor to be initialized")
	}

	return exec
}

func newBatchMutationEngine(t *testing.T) *Runtime {
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
	return newRuntimeWithDB(db, SQLiteDialect{})
}

func newOptimisticMutationEngine(t *testing.T) *Runtime {
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
		email TEXT NOT NULL UNIQUE,
		version INTEGER NOT NULL
	)`); err != nil {
		t.Fatalf("create users table: %v", err)
	}
	return newRuntimeWithDB(db, SQLiteDialect{})
}

func TestEngineQueryUsesContext(t *testing.T) {
	db := newBatchMutationEngine(t)
	exec := requireRuntimeExecutor(t, db)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := exec.QueryContext(ctx, `SELECT id FROM users`)
	if err == nil {
		t.Fatal("expected canceled context to fail query")
	}
	if !strings.Contains(err.Error(), context.Canceled.Error()) {
		t.Fatalf("expected query to surface context cancellation, got %v", err)
	}
}

func TestEngineExecUsesContext(t *testing.T) {
	db := newBatchMutationEngine(t)
	exec := requireRuntimeExecutor(t, db)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := exec.ExecContext(ctx, `INSERT INTO users (name, email) VALUES ('alice', 'alice@example.com')`)
	if err == nil {
		t.Fatal("expected canceled context to fail exec")
	}
	if !strings.Contains(err.Error(), context.Canceled.Error()) {
		t.Fatalf("expected exec to surface context cancellation, got %v", err)
	}
}

func TestRuntimeExecutorIsNilBeforeInit(t *testing.T) {
	db := &Runtime{}
	if exec := db.Executor(); exec != nil {
		t.Fatalf("expected nil executor before Init, got %#v", exec)
	}
}

func TestNilRuntimeExecutorIsNil(t *testing.T) {
	var db *Runtime
	if exec := db.Executor(); exec != nil {
		t.Fatalf("expected nil executor for nil runtime, got %#v", exec)
	}
}
