package tsq

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgconn"
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
	return &Runtime{
		traceManager: newTraceManager(),
		engine:       newEngine(db, dialect),
	}
}

func requireInitializedRuntime(t *testing.T, runtime *Runtime) *Runtime {
	t.Helper()

	if err := validateTxRuntime(runtime); err != nil {
		t.Fatalf("expected initialized runtime, got %v", err)
	}

	return runtime
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
	exec := requireInitializedRuntime(t, db)
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
	exec := requireInitializedRuntime(t, db)
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

func TestRuntimeAsExecutorRequiresInitBeforeQuery(t *testing.T) {
	db := &Runtime{}

	_, err := db.QueryContext(context.Background(), `SELECT 1`)
	if err == nil {
		t.Fatal("expected uninitialized runtime query to fail")
	}
	if !strings.Contains(err.Error(), "construct it with NewRuntime") {
		t.Fatalf("expected initialization guidance, got %v", err)
	}
}

func TestNilRuntimeAsExecutorRequiresInit(t *testing.T) {
	var db *Runtime

	_, err := db.ExecContext(context.Background(), `SELECT 1`)
	if err == nil {
		t.Fatal("expected nil runtime exec to fail")
	}
	if !strings.Contains(err.Error(), "runtime cannot be nil") {
		t.Fatalf("expected nil runtime guidance, got %v", err)
	}
}

func TestRuntimeQueryRowContextRequiresInit(t *testing.T) {
	db := &Runtime{}

	var count int
	err := db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM users`).Scan(&count)
	if err == nil {
		t.Fatal("expected uninitialized runtime query row to fail")
	}
	if !strings.Contains(err.Error(), "construct it with NewRuntime") {
		t.Fatalf("expected initialization guidance, got %v", err)
	}
}

func TestRuntimeWithTxCommitsAndCarriesDialect(t *testing.T) {
	db := newBatchMutationEngine(t)

	err := db.WithTx(context.Background(), nil, func(ctx context.Context, txExec SQLExecutor) error {
		return Insert(ctx, txExec, &batchMutationUser{
			Name:  "alice",
			Email: "alice@example.com",
		})
	})
	if err != nil {
		t.Fatalf("expected transaction to commit, got %v", err)
	}

	var count int
	if err := db.DB().QueryRowContext(context.Background(), `SELECT COUNT(*) FROM users`).Scan(&count); err != nil {
		t.Fatalf("count committed rows: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected one committed row, got %d", count)
	}
}

func TestRuntimeWithTxRollsBackOnCallbackError(t *testing.T) {
	db := newBatchMutationEngine(t)
	wantErr := errors.New("boom")

	err := db.WithTx(context.Background(), nil, func(ctx context.Context, txExec SQLExecutor) error {
		if err := Insert(ctx, txExec, &batchMutationUser{
			Name:  "alice",
			Email: "alice@example.com",
		}); err != nil {
			return err
		}

		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected callback error, got %v", err)
	}

	var count int
	if err := db.DB().QueryRowContext(context.Background(), `SELECT COUNT(*) FROM users`).Scan(&count); err != nil {
		t.Fatalf("count rolled back rows: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected rollback to leave zero rows, got %d", count)
	}
}

func TestRuntimeWithTxRequiresInitializedRuntime(t *testing.T) {
	runtime := &Runtime{traceManager: newTraceManager()}

	err := runtime.WithTx(context.Background(), nil, func(context.Context, SQLExecutor) error {
		return nil
	})
	if err == nil {
		t.Fatal("expected uninitialized runtime to fail")
	}
	if !strings.Contains(err.Error(), "construct it with NewRuntime") {
		t.Fatalf("expected initialization guidance, got %v", err)
	}
}

func TestRuntimeWithTxRejectsNilCallback(t *testing.T) {
	db := newBatchMutationEngine(t)

	err := db.WithTx(context.Background(), nil, nil)
	if err == nil {
		t.Fatal("expected nil callback to fail")
	}
	if !strings.Contains(err.Error(), "transaction function cannot be nil") {
		t.Fatalf("unexpected nil callback error: %v", err)
	}
}

func TestRuntimeWithTxRetriesOptimisticLockWithDefaultPolicy(t *testing.T) {
	db := newBatchMutationEngine(t)
	attempts := 0

	err := db.WithTx(context.Background(), &TxOptions{Retry: IsOptimisticLockError}, func(ctx context.Context, txExec SQLExecutor) error {
		attempts++
		if attempts < 3 {
			return &ErrOptimisticLockConflict{}
		}

		return Insert(ctx, txExec, &batchMutationUser{
			Name:  "alice",
			Email: "alice@example.com",
		})
	})
	if err != nil {
		t.Fatalf("expected optimistic lock retry to succeed, got %v", err)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}
}

func TestRuntimeWithTxOptimisticLockRetryHonorsCustomPolicy(t *testing.T) {
	db := newBatchMutationEngine(t)
	attempts := 0
	wantErr := &ErrOptimisticLockConflict{}

	err := db.WithTx(context.Background(), &TxOptions{
		Retry: IsOptimisticLockError,
		RetryConfig: &TxRetryConfig{
			MaxAttempts:       2,
			InitialBackoff:    0,
			MaxBackoff:        0,
			BackoffMultiplier: 1,
		},
	}, func(context.Context, SQLExecutor) error {
		attempts++
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected optimistic lock conflict, got %v", err)
	}
	if attempts != 2 {
		t.Fatalf("expected custom retry policy to stop after 2 attempts, got %d", attempts)
	}
}

func TestRuntimeWithTxRejectsInvalidRetryPolicy(t *testing.T) {
	db := newBatchMutationEngine(t)

	err := db.WithTx(context.Background(), &TxOptions{
		Retry: IsOptimisticLockError,
		RetryConfig: &TxRetryConfig{
			MaxAttempts:       0,
			InitialBackoff:    0,
			MaxBackoff:        0,
			BackoffMultiplier: 1,
		},
	}, func(context.Context, SQLExecutor) error {
		return nil
	})
	if err == nil {
		t.Fatal("expected invalid retry policy to fail")
	}
	if !strings.Contains(err.Error(), "max attempts") {
		t.Fatalf("unexpected invalid retry policy error: %v", err)
	}
}

func TestWithTx1ReturnsValue(t *testing.T) {
	db := newBatchMutationEngine(t)

	got, err := WithTx1(db, context.Background(), nil, func(ctx context.Context, txExec SQLExecutor) (int, error) {
		if err := Insert(ctx, txExec, &batchMutationUser{
			Name:  "alice",
			Email: "alice@example.com",
		}); err != nil {
			return 0, err
		}

		return 41, nil
	})
	if err != nil {
		t.Fatalf("expected WithTx1 to succeed, got %v", err)
	}
	if got != 41 {
		t.Fatalf("expected WithTx1 result 41, got %d", got)
	}
}

func TestWithTx2ReturnsValues(t *testing.T) {
	db := newBatchMutationEngine(t)

	first, second, err := WithTx2(db, context.Background(), &TxOptions{
		Retry: IsOptimisticLockError,
		RetryConfig: &TxRetryConfig{
			MaxAttempts:       2,
			InitialBackoff:    0,
			MaxBackoff:        0,
			BackoffMultiplier: 1,
		},
	}, func(ctx context.Context, txExec SQLExecutor) (int, string, error) {
		if err := Insert(ctx, txExec, &batchMutationUser{
			Name:  "alice",
			Email: "alice@example.com",
		}); err != nil {
			return 0, "", err
		}

		return 7, "ok", nil
	})
	if err != nil {
		t.Fatalf("expected WithTx2 to succeed, got %v", err)
	}
	if first != 7 || second != "ok" {
		t.Fatalf("expected WithTx2 result (7, ok), got (%d, %q)", first, second)
	}
}

func TestRuntimeWithTxRetryRespectsContextCancellationBetweenAttempts(t *testing.T) {
	db := newBatchMutationEngine(t)
	ctx, cancel := context.WithCancel(context.Background())
	attempts := 0

	err := db.WithTx(ctx, &TxOptions{
		Retry: IsOptimisticLockError,
		RetryConfig: &TxRetryConfig{
			MaxAttempts:       3,
			InitialBackoff:    10 * time.Millisecond,
			MaxBackoff:        10 * time.Millisecond,
			BackoffMultiplier: 1,
		},
	}, func(context.Context, SQLExecutor) error {
		attempts++
		cancel()
		return &ErrOptimisticLockConflict{}
	})
	if err == nil {
		t.Fatal("expected canceled context to stop retries")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
	if attempts != 1 {
		t.Fatalf("expected cancellation to stop after first attempt, got %d attempts", attempts)
	}
}

func TestIsOptimisticLockError(t *testing.T) {
	if !IsOptimisticLockError(&ErrOptimisticLockConflict{}) {
		t.Fatal("expected optimistic lock conflict to be detected")
	}
	if IsOptimisticLockError(errors.New("boom")) {
		t.Fatal("expected non optimistic lock error to be ignored")
	}
}

func TestIsRetryableNetworkError(t *testing.T) {
	if !IsRetryableNetworkError(driver.ErrBadConn) {
		t.Fatal("expected driver.ErrBadConn to be retryable")
	}
	if !IsRetryableNetworkError(&net.DNSError{IsTimeout: true}) {
		t.Fatal("expected timeout network error to be retryable")
	}
	if IsRetryableNetworkError(context.Canceled) {
		t.Fatal("expected context cancellation to stay non-retryable")
	}
}

func TestIsRetryableTransactionConflictError(t *testing.T) {
	if !IsRetryableTransactionConflictError(&pgconn.PgError{Code: "40001"}) {
		t.Fatal("expected postgres serialization failure to be retryable")
	}
	if IsRetryableTransactionConflictError(errors.New("boom")) {
		t.Fatal("expected generic error to stay non-retryable")
	}
}

func TestRetryHelpersCanBeUsedAsPredicates(t *testing.T) {
	if !IsRetryableNetworkError(driver.ErrBadConn) {
		t.Fatal("expected network retry helper to accept driver bad connections")
	}
	if !IsRetryableTransactionConflictError(&pgconn.PgError{Code: "40P01"}) {
		t.Fatal("expected transaction conflict helper to accept deadlocks")
	}
	if !IsCommonTransactionRetryableError(&ErrOptimisticLockConflict{}) {
		t.Fatal("expected combined helper to include optimistic lock conflicts")
	}
}

func TestShouldRetryTxSkipsCommitStage(t *testing.T) {
	opts := &normalizedTxOptions{
		retry:       IsRetryableNetworkError,
		retryConfig: DefaultTxRetryConfig(),
	}

	if shouldRetryTx(driver.ErrBadConn, txRetryStageCommit, opts, 1) {
		t.Fatal("expected commit-stage errors to stay non-retryable")
	}
	if !shouldRetryTx(driver.ErrBadConn, txRetryStageBody, opts, 1) {
		t.Fatal("expected body-stage network errors to be retryable")
	}
}
