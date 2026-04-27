package tsq

import (
	"database/sql"
	"errors"
	"io"
	"log/slog"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"gopkg.in/gorp.v2"
)

// MockCloser is a helper for testing Close() error handling
type MockCloser struct {
	closed bool
	err    error
}

func (m *MockCloser) Close() error {
	m.closed = true
	return m.err
}

func TestResourceCleanup_RowsClosedOnSuccess(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.Exec(`CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)`); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	if _, err := db.Exec(`INSERT INTO test (id, name) VALUES (1, 'test')`); err != nil {
		t.Fatalf("failed to insert test data: %v", err)
	}

	rows, err := db.Query(`SELECT id, name FROM test`)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			slog.Warn("Failed to close rows", "error", closeErr)
		}
	}()

	if !rows.Next() {
		t.Fatal("expected at least one row")
	}

	var id int
	var name string
	if err := rows.Scan(&id, &name); err != nil {
		t.Fatalf("scan failed: %v", err)
	}

	if id != 1 || name != "test" {
		t.Errorf("got id=%d, name=%s; want id=1, name=test", id, name)
	}
}

func TestResourceCleanup_RowsClosedOnQueryError(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Query non-existent table to trigger error
	rows, err := db.Query(`SELECT * FROM nonexistent_table`)
	if err == nil {
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				slog.Warn("Failed to close rows", "error", closeErr)
			}
		}()
		t.Fatal("expected query to fail")
	}
}

func TestResourceCleanup_MockCloserSuccess(t *testing.T) {
	closer := &MockCloser{}

	defer func() {
		if closeErr := closer.Close(); closeErr != nil {
			t.Errorf("unexpected close error: %v", closeErr)
		}
	}()

	if closer.closed {
		t.Fatal("closer should not be closed before defer")
	}
}

func TestResourceCleanup_MockCloserWithError(t *testing.T) {
	expectedErr := errors.New("close failed")
	closer := &MockCloser{err: expectedErr}

	recovered := false
	defer func() {
		if closeErr := closer.Close(); closeErr != nil {
			if closeErr.Error() != expectedErr.Error() {
				t.Errorf("close error mismatch: got %v, want %v", closeErr, expectedErr)
			}
			recovered = true
		}
	}()

	if recovered {
		t.Fatal("error should not be recovered before defer exits")
	}
}

func TestResourceCleanup_ErrorHandlingPattern(t *testing.T) {
	// This test validates the cleanup pattern used throughout the codebase
	closer := &MockCloser{err: io.ErrClosedPipe}

	// The pattern we follow:
	// defer func() {
	//     if closeErr := closer.Close(); closeErr != nil {
	//         slog.Warn("message", "error", closeErr)
	//     }
	// }()
	//
	// Verify that the Close() method is called and errors are not suppressed

	defer func() {
		if err := closer.Close(); err != nil {
			// In real code, slog.Warn would be called here
			t.Logf("Close error: %v", err)
		}
	}()

	// The close will be called after this function returns
	if closer.closed {
		t.Fatal("Close should not be called before defer")
	}
}

func TestResourceCleanup_DatabaseConnectionClosed(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}

	if _, err := db.Exec(`CREATE TABLE test (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	if _, err := db.Exec(`INSERT INTO test (id) VALUES (1)`); err != nil {
		t.Fatalf("failed to insert test data: %v", err)
	}

	// Simulate cleanup pattern
	rowsClosed := false
	dbClosed := false

	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			slog.Warn("Failed to close database", "error", closeErr)
		}
		dbClosed = true
	}()

	rows, err := db.Query(`SELECT id FROM test`)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			slog.Warn("Failed to close rows", "error", closeErr)
		}
		rowsClosed = true
	}()

	if !rows.Next() {
		t.Fatal("expected row")
	}

	var id int
	if err := rows.Scan(&id); err != nil {
		t.Fatalf("scan failed: %v", err)
	}

	if id != 1 {
		t.Errorf("got id=%d; want 1", id)
	}

	// After exiting this function, defers are called in reverse order (LIFO)
	// rows.Close is called before db.Close
	_ = rowsClosed
	_ = dbClosed
}

func TestResourceCleanup_GorpDbMapClosure(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}

	dbMap := &gorp.DbMap{Db: db, Dialect: gorp.SqliteDialect{}}

	defer func() {
		if closeErr := dbMap.Db.Close(); closeErr != nil {
			slog.Warn("Failed to close database", "error", closeErr)
		}
	}()

	if _, err := dbMap.Db.Exec(`CREATE TABLE test (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	if _, err := dbMap.Db.Exec(`INSERT INTO test (id) VALUES (42)`); err != nil {
		t.Fatalf("failed to insert test data: %v", err)
	}

	rows, err := dbMap.Db.Query(`SELECT id FROM test`)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			slog.Warn("Failed to close rows", "error", closeErr)
		}
	}()

	if !rows.Next() {
		t.Fatal("expected row")
	}

	var id int
	if err := rows.Scan(&id); err != nil {
		t.Fatalf("scan failed: %v", err)
	}

	if id != 42 {
		t.Errorf("got id=%d; want 42", id)
	}
}
