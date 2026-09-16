package tsq

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
)

// TestValidateRegisteredTableIdentifiersRejectsOversizedNames covers the three
// identifier kinds against a dialect the unit suite never connects to.
//
// The check used to be configurable, and its "warn" mode collected violations
// into a slice that an earlier version then dropped on the floor. There is no
// mode any more: an identifier the dialect will truncate names an object that
// does not match what the rendered queries reference, so it fails construction.
func TestValidateRegisteredTableIdentifiersRejectsOversizedNames(t *testing.T) {
	mysql := tsqdialect.MySQLDialect{}

	tests := []struct {
		name  string
		build func(string) *Runtime
	}{
		{
			name: "table name",
			build: func(long string) *Runtime {
				return &Runtime{db: &sql.DB{}, dialect: mysql, tables: []*registeredTable{{
					Table: newMockTable(long),
				}}}
			},
		},
		{
			name: "column name",
			build: func(long string) *Runtime {
				table, _ := newStrictMockTable("users", long)

				return &Runtime{db: &sql.DB{}, dialect: mysql, tables: []*registeredTable{{
					Table: table,
				}}}
			},
		},
		{
			name: "index name",
			build: func(long string) *Runtime {
				table, _ := newStrictMockTable("users", "id")

				return &Runtime{db: &sql.DB{}, dialect: mysql, tables: []*registeredTable{{
					Table:   table,
					Indexes: []TableIndex{{Name: long, Fields: []string{"id"}}},
				}}}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			long := firstRejectedIdentifier(t, mysql, "x")

			err := test.build(long).validateRegisteredTableIdentifiers()
			if err == nil || !strings.Contains(err.Error(), long) {
				t.Fatalf("expected the oversized identifier to be reported, got %v", err)
			}
		})
	}
}

func TestNewRuntimeContextHonorsCancellation(t *testing.T) {
	_, dsn := newSQLiteIndexTestEngine(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := NewRuntime(ctx, "sqlite", dsn, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancelled context to abort bootstrap, got %v", err)
	}
}

func TestNewRuntimeContextRejectsNilContext(t *testing.T) {
	_, dsn := newSQLiteIndexTestEngine(t)

	//nolint:staticcheck // passing nil on purpose to exercise the guard
	if _, err := NewRuntime(nil, "sqlite", dsn, nil); err == nil {
		t.Fatal("expected nil context to be rejected")
	}
}

func TestRuntimeCloseReleasesDBAndIsNilSafe(t *testing.T) {
	_, dsn := newSQLiteIndexTestEngine(t)

	rt, err := NewRuntime(context.Background(), "sqlite", dsn, nil)
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}

	if err := rt.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if err := rt.DB().PingContext(context.Background()); err == nil {
		t.Fatal("expected closed pool to reject ping")
	}

	var nilRuntime *Runtime
	if err := nilRuntime.Close(); err != nil {
		t.Fatalf("expected nil runtime Close to be a no-op, got %v", err)
	}
}

func TestRuntimeMaxPageSizeDefaultsAndOverrides(t *testing.T) {
	_, dsn := newSQLiteIndexTestEngine(t)

	rt, err := NewRuntime(context.Background(), "sqlite", dsn, nil)
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}
	t.Cleanup(func() { _ = rt.Close() })

	if got := rt.MaxPageSize(); got != DefaultMaxPageSize {
		t.Fatalf("expected default max page size %d, got %d", DefaultMaxPageSize, got)
	}

	var nilRuntime *Runtime
	if got := nilRuntime.MaxPageSize(); got != DefaultMaxPageSize {
		t.Fatalf("expected nil runtime to report default max page size, got %d", got)
	}

	custom, err := NewRuntime(context.Background(), "sqlite", dsn, nil, WithMaxPageSize(5000))
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}
	t.Cleanup(func() { _ = custom.Close() })

	if got := custom.MaxPageSize(); got != 5000 {
		t.Fatalf("expected custom max page size 5000, got %d", got)
	}

	page := normalizePageReqWithLimit(&PageRequest{Size: 3000}, pageSizeLimitForExecutor(custom))
	if page.Size != 3000 {
		t.Fatalf("expected runtime limit to allow size 3000, got %d", page.Size)
	}

	page = normalizePageReqWithLimit(&PageRequest{Size: 3000}, pageSizeLimitForExecutor(custom.DB()))
	if page.Size != DefaultMaxPageSize {
		t.Fatalf("expected bare *sql.DB to fall back to default cap, got %d", page.Size)
	}

	if _, err := NewRuntime(context.Background(), "sqlite", dsn, nil, WithMaxPageSize(-1)); err == nil {
		t.Fatal("expected negative max page size to be rejected")
	}
}

func TestLogForExecutorRoutesToRuntimeLogger(t *testing.T) {
	_, dsn := newSQLiteIndexTestEngine(t)
	logger := &recordingLogger{}

	rt, err := NewRuntime(context.Background(), "sqlite", dsn, nil, WithLogger(logger))
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}
	t.Cleanup(func() { _ = rt.Close() })

	logForExecutor(context.Background(), rt, slog.LevelWarn, "routed", "k", "v")

	if logger.count("routed") != 1 {
		t.Fatalf("expected runtime logger to receive the message, got %v", logger.messages)
	}

	// An executor without a runtime must not panic and must not reach the runtime logger.
	logForExecutor(context.Background(), rt.DB(), slog.LevelDebug, "unrouted")

	if logger.count("unrouted") != 0 {
		t.Fatal("expected bare *sql.DB executor to bypass the runtime logger")
	}
}

// TestLogSQLRoutesRenderedStatementsToRuntimeLogger is the end-to-end check on
// RuntimeOptions.LogSQL.
//
// Before it existed, the read path logged SQL only behind an unexported context key
// that no exported symbol could set, so every one of those log statements was
// unreachable in the published library while looking, in the source, like a working
// feature. The assertion that matters is that the record lands on the runtime's own
// Logger rather than slog.Default().
func TestLogSQLRoutesRenderedStatementsToRuntimeLogger(t *testing.T) {
	dsn := filepath.Join(t.TempDir(), "logsql.db")

	seed, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}

	if _, err := seed.ExecContext(context.Background(), `CREATE TABLE users (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL,
		email TEXT NOT NULL UNIQUE
	)`); err != nil {
		t.Fatalf("create users table: %v", err)
	}

	if err := seed.Close(); err != nil {
		t.Fatalf("close seed connection: %v", err)
	}

	logger := &recordingLogger{}

	rt, err := NewRuntime(context.Background(), "sqlite", dsn, nil, WithLogger(logger), WithSQLLogging())
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}

	t.Cleanup(func() { _ = rt.Close() })

	query, err := Select(batchMutationUserColumns()...).From(batchMutationUser{}).Build()
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	if _, err := query.List(context.Background(), rt); err != nil {
		t.Fatalf("List() error = %v", err)
	}

	if logger.count("list") != 1 {
		t.Fatalf("expected the rendered list statement on the runtime logger, got %v", logger.messages)
	}
}

// TestLogSQLDefaultsOff keeps query execution silent unless asked: SQL text and bound
// arguments can carry secrets, so logging them must be something the caller opts into.
func TestLogSQLDefaultsOff(t *testing.T) {
	dsn := filepath.Join(t.TempDir(), "nologsql.db")

	seed, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}

	if _, err := seed.ExecContext(context.Background(), `CREATE TABLE users (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL,
		email TEXT NOT NULL UNIQUE
	)`); err != nil {
		t.Fatalf("create users table: %v", err)
	}

	if err := seed.Close(); err != nil {
		t.Fatalf("close seed connection: %v", err)
	}

	logger := &recordingLogger{}

	rt, err := NewRuntime(context.Background(), "sqlite", dsn, nil, WithLogger(logger))
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}

	t.Cleanup(func() { _ = rt.Close() })

	query, err := Select(batchMutationUserColumns()...).From(batchMutationUser{}).Build()
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	if _, err := query.List(context.Background(), rt); err != nil {
		t.Fatalf("List() error = %v", err)
	}

	if logger.count("list") != 0 {
		t.Fatalf("expected no SQL on the logger with LogSQL off, got %v", logger.messages)
	}
}

// TestNewRuntimeFromDBLeavesTheCallersPoolOpen pins the ownership rule. A runtime
// built over a pool it did not open must not close it: the caller may still be
// using that pool for work TSQ knows nothing about, which is the whole reason the
// constructor exists.
func TestNewRuntimeFromDBLeavesTheCallersPoolOpen(t *testing.T) {
	_, dsn := newSQLiteIndexTestEngine(t)

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}

	t.Cleanup(func() { _ = db.Close() })

	runtime, err := NewRuntimeFromDB(context.Background(), db, tsqdialect.SQLiteDialect{}, nil)
	if err != nil {
		t.Fatalf("NewRuntimeFromDB() error = %v", err)
	}

	if err := runtime.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if err := db.PingContext(context.Background()); err != nil {
		t.Fatalf("expected the caller's pool to stay usable after Close, got %v", err)
	}
}

func TestNewRuntimeFromDBRejectsMissingArguments(t *testing.T) {
	_, dsn := newSQLiteIndexTestEngine(t)

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}

	t.Cleanup(func() { _ = db.Close() })

	if _, err := NewRuntimeFromDB(context.Background(), nil, tsqdialect.SQLiteDialect{}, nil); err == nil {
		t.Fatal("expected a nil pool to be rejected")
	}

	if _, err := NewRuntimeFromDB(context.Background(), db, nil, nil); err == nil {
		t.Fatal("expected a nil dialect to be rejected")
	}
}

// TestNewRuntimeClosesThePoolItOpened is the other half of the ownership rule.
func TestNewRuntimeClosesThePoolItOpened(t *testing.T) {
	_, dsn := newSQLiteIndexTestEngine(t)

	runtime, err := NewRuntime(context.Background(), "sqlite", dsn, nil)
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}

	if err := runtime.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if err := runtime.DB().PingContext(context.Background()); err == nil {
		t.Fatal("expected the runtime's own pool to be closed")
	}
}
