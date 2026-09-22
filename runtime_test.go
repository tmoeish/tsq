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
	sqld "github.com/tmoeish/tsq/v5/internal/sqldialect"
)

// TestValidateRegisteredTableIdentifiersRejectsOversizedNames covers the three
// identifier kinds against a dialect the unit suite never connects to.
//
// The check used to be configurable, and its "warn" mode collected violations
// into a slice that an earlier version then dropped on the floor. There is no
// mode any more: an identifier the dialect will truncate names an object that
// does not match what the rendered queries reference, so it fails construction.
func TestValidateRegisteredTableIdentifiersRejectsOversizedNames(t *testing.T) {
	mysql := sqld.MySQLDialect{}
	long := firstRejectedIdentifier(t, mysql)

	tests := map[string]Table{
		"table name":  wideTable(long, []string{"name"}, nil, nil),
		"column name": wideTable("users", []string{long}, nil, nil),
		"index name":  wideTable("users", []string{"name"}, nil, []TableIndex{{Name: long, Columns: []string{"name"}}}),
	}

	for name, table := range tests {
		t.Run(name, func(t *testing.T) {
			registered, err := registerTables([]Table{table})
			if err != nil {
				t.Fatalf("registerTables() error = %v", err)
			}

			rt := &Runtime{db: &sql.DB{}, dialect: mysql, tables: registered}

			err = rt.validateRegisteredTableIdentifiers()
			if err == nil || !strings.Contains(err.Error(), long) {
				t.Fatalf("expected the oversized identifier to be reported, got %v", err)
			}
		})
	}
}

// TestRegisteredIndexesShareNothingWithTheTable covers the copy registerTables
// makes: a shallow one shared each index's column list with the table, so a
// policy that edited the registration would edit the definition too.
func TestRegisteredIndexesShareNothingWithTheTable(t *testing.T) {
	table := wideTable("users", []string{"name"}, nil, []TableIndex{{Name: "idx_users_name", Columns: []string{"name"}}})

	registered, err := registerTables([]Table{table})
	if err != nil {
		t.Fatal(err)
	}

	registered[0].Indexes[0].Columns[0] = "changed"

	if got := table.definition().indexes[0].Columns[0]; got != "name" {
		t.Fatalf("the table's index column = %q after editing the registration; want it untouched", got)
	}
}

// firstRejectedIdentifier returns the shortest identifier dialect rejects as too long.
func firstRejectedIdentifier(t *testing.T, dialect sqld.Dialect) string {
	t.Helper()

	identifier := "x"
	for range 1024 {
		if err := sqld.ValidateIdentifier(dialect, identifier); err != nil {
			return identifier
		}

		identifier += "x"
	}

	t.Fatalf("no identifier is too long for %s", dialect.Name())

	return ""
}

func TestNewRuntimeContextHonorsCancellation(t *testing.T) {
	_, dsn := newSQLiteIndexTestEngine(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := Open(ctx, "sqlite", dsn, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancelled context to abort bootstrap, got %v", err)
	}
}

func TestNewRuntimeContextRejectsNilContext(t *testing.T) {
	_, dsn := newSQLiteIndexTestEngine(t)

	//nolint:staticcheck // passing nil on purpose to exercise the guard
	if _, err := Open(nil, "sqlite", dsn, nil); err == nil {
		t.Fatal("expected nil context to be rejected")
	}
}

func TestRuntimeCloseReleasesDBAndIsNilSafe(t *testing.T) {
	_, dsn := newSQLiteIndexTestEngine(t)

	rt, err := Open(context.Background(), "sqlite", dsn, nil)
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

	rt, err := Open(context.Background(), "sqlite", dsn, nil)
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}
	t.Cleanup(func() { _ = rt.Close() })

	if got := rt.maxPage(); got != DefaultMaxPageSize {
		t.Fatalf("expected default max page size %d, got %d", DefaultMaxPageSize, got)
	}

	var nilRuntime *Runtime
	if got := nilRuntime.maxPage(); got != DefaultMaxPageSize {
		t.Fatalf("expected nil runtime to report default max page size, got %d", got)
	}

	custom, err := Open(context.Background(), "sqlite", dsn, nil, WithMaxPageSize(5000))
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}
	t.Cleanup(func() { _ = custom.Close() })

	if got := custom.maxPage(); got != 5000 {
		t.Fatalf("expected custom max page size 5000, got %d", got)
	}

	page := Paging{Size: 3000}.normalized(runtimeForExecutor(custom).maxPage())
	if page.Size != 3000 {
		t.Fatalf("expected runtime limit to allow size 3000, got %d", page.Size)
	}

	wrapped := WrapExecutor(custom.DB(), onSQLite)

	page = Paging{Size: 3000}.normalized(runtimeForExecutor(wrapped).maxPage())
	if page.Size != DefaultMaxPageSize {
		t.Fatalf("expected a wrapped pool to fall back to the default cap, got %d", page.Size)
	}

	// Zero is refused too: it used to pass and fall back to the default, so a size
	// read from an unset config value silently became 1000.
	for _, size := range []int{-1, 0} {
		if _, err := Open(context.Background(), "sqlite", dsn, nil, WithMaxPageSize(size)); err == nil {
			t.Fatalf("expected max page size %d to be rejected", size)
		}
	}
}

func TestLogForExecutorRoutesToRuntimeLogger(t *testing.T) {
	_, dsn := newSQLiteIndexTestEngine(t)
	logger := &recordingLogger{}

	rt, err := Open(context.Background(), "sqlite", dsn, nil, WithLogger(logger))
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}
	t.Cleanup(func() { _ = rt.Close() })

	logForExecutor(context.Background(), rt, slog.LevelWarn, "routed", "k", "v")

	if logger.count("routed") != 1 {
		t.Fatalf("expected runtime logger to receive the message, got %v", logger.messages)
	}

	// An executor without a runtime must not panic and must not reach the runtime logger.
	logForExecutor(context.Background(), WrapExecutor(rt.DB(), onSQLite), slog.LevelDebug, "unrouted")

	if logger.count("unrouted") != 0 {
		t.Fatal("expected bare *sql.DB executor to bypass the runtime logger")
	}
}

// TestLogSQLRoutesRenderedStatementsToRuntimeLogger is the end-to-end check on
// WithSQLLogging.
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

	rt, err := Open(context.Background(), "sqlite", dsn, nil, WithLogger(logger), WithSQLLogging())
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}

	t.Cleanup(func() { _ = rt.Close() })

	named := namedTable("users")

	query, err := Select(named.Columns()...).From(named).Build()
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

	rt, err := Open(context.Background(), "sqlite", dsn, nil, WithLogger(logger))
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}

	t.Cleanup(func() { _ = rt.Close() })

	named := namedTable("users")

	query, err := Select(named.Columns()...).From(named).Build()
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

	runtime, err := NewRuntime(context.Background(), db, tsqdialect.SQLite, nil)
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
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

	if _, err := NewRuntime(context.Background(), nil, tsqdialect.SQLite, nil); err == nil {
		t.Fatal("expected a nil pool to be rejected")
	}

	if _, err := NewRuntime(context.Background(), db, "", nil); err == nil {
		t.Fatal("expected a nil dialect to be rejected")
	}
}

// TestNewRuntimeClosesThePoolItOpened is the other half of the ownership rule.
func TestNewRuntimeClosesThePoolItOpened(t *testing.T) {
	_, dsn := newSQLiteIndexTestEngine(t)

	runtime, err := Open(context.Background(), "sqlite", dsn, nil)
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
