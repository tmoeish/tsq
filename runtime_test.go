package tsq

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"

	tsqdialect "github.com/tmoeish/tsq/v4/dialect"
)

func TestResolveIdentifierValidationModeDefaultsToStrict(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mode    IdentifierValidationMode
		want    IdentifierValidationMode
		wantErr bool
	}{
		{name: "empty defaults to strict", mode: "", want: IdentifierValidationStrict},
		{name: "strict", mode: IdentifierValidationStrict, want: IdentifierValidationStrict},
		{name: "warn", mode: IdentifierValidationWarn, want: IdentifierValidationWarn},
		{name: "skip", mode: IdentifierValidationSkip, want: IdentifierValidationSkip},
		{name: "unknown rejected", mode: "lenient", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := resolveIdentifierValidationMode(tt.mode)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error for mode %q", tt.mode)
				}

				return
			}

			if err != nil {
				t.Fatalf("resolveIdentifierValidationMode(%q) error = %v", tt.mode, err)
			}

			if got != tt.want {
				t.Fatalf("resolveIdentifierValidationMode(%q) = %q, want %q", tt.mode, got, tt.want)
			}
		})
	}
}

func TestNewRuntimeRejectsUnknownIdentifierValidationMode(t *testing.T) {
	_, dsn := newSQLiteIndexTestEngine(t)

	_, err := NewRuntime("sqlite", dsn, nil, &RuntimeOptions{IdentifierValidationMode: "lenient"})
	if err == nil || !strings.Contains(err.Error(), "identifier validation mode") {
		t.Fatalf("expected unknown identifier validation mode to be rejected, got %v", err)
	}
}

func TestValidateRegisteredTableIdentifiersWarnModeReportsViolations(t *testing.T) {
	longTableName := firstRejectedIdentifier(t, tsqdialect.MySQLDialect{}, "u")
	runtime := &Runtime{
		db:      &sql.DB{},
		dialect: tsqdialect.MySQLDialect{},
		tables: []*registeredTable{{
			Table: newMockTable(longTableName),
		}},
	}

	// Warn mode must surface the violations to the caller (which logs them);
	// an empty error here is exactly the silent-swallow bug this guards against.
	err := runtime.validateRegisteredTableIdentifiers(IdentifierValidationWarn)
	if err == nil || !strings.Contains(err.Error(), longTableName) {
		t.Fatalf("expected warn mode to report the oversized identifier, got %v", err)
	}
}

func TestNewRuntimeContextHonorsCancellation(t *testing.T) {
	_, dsn := newSQLiteIndexTestEngine(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := NewRuntimeContext(ctx, "sqlite", dsn, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancelled context to abort bootstrap, got %v", err)
	}
}

func TestNewRuntimeContextRejectsNilContext(t *testing.T) {
	_, dsn := newSQLiteIndexTestEngine(t)

	//nolint:staticcheck // passing nil on purpose to exercise the guard
	if _, err := NewRuntimeContext(nil, "sqlite", dsn, nil); err == nil {
		t.Fatal("expected nil context to be rejected")
	}
}

func TestRuntimeCloseReleasesDBAndIsNilSafe(t *testing.T) {
	_, dsn := newSQLiteIndexTestEngine(t)

	rt, err := NewRuntime("sqlite", dsn, nil)
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

	rt, err := NewRuntime("sqlite", dsn, nil)
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

	custom, err := NewRuntime("sqlite", dsn, nil, &RuntimeOptions{MaxPageSize: 5000})
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

	if _, err := NewRuntime("sqlite", dsn, nil, &RuntimeOptions{MaxPageSize: -1}); err == nil {
		t.Fatal("expected negative max page size to be rejected")
	}
}

func TestLogForExecutorRoutesToRuntimeLogger(t *testing.T) {
	_, dsn := newSQLiteIndexTestEngine(t)
	logger := &recordingLogger{}

	rt, err := NewRuntime("sqlite", dsn, nil, &RuntimeOptions{Logger: logger})
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

	rt, err := NewRuntime("sqlite", dsn, nil, &RuntimeOptions{Logger: logger, LogSQL: true})
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

	rt, err := NewRuntime("sqlite", dsn, nil, &RuntimeOptions{Logger: logger})
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
