package tsq

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
)

type recordingLogger struct {
	mu       sync.Mutex
	messages []string
}

func (l *recordingLogger) Enabled(context.Context, slog.Level) bool {
	return true
}

func (l *recordingLogger) LogAttrs(_ context.Context, _ slog.Level, msg string, _ ...slog.Attr) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.messages = append(l.messages, msg)
}

func (l *recordingLogger) count(msg string) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	total := 0
	for _, item := range l.messages {
		if item == msg {
			total++
		}
	}

	return total
}

func TestNewRuntimeTablePolicyCreateMissingCreatesTable(t *testing.T) {
	db, dsn := newSQLiteIndexTestEngine(t)
	table, _ := newStrictMockTable("users", "id", "name")

	runtime, err := Open(context.Background(),
		"sqlite",
		dsn,
		[]Table{registered(table, []tsqdialect.ColumnSpec{
			{
				Name:          "id",
				Type:          tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64},
				PrimaryKey:    true,
				AutoIncrement: true,
			},
			{
				Name: "name",
				Type: tsqdialect.ColumnType{Kind: tsqdialect.KindString, Size: 120},
			},
		})},
		WithTablePolicy(SchemaPolicyCreateMissing))
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}

	columns, found, err := runtime.Dialect().InspectColumns(context.Background(), db, "users")
	if err != nil {
		t.Fatalf("InspectColumns() error = %v", err)
	}
	if !found {
		t.Fatal("expected users table to be created")
	}
	if len(columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(columns))
	}
}

func TestNewRuntimeTablePolicyReconcileAddsMissingColumn(t *testing.T) {
	db, dsn := newSQLiteIndexTestEngine(t)
	if _, err := db.DB().ExecContext(context.Background(), `CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT)`); err != nil {
		t.Fatalf("failed to create seed table: %v", err)
	}

	table, _ := newStrictMockTable("users", "id", "name")
	runtime, err := Open(context.Background(),
		"sqlite",
		dsn,
		[]Table{registered(table, []tsqdialect.ColumnSpec{
			{
				Name:          "id",
				Type:          tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64},
				PrimaryKey:    true,
				AutoIncrement: true,
			},
			{
				Name: "name",
				Type: tsqdialect.ColumnType{Kind: tsqdialect.KindString, Size: 120},
			},
		})},
		WithTablePolicy(SchemaPolicyReconcile))
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}

	columns, found, err := runtime.Dialect().InspectColumns(context.Background(), runtime, "users")
	if err != nil {
		t.Fatalf("InspectColumns() error = %v", err)
	}
	if !found {
		t.Fatal("expected users table to exist")
	}
	if len(columns) != 2 {
		t.Fatalf("expected reconcile to add missing column, got %d columns", len(columns))
	}
}

func TestColumnsEqualIgnoresAutoIncrementSequenceDefault(t *testing.T) {
	dialect := tsqdialect.PostgresDialect{}
	inspected := tsqdialect.ColumnSpec{
		Name:          "id",
		Type:          tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64},
		PrimaryKey:    true,
		AutoIncrement: true,
		Default:       "nextval('users_id_seq'::regclass)",
		NativeType:    "bigint",
	}
	declared := tsqdialect.ColumnSpec{
		Name:          "id",
		Type:          tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64},
		PrimaryKey:    true,
		AutoIncrement: true,
	}

	if !columnsEqual(dialect, inspected, declared) {
		t.Fatal("auto-increment sequence default must not be treated as schema drift")
	}
}

func TestNewRuntimeReconcileRawTypeTextProducesNoDDL(t *testing.T) {
	db, dsn := newSQLiteIndexTestEngine(t)
	setup := `CREATE TABLE notes (id INTEGER PRIMARY KEY AUTOINCREMENT, body TEXT)`
	if _, err := db.DB().ExecContext(context.Background(), setup); err != nil {
		t.Fatalf("failed to create seed table: %v", err)
	}

	logger := &recordingLogger{}
	table, _ := newStrictMockTable("notes", "id", "body")
	registration := registered(table, []tsqdialect.ColumnSpec{
		{
			Name:          "id",
			Type:          tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64},
			PrimaryKey:    true,
			AutoIncrement: true,
		},
		{
			Name: "body",
			Type: tsqdialect.ColumnType{RawType: "TEXT", Nullable: true},
		},
	})

	for restart := range 2 {
		_, err := Open(context.Background(),
			"sqlite",
			dsn,
			[]Table{registration},
			WithTablePolicy(SchemaPolicyReconcile), WithLogger(logger))
		if err != nil {
			t.Fatalf("NewRuntime() restart %d error = %v", restart, err)
		}
	}

	if ddl := logger.count("applied ddl"); ddl != 0 {
		t.Fatalf("expected raw TEXT column to round-trip with zero DDL, got %d statements", ddl)
	}
}

func TestNewRuntimeReconcileRebuildPreservesDataAndIndexes(t *testing.T) {
	db, dsn := newSQLiteIndexTestEngine(t)
	statements := []string{
		`CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, age INTEGER, name VARCHAR(120))`,
		`CREATE INDEX idx_users_name ON users(name)`,
		`INSERT INTO users (age, name) VALUES (30, 'amy')`,
	}
	for _, statement := range statements {
		if _, err := db.DB().ExecContext(context.Background(), statement); err != nil {
			t.Fatalf("failed to execute setup statement %q: %v", statement, err)
		}
	}

	table, _ := newStrictMockTable("users", "id", "age", "name")
	registration := registered(table, []tsqdialect.ColumnSpec{
		{
			Name:          "id",
			Type:          tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64},
			PrimaryKey:    true,
			AutoIncrement: true,
		},
		{
			// INTEGER -> VARCHAR drift forces the SQLite rebuild path.
			Name: "age",
			Type: tsqdialect.ColumnType{Kind: tsqdialect.KindString, Size: 60, Nullable: true},
		},
		{
			Name: "name",
			Type: tsqdialect.ColumnType{Kind: tsqdialect.KindString, Size: 120, Nullable: true},
		},
	})

	runtime, err := Open(context.Background(),
		"sqlite",
		dsn,
		[]Table{registration},
		WithTablePolicy(SchemaPolicyReconcile), WithIndexPolicy(SchemaPolicyManual))
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}

	columns, found, err := runtime.Dialect().InspectColumns(context.Background(), runtime, "users")
	if err != nil || !found {
		t.Fatalf("InspectColumns() found=%v error = %v", found, err)
	}

	rebuilt := false
	for _, column := range columns {
		if column.Name == "age" && column.Type.Kind == tsqdialect.KindString {
			rebuilt = true
		}
	}
	if !rebuilt {
		t.Fatalf("expected rebuild to retype age column, got %+v", columns)
	}

	var name string
	if err := runtime.QueryRowContext(context.Background(), `SELECT name FROM users WHERE age = '30'`).Scan(&name); err != nil {
		t.Fatalf("expected rebuild to preserve row data: %v", err)
	}
	if name != "amy" {
		t.Fatalf("unexpected row data after rebuild: %q", name)
	}

	if _, found := inspectRegisteredIndex(t, runtime, "users", "idx_users_name"); !found {
		t.Fatal("expected rebuild to restore pre-existing secondary index even with manual index policy")
	}

	// A second bootstrap must be a no-op: the rebuilt schema now matches.
	logger := &recordingLogger{}
	if _, err := Open(context.Background(),
		"sqlite",
		dsn,
		[]Table{registration},
		WithTablePolicy(SchemaPolicyReconcile), WithIndexPolicy(SchemaPolicyManual), WithLogger(logger)); err != nil {
		t.Fatalf("second NewRuntime() error = %v", err)
	}
	if ddl := logger.count("applied ddl"); ddl != 0 {
		t.Fatalf("expected reconcile to converge after rebuild, got %d DDL statements", ddl)
	}
}

// TestResolveRuntimeDialectAcceptsEverySQLiteDriverName covers both registered
// names: modernc.org/sqlite is "sqlite" and mattn/go-sqlite3 is "sqlite3". The
// latter used to be refused, from the days when TSQ shipped with the CGO driver
// and dropped it; the SQL is the same, and the CGO driver's error type is now
// classified too (see mattnSQLiteErrorCode).
func TestResolveRuntimeDialectAcceptsEverySQLiteDriverName(t *testing.T) {
	for _, name := range []string{"sqlite", "sqlite3", "SQLite3"} {
		d, err := resolveRuntimeDialect(name)
		if err != nil || d.Name() != tsqdialect.SQLite {
			t.Errorf("resolveRuntimeDialect(%q) = %v, %v", name, d, err)
		}
	}

	_, err := resolveRuntimeDialect("oracle")
	if err == nil || !strings.Contains(err.Error(), "expected sqlite, sqlite3, mysql") {
		t.Fatalf("unknown driver = %v", err)
	}
}
