package tsq

import (
	"context"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
	sqld "github.com/tmoeish/tsq/v5/internal/sqldialect"
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

	columns, found, err := runtime.dialect.InspectColumns(context.Background(), db, "users")
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

	columns, found, err := runtime.dialect.InspectColumns(context.Background(), runtime, "users")
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
	dialect := sqld.PostgresDialect{}
	inspected := sqld.Column{
		Name:          "id",
		Type:          tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64},
		PrimaryKey:    true,
		AutoIncrement: true,
		Default:       "nextval('users_id_seq'::regclass)", NativeType: "bigint",
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

	columns, found, err := runtime.dialect.InspectColumns(context.Background(), runtime, "users")
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

// retypedUsers declares users(id, age, name) with age as a string, so a live table
// with an INTEGER age takes the SQLite rebuild path.
func retypedUsers() Table {
	table, _ := newStrictMockTable("users", "id", "age", "name")

	return registered(table, []tsqdialect.ColumnSpec{
		{Name: "id", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64}, PrimaryKey: true, AutoIncrement: true},
		{Name: "age", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindString, Size: 60, Nullable: true}},
		{Name: "name", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindString, Size: 120, Nullable: true}},
	})
}

// schemaObjects lists the statements SQLite keeps for the table's indexes and
// triggers.
func schemaObjects(t *testing.T, db Executor, table string) map[string]string {
	t.Helper()

	rows, err := db.QueryContext(context.Background(),
		`SELECT name, sql FROM sqlite_master WHERE tbl_name = ? AND type IN ('index', 'trigger') AND sql IS NOT NULL`, table)
	if err != nil {
		t.Fatal(err)
	}

	defer func() { _ = rows.Close() }()

	objects := map[string]string{}

	for rows.Next() {
		var name, statement string
		if err := rows.Scan(&name, &statement); err != nil {
			t.Fatal(err)
		}

		objects[name] = statement
	}

	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	return objects
}

// TestReconcileRebuildKeepsIndexesAndTriggersAsCreated covers what the rebuild
// recreates. It used to rebuild each index from its name and plain columns, which
// dropped an expression index, turned (age, lower(name)) into (age), lost a
// partial index's WHERE and every trigger. Listing an expression index also
// failed outright: SQLite reports its expression with a NULL column name.
func TestReconcileRebuildKeepsIndexesAndTriggersAsCreated(t *testing.T) {
	ctx := context.Background()
	db, dsn := newSQLiteIndexTestEngine(t)

	for _, statement := range []string{
		`CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, age INTEGER, name VARCHAR(120))`,
		`CREATE INDEX idx_users_lower_name ON users(lower(name))`,
		`CREATE INDEX idx_users_age_name ON users(age, lower(name))`,
		`CREATE UNIQUE INDEX ux_users_name_adult ON users(name) WHERE age >= 18`,
		`CREATE TRIGGER trg_users_touch AFTER UPDATE ON users BEGIN SELECT 1; END`,
		`INSERT INTO users (age, name) VALUES (30, 'amy')`,
	} {
		if _, err := db.DB().ExecContext(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}

	before := schemaObjects(t, db, "users")

	// Listing the indexes must cope with the expression column.
	if _, err := Open(ctx, "sqlite", dsn, []Table{retypedUsers()},
		WithTablePolicy(SchemaPolicyManual), WithIndexPolicy(SchemaPolicyCreateMissing)); err != nil {
		t.Fatalf("Open with an expression index = %v", err)
	}

	rt, err := Open(ctx, "sqlite", dsn, []Table{retypedUsers()},
		WithTablePolicy(SchemaPolicyReconcile), WithIndexPolicy(SchemaPolicyManual))
	if err != nil {
		t.Fatalf("rebuild = %v", err)
	}

	t.Cleanup(func() { _ = rt.Close() })

	columns, _, err := rt.dialect.InspectColumns(ctx, rt, "users")
	if err != nil || !slices.ContainsFunc(columns, func(c sqld.Column) bool { return c.Name == "age" && c.Type.Kind == tsqdialect.KindString }) {
		t.Fatalf("columns = %+v, %v; want age rebuilt as a string", columns, err)
	}

	if after := schemaObjects(t, rt, "users"); !maps.Equal(before, after) {
		t.Fatalf("objects after the rebuild = %v\nwant them as created: %v", after, before)
	}
}

// TestReconcileRefusesARebuildThatWouldLoseSomething covers what the rebuild
// cannot carry over. The new table is created from the declared columns, so a
// constraint in the old CREATE TABLE, or anything elsewhere that refers to the
// table, would be lost or left dangling. The table must be untouched.
func TestReconcileRefusesARebuildThatWouldLoseSomething(t *testing.T) {
	for name, tt := range map[string]struct {
		setup []string
		want  string
	}{
		"unique constraint": {
			setup: []string{`CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, age INTEGER, name VARCHAR(120) UNIQUE)`},
			want:  "a UNIQUE constraint on (name)",
		},
		"check constraint": {
			setup: []string{`CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, age INTEGER CHECK (age > 0), name VARCHAR(120))`},
			want:  "a CHECK constraint",
		},
		"foreign key of the table": {
			setup: []string{
				`CREATE TABLE teams (id INTEGER PRIMARY KEY)`,
				`CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, age INTEGER, name VARCHAR(120), team_id INTEGER REFERENCES teams(id))`,
			},
			want: "its foreign key to teams",
		},
		"foreign key to the table": {
			setup: []string{
				`CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, age INTEGER, name VARCHAR(120))`,
				`CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER REFERENCES users(id))`,
			},
			want: "the foreign key of posts that references it",
		},
		"view": {
			setup: []string{
				`CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, age INTEGER, name VARCHAR(120))`,
				`CREATE VIEW adults AS SELECT name FROM users WHERE age >= 18`,
			},
			want: "the view adults, which refers to it",
		},
		"trigger of another table": {
			setup: []string{
				`CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, age INTEGER, name VARCHAR(120))`,
				`CREATE TABLE audit (id INTEGER PRIMARY KEY)`,
				`CREATE TRIGGER trg_audit AFTER INSERT ON audit BEGIN DELETE FROM users WHERE id = NEW.id; END`,
			},
			want: "the trigger trg_audit, which refers to it",
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			db, dsn := newSQLiteIndexTestEngine(t)

			for _, statement := range tt.setup {
				if _, err := db.DB().ExecContext(ctx, statement); err != nil {
					t.Fatalf("%s: %v", statement, err)
				}
			}

			_, err := Open(ctx, "sqlite", dsn, []Table{retypedUsers()},
				WithTablePolicy(SchemaPolicyReconcile), WithIndexPolicy(SchemaPolicyManual))
			if err == nil || !strings.Contains(err.Error(), tt.want) || !strings.Contains(err.Error(), "migration") {
				t.Fatalf("Open = %v; want the rebuild refused naming %q", err, tt.want)
			}

			columns, _, err := db.dialect.InspectColumns(ctx, db, "users")
			if err != nil || !slices.ContainsFunc(columns, func(c sqld.Column) bool { return c.Name == "age" && c.Type.Kind == tsqdialect.KindInt }) {
				t.Fatalf("columns = %+v, %v; want the table untouched", columns, err)
			}
		})
	}
}

// TestReconcileDropsUndeclaredColumns pins a decision: Reconcile makes the table
// match the declaration, which includes dropping a column the struct no longer has,
// with its data. It is the prototype setting, where the database follows the code;
// production keeps Manual. The other policies leave the column and refuse to
// start. No policy drops a table (runtime_schema_isolation_test.go).
func TestReconcileDropsUndeclaredColumns(t *testing.T) {
	ctx := context.Background()
	db, dsn := newSQLiteIndexTestEngine(t)

	for _, statement := range []string{
		`CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, age INTEGER, name VARCHAR(120), legacy TEXT)`,
		`INSERT INTO users (age, name, legacy) VALUES (30, 'amy', 'old')`,
	} {
		if _, err := db.DB().ExecContext(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}

	table, _ := newStrictMockTable("users", "id", "age", "name")
	declared := registered(table, []tsqdialect.ColumnSpec{
		{Name: "id", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64}, PrimaryKey: true, AutoIncrement: true},
		{Name: "age", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64, Nullable: true}},
		{Name: "name", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindString, Size: 120, Nullable: true}},
	})

	hasLegacy := func() bool {
		t.Helper()

		columns, _, err := db.dialect.InspectColumns(ctx, db, "users")
		if err != nil {
			t.Fatal(err)
		}

		return slices.ContainsFunc(columns, func(c sqld.Column) bool { return c.Name == "legacy" })
	}

	for _, policy := range []SchemaPolicy{SchemaPolicyValidate, SchemaPolicyCreateMissing} {
		if _, err := Open(ctx, "sqlite", dsn, []Table{declared}, WithTablePolicy(policy), WithIndexPolicy(SchemaPolicyManual)); err == nil || !hasLegacy() {
			t.Fatalf("%s = %v; want a refusal that leaves the column", policy, err)
		}
	}

	rt, err := Open(ctx, "sqlite", dsn, []Table{declared}, WithTablePolicy(SchemaPolicyReconcile), WithIndexPolicy(SchemaPolicyManual))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = rt.Close() })

	if hasLegacy() {
		t.Fatal("Reconcile kept a column the table no longer declares")
	}

	var name string
	if err := rt.QueryRowContext(ctx, `SELECT name FROM users WHERE age = 30`).Scan(&name); err != nil || name != "amy" {
		t.Fatalf("declared columns after the drop: %q, %v", name, err)
	}
}

// TestFailedDDLIsNotLoggedAsApplied covers the "applied ddl" record, which was
// written before the statement ran, so a failed ALTER was reported as applied.
func TestFailedDDLIsNotLoggedAsApplied(t *testing.T) {
	ctx := context.Background()
	db, dsn := newSQLiteIndexTestEngine(t)

	for _, statement := range []string{
		`CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name VARCHAR(120))`,
		`INSERT INTO users (name) VALUES ('amy')`,
	} {
		if _, err := db.DB().ExecContext(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}

	// A NOT NULL column with no default cannot be added to a table with rows.
	table, _ := newStrictMockTable("users", "id", "name", "age")
	declared := registered(table, []tsqdialect.ColumnSpec{
		{Name: "id", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64}, PrimaryKey: true, AutoIncrement: true},
		{Name: "name", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindString, Size: 120, Nullable: true}},
		{Name: "age", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64}},
	})

	logger := &recordingLogger{}
	if _, err := Open(ctx, "sqlite", dsn, []Table{declared},
		WithTablePolicy(SchemaPolicyReconcile), WithIndexPolicy(SchemaPolicyManual), WithLogger(logger)); err == nil {
		t.Fatal("expected the ALTER to fail")
	}

	if applied := logger.count("applied ddl"); applied != 0 {
		t.Fatalf("%d failed statements were logged as applied", applied)
	}
}

// TestColumnDefaultsCompareByMeaning covers the default comparison of schema
// reconcile. It cut a value at its first "::", so PostgreSQL's 'a::b'::text never
// matched a declared 'a::b' and every boot set the default again, and it lowered
// everything, so 'Active' matched 'active' and a changed default was never seen.
func TestColumnDefaultsCompareByMeaning(t *testing.T) {
	for _, tt := range []struct {
		left, right string
		same        bool
	}{
		{"'USD'", "'USD'::character varying", true},
		{"'a::b'", "'a::b'::text", true},
		{"'it''s'", "'it''s'::text", true},
		{"CURRENT_TIMESTAMP", "current_timestamp", true},
		{"'USD'", "USD", true}, // MySQL reads a string default back unquoted
		{"'Active'", "'active'::text", false},
		{"'a'", "'b'", false},
	} {
		if got := sameDefault(tt.left, tt.right); got != tt.same {
			t.Errorf("sameDefault(%q, %q) = %v, want %v", tt.left, tt.right, got, tt.same)
		}
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
