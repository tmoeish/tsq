package tsq

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"

	tsqdialect "github.com/tmoeish/tsq/v4/dialect"
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

	runtime, err := NewRuntime(
		"sqlite",
		dsn,
		[]TableRegistration{{
			Table: table,
			Columns: []tsqdialect.DDLColumnSpec{
				{
					Name:          "id",
					Type:          tsqdialect.DDLColumnType{Kind: tsqdialect.DDLColumnKindInt, Bits: 64},
					PrimaryKey:    true,
					AutoIncrement: true,
				},
				{
					Name: "name",
					Type: tsqdialect.DDLColumnType{Kind: tsqdialect.DDLColumnKindString, Size: 120},
				},
			},
		}},
		&RuntimeOptions{TablePolicy: SchemaPolicyCreateMissing},
	)
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}

	columns, found, err := runtime.SQLDialect().InspectTableColumns(context.Background(), db, "users")
	if err != nil {
		t.Fatalf("InspectTableColumns() error = %v", err)
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
	runtime, err := NewRuntime(
		"sqlite",
		dsn,
		[]TableRegistration{{
			Table: table,
			Columns: []tsqdialect.DDLColumnSpec{
				{
					Name:          "id",
					Type:          tsqdialect.DDLColumnType{Kind: tsqdialect.DDLColumnKindInt, Bits: 64},
					PrimaryKey:    true,
					AutoIncrement: true,
				},
				{
					Name: "name",
					Type: tsqdialect.DDLColumnType{Kind: tsqdialect.DDLColumnKindString, Size: 120},
				},
			},
		}},
		&RuntimeOptions{TablePolicy: SchemaPolicyReconcile},
	)
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}

	columns, found, err := runtime.SQLDialect().InspectTableColumns(context.Background(), runtime, "users")
	if err != nil {
		t.Fatalf("InspectTableColumns() error = %v", err)
	}
	if !found {
		t.Fatal("expected users table to exist")
	}
	if len(columns) != 2 {
		t.Fatalf("expected reconcile to add missing column, got %d columns", len(columns))
	}
}

func TestNewRuntimeIndexPolicyManagedDropsUndeclaredIndexes(t *testing.T) {
	db, dsn := newSQLiteIndexTestEngine(t)
	statements := []string{
		`CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, email TEXT)`,
		`CREATE INDEX idx_users_email ON users(email)`,
	}
	for _, statement := range statements {
		if _, err := db.DB().ExecContext(context.Background(), statement); err != nil {
			t.Fatalf("failed to execute setup statement %q: %v", statement, err)
		}
	}

	table, _ := newStrictMockTable("users", "id", "name", "email")
	runtime, err := NewRuntime(
		"sqlite",
		dsn,
		[]TableRegistration{{
			Table: table,
			Indexes: []TableIndex{
				{Name: "idx_users_name", Fields: []string{"name"}},
			},
		}},
		&RuntimeOptions{IndexPolicy: SchemaPolicyManaged},
	)
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}

	_, found := inspectRegisteredIndex(t, runtime, "users", "idx_users_name")
	if !found {
		t.Fatal("expected managed policy to create declared index")
	}
	_, found = inspectRegisteredIndex(t, runtime, "users", "idx_users_email")
	if found {
		t.Fatal("expected managed policy to drop undeclared same-table index")
	}
}

func TestNewRuntimeTablePolicyManagedDropsOnlyTrackedTables(t *testing.T) {
	db, dsn := newSQLiteIndexTestEngine(t)
	if _, err := db.DB().ExecContext(context.Background(), `CREATE TABLE admins (id INTEGER PRIMARY KEY AUTOINCREMENT)`); err != nil {
		t.Fatalf("failed to create unrelated table: %v", err)
	}

	table, _ := newStrictMockTable("users", "id")
	_, err := NewRuntime(
		"sqlite",
		dsn,
		[]TableRegistration{{
			Table: table,
			Columns: []tsqdialect.DDLColumnSpec{{
				Name:          "id",
				Type:          tsqdialect.DDLColumnType{Kind: tsqdialect.DDLColumnKindInt, Bits: 64},
				PrimaryKey:    true,
				AutoIncrement: true,
			}},
		}},
		&RuntimeOptions{TablePolicy: SchemaPolicyManaged},
	)
	if err != nil {
		t.Fatalf("initial managed NewRuntime() error = %v", err)
	}

	runtime, err := NewRuntime(
		"sqlite",
		dsn,
		nil,
		&RuntimeOptions{TablePolicy: SchemaPolicyManaged},
	)
	if err != nil {
		t.Fatalf("second managed NewRuntime() error = %v", err)
	}

	if _, found, err := runtime.SQLDialect().InspectTableColumns(context.Background(), runtime, "users"); err != nil {
		t.Fatalf("InspectTableColumns(users) error = %v", err)
	} else if found {
		t.Fatal("expected tracked users table to be dropped")
	}

	if _, found, err := runtime.SQLDialect().InspectTableColumns(context.Background(), runtime, "admins"); err != nil {
		t.Fatalf("InspectTableColumns(admins) error = %v", err)
	} else if !found {
		t.Fatal("expected unrelated admins table to remain")
	}
}

func TestColumnsEqualIgnoresAutoIncrementSequenceDefault(t *testing.T) {
	dialect := tsqdialect.PostgresDialect{}
	inspected := tsqdialect.DDLColumnSpec{
		Name:          "id",
		Type:          tsqdialect.DDLColumnType{Kind: tsqdialect.DDLColumnKindInt, Bits: 64},
		PrimaryKey:    true,
		AutoIncrement: true,
		Default:       "nextval('users_id_seq'::regclass)",
		NativeType:    "bigint",
	}
	declared := tsqdialect.DDLColumnSpec{
		Name:          "id",
		Type:          tsqdialect.DDLColumnType{Kind: tsqdialect.DDLColumnKindInt, Bits: 64},
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
	registration := TableRegistration{
		Table: table,
		Columns: []tsqdialect.DDLColumnSpec{
			{
				Name:          "id",
				Type:          tsqdialect.DDLColumnType{Kind: tsqdialect.DDLColumnKindInt, Bits: 64},
				PrimaryKey:    true,
				AutoIncrement: true,
			},
			{
				Name: "body",
				Type: tsqdialect.DDLColumnType{RawType: "TEXT", Nullable: true},
			},
		},
	}

	for restart := range 2 {
		_, err := NewRuntime(
			"sqlite",
			dsn,
			[]TableRegistration{registration},
			&RuntimeOptions{TablePolicy: SchemaPolicyReconcile, Logger: logger},
		)
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
	registration := TableRegistration{
		Table: table,
		Columns: []tsqdialect.DDLColumnSpec{
			{
				Name:          "id",
				Type:          tsqdialect.DDLColumnType{Kind: tsqdialect.DDLColumnKindInt, Bits: 64},
				PrimaryKey:    true,
				AutoIncrement: true,
			},
			{
				// INTEGER -> VARCHAR drift forces the SQLite rebuild path.
				Name: "age",
				Type: tsqdialect.DDLColumnType{Kind: tsqdialect.DDLColumnKindString, Size: 60, Nullable: true},
			},
			{
				Name: "name",
				Type: tsqdialect.DDLColumnType{Kind: tsqdialect.DDLColumnKindString, Size: 120, Nullable: true},
			},
		},
	}

	runtime, err := NewRuntime(
		"sqlite",
		dsn,
		[]TableRegistration{registration},
		&RuntimeOptions{TablePolicy: SchemaPolicyReconcile, IndexPolicy: SchemaPolicyManual},
	)
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}

	columns, found, err := runtime.SQLDialect().InspectTableColumns(context.Background(), runtime, "users")
	if err != nil || !found {
		t.Fatalf("InspectTableColumns() found=%v error = %v", found, err)
	}

	rebuilt := false
	for _, column := range columns {
		if column.Name == "age" && column.Type.Kind == tsqdialect.DDLColumnKindString {
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
	if _, err := NewRuntime(
		"sqlite",
		dsn,
		[]TableRegistration{registration},
		&RuntimeOptions{TablePolicy: SchemaPolicyReconcile, IndexPolicy: SchemaPolicyManual, Logger: logger},
	); err != nil {
		t.Fatalf("second NewRuntime() error = %v", err)
	}
	if ddl := logger.count("applied ddl"); ddl != 0 {
		t.Fatalf("expected reconcile to converge after rebuild, got %d DDL statements", ddl)
	}
}

func TestResolveRuntimeDialectRejectsLegacySQLite3DriverName(t *testing.T) {
	_, err := resolveRuntimeDialect("sqlite3")
	if err == nil {
		t.Fatal("expected sqlite3 driver name to be rejected")
	}
	if !strings.Contains(err.Error(), "expected sqlite, mysql, postgres, pgx, or pq") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func auditUserColumns() []tsqdialect.DDLColumnSpec {
	return []tsqdialect.DDLColumnSpec{
		{Name: "id", Type: tsqdialect.DDLColumnType{Kind: tsqdialect.DDLColumnKindInt, Bits: 64}, PrimaryKey: true, AutoIncrement: true},
		{Name: "name", Type: tsqdialect.DDLColumnType{Kind: tsqdialect.DDLColumnKindString, Size: 120}, Default: "''"},
	}
}

func openAuditRuntime(t *testing.T, seed []string, options *RuntimeOptions) (*Runtime, error) {
	t.Helper()

	db, dsn := newSQLiteIndexTestEngine(t)
	for _, statement := range seed {
		if _, err := db.DB().ExecContext(context.Background(), statement); err != nil {
			t.Fatalf("seed %q: %v", statement, err)
		}
	}

	table, _ := newStrictMockTable("users", "id", "name")

	rt, err := NewRuntime("sqlite", dsn, []TableRegistration{{Table: table, Columns: auditUserColumns()}}, options)
	if rt != nil {
		t.Cleanup(func() { _ = rt.Close() })
	}

	return rt, err
}

// TestCreateMissingAddsAMissingColumn covers CreateMissing, which creates missing
// declared objects and used to refuse to start over a missing column instead.
func TestCreateMissingAddsAMissingColumn(t *testing.T) {
	rt, err := openAuditRuntime(t, []string{`CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT)`}, &RuntimeOptions{TablePolicy: SchemaPolicyCreateMissing})
	if err != nil {
		t.Fatalf("CreateMissing with a missing column = %v", err)
	}

	if _, err := rt.DB().ExecContext(context.Background(), `INSERT INTO users (name) VALUES ('a')`); err != nil {
		t.Fatalf("the added column = %v", err)
	}
}

// TestReconcileRebuildKeepsTheAutoincrementCounter covers the SQLite rebuild,
// which dropped the table's sqlite_sequence row: the new table counted on from
// its largest copied key and handed out the key of a deleted row again.
func TestReconcileRebuildKeepsTheAutoincrementCounter(t *testing.T) {
	rt, err := openAuditRuntime(t, []string{
		`CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name INTEGER)`,
		`INSERT INTO users (name) VALUES (1), (2), (3)`,
		`DELETE FROM users WHERE id = 3`,
	}, &RuntimeOptions{TablePolicy: SchemaPolicyReconcile})
	if err != nil {
		t.Fatal(err)
	}

	result, err := rt.DB().ExecContext(context.Background(), `INSERT INTO users (name) VALUES ('x')`)
	if err != nil {
		t.Fatal(err)
	}

	if id, err := result.LastInsertId(); err != nil || id != 4 {
		t.Fatalf("key after rebuild = %d, %v; want 4, not the deleted row's 3", id, err)
	}
}

// TestSQLiteSchemaFollowsTheEngineSpelling covers two tables SQLite considers
// the declared one: a name in another case (users as "Users"), read as missing,
// and a hand-written id INTEGER PRIMARY KEY without AUTOINCREMENT, which is the
// rowid either way and was read as drift.
func TestSQLiteSchemaFollowsTheEngineSpelling(t *testing.T) {
	for name, create := range map[string]string{
		"name in another case": `CREATE TABLE "Users" (id INTEGER PRIMARY KEY AUTOINCREMENT, name VARCHAR(120) NOT NULL DEFAULT '')`,
		"rowid key":            `CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(120) NOT NULL DEFAULT '')`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := openAuditRuntime(t, []string{create}, &RuntimeOptions{TablePolicy: SchemaPolicyValidate}); err != nil {
				t.Fatalf("Validate = %v", err)
			}
		})
	}
}
