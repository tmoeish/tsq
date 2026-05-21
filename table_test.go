package tsq

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestRegisterTableRejectsNilInputs(t *testing.T) {
	oldRuntime := defaultRuntime
	defaultRuntime = NewRuntime()
	t.Cleanup(func() {
		defaultRuntime = oldRuntime
	})
	tests := []struct {
		name          string
		fn            func() error
		expectedError RegistrationErrorType
	}{{name: "nil table", fn: func() error {
		return RegisterTable(nil)
	}, expectedError: RegistrationErrorNilTable}, {name: "invalid index metadata", fn: func() error {
		return RegisterTable(newMockTable("users"), TableIndex{Name: "idx_users_missing", Fields: []string{"missing"}})
	}, expectedError: RegistrationErrorInvalidIndex}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.fn()
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			var regErr *RegistrationError
			if !errors.As(err, &regErr) {
				t.Fatalf("expected RegistrationError, got %T", err)
			}
			if regErr.Type != tt.expectedError {
				t.Errorf("expected error type %v, got %v", tt.expectedError, regErr.Type)
			}
		})
	}
}

func TestRegisterTableRejectsDuplicate(t *testing.T) {
	oldRuntime := defaultRuntime
	defaultRuntime = NewRuntime()
	t.Cleanup(func() {
		defaultRuntime = oldRuntime
	})
	table := newMockTable("users")
	err1 := RegisterTable(table)
	if err1 != nil {
		t.Fatalf("first registration should succeed, got error: %v", err1)
	}
	err2 := RegisterTable(table)
	if err2 == nil {
		t.Fatal("expected duplicate table registration to fail")
	}
	var regErr *RegistrationError
	if !errors.As(err2, &regErr) {
		t.Fatalf("expected RegistrationError, got %T", err2)
	}
	if regErr.Type != RegistrationErrorDuplicate {
		t.Errorf("expected error type %v, got %v", RegistrationErrorDuplicate, regErr.Type)
	}
}

func TestRuntimeRegisterTableRejectsNilRuntime(t *testing.T) {
	var r *Runtime
	table := newMockTable("users")
	err := r.RegisterTable(table)
	if err == nil {
		t.Fatal("expected nil runtime to return error")
	}
	var regErr *RegistrationError
	if !errors.As(err, &regErr) {
		t.Fatalf("expected RegistrationError, got %T", err)
	}
	if regErr.Type != RegistrationErrorNilRuntime {
		t.Errorf("expected error type %v, got %v", RegistrationErrorNilRuntime, regErr.Type)
	}
}

func TestSnapshotRegisteredTablesReturnsDeterministicOrder(t *testing.T) {
	oldRuntime := defaultRuntime
	defaultRuntime = NewRuntime()
	defaultRuntime.registry = &registry{tables: map[string]*registeredTable{"users": {Table: newMockTable("users")}, "accounts": {Table: newMockTable("accounts")}}}
	t.Cleanup(func() {
		defaultRuntime = oldRuntime
	})
	snapshot := snapshotRegisteredTables()
	if len(snapshot) != 2 {
		t.Fatalf("expected 2 registered tables, got %d", len(snapshot))
	}
	if got := snapshot[0].Table.Table(); got != "accounts" {
		t.Fatalf("expected deterministic alphabetical order, got first table %q", got)
	}
	if got := snapshot[1].Table.Table(); got != "users" {
		t.Fatalf("expected deterministic alphabetical order, got second table %q", got)
	}
}

func TestInitDeduplicatesProvidedTracers(t *testing.T) {
	oldRuntime := defaultRuntime
	defaultRuntime = NewRuntime()
	t.Cleanup(func() {
		defaultRuntime = oldRuntime
	})
	tracer := func(next TraceFn) TraceFn {
		return next
	}
	db := newSQLiteIndexTestEngine(t)
	if err := Init(db.DB(), db.SQLDialect(), &InitOptions{Tracers: []Tracer{tracer}}); err != nil {
		t.Fatalf("failed to init tsq: %v", err)
	}
	if err := Init(db.DB(), db.SQLDialect(), &InitOptions{Tracers: []Tracer{tracer}}); err != nil {
		t.Fatalf("failed to init tsq: %v", err)
	}
	if got := len(GetTracers()); got != 1 {
		t.Fatalf("expected Init to deduplicate tracers, got %d", got)
	}
}

func TestRuntimeKeepsRegistrationsAndTracersIsolated(t *testing.T) {
	left := NewRuntime()
	right := NewRuntime()
	left.RegisterTable(newMockTable("users"))
	right.RegisterTable(newMockTable("users"))
	left.AddTracer(func(next TraceFn) TraceFn {
		return next
	})
	if got := len(left.snapshotRegisteredTables()); got != 1 {
		t.Fatalf("expected left runtime registration count 1, got %d", got)
	}
	if got := len(right.snapshotRegisteredTables()); got != 1 {
		t.Fatalf("expected right runtime registration count 1, got %d", got)
	}
	if got := len(left.GetTracers()); got != 1 {
		t.Fatalf("expected left runtime tracer count 1, got %d", got)
	}
	if got := len(right.GetTracers()); got != 0 {
		t.Fatalf("expected right runtime tracer count 0, got %d", got)
	}
}

func newSQLiteIndexTestEngine(t *testing.T) *Runtime {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	return newRuntimeWithDB(db, SQLiteDialect{})
}

func TestCurrentDialectDetection(t *testing.T) {
	r := NewRuntime()
	if r.SQLDialect() != nil {
		t.Errorf("expected nil dialect before Init, got %v", r.SQLDialect())
	}
	db := newSQLiteIndexTestEngine(t)
	if err := r.Init(db.DB(), db.SQLDialect()); err != nil {
		t.Fatalf("failed to init runtime: %v", err)
	}
	dialect := r.SQLDialect()
	if dialect == nil {
		t.Errorf("expected non-nil dialect after Init with SQLite")
	}
	if dialect != nil && dialect.Name() != DialectSQLite {
		t.Logf("detected dialect: %s", dialect.Name())
	}
}

func registerIndexRuntime(t *testing.T, tableName string, unique bool, indexName string, fields []string) *Runtime {
	t.Helper()
	runtime := NewRuntime()
	table, _ := newStrictMockTable(tableName, fields...)
	if err := runtime.RegisterTable(table, TableIndex{
		Name:   indexName,
		Fields: fields,
		Unique: unique,
	}); err != nil {
		t.Fatalf("failed to register test table: %v", err)
	}
	return runtime
}

func TestRuntimeEngineAccess(t *testing.T) {
	r := NewRuntime()
	if r.DB() != nil {
		t.Errorf("expected nil DB before Init, got non-nil")
	}
	db := newSQLiteIndexTestEngine(t)
	if err := r.Init(db.DB(), db.SQLDialect()); err != nil {
		t.Fatalf("failed to init runtime: %v", err)
	}
	currentDB := r.DB()
	if currentDB == nil {
		t.Errorf("expected non-nil DB after Init, got nil")
	}
	if currentDB != db.DB() || r.SQLDialect() != db.SQLDialect() {
		t.Errorf("expected runtime to return same underlying DB and dialect")
	}
}

func TestInitFailureRestoresPreviousRuntimeStateAfterStrictValidation(t *testing.T) {
	r := NewRuntime()
	previousDB := newSQLiteIndexTestEngine(t)
	if err := r.Init(previousDB.DB(), previousDB.SQLDialect(), &InitOptions{IndexMode: IndexInitValidate}); err != nil {
		t.Fatalf("failed to initialize previous runtime state: %v", err)
	}
	expectedEngine := r.engine
	failingDB := newSQLiteIndexTestEngine(t)
	failingDB.engine.dialect = MySQLDialect{}
	longTableName := strings.Repeat("u", maxIdentifierLengthMySQL+1)
	if err := r.RegisterTable(newMockTable(longTableName)); err != nil {
		t.Fatalf("failed to register invalid table: %v", err)
	}
	err := r.Init(failingDB.DB(), failingDB.SQLDialect(), &InitOptions{IndexMode: IndexInitSkip, IdentifierValidationMode: "strict"})
	if err == nil {
		t.Fatal("expected strict identifier validation to fail")
	}
	if r.engine != expectedEngine {
		t.Fatal("expected runtime engine to be restored after failed init")
	}
	if got := expectedEngine.effectiveIndexInitMode(); got != IndexInitValidate {
		t.Fatalf("expected previous db index mode %q after rollback, got %q", IndexInitValidate, got)
	}
}

func TestInitFailureRestoresPreviousRuntimeStateAfterIndexInitError(t *testing.T) {
	r := NewRuntime()
	previousDB := newSQLiteIndexTestEngine(t)
	if err := r.Init(previousDB.DB(), previousDB.SQLDialect(), &InitOptions{IndexMode: IndexInitValidate}); err != nil {
		t.Fatalf("failed to initialize previous runtime state: %v", err)
	}
	expectedEngine := r.engine
	failingDB := newSQLiteIndexTestEngine(t)
	if _, err := failingDB.DB().ExecContext(context.Background(), "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("failed to create users table: %v", err)
	}
	table, _ := newStrictMockTable("users", "name")
	if err := r.RegisterTable(table, TableIndex{
		Name:   "ux_users_name",
		Fields: []string{"name"},
		Unique: true,
	}); err != nil {
		t.Fatalf("failed to register failing table: %v", err)
	}
	err := r.Init(failingDB.DB(), failingDB.SQLDialect(), &InitOptions{IndexMode: IndexInitValidate})
	if err == nil {
		t.Fatal("expected missing registered index to fail init")
	}
	if r.engine != expectedEngine {
		t.Fatal("expected runtime engine to be restored after index init failure")
	}
	if got := expectedEngine.effectiveIndexInitMode(); got != IndexInitValidate {
		t.Fatalf("expected previous db index mode %q after rollback, got %q", IndexInitValidate, got)
	}
}
