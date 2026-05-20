package tsq

import (
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
		return RegisterTable(nil, func(db *Engine) error {
			return nil
		})
	}, expectedError: RegistrationErrorNilTable}, {name: "nil init func", fn: func() error {
		return RegisterTable(newMockTable("users"), nil)
	}, expectedError: RegistrationErrorNilInitFunc}}
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
	err1 := RegisterTable(table, func(db *Engine) error {
		return nil
	})
	if err1 != nil {
		t.Fatalf("first registration should succeed, got error: %v", err1)
	}
	err2 := RegisterTable(table, func(db *Engine) error {
		return nil
	})
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
	err := r.RegisterTable(table, func(db *Engine) error {
		return nil
	})
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
	defaultRuntime.registry = &registry{tables: map[string]*registeredTable{"users": {Table: newMockTable("users"), InitFunc: func(db *Engine) error {
		return nil
	}}, "accounts": {Table: newMockTable("accounts"), InitFunc: func(db *Engine) error {
		return nil
	}}}}
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
	if err := Init(db.DB, db.Dialect, &InitOptions{Tracers: []Tracer{tracer}}); err != nil {
		t.Fatalf("failed to init tsq: %v", err)
	}
	if err := Init(db.DB, db.Dialect, &InitOptions{Tracers: []Tracer{tracer}}); err != nil {
		t.Fatalf("failed to init tsq: %v", err)
	}
	if got := len(GetTracers()); got != 1 {
		t.Fatalf("expected Init to deduplicate tracers, got %d", got)
	}
}

func TestRuntimeKeepsRegistrationsAndTracersIsolated(t *testing.T) {
	left := NewRuntime()
	right := NewRuntime()
	left.RegisterTable(newMockTable("users"), func(db *Engine) error {
		return nil
	})
	right.RegisterTable(newMockTable("users"), func(db *Engine) error {
		return nil
	})
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

func newSQLiteIndexTestEngine(t *testing.T) *Engine {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	return &Engine{DB: db, Dialect: SQLiteDialect{}}
}

func TestCurrentDialectDetection(t *testing.T) {
	r := NewRuntime()
	if r.Dialect() != "" {
		t.Errorf("expected empty dialect before Init, got %q", r.Dialect())
	}
	db := newSQLiteIndexTestEngine(t)
	if err := r.Init(db.DB, db.Dialect); err != nil {
		t.Fatalf("failed to init runtime: %v", err)
	}
	dialect := r.Dialect()
	if dialect == "" {
		t.Errorf("expected non-empty dialect after Init with SQLite")
	}
	if dialect != DialectSQLite {
		t.Logf("detected dialect: %s", dialect)
	}
}

func registerIndexRuntime(t *testing.T, tableName string, unique bool, indexName string, fields []string) *Runtime {
	t.Helper()
	runtime := NewRuntime()
	if err := runtime.RegisterTable(newMockTable(tableName), func(db *Engine) error {
		return UpsertIndex(db, tableName, unique, indexName, fields)
	}); err != nil {
		t.Fatalf("failed to register test table: %v", err)
	}
	return runtime
}

func TestRuntimeEngineAccess(t *testing.T) {
	r := NewRuntime()
	if r.Engine() != nil {
		t.Errorf("expected nil engine before Init, got non-nil")
	}
	db := newSQLiteIndexTestEngine(t)
	if err := r.Init(db.DB, db.Dialect); err != nil {
		t.Fatalf("failed to init runtime: %v", err)
	}
	currentDB := r.Engine()
	if currentDB == nil {
		t.Errorf("expected non-nil engine after Init, got nil")
	}
	if currentDB.DB != db.DB || currentDB.Dialect != db.Dialect {
		t.Errorf("expected Engine to return same underlying DB and dialect")
	}
}

func TestInitFailureRestoresPreviousRuntimeStateAfterStrictValidation(t *testing.T) {
	r := NewRuntime()
	previousDB := newSQLiteIndexTestEngine(t)
	previousEvents := make([]SchemaEvent, 0, 1)
	previousHandler := func(event SchemaEvent) {
		previousEvents = append(previousEvents, event)
	}
	if err := r.Init(previousDB.DB, previousDB.Dialect, &InitOptions{IndexMode: IndexInitValidate, SchemaEventHandler: previousHandler}); err != nil {
		t.Fatalf("failed to initialize previous runtime state: %v", err)
	}
	expectedEngine := r.Engine()
	failingDB := newSQLiteIndexTestEngine(t)
	failingDB.Dialect = MySQLDialect{}
	failingEvents := make([]SchemaEvent, 0, 1)
	failingHandler := func(event SchemaEvent) {
		failingEvents = append(failingEvents, event)
	}
	longTableName := strings.Repeat("u", maxIdentifierLengthMySQL+1)
	if err := r.RegisterTable(newMockTable(longTableName), func(db *Engine) error {
		return nil
	}); err != nil {
		t.Fatalf("failed to register invalid table: %v", err)
	}
	err := r.Init(failingDB.DB, failingDB.Dialect, &InitOptions{IndexMode: IndexInitSkip, IdentifierValidationMode: "strict", SchemaEventHandler: failingHandler})
	if err == nil {
		t.Fatal("expected strict identifier validation to fail")
	}
	if r.Engine() != expectedEngine {
		t.Fatal("expected runtime engine to be restored after failed init")
	}
	if got := loadDBSchemaConfig(expectedEngine).indexInitMode; got != IndexInitValidate {
		t.Fatalf("expected previous db index mode %q after rollback, got %q", IndexInitValidate, got)
	}
	if err := expectedEngine.emitSchemaEvent(SchemaEvent{Kind: SchemaEventValidateIndex, Table: "users", Name: "ux_users_name"}); err != nil {
		t.Fatalf("expected previous db handler to remain active after rollback, got %v", err)
	}
	if len(previousEvents) != 1 {
		t.Fatalf("expected previous db handler to receive one event after rollback, got %d", len(previousEvents))
	}
	if got := loadDBSchemaConfig(failingDB).indexInitMode; got != IndexInitUpsert {
		t.Fatalf("expected failing db index mode to rollback to default %q, got %q", IndexInitUpsert, got)
	}
	if err := failingDB.emitSchemaEvent(SchemaEvent{Kind: SchemaEventValidateIndex, Table: "users", Name: "ux_users_name"}); err != nil {
		t.Fatalf("expected failing db to have no persisted handler after rollback, got %v", err)
	}
	if len(failingEvents) != 0 {
		t.Fatalf("expected failing db handler to be rolled back, got %d events", len(failingEvents))
	}
}

func TestInitFailureRestoresPreviousRuntimeStateAfterInitFuncError(t *testing.T) {
	r := NewRuntime()
	previousDB := newSQLiteIndexTestEngine(t)
	previousEvents := make([]SchemaEvent, 0, 1)
	previousHandler := func(event SchemaEvent) {
		previousEvents = append(previousEvents, event)
	}
	if err := r.Init(previousDB.DB, previousDB.Dialect, &InitOptions{IndexMode: IndexInitValidate, SchemaEventHandler: previousHandler}); err != nil {
		t.Fatalf("failed to initialize previous runtime state: %v", err)
	}
	expectedEngine := r.Engine()
	failingDB := newSQLiteIndexTestEngine(t)
	failingEvents := make([]SchemaEvent, 0, 1)
	failingHandler := func(event SchemaEvent) {
		failingEvents = append(failingEvents, event)
	}
	if err := r.RegisterTable(newMockTable("users"), func(db *Engine) error {
		return errors.New("boom")
	}); err != nil {
		t.Fatalf("failed to register failing table: %v", err)
	}
	err := r.Init(failingDB.DB, failingDB.Dialect, &InitOptions{IndexMode: IndexInitValidate, SchemaEventHandler: failingHandler})
	if err == nil {
		t.Fatal("expected init func failure to fail init")
	}
	if r.Engine() != expectedEngine {
		t.Fatal("expected runtime engine to be restored after init func failure")
	}
	if got := loadDBSchemaConfig(expectedEngine).indexInitMode; got != IndexInitValidate {
		t.Fatalf("expected previous db index mode %q after rollback, got %q", IndexInitValidate, got)
	}
	if err := expectedEngine.emitSchemaEvent(SchemaEvent{Kind: SchemaEventValidateIndex, Table: "users", Name: "ux_users_name"}); err != nil {
		t.Fatalf("expected previous db handler to remain active after rollback, got %v", err)
	}
	if len(previousEvents) != 1 {
		t.Fatalf("expected previous db handler to receive one event after rollback, got %d", len(previousEvents))
	}
	if got := loadDBSchemaConfig(failingDB).indexInitMode; got != IndexInitUpsert {
		t.Fatalf("expected failing db index mode to rollback to default %q, got %q", IndexInitUpsert, got)
	}
	if err := failingDB.emitSchemaEvent(SchemaEvent{Kind: SchemaEventValidateIndex, Table: "users", Name: "ux_users_name"}); err != nil {
		t.Fatalf("expected failing db to have no persisted handler after rollback, got %v", err)
	}
	if len(failingEvents) != 0 {
		t.Fatalf("expected failing db handler to be rolled back, got %d events", len(failingEvents))
	}
}
