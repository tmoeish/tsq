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
	}{
		{
			name: "nil table",
			fn: func() error {
				return RegisterTable(nil, func(db *Engine) error { return nil })
			},
			expectedError: RegistrationErrorNilTable,
		},
		{
			name: "nil init func",
			fn: func() error {
				return RegisterTable(newMockTable("users"), nil)
			},
			expectedError: RegistrationErrorNilInitFunc,
		},
	}

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
	err1 := RegisterTable(table, func(db *Engine) error { return nil })
	if err1 != nil {
		t.Fatalf("first registration should succeed, got error: %v", err1)
	}

	err2 := RegisterTable(table, func(db *Engine) error { return nil })
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
	var r *Runtime // nil runtime
	table := newMockTable("users")

	err := r.RegisterTable(table, func(db *Engine) error { return nil })
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
	defaultRuntime.registry = &registry{tables: map[string]*registeredTable{
		"users": {
			Table:    newMockTable("users"),
			InitFunc: func(db *Engine) error { return nil },
		},
		"accounts": {
			Table:    newMockTable("accounts"),
			InitFunc: func(db *Engine) error { return nil },
		},
	}}

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

	tracer := func(next TraceFn) TraceFn { return next }

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

	left.RegisterTable(newMockTable("users"), func(db *Engine) error { return nil })
	right.RegisterTable(newMockTable("users"), func(db *Engine) error { return nil })

	left.AddTracer(func(next TraceFn) TraceFn { return next })

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

func TestUpsertIndexRejectsInvalidIdentifiers(t *testing.T) {
	db := &Engine{Dialect: MySQLDialect{}}

	err := UpsertIndex(db, "users;drop", false, "idx_users_id", []string{"id"})
	if err == nil {
		t.Fatal("expected invalid table name to return an error")
	}

	err = UpsertIndex(db, "users", false, "idx users id", []string{"id"})
	if err == nil {
		t.Fatal("expected invalid index name to return an error")
	}

	err = UpsertIndex(db, "users", false, "idx_users_id", []string{"id", "name desc"})
	if err == nil {
		t.Fatal("expected invalid field name to return an error")
	}
}

func TestUpsertIndexRejectsEmptyFields(t *testing.T) {
	db := &Engine{Dialect: MySQLDialect{}}

	err := UpsertIndex(db, "users", false, "idx_users_id", nil)
	if err == nil {
		t.Fatal("expected empty index fields to return an error")
	}
}

func TestUpsertIndexRejectsNilEngine(t *testing.T) {
	err := UpsertIndex(nil, "users", false, "idx_users_id", []string{"id"})
	if err == nil {
		t.Fatal("expected nil engine to return an error")
	}
}

func TestInitRejectsNilEngine(t *testing.T) {
	if err := Init(nil, nil); err == nil {
		t.Fatal("expected nil engine to return an error")
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

func TestUpsertIndexSQLiteRejectsConflictingTableReuse(t *testing.T) {
	db := newSQLiteIndexTestEngine(t)

	statements := []string{
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
		"CREATE TABLE orgs (id INTEGER PRIMARY KEY, name TEXT)",
		"CREATE UNIQUE INDEX ux_name ON users(name)",
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(context.Background(), statement); err != nil {
			t.Fatalf("failed to execute setup statement %q: %v", statement, err)
		}
	}

	err := UpsertIndex(db, "orgs", true, "ux_name", []string{"name"})
	if err == nil {
		t.Fatal("expected conflicting sqlite index name to return an error")
	}
}

func TestUpsertIndexSQLiteRejectsDefinitionMismatch(t *testing.T) {
	db := newSQLiteIndexTestEngine(t)

	statements := []string{
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)",
		"CREATE UNIQUE INDEX ux_users_name ON users(email)",
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(context.Background(), statement); err != nil {
			t.Fatalf("failed to execute setup statement %q: %v", statement, err)
		}
	}

	err := UpsertIndex(db, "users", true, "ux_users_name", []string{"name"})
	if err == nil {
		t.Fatal("expected mismatched sqlite index definition to return an error")
	}
}

func TestUpsertIndexSQLiteAcceptsMatchingDefinition(t *testing.T) {
	db := newSQLiteIndexTestEngine(t)

	statements := []string{
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
		"CREATE UNIQUE INDEX ux_users_name ON users(name)",
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(context.Background(), statement); err != nil {
			t.Fatalf("failed to execute setup statement %q: %v", statement, err)
		}
	}

	if err := UpsertIndex(db, "users", true, "ux_users_name", []string{"name"}); err != nil {
		t.Fatalf("expected matching sqlite index definition to pass, got %v", err)
	}
}

func TestInitIndexModeValidateReturnsMissingIndexError(t *testing.T) {
	db := newSQLiteIndexTestEngine(t)
	if _, err := db.ExecContext(context.Background(), "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("failed to create users table: %v", err)
	}

	runtime := registerIndexRuntime(t, "users", true, "ux_users_name", []string{"name"})

	err := runtime.Init(db.DB, db.Dialect, &InitOptions{IndexMode: IndexInitValidate})
	if err == nil {
		t.Fatal("expected validate mode to fail when index is missing")
	}

	var missing *ErrIndexMissing
	if !errors.As(err, &missing) {
		t.Fatalf("expected ErrIndexMissing, got %T (%v)", err, err)
	}

	if missing.Name != "ux_users_name" || missing.Table != "users" {
		t.Fatalf("unexpected missing index error: %#v", missing)
	}

	_, found, inspectErr := inspectSQLiteIndexDefinition(db, "ux_users_name")
	if inspectErr != nil {
		t.Fatalf("failed to inspect sqlite index: %v", inspectErr)
	}
	if found {
		t.Fatal("validate mode should not create missing indexes")
	}
}

func TestInitIndexModeUpsertCreatesMissingIndex(t *testing.T) {
	db := newSQLiteIndexTestEngine(t)
	if _, err := db.ExecContext(context.Background(), "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("failed to create users table: %v", err)
	}

	runtime := registerIndexRuntime(t, "users", true, "ux_users_name", []string{"name"})
	if err := runtime.Init(db.DB, db.Dialect, &InitOptions{IndexMode: IndexInitUpsert}); err != nil {
		t.Fatalf("expected upsert mode to create missing index, got %v", err)
	}

	definition, found, err := inspectSQLiteIndexDefinition(db, "ux_users_name")
	if err != nil {
		t.Fatalf("failed to inspect sqlite index: %v", err)
	}
	if !found {
		t.Fatal("expected upsert mode to create missing index")
	}
	if !definition.Unique || len(definition.Fields) != 1 || definition.Fields[0] != "name" {
		t.Fatalf("unexpected sqlite index definition: %#v", definition)
	}
}

func TestInitCompatibilityUpsertIndexesTrueStillCreatesIndex(t *testing.T) {
	db := newSQLiteIndexTestEngine(t)
	if _, err := db.ExecContext(context.Background(), "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("failed to create users table: %v", err)
	}

	runtime := registerIndexRuntime(t, "users", true, "ux_users_name", []string{"name"})
	if err := runtime.Init(db.DB, db.Dialect, &InitOptions{UpsertIndexes: true}); err != nil {
		t.Fatalf("expected legacy init upsert=true to keep working, got %v", err)
	}

	_, found, err := inspectSQLiteIndexDefinition(db, "ux_users_name")
	if err != nil {
		t.Fatalf("failed to inspect sqlite index: %v", err)
	}
	if !found {
		t.Fatal("expected legacy init upsert=true to create the index")
	}
}

func TestInitCompatibilityUpsertIndexesFalseStillSkipsIndexInit(t *testing.T) {
	db := newSQLiteIndexTestEngine(t)
	if _, err := db.ExecContext(context.Background(), "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("failed to create users table: %v", err)
	}

	runtime := registerIndexRuntime(t, "users", true, "ux_users_name", []string{"name"})
	if err := runtime.Init(db.DB, db.Dialect, &InitOptions{UpsertIndexes: false}); err != nil {
		t.Fatalf("expected legacy init upsert=false to skip index creation, got %v", err)
	}

	_, found, err := inspectSQLiteIndexDefinition(db, "ux_users_name")
	if err != nil {
		t.Fatalf("failed to inspect sqlite index: %v", err)
	}
	if found {
		t.Fatal("expected legacy init upsert=false to skip index creation")
	}
}

func TestInitSchemaEventCreateIndex(t *testing.T) {
	db := newSQLiteIndexTestEngine(t)
	if _, err := db.ExecContext(context.Background(), "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("failed to create users table: %v", err)
	}

	runtime := registerIndexRuntime(t, "users", true, "ux_users_name", []string{"name"})
	events := make([]SchemaEvent, 0)
	if err := runtime.Init(db.DB, db.Dialect, &InitOptions{
		IndexMode: IndexInitUpsert,
		SchemaEventHandler: func(event SchemaEvent) {
			events = append(events, event)
		},
	}); err != nil {
		t.Fatalf("expected schema event init to succeed, got %v", err)
	}

	if len(events) != 1 {
		t.Fatalf("expected one schema event, got %d", len(events))
	}
	if events[0].Kind != SchemaEventCreateIndex || events[0].Name != "ux_users_name" {
		t.Fatalf("unexpected create index event: %#v", events[0])
	}
	if events[0].SQL == "" {
		t.Fatal("expected create index event to include SQL")
	}
}

func TestInitSchemaEventValidateIndex(t *testing.T) {
	db := newSQLiteIndexTestEngine(t)
	statements := []string{
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
		"CREATE UNIQUE INDEX ux_users_name ON users(name)",
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(context.Background(), statement); err != nil {
			t.Fatalf("failed to execute setup statement %q: %v", statement, err)
		}
	}

	runtime := registerIndexRuntime(t, "users", true, "ux_users_name", []string{"name"})
	events := make([]SchemaEvent, 0)
	if err := runtime.Init(db.DB, db.Dialect, &InitOptions{
		IndexMode: IndexInitValidate,
		SchemaEventHandler: func(event SchemaEvent) {
			events = append(events, event)
		},
	}); err != nil {
		t.Fatalf("expected validate mode with existing index to succeed, got %v", err)
	}

	if len(events) != 1 {
		t.Fatalf("expected one schema event, got %d", len(events))
	}
	if events[0].Kind != SchemaEventValidateIndex || events[0].Name != "ux_users_name" {
		t.Fatalf("unexpected validate index event: %#v", events[0])
	}
}

func TestInitSchemaEventHandlerPanicReturnsError(t *testing.T) {
	db := newSQLiteIndexTestEngine(t)
	if _, err := db.ExecContext(context.Background(), "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("failed to create users table: %v", err)
	}

	runtime := registerIndexRuntime(t, "users", true, "ux_users_name", []string{"name"})
	err := runtime.Init(db.DB, db.Dialect, &InitOptions{
		IndexMode: IndexInitUpsert,
		SchemaEventHandler: func(event SchemaEvent) {
			panic("boom")
		},
	})
	if err == nil {
		t.Fatal("expected schema event handler panic to fail init")
	}
	if !strings.Contains(err.Error(), "schema event handler panicked") {
		t.Fatalf("unexpected schema event panic error: %v", err)
	}
}

func TestInitPersistsIndexModeOnEngine(t *testing.T) {
	db := newSQLiteIndexTestEngine(t)
	statements := []string{
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
		"CREATE UNIQUE INDEX ux_users_name ON users(name)",
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(context.Background(), statement); err != nil {
			t.Fatalf("failed to execute setup statement %q: %v", statement, err)
		}
	}

	runtime := registerIndexRuntime(t, "users", true, "ux_users_name", []string{"name"})
	if err := runtime.Init(db.DB, db.Dialect, &InitOptions{IndexMode: IndexInitValidate}); err != nil {
		t.Fatalf("expected validate mode init to succeed, got %v", err)
	}

	engine := runtime.Engine()
	if got := engine.effectiveIndexInitMode(); got != IndexInitValidate {
		t.Fatalf("expected db index mode %q after init, got %q", IndexInitValidate, got)
	}

	if got := loadDBSchemaConfig(engine).indexInitMode; got != IndexInitValidate {
		t.Fatalf("expected stored db index mode %q after init, got %q", IndexInitValidate, got)
	}
}

func TestInitPersistsSchemaEventHandlerOnEngine(t *testing.T) {
	db := newSQLiteIndexTestEngine(t)
	statements := []string{
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
		"CREATE UNIQUE INDEX ux_users_name ON users(name)",
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(context.Background(), statement); err != nil {
			t.Fatalf("failed to execute setup statement %q: %v", statement, err)
		}
	}

	runtime := registerIndexRuntime(t, "users", true, "ux_users_name", []string{"name"})
	events := make([]SchemaEvent, 0, 3)
	handler := func(event SchemaEvent) {
		events = append(events, event)
	}
	if err := runtime.Init(db.DB, db.Dialect, &InitOptions{
		IndexMode:          IndexInitValidate,
		SchemaEventHandler: handler,
	}); err != nil {
		t.Fatalf("expected validate mode init to succeed, got %v", err)
	}

	engine := runtime.Engine()
	if err := engine.emitSchemaEvent(SchemaEvent{Kind: SchemaEventValidateIndex, Table: "users", Name: "ux_users_name"}); err != nil {
		t.Fatalf("expected db emitSchemaEvent to use persisted handler, got %v", err)
	}

	if err := engine.emitSchemaEvent(SchemaEvent{Kind: SchemaEventValidateIndex, Table: "users", Name: "ux_users_name"}); err != nil {
		t.Fatalf("expected repeated db emitSchemaEvent to use persisted handler, got %v", err)
	}

	if len(events) != 3 {
		t.Fatalf("expected init plus two manual schema events, got %d", len(events))
	}
}

func TestCurrentDialectDetection(t *testing.T) {
	r := NewRuntime()

	// Before Init, should return empty string
	if r.Dialect() != "" {
		t.Errorf("expected empty dialect before Init, got %q", r.Dialect())
	}

	// After Init with SQLite, should detect dialect
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

func registerIndexRuntime(
	t *testing.T,
	tableName string,
	unique bool,
	indexName string,
	fields []string,
) *Runtime {
	t.Helper()

	runtime := NewRuntime()
	if err := runtime.RegisterTable(
		newMockTable(tableName),
		func(db *Engine) error {
			return UpsertIndex(db, tableName, unique, indexName, fields)
		},
	); err != nil {
		t.Fatalf("failed to register test table: %v", err)
	}

	return runtime
}

func TestCurrentDBAccess(t *testing.T) {
	r := NewRuntime()

	// Before Init, should return nil
	if r.Engine() != nil {
		t.Errorf("expected nil CurrentDB before Init, got non-nil")
	}

	// After Init, should return the DB
	db := newSQLiteIndexTestEngine(t)
	if err := r.Init(db.DB, db.Dialect); err != nil {
		t.Fatalf("failed to init runtime: %v", err)
	}

	currentDB := r.Engine()
	if currentDB == nil {
		t.Errorf("expected non-nil CurrentDB after Init, got nil")
	}
	if currentDB.DB != db.DB || currentDB.Dialect != db.Dialect {
		t.Errorf("expected CurrentDB to return same underlying DB and Dialect")
	}
}

func TestValidateIdentifiersForDialect(t *testing.T) {
	r := NewRuntime()

	// Before Init, should return error
	err := r.ValidateIdentifiersForDialect()
	if err == nil {
		t.Errorf("expected error before Init, got nil")
	}

	// After Init, should succeed (no invalid identifiers registered)
	db := newSQLiteIndexTestEngine(t)
	if err := r.Init(db.DB, db.Dialect); err != nil {
		t.Fatalf("failed to init runtime: %v", err)
	}

	err = r.ValidateIdentifiersForDialect()
	if err != nil {
		t.Errorf("ValidateIdentifiersForDialect after Init should succeed, got error: %v", err)
	}
}

func TestInitFailureRestoresPreviousRuntimeStateAfterStrictValidation(t *testing.T) {
	r := NewRuntime()

	previousDB := newSQLiteIndexTestEngine(t)
	previousEvents := make([]SchemaEvent, 0, 1)
	previousHandler := func(event SchemaEvent) {
		previousEvents = append(previousEvents, event)
	}
	if err := r.Init(previousDB.DB, previousDB.Dialect, &InitOptions{
		IndexMode:          IndexInitValidate,
		SchemaEventHandler: previousHandler,
	}); err != nil {
		t.Fatalf("failed to initialize previous runtime state: %v", err)
	}
	expectedEngine := r.Engine()

	failingDB := newSQLiteIndexTestEngine(t)
	failingDB.Dialect = MySQLDialect{}
	failingEvents := make([]SchemaEvent, 0, 1)
	failingHandler := func(event SchemaEvent) {
		failingEvents = append(failingEvents, event)
	}

	longTableName := strings.Repeat("u", MaxIdentifierLengthMySQL+1)
	if err := r.RegisterTable(newMockTable(longTableName), func(db *Engine) error { return nil }); err != nil {
		t.Fatalf("failed to register invalid table: %v", err)
	}

	err := r.Init(failingDB.DB, failingDB.Dialect, &InitOptions{
		IndexMode:                IndexInitSkip,
		IdentifierValidationMode: "strict",
		SchemaEventHandler:       failingHandler,
	})
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
	if err := r.Init(previousDB.DB, previousDB.Dialect, &InitOptions{
		IndexMode:          IndexInitValidate,
		SchemaEventHandler: previousHandler,
	}); err != nil {
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

	err := r.Init(failingDB.DB, failingDB.Dialect, &InitOptions{
		IndexMode:          IndexInitValidate,
		SchemaEventHandler: failingHandler,
	})
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

func TestValidateIdentifiersForDialectChecksTableColumns(t *testing.T) {
	r := NewRuntime()

	db := newSQLiteIndexTestEngine(t)
	db.Dialect = MySQLDialect{}

	longColumnName := strings.Repeat("c", MaxIdentifierLengthMySQL+1)
	table, _ := newStrictMockTable("users", longColumnName)
	if err := r.RegisterTable(table, func(db *Engine) error { return nil }); err != nil {
		t.Fatalf("failed to register table with long column name: %v", err)
	}

	if err := r.Init(db.DB, db.Dialect, &InitOptions{IdentifierValidationMode: "skip"}); err != nil {
		t.Fatalf("failed to initialize runtime with validation skipped: %v", err)
	}

	err := r.ValidateIdentifiersForDialect()
	if err == nil {
		t.Fatal("expected ValidateIdentifiersForDialect to reject oversized regular column names")
	}
	if !strings.Contains(err.Error(), longColumnName) {
		t.Fatalf("expected validation error to mention oversized column name, got: %v", err)
	}
}
