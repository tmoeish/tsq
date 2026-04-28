package tsq

import (
	"database/sql"
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
				return RegisterTable(nil, func(db *DbMap) {}, func(db *DbMap) error { return nil })
			},
			expectedError: RegistrationErrorNilTable,
		},
		{
			name: "nil add table func",
			fn: func() error {
				return RegisterTable(newMockTable("users"), nil, func(db *DbMap) error { return nil })
			},
			expectedError: RegistrationErrorNilAddFunc,
		},
		{
			name: "nil init func",
			fn: func() error {
				return RegisterTable(newMockTable("users"), func(db *DbMap) {}, nil)
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

			regErr, ok := err.(*RegistrationError)
			if !ok {
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
	err1 := RegisterTable(table, func(db *DbMap) {}, func(db *DbMap) error { return nil })
	if err1 != nil {
		t.Fatalf("first registration should succeed, got error: %v", err1)
	}

	err2 := RegisterTable(table, func(db *DbMap) {}, func(db *DbMap) error { return nil })
	if err2 == nil {
		t.Fatal("expected duplicate table registration to fail")
	}

	regErr, ok := err2.(*RegistrationError)
	if !ok {
		t.Fatalf("expected RegistrationError, got %T", err2)
	}

	if regErr.Type != RegistrationErrorDuplicate {
		t.Errorf("expected error type %v, got %v", RegistrationErrorDuplicate, regErr.Type)
	}
}

func TestRuntimeRegisterTableRejectsNilRuntime(t *testing.T) {
	var r *Runtime // nil runtime
	table := newMockTable("users")

	err := r.RegisterTable(table, func(db *DbMap) {}, func(db *DbMap) error { return nil })
	if err == nil {
		t.Fatal("expected nil runtime to return error")
	}

	regErr, ok := err.(*RegistrationError)
	if !ok {
		t.Fatalf("expected RegistrationError, got %T", err)
	}

	if regErr.Type != RegistrationErrorNilRuntime {
		t.Errorf("expected error type %v, got %v", RegistrationErrorNilRuntime, regErr.Type)
	}
}

func TestSnapshotRegisteredTablesReturnsDeterministicOrder(t *testing.T) {
	oldRuntime := defaultRuntime
	defaultRuntime = NewRuntime()
	defaultRuntime.registry = &Registry{tables: map[string]*RegisteredTable{
		"users": {
			Table:        newMockTable("users"),
			AddTableFunc: func(db *DbMap) {},
			InitFunc:     func(db *DbMap) error { return nil },
		},
		"accounts": {
			Table:        newMockTable("accounts"),
			AddTableFunc: func(db *DbMap) {},
			InitFunc:     func(db *DbMap) error { return nil },
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

	tracer := func(next Fn) Fn { return next }

	if err := Init(newSQLiteIndexTestDBMap(t), false, false, tracer); err != nil {
		t.Fatalf("unexpected init error: %v", err)
	}

	if err := Init(newSQLiteIndexTestDBMap(t), false, false, tracer); err != nil {
		t.Fatalf("unexpected init error: %v", err)
	}

	if got := len(GetTracers()); got != 1 {
		t.Fatalf("expected Init to deduplicate tracers, got %d", got)
	}
}

func TestRuntimeKeepsRegistrationsAndTracersIsolated(t *testing.T) {
	left := NewRuntime()
	right := NewRuntime()

	left.RegisterTable(newMockTable("users"), func(db *DbMap) {}, func(db *DbMap) error { return nil })
	right.RegisterTable(newMockTable("users"), func(db *DbMap) {}, func(db *DbMap) error { return nil })

	left.AddTracer(func(next Fn) Fn { return next })

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
	db := &DbMap{Dialect: MySQLDialect{}}

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
	db := &DbMap{Dialect: MySQLDialect{}}

	err := UpsertIndex(db, "users", false, "idx_users_id", nil)
	if err == nil {
		t.Fatal("expected empty index fields to return an error")
	}
}

func TestUpsertIndexRejectsNilDbMap(t *testing.T) {
	err := UpsertIndex(nil, "users", false, "idx_users_id", []string{"id"})
	if err == nil {
		t.Fatal("expected nil db map to return an error")
	}
}

func TestInitRejectsNilDbMap(t *testing.T) {
	if err := Init(nil, false, false); err == nil {
		t.Fatal("expected nil db map to return an error")
	}
}

func newSQLiteIndexTestDBMap(t *testing.T) *DbMap {
	t.Helper()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}

	t.Cleanup(func() {
		_ = db.Close()
	})

	return &DbMap{Db: db, Dialect: SqliteDialect{}}
}

func TestUpsertIndexSQLiteRejectsConflictingTableReuse(t *testing.T) {
	db := newSQLiteIndexTestDBMap(t)

	statements := []string{
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
		"CREATE TABLE orgs (id INTEGER PRIMARY KEY, name TEXT)",
		"CREATE UNIQUE INDEX ux_name ON users(name)",
	}
	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("failed to execute setup statement %q: %v", statement, err)
		}
	}

	err := UpsertIndex(db, "orgs", true, "ux_name", []string{"name"})
	if err == nil {
		t.Fatal("expected conflicting sqlite index name to return an error")
	}
}

func TestUpsertIndexSQLiteRejectsDefinitionMismatch(t *testing.T) {
	db := newSQLiteIndexTestDBMap(t)

	statements := []string{
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)",
		"CREATE UNIQUE INDEX ux_users_name ON users(email)",
	}
	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("failed to execute setup statement %q: %v", statement, err)
		}
	}

	err := UpsertIndex(db, "users", true, "ux_users_name", []string{"name"})
	if err == nil {
		t.Fatal("expected mismatched sqlite index definition to return an error")
	}
}

func TestUpsertIndexSQLiteAcceptsMatchingDefinition(t *testing.T) {
	db := newSQLiteIndexTestDBMap(t)

	statements := []string{
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
		"CREATE UNIQUE INDEX ux_users_name ON users(name)",
	}
	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("failed to execute setup statement %q: %v", statement, err)
		}
	}

	if err := UpsertIndex(db, "users", true, "ux_users_name", []string{"name"}); err != nil {
		t.Fatalf("expected matching sqlite index definition to pass, got %v", err)
	}
}

func TestCurrentDialectDetection(t *testing.T) {
	r := NewRuntime()

	// Before Init, should return empty string
	if r.CurrentDialect() != "" {
		t.Errorf("expected empty dialect before Init, got %q", r.CurrentDialect())
	}

	// After Init with SQLite, should detect dialect
	db := newSQLiteIndexTestDBMap(t)
	if err := r.InitWithOptions(db, &InitOptions{}); err != nil {
		t.Fatalf("failed to init runtime: %v", err)
	}

	dialect := r.CurrentDialect()
	if dialect == "" {
		t.Errorf("expected non-empty dialect after Init with SQLite")
	}
	if dialect != "sqlite" {
		t.Logf("detected dialect: %s", dialect)
	}
}

func TestCurrentDBAccess(t *testing.T) {
	r := NewRuntime()

	// Before Init, should return nil
	if r.CurrentDB() != nil {
		t.Errorf("expected nil CurrentDB before Init, got non-nil")
	}

	// After Init, should return the DB
	db := newSQLiteIndexTestDBMap(t)
	if err := r.InitWithOptions(db, &InitOptions{}); err != nil {
		t.Fatalf("failed to init runtime: %v", err)
	}

	currentDB := r.CurrentDB()
	if currentDB == nil {
		t.Errorf("expected non-nil CurrentDB after Init, got nil")
	}
	if currentDB != db {
		t.Errorf("expected CurrentDB to return same DB instance")
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
	db := newSQLiteIndexTestDBMap(t)
	if err := r.InitWithOptions(db, &InitOptions{}); err != nil {
		t.Fatalf("failed to init runtime: %v", err)
	}

	err = r.ValidateIdentifiersForDialect()
	if err != nil {
		t.Errorf("ValidateIdentifiersForDialect after Init should succeed, got error: %v", err)
	}
}
