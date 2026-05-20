package tsq

import (
	"context"
	"errors"
	_ "github.com/mattn/go-sqlite3"
	"strings"
	"testing"
)

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
func TestUpsertIndexSQLiteRejectsConflictingTableReuse(t *testing.T) {
	db := newSQLiteIndexTestEngine(t)
	statements := []string{"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", "CREATE TABLE orgs (id INTEGER PRIMARY KEY, name TEXT)", "CREATE UNIQUE INDEX ux_name ON users(name)"}
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
	statements := []string{"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)", "CREATE UNIQUE INDEX ux_users_name ON users(email)"}
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
	statements := []string{"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", "CREATE UNIQUE INDEX ux_users_name ON users(name)"}
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
	if err := runtime.Init(db.DB, db.Dialect, &InitOptions{IndexMode: IndexInitUpsert, SchemaEventHandler: func(event SchemaEvent) {
		events = append(events, event)
	}}); err != nil {
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
	statements := []string{"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", "CREATE UNIQUE INDEX ux_users_name ON users(name)"}
	for _, statement := range statements {
		if _, err := db.ExecContext(context.Background(), statement); err != nil {
			t.Fatalf("failed to execute setup statement %q: %v", statement, err)
		}
	}
	runtime := registerIndexRuntime(t, "users", true, "ux_users_name", []string{"name"})
	events := make([]SchemaEvent, 0)
	if err := runtime.Init(db.DB, db.Dialect, &InitOptions{IndexMode: IndexInitValidate, SchemaEventHandler: func(event SchemaEvent) {
		events = append(events, event)
	}}); err != nil {
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
	err := runtime.Init(db.DB, db.Dialect, &InitOptions{IndexMode: IndexInitUpsert, SchemaEventHandler: func(event SchemaEvent) {
		panic("boom")
	}})
	if err == nil {
		t.Fatal("expected schema event handler panic to fail init")
	}
	if !strings.Contains(err.Error(), "schema event handler panicked") {
		t.Fatalf("unexpected schema event panic error: %v", err)
	}
}
func TestInitPersistsIndexModeOnEngine(t *testing.T) {
	db := newSQLiteIndexTestEngine(t)
	statements := []string{"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", "CREATE UNIQUE INDEX ux_users_name ON users(name)"}
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
	statements := []string{"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", "CREATE UNIQUE INDEX ux_users_name ON users(name)"}
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
	if err := runtime.Init(db.DB, db.Dialect, &InitOptions{IndexMode: IndexInitValidate, SchemaEventHandler: handler}); err != nil {
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
func TestValidateIdentifiersForDialect(t *testing.T) {
	r := NewRuntime()
	err := r.ValidateIdentifiersForDialect()
	if err == nil {
		t.Errorf("expected error before Init, got nil")
	}
	db := newSQLiteIndexTestEngine(t)
	if err := r.Init(db.DB, db.Dialect); err != nil {
		t.Fatalf("failed to init runtime: %v", err)
	}
	err = r.ValidateIdentifiersForDialect()
	if err != nil {
		t.Errorf("ValidateIdentifiersForDialect after Init should succeed, got error: %v", err)
	}
}
func TestValidateIdentifiersForDialectChecksTableColumns(t *testing.T) {
	r := NewRuntime()
	db := newSQLiteIndexTestEngine(t)
	db.Dialect = MySQLDialect{}
	longColumnName := strings.Repeat("c", MaxIdentifierLengthMySQL+1)
	table, _ := newStrictMockTable("users", longColumnName)
	if err := r.RegisterTable(table, func(db *Engine) error {
		return nil
	}); err != nil {
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
