package tsq

import (
	"context"
	"errors"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func inspectRegisteredIndex(t *testing.T, db *Runtime, table, idx string) (IndexDefinition, bool) {
	t.Helper()

	definition, found, err := db.SQLDialect().InspectIndexDefinition(context.Background(), db, table, idx)
	if err != nil {
		t.Fatalf("failed to inspect index %s on %s: %v", idx, table, err)
	}

	return definition, found
}

func TestUpsertIndexRejectsInvalidIdentifiers(t *testing.T) {
	db := newEngine(nil, MySQLDialect{})
	err := upsertIndex(db, "users;drop", false, "idx_users_id", []string{"id"})
	if err == nil {
		t.Fatal("expected invalid table name to return an error")
	}
	err = upsertIndex(db, "users", false, "idx users id", []string{"id"})
	if err == nil {
		t.Fatal("expected invalid index name to return an error")
	}
	err = upsertIndex(db, "users", false, "idx_users_id", []string{"id", "name desc"})
	if err == nil {
		t.Fatal("expected invalid field name to return an error")
	}
}

func TestUpsertIndexRejectsEmptyFields(t *testing.T) {
	db := newEngine(nil, MySQLDialect{})
	err := upsertIndex(db, "users", false, "idx_users_id", nil)
	if err == nil {
		t.Fatal("expected empty index fields to return an error")
	}
}

func TestUpsertIndexRejectsNilEngine(t *testing.T) {
	err := upsertIndex(nil, "users", false, "idx_users_id", []string{"id"})
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
		if _, err := db.DB().ExecContext(context.Background(), statement); err != nil {
			t.Fatalf("failed to execute setup statement %q: %v", statement, err)
		}
	}
	err := upsertIndex(db.engine, "orgs", true, "ux_name", []string{"name"})
	if err == nil {
		t.Fatal("expected conflicting sqlite index name to return an error")
	}
}

func TestUpsertIndexSQLiteRejectsDefinitionMismatch(t *testing.T) {
	db := newSQLiteIndexTestEngine(t)
	statements := []string{"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)", "CREATE UNIQUE INDEX ux_users_name ON users(email)"}
	for _, statement := range statements {
		if _, err := db.DB().ExecContext(context.Background(), statement); err != nil {
			t.Fatalf("failed to execute setup statement %q: %v", statement, err)
		}
	}
	err := upsertIndex(db.engine, "users", true, "ux_users_name", []string{"name"})
	if err == nil {
		t.Fatal("expected mismatched sqlite index definition to return an error")
	}
}

func TestUpsertIndexSQLiteAcceptsMatchingDefinition(t *testing.T) {
	db := newSQLiteIndexTestEngine(t)
	statements := []string{"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", "CREATE UNIQUE INDEX ux_users_name ON users(name)"}
	for _, statement := range statements {
		if _, err := db.DB().ExecContext(context.Background(), statement); err != nil {
			t.Fatalf("failed to execute setup statement %q: %v", statement, err)
		}
	}
	if err := upsertIndex(db.engine, "users", true, "ux_users_name", []string{"name"}); err != nil {
		t.Fatalf("expected matching sqlite index definition to pass, got %v", err)
	}
}

func TestInitIndexModeValidateReturnsMissingIndexError(t *testing.T) {
	db := newSQLiteIndexTestEngine(t)
	if _, err := db.DB().ExecContext(context.Background(), "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("failed to create users table: %v", err)
	}
	runtime := registerIndexRuntime(t, "users", true, "ux_users_name", []string{"name"})
	err := runtime.Init(db.DB(), db.SQLDialect(), &InitOptions{IndexMode: IndexInitValidate})
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
	_, found := inspectRegisteredIndex(t, db, "users", "ux_users_name")
	if found {
		t.Fatal("validate mode should not create missing indexes")
	}
}

func TestInitIndexModeUpsertCreatesMissingIndex(t *testing.T) {
	db := newSQLiteIndexTestEngine(t)
	if _, err := db.DB().ExecContext(context.Background(), "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("failed to create users table: %v", err)
	}
	runtime := registerIndexRuntime(t, "users", true, "ux_users_name", []string{"name"})
	if err := runtime.Init(db.DB(), db.SQLDialect(), &InitOptions{IndexMode: IndexInitUpsert}); err != nil {
		t.Fatalf("expected upsert mode to create missing index, got %v", err)
	}
	definition, found := inspectRegisteredIndex(t, db, "users", "ux_users_name")
	if !found {
		t.Fatal("expected upsert mode to create missing index")
	}
	if !definition.Unique || len(definition.Fields) != 1 || definition.Fields[0] != "name" {
		t.Fatalf("unexpected sqlite index definition: %#v", definition)
	}
}

func TestInitCompatibilityUpsertIndexesTrueStillCreatesIndex(t *testing.T) {
	db := newSQLiteIndexTestEngine(t)
	if _, err := db.DB().ExecContext(context.Background(), "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("failed to create users table: %v", err)
	}
	runtime := registerIndexRuntime(t, "users", true, "ux_users_name", []string{"name"})
	if err := runtime.Init(db.DB(), db.SQLDialect(), &InitOptions{UpsertIndexes: true}); err != nil {
		t.Fatalf("expected legacy init upsert=true to keep working, got %v", err)
	}
	_, found := inspectRegisteredIndex(t, db, "users", "ux_users_name")
	if !found {
		t.Fatal("expected legacy init upsert=true to create the index")
	}
}

func TestInitCompatibilityUpsertIndexesFalseStillSkipsIndexInit(t *testing.T) {
	db := newSQLiteIndexTestEngine(t)
	if _, err := db.DB().ExecContext(context.Background(), "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("failed to create users table: %v", err)
	}
	runtime := registerIndexRuntime(t, "users", true, "ux_users_name", []string{"name"})
	if err := runtime.Init(db.DB(), db.SQLDialect(), &InitOptions{UpsertIndexes: false}); err != nil {
		t.Fatalf("expected legacy init upsert=false to skip index creation, got %v", err)
	}
	_, found := inspectRegisteredIndex(t, db, "users", "ux_users_name")
	if found {
		t.Fatal("expected legacy init upsert=false to skip index creation")
	}
}

func TestInitValidateModeAcceptsExistingRegisteredIndex(t *testing.T) {
	db := newSQLiteIndexTestEngine(t)
	statements := []string{"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", "CREATE UNIQUE INDEX ux_users_name ON users(name)"}
	for _, statement := range statements {
		if _, err := db.DB().ExecContext(context.Background(), statement); err != nil {
			t.Fatalf("failed to execute setup statement %q: %v", statement, err)
		}
	}
	runtime := registerIndexRuntime(t, "users", true, "ux_users_name", []string{"name"})
	if err := runtime.Init(db.DB(), db.SQLDialect(), &InitOptions{IndexMode: IndexInitValidate}); err != nil {
		t.Fatalf("expected validate mode with existing index to succeed, got %v", err)
	}
}

func TestInitPersistsIndexModeOnEngine(t *testing.T) {
	db := newSQLiteIndexTestEngine(t)
	statements := []string{"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", "CREATE UNIQUE INDEX ux_users_name ON users(name)"}
	for _, statement := range statements {
		if _, err := db.DB().ExecContext(context.Background(), statement); err != nil {
			t.Fatalf("failed to execute setup statement %q: %v", statement, err)
		}
	}
	runtime := registerIndexRuntime(t, "users", true, "ux_users_name", []string{"name"})
	if err := runtime.Init(db.DB(), db.SQLDialect(), &InitOptions{IndexMode: IndexInitValidate}); err != nil {
		t.Fatalf("expected validate mode init to succeed, got %v", err)
	}
	if got := runtime.engine.effectiveIndexInitMode(); got != IndexInitValidate {
		t.Fatalf("expected db index mode %q after init, got %q", IndexInitValidate, got)
	}
}

func TestValidateIdentifiersForDialect(t *testing.T) {
	r := NewRuntime()
	err := r.ValidateIdentifiersForDialect()
	if err == nil {
		t.Errorf("expected error before Init, got nil")
	}
	db := newSQLiteIndexTestEngine(t)
	if err := r.Init(db.DB(), db.SQLDialect()); err != nil {
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
	db.engine.dialect = MySQLDialect{}
	longColumnName := firstRejectedIdentifier(t, MySQLDialect{}, "c")
	table, _ := newStrictMockTable("users", longColumnName)
	if err := r.RegisterTable(table); err != nil {
		t.Fatalf("failed to register table with long column name: %v", err)
	}
	if err := r.Init(db.DB(), db.SQLDialect(), &InitOptions{IdentifierValidationMode: "skip"}); err != nil {
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

func TestValidateIdentifiersForDialectChecksIndexNames(t *testing.T) {
	r := NewRuntime()
	db := newSQLiteIndexTestEngine(t)
	db.engine.dialect = MySQLDialect{}
	longIndexName := firstRejectedIdentifier(t, MySQLDialect{}, "i")
	table, _ := newStrictMockTable("users", "id")
	if err := r.RegisterTable(table, TableIndex{
		Name:   longIndexName,
		Fields: []string{"id"},
	}); err != nil {
		t.Fatalf("failed to register table with long index name: %v", err)
	}
	if err := r.Init(db.DB(), db.SQLDialect(), &InitOptions{IdentifierValidationMode: "skip"}); err != nil {
		t.Fatalf("failed to initialize runtime with validation skipped: %v", err)
	}

	err := r.ValidateIdentifiersForDialect()
	if err == nil {
		t.Fatal("expected ValidateIdentifiersForDialect to reject oversized index names")
	}
	if !strings.Contains(err.Error(), longIndexName) {
		t.Fatalf("expected validation error to mention oversized index name, got: %v", err)
	}
}
