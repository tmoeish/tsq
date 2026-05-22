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

func newRegisteredIndexRuntime(
	t *testing.T,
	db *Runtime,
	tableName string,
	unique bool,
	indexName string,
	fields []string,
	options ...*InitOptions,
) *Runtime {
	t.Helper()

	table, _ := newStrictMockTable(tableName, fields...)
	runtime, err := NewRuntime(
		db.DB(),
		db.SQLDialect(),
		[]TableRegistration{{
			Table:   table,
			Indexes: []TableIndex{{Name: indexName, Fields: fields, Unique: unique}},
		}},
		options...,
	)
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}

	return runtime
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

func TestNewRuntimeRejectsNilDB(t *testing.T) {
	if _, err := NewRuntime(nil, nil, nil); err == nil {
		t.Fatal("expected nil db/dialect to return an error")
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

func TestNewRuntimeIndexModeValidateReturnsMissingIndexError(t *testing.T) {
	db := newSQLiteIndexTestEngine(t)
	if _, err := db.DB().ExecContext(context.Background(), "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("failed to create users table: %v", err)
	}

	_, err := NewRuntime(
		db.DB(),
		db.SQLDialect(),
		[]TableRegistration{{
			Table:   mustStrictMockTable(t, "users", "name"),
			Indexes: []TableIndex{{Name: "ux_users_name", Fields: []string{"name"}, Unique: true}},
		}},
		&InitOptions{IndexMode: IndexInitValidate},
	)
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

func TestNewRuntimeIndexModeUpsertCreatesMissingIndex(t *testing.T) {
	db := newSQLiteIndexTestEngine(t)
	if _, err := db.DB().ExecContext(context.Background(), "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("failed to create users table: %v", err)
	}

	runtime := newRegisteredIndexRuntime(t, db, "users", true, "ux_users_name", []string{"name"}, &InitOptions{IndexMode: IndexInitUpsert})
	definition, found := inspectRegisteredIndex(t, runtime, "users", "ux_users_name")
	if !found {
		t.Fatal("expected upsert mode to create missing index")
	}
	if !definition.Unique || len(definition.Fields) != 1 || definition.Fields[0] != "name" {
		t.Fatalf("unexpected sqlite index definition: %#v", definition)
	}
}

func TestNewRuntimeCompatibilityUpsertIndexesTrueStillCreatesIndex(t *testing.T) {
	db := newSQLiteIndexTestEngine(t)
	if _, err := db.DB().ExecContext(context.Background(), "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("failed to create users table: %v", err)
	}

	runtime := newRegisteredIndexRuntime(t, db, "users", true, "ux_users_name", []string{"name"}, &InitOptions{UpsertIndexes: true})
	_, found := inspectRegisteredIndex(t, runtime, "users", "ux_users_name")
	if !found {
		t.Fatal("expected legacy upsert=true to create the index")
	}
}

func TestNewRuntimeCompatibilityUpsertIndexesFalseStillSkipsIndexInit(t *testing.T) {
	db := newSQLiteIndexTestEngine(t)
	if _, err := db.DB().ExecContext(context.Background(), "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("failed to create users table: %v", err)
	}

	runtime := newRegisteredIndexRuntime(t, db, "users", true, "ux_users_name", []string{"name"}, &InitOptions{UpsertIndexes: false})
	_, found := inspectRegisteredIndex(t, runtime, "users", "ux_users_name")
	if found {
		t.Fatal("expected legacy upsert=false to skip index creation")
	}
}

func TestNewRuntimeValidateModeAcceptsExistingRegisteredIndex(t *testing.T) {
	db := newSQLiteIndexTestEngine(t)
	statements := []string{"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", "CREATE UNIQUE INDEX ux_users_name ON users(name)"}
	for _, statement := range statements {
		if _, err := db.DB().ExecContext(context.Background(), statement); err != nil {
			t.Fatalf("failed to execute setup statement %q: %v", statement, err)
		}
	}

	if _, err := NewRuntime(
		db.DB(),
		db.SQLDialect(),
		[]TableRegistration{{
			Table:   mustStrictMockTable(t, "users", "name"),
			Indexes: []TableIndex{{Name: "ux_users_name", Fields: []string{"name"}, Unique: true}},
		}},
		&InitOptions{IndexMode: IndexInitValidate},
	); err != nil {
		t.Fatalf("expected validate mode with existing index to succeed, got %v", err)
	}
}

func TestNewRuntimePersistsIndexModeOnEngine(t *testing.T) {
	db := newSQLiteIndexTestEngine(t)
	statements := []string{"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", "CREATE UNIQUE INDEX ux_users_name ON users(name)"}
	for _, statement := range statements {
		if _, err := db.DB().ExecContext(context.Background(), statement); err != nil {
			t.Fatalf("failed to execute setup statement %q: %v", statement, err)
		}
	}

	runtime := newRegisteredIndexRuntime(t, db, "users", true, "ux_users_name", []string{"name"}, &InitOptions{IndexMode: IndexInitValidate})
	if got := runtime.engine.effectiveIndexInitMode(); got != IndexInitValidate {
		t.Fatalf("expected db index mode %q after init, got %q", IndexInitValidate, got)
	}
}

func TestValidateIdentifiersForDialect(t *testing.T) {
	db := newSQLiteIndexTestEngine(t)
	r, err := NewRuntime(db.DB(), db.SQLDialect(), nil)
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}

	if err := r.ValidateIdentifiersForDialect(); err != nil {
		t.Errorf("ValidateIdentifiersForDialect after NewRuntime should succeed, got error: %v", err)
	}
}

func TestValidateIdentifiersForDialectChecksTableColumns(t *testing.T) {
	db := newSQLiteIndexTestEngine(t)
	db.engine.dialect = MySQLDialect{}
	longColumnName := firstRejectedIdentifier(t, MySQLDialect{}, "c")
	table, _ := newStrictMockTable("users", longColumnName)

	r, err := NewRuntime(
		db.DB(),
		db.SQLDialect(),
		[]TableRegistration{{Table: table}},
		&InitOptions{IdentifierValidationMode: "skip"},
	)
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}

	err = r.ValidateIdentifiersForDialect()
	if err == nil {
		t.Fatal("expected ValidateIdentifiersForDialect to reject oversized regular column names")
	}
	if !strings.Contains(err.Error(), longColumnName) {
		t.Fatalf("expected validation error to mention oversized column name, got: %v", err)
	}
}

func TestValidateIdentifiersForDialectChecksIndexNames(t *testing.T) {
	db := newSQLiteIndexTestEngine(t)
	db.engine.dialect = MySQLDialect{}
	longIndexName := firstRejectedIdentifier(t, MySQLDialect{}, "i")
	table, _ := newStrictMockTable("users", "id")

	r, err := NewRuntime(
		db.DB(),
		db.SQLDialect(),
		[]TableRegistration{{
			Table:   table,
			Indexes: []TableIndex{{Name: longIndexName, Fields: []string{"id"}}},
		}},
		&InitOptions{IdentifierValidationMode: "skip"},
	)
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}

	err = r.ValidateIdentifiersForDialect()
	if err == nil {
		t.Fatal("expected ValidateIdentifiersForDialect to reject oversized index names")
	}
	if !strings.Contains(err.Error(), longIndexName) {
		t.Fatalf("expected validation error to mention oversized index name, got: %v", err)
	}
}

func mustStrictMockTable(t *testing.T, tableName string, fields ...string) Table {
	t.Helper()

	table, _ := newStrictMockTable(tableName, fields...)
	return table
}
