package tsq

import (
	"context"
	"errors"
	"testing"

	_ "modernc.org/sqlite"

	sqld "github.com/tmoeish/tsq/v5/internal/sqldialect"
)

func inspectRegisteredIndex(t *testing.T, db *Runtime, table, idx string) (sqld.Index, bool) {
	t.Helper()

	definition, found, err := db.dialect.InspectIndex(context.Background(), db, table, idx)
	if err != nil {
		t.Fatalf("failed to inspect index %s on %s: %v", idx, table, err)
	}

	return definition, found
}

func newRegisteredIndexRuntime(
	t *testing.T,
	db *Runtime,
	dsn string,
	tableName string,
	unique bool,
	indexName string,
	fields []string,
	options ...RuntimeOption,
) *Runtime {
	t.Helper()

	table, _ := newStrictMockTable(tableName, fields...)
	runtime, err := Open(context.Background(),
		"sqlite",
		dsn,
		[]Table{registered(table, nil, []TableIndex{{Name: indexName, Fields: fields, Unique: unique}}...)},
		options...)
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}

	return runtime
}

func TestUpsertIndexRejectsInvalidIdentifiers(t *testing.T) {
	err := upsertIndex(context.Background(), nil, sqld.MySQLDialect{}, SchemaPolicyCreateMissing, "users;drop", false, "idx_users_id", []string{"id"})
	if err == nil {
		t.Fatal("expected nil db to return an error")
	}
	db, _ := newSQLiteIndexTestEngine(t)
	err = upsertIndex(context.Background(), db.DB(), sqld.MySQLDialect{}, SchemaPolicyCreateMissing, "users;drop", false, "idx_users_id", []string{"id"})
	if err == nil {
		t.Fatal("expected invalid table name to return an error")
	}
	err = upsertIndex(context.Background(), db.DB(), sqld.MySQLDialect{}, SchemaPolicyCreateMissing, "users", false, "idx users id", []string{"id"})
	if err == nil {
		t.Fatal("expected invalid index name to return an error")
	}
	err = upsertIndex(context.Background(), db.DB(), sqld.MySQLDialect{}, SchemaPolicyCreateMissing, "users", false, "idx_users_id", []string{"id", "name desc"})
	if err == nil {
		t.Fatal("expected invalid field name to return an error")
	}
}

func TestUpsertIndexRejectsEmptyFields(t *testing.T) {
	db, _ := newSQLiteIndexTestEngine(t)
	err := upsertIndex(context.Background(), db.DB(), sqld.MySQLDialect{}, SchemaPolicyCreateMissing, "users", false, "idx_users_id", nil)
	if err == nil {
		t.Fatal("expected empty index fields to return an error")
	}
}

func TestUpsertIndexRejectsNilDB(t *testing.T) {
	err := upsertIndex(context.Background(), nil, sqld.MySQLDialect{}, SchemaPolicyCreateMissing, "users", false, "idx_users_id", []string{"id"})
	if err == nil {
		t.Fatal("expected nil db to return an error")
	}
}

func TestNewRuntimeRejectsNilDB(t *testing.T) {
	if _, err := Open(context.Background(), "", "", nil); err == nil {
		t.Fatal("expected empty driver/dsn to return an error")
	}
}

func TestUpsertIndexSQLiteRejectsConflictingTableReuse(t *testing.T) {
	db, _ := newSQLiteIndexTestEngine(t)
	statements := []string{"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", "CREATE TABLE orgs (id INTEGER PRIMARY KEY, name TEXT)", "CREATE UNIQUE INDEX ux_name ON users(name)"}
	for _, statement := range statements {
		if _, err := db.DB().ExecContext(context.Background(), statement); err != nil {
			t.Fatalf("failed to execute setup statement %q: %v", statement, err)
		}
	}
	err := upsertIndex(context.Background(), db.DB(), db.dialect, SchemaPolicyCreateMissing, "orgs", true, "ux_name", []string{"name"})
	if err == nil {
		t.Fatal("expected conflicting sqlite index name to return an error")
	}
}

func TestUpsertIndexSQLiteRejectsDefinitionMismatch(t *testing.T) {
	db, _ := newSQLiteIndexTestEngine(t)
	statements := []string{"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)", "CREATE UNIQUE INDEX ux_users_name ON users(email)"}
	for _, statement := range statements {
		if _, err := db.DB().ExecContext(context.Background(), statement); err != nil {
			t.Fatalf("failed to execute setup statement %q: %v", statement, err)
		}
	}
	err := upsertIndex(context.Background(), db.DB(), db.dialect, SchemaPolicyCreateMissing, "users", true, "ux_users_name", []string{"name"})
	if err == nil {
		t.Fatal("expected mismatched sqlite index definition to return an error")
	}
}

func TestUpsertIndexSQLiteAcceptsMatchingDefinition(t *testing.T) {
	db, _ := newSQLiteIndexTestEngine(t)
	statements := []string{"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", "CREATE UNIQUE INDEX ux_users_name ON users(name)"}
	for _, statement := range statements {
		if _, err := db.DB().ExecContext(context.Background(), statement); err != nil {
			t.Fatalf("failed to execute setup statement %q: %v", statement, err)
		}
	}
	if err := upsertIndex(context.Background(), db.DB(), db.dialect, SchemaPolicyCreateMissing, "users", true, "ux_users_name", []string{"name"}); err != nil {
		t.Fatalf("expected matching sqlite index definition to pass, got %v", err)
	}
}

func TestNewRuntimeIndexModeValidateReturnsMissingIndexError(t *testing.T) {
	db, dsn := newSQLiteIndexTestEngine(t)
	if _, err := db.DB().ExecContext(context.Background(), "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("failed to create users table: %v", err)
	}

	_, err := Open(context.Background(),
		"sqlite",
		dsn,
		[]Table{registered(mustStrictMockTable(t, "users", "name"), nil, []TableIndex{{Name: "ux_users_name", Fields: []string{"name"}, Unique: true}}...)},
		WithIndexPolicy(SchemaPolicyValidate))
	if err == nil {
		t.Fatal("expected validate mode to fail when index is missing")
	}

	var missing *MissingIndexError
	if !errors.As(err, &missing) {
		t.Fatalf("expected MissingIndexError, got %T (%v)", err, err)
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
	db, dsn := newSQLiteIndexTestEngine(t)
	if _, err := db.DB().ExecContext(context.Background(), "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("failed to create users table: %v", err)
	}

	runtime := newRegisteredIndexRuntime(t, db, dsn, "users", true, "ux_users_name", []string{"name"}, WithIndexPolicy(SchemaPolicyCreateMissing))
	definition, found := inspectRegisteredIndex(t, runtime, "users", "ux_users_name")
	if !found {
		t.Fatal("expected upsert mode to create missing index")
	}
	if !definition.Unique || len(definition.Fields) != 1 || definition.Fields[0] != "name" {
		t.Fatalf("unexpected sqlite index definition: %#v", definition)
	}
}

func TestNewRuntimeValidateModeAcceptsExistingRegisteredIndex(t *testing.T) {
	db, dsn := newSQLiteIndexTestEngine(t)
	statements := []string{"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", "CREATE UNIQUE INDEX ux_users_name ON users(name)"}
	for _, statement := range statements {
		if _, err := db.DB().ExecContext(context.Background(), statement); err != nil {
			t.Fatalf("failed to execute setup statement %q: %v", statement, err)
		}
	}

	if _, err := Open(context.Background(),
		"sqlite",
		dsn,
		[]Table{registered(mustStrictMockTable(t, "users", "name"), nil, []TableIndex{{Name: "ux_users_name", Fields: []string{"name"}, Unique: true}}...)},
		WithIndexPolicy(SchemaPolicyValidate)); err != nil {
		t.Fatalf("expected validate mode with existing index to succeed, got %v", err)
	}
}

func TestNewRuntimePersistsIndexModeOnEngine(t *testing.T) {
	db, dsn := newSQLiteIndexTestEngine(t)
	statements := []string{"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", "CREATE UNIQUE INDEX ux_users_name ON users(name)"}
	for _, statement := range statements {
		if _, err := db.DB().ExecContext(context.Background(), statement); err != nil {
			t.Fatalf("failed to execute setup statement %q: %v", statement, err)
		}
	}

	runtime := newRegisteredIndexRuntime(t, db, dsn, "users", true, "ux_users_name", []string{"name"}, WithIndexPolicy(SchemaPolicyValidate))
	if runtime.indexPolicy != SchemaPolicyValidate {
		t.Fatalf("expected runtime index policy %q after init, got %q", SchemaPolicyValidate, runtime.indexPolicy)
	}
}
