package tsq

import (
	"context"
	"errors"
	"strings"
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
		[]Table{registered(table, nil, []TableIndex{{Name: indexName, Columns: fields, Unique: unique}}...)},
		options...)
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}

	return runtime
}

func TestNewRuntimeRejectsNilDB(t *testing.T) {
	if _, err := Open(context.Background(), "", "", nil); err == nil {
		t.Fatal("expected empty driver/dsn to return an error")
	}
}

// TestIndexPolicyChecksDeclaredIndexes covers the index checks at startup, on the
// path a runtime takes (applyIndexPolicyForTable). They used to be asserted
// against upsertIndex, a second copy of that logic nothing called, which had
// already drifted from the real one.
func TestIndexPolicyChecksDeclaredIndexes(t *testing.T) {
	for name, tt := range map[string]struct {
		setup   []string
		table   string
		columns []string
		index   TableIndex
		wantErr string
	}{
		"invalid index name": {
			setup: []string{"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"},
			table: "users", columns: []string{"name"},
			index:   TableIndex{Name: "idx users name", Columns: []string{"name"}},
			wantErr: "index name: invalid SQL identifier",
		},
		"index name used by another table": {
			setup: []string{
				"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
				"CREATE TABLE orgs (id INTEGER PRIMARY KEY, name TEXT)",
				"CREATE UNIQUE INDEX ux_name ON users(name)",
			},
			table: "orgs", columns: []string{"name"},
			index:   TableIndex{Name: "ux_name", Columns: []string{"name"}, Unique: true},
			wantErr: "already exists",
		},
		"definition mismatch": {
			setup: []string{
				"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)",
				"CREATE UNIQUE INDEX ux_users_name ON users(email)",
			},
			table: "users", columns: []string{"name", "email"},
			index:   TableIndex{Name: "ux_users_name", Columns: []string{"name"}, Unique: true},
			wantErr: "has definition",
		},
		"matching definition": {
			setup: []string{
				"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
				"CREATE UNIQUE INDEX ux_users_name ON users(name)",
			},
			table: "users", columns: []string{"name"},
			index: TableIndex{Name: "ux_users_name", Columns: []string{"name"}, Unique: true},
		},
	} {
		t.Run(name, func(t *testing.T) {
			db, dsn := newSQLiteIndexTestEngine(t)

			for _, statement := range tt.setup {
				if _, err := db.DB().ExecContext(context.Background(), statement); err != nil {
					t.Fatalf("%s: %v", statement, err)
				}
			}

			table, _ := newStrictMockTable(tt.table, tt.columns...)

			rt, err := Open(context.Background(), "sqlite", dsn, []Table{registered(table, nil, tt.index)}, WithIndexPolicy(SchemaPolicyCreateMissing))
			if rt != nil {
				_ = rt.Close()
			}

			switch {
			case tt.wantErr == "" && err != nil:
				t.Fatalf("Open = %v; want the matching index accepted", err)
			case tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)):
				t.Fatalf("Open = %v; want an error mentioning %q", err, tt.wantErr)
			}
		})
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
		[]Table{registered(mustStrictMockTable(t, "users", "name"), nil, []TableIndex{{Name: "ux_users_name", Columns: []string{"name"}, Unique: true}}...)},
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
		[]Table{registered(mustStrictMockTable(t, "users", "name"), nil, []TableIndex{{Name: "ux_users_name", Columns: []string{"name"}, Unique: true}}...)},
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
