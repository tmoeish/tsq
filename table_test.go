package tsq

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestBuildRegisteredTablesRejectsNilInputs(t *testing.T) {
	tests := []struct {
		name          string
		registrations []TableRegistration
		expectedError RegistrationErrorType
	}{
		{
			name:          "nil table",
			registrations: []TableRegistration{{Table: nil}},
			expectedError: RegistrationErrorNilTable,
		},
		{
			name: "invalid index metadata",
			registrations: []TableRegistration{{
				Table:   newMockTable("users"),
				Indexes: []TableIndex{{Name: "idx_users_missing", Fields: []string{"missing"}}},
			}},
			expectedError: RegistrationErrorInvalidIndex,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := buildRegisteredTables(tt.registrations)
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

func TestBuildRegisteredTablesRejectsDuplicate(t *testing.T) {
	table := newMockTable("users")

	_, err := buildRegisteredTables([]TableRegistration{
		{Table: table},
		{Table: table},
	})
	if err == nil {
		t.Fatal("expected duplicate table registration to fail")
	}

	var regErr *RegistrationError
	if !errors.As(err, &regErr) {
		t.Fatalf("expected RegistrationError, got %T", err)
	}
	if regErr.Type != RegistrationErrorDuplicate {
		t.Errorf("expected error type %v, got %v", RegistrationErrorDuplicate, regErr.Type)
	}
}

func TestBuildRegisteredTablesReturnsDeterministicOrder(t *testing.T) {
	snapshot, err := buildRegisteredTables([]TableRegistration{
		{Table: newMockTable("users")},
		{Table: newMockTable("accounts")},
	})
	if err != nil {
		t.Fatalf("buildRegisteredTables() error = %v", err)
	}

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

func newSQLiteIndexTestEngine(t *testing.T) (*Runtime, string) {
	t.Helper()

	dsn := filepath.Join(t.TempDir(), "test.db")
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatalf("failed to open sqlite database: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	return newRuntimeWithDB(db, SQLiteDialect{}), dsn
}

func TestCurrentDialectDetection(t *testing.T) {
	_, dsn := newSQLiteIndexTestEngine(t)
	r, err := NewRuntime("sqlite3", dsn, nil)
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}

	dialect := r.SQLDialect()
	if dialect == nil {
		t.Errorf("expected non-nil dialect after NewRuntime with SQLite")
	}
	if dialect != nil && dialect.Name() != DialectSQLite {
		t.Logf("detected dialect: %s", dialect.Name())
	}
}

func TestRuntimeEngineAccess(t *testing.T) {
	db, dsn := newSQLiteIndexTestEngine(t)
	r, err := NewRuntime("sqlite3", dsn, nil)
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}

	currentDB := r.DB()
	if currentDB == nil {
		t.Errorf("expected non-nil DB after NewRuntime, got nil")
	}
	if r.SQLDialect() == nil || r.SQLDialect().Name() != db.SQLDialect().Name() {
		t.Errorf("expected runtime to resolve the same dialect")
	}
}

func TestNewRuntimeFailsOnStrictValidation(t *testing.T) {
	longTableName := firstRejectedIdentifier(t, MySQLDialect{}, "u")
	runtime := &Runtime{
		db:      &sql.DB{},
		dialect: MySQLDialect{},
		tables: []*registeredTable{{
			Table: newMockTable(longTableName),
		}},
	}

	err := runtime.validateRegisteredTableIdentifiers("strict")
	if err == nil {
		t.Fatal("expected strict identifier validation to fail")
	}
}

func TestNewRuntimeFailsOnMissingRegisteredIndex(t *testing.T) {
	failingDB, dsn := newSQLiteIndexTestEngine(t)
	if _, err := failingDB.DB().ExecContext(context.Background(), "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("failed to create users table: %v", err)
	}

	table, _ := newStrictMockTable("users", "name")
	_, err := NewRuntime(
		"sqlite3",
		dsn,
		[]TableRegistration{{
			Table:   table,
			Indexes: []TableIndex{{Name: "ux_users_name", Fields: []string{"name"}, Unique: true}},
		}},
		&RuntimeOptions{IndexPolicy: SchemaPolicyValidate},
	)
	if err == nil {
		t.Fatal("expected missing registered index to fail NewRuntime")
	}
}
