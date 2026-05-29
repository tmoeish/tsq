package tsq

import (
	"context"
	"strings"
	"testing"

	tsqdialect "github.com/tmoeish/tsq/v4/dialect"
)

func TestNewRuntimeTablePolicyCreateMissingCreatesTable(t *testing.T) {
	db, dsn := newSQLiteIndexTestEngine(t)
	table, _ := newStrictMockTable("users", "id", "name")

	runtime, err := NewRuntime(
		"sqlite",
		dsn,
		[]TableRegistration{{
			Table: table,
			Columns: []tsqdialect.DDLColumnSpec{
				{
					Name:          "id",
					Type:          tsqdialect.DDLColumnType{Kind: tsqdialect.DDLColumnKindInt, Bits: 64},
					PrimaryKey:    true,
					AutoIncrement: true,
				},
				{
					Name: "name",
					Type: tsqdialect.DDLColumnType{Kind: tsqdialect.DDLColumnKindString, Size: 120},
				},
			},
		}},
		&RuntimeOptions{TablePolicy: SchemaPolicyCreateMissing},
	)
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}

	columns, found, err := runtime.SQLDialect().InspectTableColumns(context.Background(), db, "users")
	if err != nil {
		t.Fatalf("InspectTableColumns() error = %v", err)
	}
	if !found {
		t.Fatal("expected users table to be created")
	}
	if len(columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(columns))
	}
}

func TestNewRuntimeTablePolicyReconcileAddsMissingColumn(t *testing.T) {
	db, dsn := newSQLiteIndexTestEngine(t)
	if _, err := db.DB().ExecContext(context.Background(), `CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT)`); err != nil {
		t.Fatalf("failed to create seed table: %v", err)
	}

	table, _ := newStrictMockTable("users", "id", "name")
	runtime, err := NewRuntime(
		"sqlite",
		dsn,
		[]TableRegistration{{
			Table: table,
			Columns: []tsqdialect.DDLColumnSpec{
				{
					Name:          "id",
					Type:          tsqdialect.DDLColumnType{Kind: tsqdialect.DDLColumnKindInt, Bits: 64},
					PrimaryKey:    true,
					AutoIncrement: true,
				},
				{
					Name: "name",
					Type: tsqdialect.DDLColumnType{Kind: tsqdialect.DDLColumnKindString, Size: 120},
				},
			},
		}},
		&RuntimeOptions{TablePolicy: SchemaPolicyReconcile},
	)
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}

	columns, found, err := runtime.SQLDialect().InspectTableColumns(context.Background(), runtime, "users")
	if err != nil {
		t.Fatalf("InspectTableColumns() error = %v", err)
	}
	if !found {
		t.Fatal("expected users table to exist")
	}
	if len(columns) != 2 {
		t.Fatalf("expected reconcile to add missing column, got %d columns", len(columns))
	}
}

func TestNewRuntimeIndexPolicyManagedDropsUndeclaredIndexes(t *testing.T) {
	db, dsn := newSQLiteIndexTestEngine(t)
	statements := []string{
		`CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, email TEXT)`,
		`CREATE INDEX idx_users_email ON users(email)`,
	}
	for _, statement := range statements {
		if _, err := db.DB().ExecContext(context.Background(), statement); err != nil {
			t.Fatalf("failed to execute setup statement %q: %v", statement, err)
		}
	}

	table, _ := newStrictMockTable("users", "id", "name", "email")
	runtime, err := NewRuntime(
		"sqlite",
		dsn,
		[]TableRegistration{{
			Table: table,
			Indexes: []TableIndex{
				{Name: "idx_users_name", Fields: []string{"name"}},
			},
		}},
		&RuntimeOptions{IndexPolicy: SchemaPolicyManaged},
	)
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}

	_, found := inspectRegisteredIndex(t, runtime, "users", "idx_users_name")
	if !found {
		t.Fatal("expected managed policy to create declared index")
	}
	_, found = inspectRegisteredIndex(t, runtime, "users", "idx_users_email")
	if found {
		t.Fatal("expected managed policy to drop undeclared same-table index")
	}
}

func TestNewRuntimeTablePolicyManagedDropsOnlyTrackedTables(t *testing.T) {
	db, dsn := newSQLiteIndexTestEngine(t)
	if _, err := db.DB().ExecContext(context.Background(), `CREATE TABLE admins (id INTEGER PRIMARY KEY AUTOINCREMENT)`); err != nil {
		t.Fatalf("failed to create unrelated table: %v", err)
	}

	table, _ := newStrictMockTable("users", "id")
	_, err := NewRuntime(
		"sqlite",
		dsn,
		[]TableRegistration{{
			Table: table,
			Columns: []tsqdialect.DDLColumnSpec{{
				Name:          "id",
				Type:          tsqdialect.DDLColumnType{Kind: tsqdialect.DDLColumnKindInt, Bits: 64},
				PrimaryKey:    true,
				AutoIncrement: true,
			}},
		}},
		&RuntimeOptions{TablePolicy: SchemaPolicyManaged},
	)
	if err != nil {
		t.Fatalf("initial managed NewRuntime() error = %v", err)
	}

	runtime, err := NewRuntime(
		"sqlite",
		dsn,
		nil,
		&RuntimeOptions{TablePolicy: SchemaPolicyManaged},
	)
	if err != nil {
		t.Fatalf("second managed NewRuntime() error = %v", err)
	}

	if _, found, err := runtime.SQLDialect().InspectTableColumns(context.Background(), runtime, "users"); err != nil {
		t.Fatalf("InspectTableColumns(users) error = %v", err)
	} else if found {
		t.Fatal("expected tracked users table to be dropped")
	}

	if _, found, err := runtime.SQLDialect().InspectTableColumns(context.Background(), runtime, "admins"); err != nil {
		t.Fatalf("InspectTableColumns(admins) error = %v", err)
	} else if !found {
		t.Fatal("expected unrelated admins table to remain")
	}
}

func TestResolveRuntimeDialectRejectsLegacySQLite3DriverName(t *testing.T) {
	_, err := resolveRuntimeDialect("sqlite3")
	if err == nil {
		t.Fatal("expected sqlite3 driver name to be rejected")
	}
	if !strings.Contains(err.Error(), "expected sqlite, mysql, postgres, pgx, or pq") {
		t.Fatalf("unexpected error: %v", err)
	}
}
