package tsq

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"

	tsqdialect "github.com/tmoeish/tsq/v4/dialect"
)

// ownedTable is a minimal generated-shaped table whose physical name is chosen per
// test, so one test can stand up two runtimes that manage disjoint tables.
type ownedTable struct {
	ID   int64
	Name string

	physical string
}

func (t ownedTable) TSQOwner() {}

func (t ownedTable) Table() string { return t.physical }

func (t ownedTable) Cols() []SQLColumn { return SQLColumns(ownedTableColumns(t.physical)...) }

func (ownedTable) SearchColumns() []SearchColumn { return nil }

func (ownedTable) PrimaryKeys() []string { return []string{"id"} }

func (ownedTable) AutoIncrement() bool { return true }

func (ownedTable) VersionColumn() string { return "" }

func ownedTableColumns(physical string) []BoundColumn[ownedTable] {
	return []BoundColumn[ownedTable]{
		NewColForTableTest[ownedTable, int64](ownedTable{physical: physical}, "id", func(t *ownedTable) *int64 { return &t.ID }),
		NewColForTableTest[ownedTable, string](ownedTable{physical: physical}, "name", func(t *ownedTable) *string { return &t.Name }),
	}
}

// NewColForTableTest binds a column to a specific table value rather than the zero
// value of the owner type, which the exported NewCol always uses.
func NewColForTableTest[O Table, T any](table O, name string, pointer func(*O) *T) Column[O, T] {
	return newColForTable[O, T](table, name, name, toScanPointer(pointer))
}

func ownedRegistration(physical string) TableRegistration {
	return TableRegistration{
		Table: ownedTable{physical: physical},
		Columns: []tsqdialect.DDLColumnSpec{
			{Name: "id", Type: tsqdialect.DDLColumnType{Kind: tsqdialect.DDLColumnKindInt, Bits: 64}, PrimaryKey: true, AutoIncrement: true},
			{Name: "name", Type: tsqdialect.DDLColumnType{Kind: tsqdialect.DDLColumnKindString, Size: 64}},
		},
	}
}

func sharedSQLiteDSN(t *testing.T) string {
	t.Helper()

	return filepath.Join(t.TempDir(), "ownership.db")
}

func tableExists(t *testing.T, dsn, name string) bool {
	t.Helper()

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}

	defer func() { _ = db.Close() }()

	_, found, err := tsqdialect.SQLiteDialect{}.InspectTableColumns(context.Background(), db, name)
	if err != nil {
		t.Fatalf("inspect %s: %v", name, err)
	}

	return found
}

// TestManagedPolicyDoesNotDropAnotherOwnersTables is the regression gate for a
// data-loss bug: two runtimes managing different tables in one database used to erase
// each other's tables on every start, because the managed-table registry was global and
// each runtime overwrote it with its own subset.
func TestManagedPolicyDoesNotDropAnotherOwnersTables(t *testing.T) {
	dsn := sharedSQLiteDSN(t)
	ctx := context.Background()

	first, err := NewRuntimeContext(ctx, "sqlite", dsn,
		[]TableRegistration{ownedRegistration("service_a")},
		&RuntimeOptions{TablePolicy: SchemaPolicyManaged, IndexPolicy: SchemaPolicyManaged, SchemaOwner: "service_a"})
	if err != nil {
		t.Fatalf("bootstrap first runtime: %v", err)
	}

	if err := first.Close(); err != nil {
		t.Fatalf("close first runtime: %v", err)
	}

	if !tableExists(t, dsn, "service_a") {
		t.Fatal("expected the first runtime to create its table")
	}

	second, err := NewRuntimeContext(ctx, "sqlite", dsn,
		[]TableRegistration{ownedRegistration("service_b")},
		&RuntimeOptions{TablePolicy: SchemaPolicyManaged, IndexPolicy: SchemaPolicyManaged, SchemaOwner: "service_b"})
	if err != nil {
		t.Fatalf("bootstrap second runtime: %v", err)
	}

	if err := second.Close(); err != nil {
		t.Fatalf("close second runtime: %v", err)
	}

	if !tableExists(t, dsn, "service_a") {
		t.Fatal("the second runtime dropped a table it does not own")
	}

	if !tableExists(t, dsn, "service_b") {
		t.Fatal("expected the second runtime to create its table")
	}

	// Restarting the first runtime must not undo the second one either.
	again, err := NewRuntimeContext(ctx, "sqlite", dsn,
		[]TableRegistration{ownedRegistration("service_a")},
		&RuntimeOptions{TablePolicy: SchemaPolicyManaged, IndexPolicy: SchemaPolicyManaged, SchemaOwner: "service_a"})
	if err != nil {
		t.Fatalf("restart first runtime: %v", err)
	}

	if err := again.Close(); err != nil {
		t.Fatalf("close restarted runtime: %v", err)
	}

	if !tableExists(t, dsn, "service_b") {
		t.Fatal("the restarted first runtime dropped a table it does not own")
	}
}

// TestManagedPolicyStillDropsItsOwnRetiredTables keeps the feature: dropping a table the
// same owner recorded and no longer declares is exactly what SchemaPolicyManaged is for.
func TestManagedPolicyStillDropsItsOwnRetiredTables(t *testing.T) {
	dsn := sharedSQLiteDSN(t)
	ctx := context.Background()

	opts := &RuntimeOptions{TablePolicy: SchemaPolicyManaged, IndexPolicy: SchemaPolicyManaged, SchemaOwner: "service_a"}

	first, err := NewRuntimeContext(ctx, "sqlite", dsn,
		[]TableRegistration{ownedRegistration("kept"), ownedRegistration("retired")}, opts)
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	_ = first.Close()

	if !tableExists(t, dsn, "retired") {
		t.Fatal("expected both tables to be created")
	}

	second, err := NewRuntimeContext(ctx, "sqlite", dsn,
		[]TableRegistration{ownedRegistration("kept")}, opts)
	if err != nil {
		t.Fatalf("restart without the retired table: %v", err)
	}

	_ = second.Close()

	if tableExists(t, dsn, "retired") {
		t.Fatal("expected the owner's undeclared table to be dropped")
	}

	if !tableExists(t, dsn, "kept") {
		t.Fatal("expected the still-declared table to survive")
	}
}

// TestManagedRegistryMigratesFromTheOwnerlessShape covers an upgrade in place: a
// database written by an older TSQ has a registry with only table_name, and a primary
// key on it that would stop two owners from recording the same table.
func TestManagedRegistryMigratesFromTheOwnerlessShape(t *testing.T) {
	dsn := sharedSQLiteDSN(t)
	ctx := context.Background()

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}

	for _, statement := range []string{
		fmt.Sprintf(`CREATE TABLE %q ("table_name" VARCHAR(255) PRIMARY KEY)`, managedTablesRegistryName),
		fmt.Sprintf(`INSERT INTO %q ("table_name") VALUES ('legacy')`, managedTablesRegistryName),
		`CREATE TABLE "legacy" ("id" INTEGER PRIMARY KEY AUTOINCREMENT)`,
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			t.Fatalf("seed legacy registry: %v", err)
		}
	}

	_ = db.Close()

	rt, err := NewRuntimeContext(ctx, "sqlite", dsn,
		[]TableRegistration{ownedRegistration("current")},
		&RuntimeOptions{TablePolicy: SchemaPolicyManaged, IndexPolicy: SchemaPolicyManaged})
	if err != nil {
		t.Fatalf("bootstrap over a legacy registry: %v", err)
	}

	defer func() { _ = rt.Close() }()

	// The legacy row was attributed to the default owner, which is what a
	// single-runtime deployment already was, so the retired table is still dropped.
	if tableExists(t, dsn, "legacy") {
		t.Fatal("expected the migrated legacy row to still be managed by the default owner")
	}

	owners, err := rt.loadManagedTableRegistry(ctx)
	if err != nil {
		t.Fatalf("load migrated registry: %v", err)
	}

	if len(owners) != 1 || owners[0] != "current" {
		t.Fatalf("expected the migrated registry to hold only the current table, got %v", owners)
	}
}

// TestResolveSchemaOwnerRejectsUnusableValues keeps the owner a plain identifier: it
// travels the same DDL and comparison paths as a table name.
func TestResolveSchemaOwnerRejectsUnusableValues(t *testing.T) {
	if got, err := resolveSchemaOwner("  "); err != nil || got != defaultSchemaOwner {
		t.Fatalf("blank owner should default, got %q %v", got, err)
	}

	if _, err := resolveSchemaOwner("bad owner"); err == nil {
		t.Fatal("expected an owner with a space to be rejected")
	}

	if _, err := resolveSchemaOwner("drop;table"); err == nil {
		t.Fatal("expected an owner with punctuation to be rejected")
	}
}
