package tsq

import (
	"context"
	"database/sql"
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

func (ownedTable) PrimaryKey() string { return "id" }

func (ownedTable) AutoIncrement() bool { return true }

func (ownedTable) ManagedColumns() ManagedColumns { return ManagedColumns{} }

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

// TestSchemaPoliciesNeverDropAnotherRuntimesTables is the regression gate for a
// data-loss bug: two runtimes managing different tables in one database erased
// each other's tables on every start, because a global registry recorded "the
// tables TSQ manages" while each runtime overwrote it with its own subset.
//
// The fix in v5 is structural rather than bookkeeping. TSQ only ever adds, so a
// runtime has nothing to decide about a table it does not declare: a global
// truth written by a writer that only knows a local truth is data loss waiting
// for a second writer.
func TestSchemaPoliciesNeverDropAnotherRuntimesTables(t *testing.T) {
	for _, policy := range []SchemaPolicy{SchemaPolicyCreateMissing, SchemaPolicyReconcile} {
		t.Run(string(policy), func(t *testing.T) {
			dsn := sharedSQLiteDSN(t)
			ctx := context.Background()

			first, err := NewRuntimeContext(ctx, "sqlite", dsn,
				[]TableRegistration{ownedRegistration("service_a")},
				&RuntimeOptions{TablePolicy: policy, IndexPolicy: policy})
			if err != nil {
				t.Fatalf("start first runtime: %v", err)
			}

			if err := first.Close(); err != nil {
				t.Fatalf("close first runtime: %v", err)
			}

			if !tableExists(t, dsn, "service_a") {
				t.Fatal("expected the first runtime to create its own table")
			}

			second, err := NewRuntimeContext(ctx, "sqlite", dsn,
				[]TableRegistration{ownedRegistration("service_b")},
				&RuntimeOptions{TablePolicy: policy, IndexPolicy: policy})
			if err != nil {
				t.Fatalf("start second runtime: %v", err)
			}

			if err := second.Close(); err != nil {
				t.Fatalf("close second runtime: %v", err)
			}

			if !tableExists(t, dsn, "service_a") {
				t.Fatal("the second runtime dropped a table it never declared")
			}

			if !tableExists(t, dsn, "service_b") {
				t.Fatal("expected the second runtime to create its own table")
			}

			// Restarting the first one must not undo the second one either.
			again, err := NewRuntimeContext(ctx, "sqlite", dsn,
				[]TableRegistration{ownedRegistration("service_a")},
				&RuntimeOptions{TablePolicy: policy, IndexPolicy: policy})
			if err != nil {
				t.Fatalf("restart first runtime: %v", err)
			}

			if err := again.Close(); err != nil {
				t.Fatalf("close restarted runtime: %v", err)
			}

			for _, name := range []string{"service_a", "service_b"} {
				if !tableExists(t, dsn, name) {
					t.Fatalf("table %s disappeared after a restart", name)
				}
			}
		})
	}
}

// TestNoManagedRegistryTableIsCreated pins the other half of the fix: the global
// bookkeeping table is gone, so nothing can overwrite it.
func TestNoManagedRegistryTableIsCreated(t *testing.T) {
	dsn := sharedSQLiteDSN(t)

	runtime, err := NewRuntimeContext(context.Background(), "sqlite", dsn,
		[]TableRegistration{ownedRegistration("service_a")},
		&RuntimeOptions{TablePolicy: SchemaPolicyReconcile, IndexPolicy: SchemaPolicyReconcile})
	if err != nil {
		t.Fatalf("start runtime: %v", err)
	}

	if err := runtime.Close(); err != nil {
		t.Fatalf("close runtime: %v", err)
	}

	if tableExists(t, dsn, "_tsq_managed_tables") {
		t.Fatal("the managed-table registry is back; it was global state written from a local view")
	}
}
