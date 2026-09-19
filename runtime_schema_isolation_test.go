package tsq

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"

	sqld "github.com/tmoeish/tsq/v5/internal/sqldialect"
)

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

	_, found, err := sqld.SQLiteDialect{}.InspectColumns(context.Background(), db, name)
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

			first, err := Open(ctx, "sqlite", dsn,
				[]Table{namedTable("service_a")},
				WithTablePolicy(policy), WithIndexPolicy(policy))
			if err != nil {
				t.Fatalf("start first runtime: %v", err)
			}

			if err := first.Close(); err != nil {
				t.Fatalf("close first runtime: %v", err)
			}

			if !tableExists(t, dsn, "service_a") {
				t.Fatal("expected the first runtime to create its own table")
			}

			second, err := Open(ctx, "sqlite", dsn,
				[]Table{namedTable("service_b")},
				WithTablePolicy(policy), WithIndexPolicy(policy))
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
			again, err := Open(ctx, "sqlite", dsn,
				[]Table{namedTable("service_a")},
				WithTablePolicy(policy), WithIndexPolicy(policy))
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

	runtime, err := Open(context.Background(), "sqlite", dsn,
		[]Table{namedTable("service_a")},
		WithTablePolicy(SchemaPolicyReconcile), WithIndexPolicy(SchemaPolicyReconcile))
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
