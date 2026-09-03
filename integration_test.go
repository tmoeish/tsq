package tsq_test

// Integration tests against real MySQL and PostgreSQL servers.
//
// The unit suite only ever talks to SQLite, so these tests are the only automated
// coverage of dialect/mysql.go and dialect/postgres.go: schema reconcile, index
// management, driver error classification, and the capability bits. They run
// whenever TSQ_MYSQL_DSN / TSQ_POSTGRES_DSN are set (CI's Integration job sets
// both) and skip otherwise, so `go test ./...` stays self-contained locally. The
// SQLite target always runs.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"

	"github.com/tmoeish/tsq/v4"
	tsqdialect "github.com/tmoeish/tsq/v4/dialect"
	"github.com/tmoeish/tsq/v4/examples/academy"
)

type integrationTarget struct {
	name   string
	driver string
	dsn    string
}

// integrationTargets lists the databases to run against. SQLite is always included
// so the suite itself is exercised on every `go test ./...`.
func integrationTargets(t *testing.T) []integrationTarget {
	t.Helper()

	targets := []integrationTarget{{
		name:   "sqlite",
		driver: "sqlite",
		dsn:    filepath.Join(t.TempDir(), "integration.db"),
	}}

	if dsn := os.Getenv("TSQ_MYSQL_DSN"); dsn != "" {
		targets = append(targets, integrationTarget{name: "mysql", driver: "mysql", dsn: dsn})
	}

	if dsn := os.Getenv("TSQ_POSTGRES_DSN"); dsn != "" {
		targets = append(targets, integrationTarget{name: "postgres", driver: "pgx", dsn: dsn})
	}

	return targets
}

func requireExternalTargets(t *testing.T, targets []integrationTarget) {
	t.Helper()

	if len(targets) == 1 {
		t.Skip("set TSQ_MYSQL_DSN and/or TSQ_POSTGRES_DSN to run against real servers")
	}
}

// ddlRecorder counts "applied ddl" log records: the runtime emits exactly one per
// executed schema statement, which makes "how much DDL did bootstrap run" observable.
type ddlRecorder struct {
	mu      sync.Mutex
	applied []string
}

func (l *ddlRecorder) Enabled(context.Context, slog.Level) bool { return true }

func (l *ddlRecorder) LogAttrs(_ context.Context, _ slog.Level, msg string, attrs ...slog.Attr) {
	if msg != "applied ddl" {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	for _, attr := range attrs {
		if attr.Key == "ddl" {
			l.applied = append(l.applied, attr.Value.String())
		}
	}
}

func (l *ddlRecorder) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()

	return len(l.applied)
}

func (l *ddlRecorder) statements() []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return slices.Clone(l.applied)
}

var academyTableNames = []string{"enrollment", "course", "learner", "instructor", "track", "_tsq_managed_tables"}

// dropAcademyTables resets the target database so every test starts from nothing.
func dropAcademyTables(t *testing.T, target integrationTarget) {
	t.Helper()

	db, err := sql.Open(target.driver, target.dsn)
	if err != nil {
		t.Fatalf("open %s: %v", target.name, err)
	}
	defer func() { _ = db.Close() }()

	for _, name := range academyTableNames {
		if _, err := db.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+name); err != nil {
			t.Fatalf("drop %s on %s: %v", name, target.name, err)
		}
	}
}

func openManaged(t *testing.T, target integrationTarget, tables []tsq.TableRegistration, policy tsq.SchemaPolicy) (*tsq.Runtime, *ddlRecorder) {
	t.Helper()

	recorder := &ddlRecorder{}

	rt, err := tsq.NewRuntimeContext(context.Background(), target.driver, target.dsn, tables, &tsq.RuntimeOptions{
		TablePolicy: policy,
		IndexPolicy: policy,
		Logger:      recorder,
	})
	if err != nil {
		t.Fatalf("NewRuntimeContext(%s, %s) error = %v", target.name, policy, err)
	}

	t.Cleanup(func() { _ = rt.Close() })

	return rt, recorder
}

// cloneRegistrations deep-copies the column specs so a test can mutate one
// declaration without touching the package-level metadata.
func cloneRegistrations(tables []tsq.TableRegistration) []tsq.TableRegistration {
	cloned := make([]tsq.TableRegistration, len(tables))
	for i, table := range tables {
		cloned[i] = table
		cloned[i].Columns = slices.Clone(table.Columns)
		cloned[i].Indexes = slices.Clone(table.Indexes)
	}

	return cloned
}

func widenLearnerCompany(t *testing.T, tables []tsq.TableRegistration, size int) []tsq.TableRegistration {
	t.Helper()

	cloned := cloneRegistrations(tables)
	for i := range cloned {
		if cloned[i].Table.Table() != "learner" {
			continue
		}

		for j := range cloned[i].Columns {
			if cloned[i].Columns[j].Name == "company" {
				cloned[i].Columns[j].Type.Size = size

				return cloned
			}
		}
	}

	t.Fatal("learner.company column not found in academy registrations")

	return nil
}

func TestIntegrationManagedSchemaBootstrapIsIdempotent(t *testing.T) {
	for _, target := range integrationTargets(t) {
		t.Run(target.name, func(t *testing.T) {
			dropAcademyTables(t, target)

			_, first := openManaged(t, target, academy.TSQTables(), tsq.SchemaPolicyManaged)
			if first.count() == 0 {
				t.Fatal("expected first managed bootstrap to create tables and indexes")
			}

			// The second bootstrap must find nothing to do. Any statement here is a
			// type round-trip that does not close (v4.2.0 shipped several of those),
			// and it would run on every process start forever.
			_, second := openManaged(t, target, academy.TSQTables(), tsq.SchemaPolicyManaged)
			if second.count() != 0 {
				t.Fatalf("expected second managed bootstrap to apply no DDL, got:\n  %s",
					strings.Join(second.statements(), "\n  "))
			}
		})
	}
}

func TestIntegrationReconcileAltersOnlyTheChangedColumn(t *testing.T) {
	for _, target := range integrationTargets(t) {
		t.Run(target.name, func(t *testing.T) {
			dropAcademyTables(t, target)
			openManaged(t, target, academy.TSQTables(), tsq.SchemaPolicyManaged)

			widened := widenLearnerCompany(t, academy.TSQTables(), 200)

			rt, recorder := openManaged(t, target, widened, tsq.SchemaPolicyReconcile)
			if rt.SQLDialect().DDLAlterColumnMode() != tsqdialect.DDLAlterColumnRebuild && recorder.count() != 1 {
				t.Fatalf("expected exactly one ALTER for the widened column, got:\n  %s",
					strings.Join(recorder.statements(), "\n  "))
			}

			// Reconcile must not have dropped the auto-increment default (the v4.2.0
			// PostgreSQL incident): inserting without an ID still assigns one.
			learner := &academy.Learner{Name: "Ada", Email: "ada@example.com", Company: "Analytical Engines"}
			if err := learner.Insert(context.Background(), rt); err != nil {
				t.Fatalf("insert after reconcile: %v", err)
			}

			if learner.ID <= 0 {
				t.Fatalf("expected database-generated ID after reconcile, got %d", learner.ID)
			}

			_, converged := openManaged(t, target, widened, tsq.SchemaPolicyReconcile)
			if converged.count() != 0 {
				t.Fatalf("expected reconcile to converge, got repeated DDL:\n  %s",
					strings.Join(converged.statements(), "\n  "))
			}
		})
	}
}

func TestIntegrationCRUDOptimisticLockAndDuplicateKeys(t *testing.T) {
	for _, target := range integrationTargets(t) {
		t.Run(target.name, func(t *testing.T) {
			ctx := context.Background()

			dropAcademyTables(t, target)
			rt, _ := openManaged(t, target, academy.TSQTables(), tsq.SchemaPolicyManaged)

			enrollment := &academy.Enrollment{LearnerID: 1, CourseID: 1, Status: academy.EnrollmentStatusActive}
			if err := enrollment.Insert(ctx, rt); err != nil {
				t.Fatalf("insert enrollment: %v", err)
			}

			if enrollment.UID <= 0 {
				t.Fatalf("expected generated UID, got %d", enrollment.UID)
			}

			stale := *enrollment

			enrollment.Score = 90
			if err := enrollment.Update(ctx, rt); err != nil {
				t.Fatalf("update enrollment: %v", err)
			}

			if enrollment.Version != stale.Version+1 {
				t.Fatalf("expected version to advance from %d, got %d", stale.Version, enrollment.Version)
			}

			stale.Score = 10

			err := stale.Update(ctx, rt)
			if !tsq.IsOptimisticLockError(err) {
				t.Fatalf("expected optimistic lock conflict for stale update, got %v", err)
			}

			// Duplicate-key detection is per driver error type; IgnoreErrors is the
			// public path that depends on it.
			learners := []*academy.Learner{
				{Name: "Grace", Email: "grace@example.com", Company: "Navy"},
				{Name: "Grace again", Email: "grace@example.com", Company: "Navy"},
			}

			err = tsq.ChunkedInsert(ctx, rt, learners, &tsq.ChunkedInsertOptions{ChunkSize: 1, IgnoreErrors: true})
			if err != nil {
				t.Fatalf("expected duplicate key to be ignored, got %v", err)
			}

			count, err := tsq.Select(academy.Learner_ID).From(academy.TableLearner).MustBuild().Count(ctx, rt)
			if err != nil {
				t.Fatalf("count learners: %v", err)
			}

			if count != 1 {
				t.Fatalf("expected exactly one learner after ignored duplicate, got %d", count)
			}

			if err := learners[0].Delete(ctx, rt); err != nil {
				t.Fatalf("delete learner: %v", err)
			}
		})
	}
}

// TestIntegrationLockConflictsAreRetryable provokes a real lock-wait failure and
// checks that the driver's error is recognised as a retryable conflict. This is the
// path that silently broke for pgx v5 when the matcher was tied to pgx v4's type.
func TestIntegrationLockConflictsAreRetryable(t *testing.T) {
	targets := integrationTargets(t)
	requireExternalTargets(t, targets)

	for _, target := range targets {
		if target.name == "sqlite" {
			continue
		}

		t.Run(target.name, func(t *testing.T) {
			ctx := context.Background()

			dropAcademyTables(t, target)
			rt, _ := openManaged(t, target, academy.TSQTables(), tsq.SchemaPolicyManaged)

			track := &academy.Track{Name: "locks", Description: "lock probe", SkillItems: []byte(`[]`)}
			if err := track.Insert(ctx, rt); err != nil {
				t.Fatalf("insert track: %v", err)
			}

			holder, err := rt.DB().BeginTx(ctx, nil)
			if err != nil {
				t.Fatalf("begin holder tx: %v", err)
			}
			defer holder.Rollback() //nolint:errcheck // best-effort cleanup

			lockSQL := fmt.Sprintf("SELECT id FROM track WHERE id = %s FOR UPDATE", rt.SQLDialect().BindVar(0))
			if _, err := holder.ExecContext(ctx, lockSQL, track.ID); err != nil {
				t.Fatalf("hold row lock: %v", err)
			}

			contender, err := rt.DB().BeginTx(ctx, nil)
			if err != nil {
				t.Fatalf("begin contender tx: %v", err)
			}
			defer contender.Rollback() //nolint:errcheck // best-effort cleanup

			var contendSQL string

			switch target.name {
			case "mysql":
				// 1205 ER_LOCK_WAIT_TIMEOUT after one second instead of the 50s default.
				if _, err := contender.ExecContext(ctx, "SET SESSION innodb_lock_wait_timeout = 1"); err != nil {
					t.Fatalf("set lock wait timeout: %v", err)
				}

				contendSQL = lockSQL
			case "postgres":
				// 55P03 lock_not_available, raised immediately.
				contendSQL = lockSQL + " NOWAIT"
			}

			_, err = contender.ExecContext(ctx, contendSQL, track.ID)
			if err == nil {
				t.Fatal("expected contended row lock to fail")
			}

			if !tsq.IsRetryableTransactionConflictError(err) {
				t.Fatalf("expected %T to be classified as a retryable conflict: %v", err, err)
			}

			if !tsq.IsCommonTransactionRetryableError(err) {
				t.Fatalf("expected common retry predicate to accept lock conflict: %v", err)
			}
		})
	}
}

// TestIntegrationCapabilitiesExecute proves the capability bits against real
// engines: every capability a dialect advertises must actually execute there.
func TestIntegrationCapabilitiesExecute(t *testing.T) {
	for _, target := range integrationTargets(t) {
		t.Run(target.name, func(t *testing.T) {
			ctx := context.Background()

			dropAcademyTables(t, target)
			rt, _ := openManaged(t, target, academy.TSQTables(), tsq.SchemaPolicyManaged)

			learner := &academy.Learner{Name: "Linus", Email: "linus@example.com", Company: "Kernel"}
			if err := learner.Insert(ctx, rt); err != nil {
				t.Fatalf("insert learner: %v", err)
			}

			dialect := rt.SQLDialect()

			if dialect.SupportsCapability(tsqdialect.CapabilityCTE) {
				recent := tsq.CTE("recent_learners",
					tsq.Select(academy.Learner_ID).From(academy.TableLearner).Where(academy.Learner_ID.GTVal(0)))
				recentID := academy.Learner_ID.WithTable(recent)

				rows, err := tsq.Select(recentID).From(recent).MustBuild().List(ctx, rt)
				if err != nil {
					t.Fatalf("CTE advertised but failed on %s: %v", target.name, err)
				}

				if len(rows) != 1 {
					t.Fatalf("expected one row through the CTE, got %d", len(rows))
				}
			}

			if dialect.SupportsCapability(tsqdialect.CapabilityIntersect) {
				query := tsq.Select(academy.Learner_ID).From(academy.TableLearner).
					Intersect(tsq.Select(academy.Learner_ID).From(academy.TableLearner)).
					MustBuild()

				rows, err := query.List(ctx, rt)
				if err != nil {
					t.Fatalf("INTERSECT advertised but failed on %s: %v", target.name, err)
				}

				if len(rows) != 1 {
					t.Fatalf("expected one row from INTERSECT, got %d", len(rows))
				}
			}

			if dialect.SupportsCapability(tsqdialect.CapabilityExcept) {
				query := tsq.Select(academy.Learner_ID).From(academy.TableLearner).
					Except(tsq.Select(academy.Learner_ID).From(academy.TableLearner)).
					MustBuild()

				rows, err := query.List(ctx, rt)
				if err != nil {
					t.Fatalf("EXCEPT advertised but failed on %s: %v", target.name, err)
				}

				if len(rows) != 0 {
					t.Fatalf("expected no rows from EXCEPT, got %d", len(rows))
				}
			}

			if dialect.SupportsCapability(tsqdialect.CapabilityFullOuterJoin) {
				query := tsq.Select(academy.Learner_ID).From(academy.TableLearner).
					FullJoin(academy.TableEnrollment, academy.Learner_ID.EQ(academy.Enrollment_LearnerID)).
					MustBuild()

				rows, err := query.List(ctx, rt)
				if err != nil {
					t.Fatalf("FULL JOIN advertised but failed on %s: %v", target.name, err)
				}

				if len(rows) != 1 {
					t.Fatalf("expected one row from FULL JOIN, got %d", len(rows))
				}
			} else {
				query := tsq.Select(academy.Learner_ID).From(academy.TableLearner).
					FullJoin(academy.TableEnrollment, academy.Learner_ID.EQ(academy.Enrollment_LearnerID)).
					MustBuild()

				_, err := query.List(ctx, rt)
				if _, ok := errors.AsType[*tsqdialect.ErrUnsupportedCapability](err); !ok {
					t.Fatalf("expected ErrUnsupportedCapability for FULL JOIN on %s, got %v", target.name, err)
				}
			}
		})
	}
}

// TestIntegrationKeywordSearchEscapesWildcards runs keyword search against every
// configured server. The escape clause the keyword path emits is fixed into the SQL
// at Build time, so one spelling has to parse and behave identically on all three
// dialects; only a real server can prove that. It is also the regression gate for
// the SQLite half of the bug: SQLite has no default LIKE escape character, so
// escaping the keyword without declaring the escape character matched nothing.
func TestIntegrationKeywordSearchEscapesWildcards(t *testing.T) {
	for _, target := range integrationTargets(t) {
		t.Run(target.name, func(t *testing.T) {
			ctx := context.Background()

			dropAcademyTables(t, target)
			rt, _ := openManaged(t, target, academy.TSQTables(), tsq.SchemaPolicyManaged)

			seed := []*academy.Learner{
				{Name: "a_b", Email: "a_b@example.test", Company: "Literal Underscore"},
				{Name: "axb", Email: "axb@example.test", Company: "Wildcard Bait"},
				{Name: "100%", Email: "pct@example.test", Company: "Literal Percent"},
				{Name: "100x", Email: "pctx@example.test", Company: "Wildcard Bait"},
				{Name: "c~d", Email: "tilde@example.test", Company: "Literal Escape Char"},
			}
			for _, learner := range seed {
				if err := learner.Insert(ctx, rt); err != nil {
					t.Fatalf("insert learner %q: %v", learner.Name, err)
				}
			}

			for _, tc := range []struct {
				keyword string
				want    string
			}{
				{keyword: "a_b", want: "a_b"},
				{keyword: "100%", want: "100%"},
				{keyword: "c~d", want: "c~d"},
			} {
				resp, err := academy.QueryLearner.Page(ctx, rt, &tsq.PageRequest{
					Page:    1,
					Size:    10,
					Keyword: tc.keyword,
				})
				if err != nil {
					t.Fatalf("keyword %q on %s: %v", tc.keyword, target.name, err)
				}

				if resp.Total != 1 {
					names := make([]string, 0, len(resp.Data))
					for _, row := range resp.Data {
						names = append(names, row.Name)
					}

					t.Fatalf("keyword %q on %s matched %d rows, want 1: %v",
						tc.keyword, target.name, resp.Total, names)
				}

				if resp.Data[0].Name != tc.want {
					t.Fatalf("keyword %q on %s matched %q, want %q",
						tc.keyword, target.name, resp.Data[0].Name, tc.want)
				}
			}

			// Escaping must not turn substring search into equality.
			resp, err := academy.QueryLearner.Page(ctx, rt, &tsq.PageRequest{Page: 1, Size: 10, Keyword: "Wildcard"})
			if err != nil {
				t.Fatalf("substring keyword on %s: %v", target.name, err)
			}

			if resp.Total != 2 {
				t.Fatalf("substring keyword on %s matched %d rows, want 2", target.name, resp.Total)
			}
		})
	}
}

// TestIntegrationChunkedInsertIgnoresDuplicatesInsideTransaction is the gate for the
// in-transaction ignore-duplicates path, and it only means anything against a real
// PostgreSQL server.
//
// PostgreSQL aborts a transaction as soon as any statement in it fails and rejects
// every later statement with 25P02 until the transaction unwinds. "Catch the duplicate
// and keep going" therefore fails on PostgreSQL alone: the ignored duplicate poisons
// the rest of the batch, and the call comes back with an error that is not even a
// duplicate-key error. SQLite and MySQL keep the transaction usable, so the unit suite
// cannot see this.
func TestIntegrationChunkedInsertIgnoresDuplicatesInsideTransaction(t *testing.T) {
	for _, target := range integrationTargets(t) {
		t.Run(target.name, func(t *testing.T) {
			ctx := context.Background()

			dropAcademyTables(t, target)
			rt, _ := openManaged(t, target, academy.TSQTables(), tsq.SchemaPolicyManaged)

			learners := []*academy.Learner{
				{Name: "Ada", Email: "ada@example.test", Company: "Analytical"},
				{Name: "Ada again", Email: "ada@example.test", Company: "Analytical"},
				{Name: "Grace", Email: "grace@example.test", Company: "Navy"},
			}

			err := rt.WithTx(ctx, nil, func(ctx context.Context, txExec tsq.SQLExecutor) error {
				if err := tsq.ChunkedInsert(ctx, txExec, learners, &tsq.ChunkedInsertOptions{
					ChunkSize:    10,
					IgnoreErrors: true,
				}); err != nil {
					return err
				}

				// The transaction must still be usable after an ignored duplicate;
				// this is the statement PostgreSQL rejects with 25P02 when it is not.
				_, err := tsq.Select(academy.Learner_ID).From(academy.TableLearner).
					MustBuild().
					Count(ctx, txExec)

				return err
			})
			if err != nil {
				t.Fatalf("chunked insert with IgnoreErrors inside a transaction on %s: %v", target.name, err)
			}

			count, err := tsq.Select(academy.Learner_ID).From(academy.TableLearner).MustBuild().Count(ctx, rt)
			if err != nil {
				t.Fatalf("count learners: %v", err)
			}

			if count != 2 {
				t.Fatalf("expected the duplicate to be skipped and 2 rows committed on %s, got %d", target.name, count)
			}
		})
	}
}

// TestIntegrationManagedPolicyIsScopedToItsOwner runs the ownership rule against every
// configured server. The registry is a real table with real DDL behind it, and the
// migration from the ownerless shape rewrites it, so the dialect-specific halves
// (inspection, DROP, re-create) only get exercised here.
func TestIntegrationManagedPolicyIsScopedToItsOwner(t *testing.T) {
	for _, target := range integrationTargets(t) {
		t.Run(target.name, func(t *testing.T) {
			ctx := context.Background()

			dropAcademyTables(t, target)

			// One runtime manages the academy tables under its own owner.
			academyRT, err := tsq.NewRuntimeContext(ctx, target.driver, target.dsn, academy.TSQTables(),
				&tsq.RuntimeOptions{
					TablePolicy: tsq.SchemaPolicyManaged,
					IndexPolicy: tsq.SchemaPolicyManaged,
					SchemaOwner: "academy",
				})
			if err != nil {
				t.Fatalf("bootstrap academy runtime on %s: %v", target.name, err)
			}

			t.Cleanup(func() { _ = academyRT.Close() })

			// A second runtime manages nothing at all under a different owner. Before
			// ownership existed this wiped every academy table on the way through.
			otherRT, err := tsq.NewRuntimeContext(ctx, target.driver, target.dsn, nil,
				&tsq.RuntimeOptions{
					TablePolicy: tsq.SchemaPolicyManaged,
					IndexPolicy: tsq.SchemaPolicyManaged,
					SchemaOwner: "other_service",
				})
			if err != nil {
				t.Fatalf("bootstrap second runtime on %s: %v", target.name, err)
			}

			if err := otherRT.Close(); err != nil {
				t.Fatalf("close second runtime: %v", err)
			}

			for _, name := range []string{"learner", "course", "enrollment"} {
				_, found, err := academyRT.SQLDialect().InspectTableColumns(ctx, academyRT.DB(), name)
				if err != nil {
					t.Fatalf("inspect %s on %s: %v", name, target.name, err)
				}

				if !found {
					t.Fatalf("table %s was dropped by a runtime that does not own it on %s", name, target.name)
				}
			}
		})
	}
}

// TestIntegrationMutationsByCondition runs UPDATE ... WHERE and DELETE ... WHERE on
// every target. The statement keeps table-qualified column references in WHERE
// and bare column names in SET, and that exact shape has to parse on all three
// dialects; a rendered-string assertion cannot prove that.
func TestIntegrationMutationsByCondition(t *testing.T) {
	for _, target := range integrationTargets(t) {
		t.Run(target.name, func(t *testing.T) {
			ctx := context.Background()

			dropAcademyTables(t, target)
			rt, _ := openManaged(t, target, academy.TSQTables(), tsq.SchemaPolicyManaged)

			rows := []*academy.Enrollment{
				{LearnerID: 1, CourseID: 1, Status: academy.EnrollmentStatusActive},
				{LearnerID: 2, CourseID: 1, Status: academy.EnrollmentStatusActive},
				{LearnerID: 3, CourseID: 2, Status: academy.EnrollmentStatusActive},
			}

			for _, row := range rows {
				if err := row.Insert(ctx, rt); err != nil {
					t.Fatalf("insert enrollment: %v", err)
				}
			}

			stale := *rows[0]

			affected, err := tsq.UpdateTable[academy.Enrollment]().
				SetVal(academy.Enrollment_Status, academy.EnrollmentStatusCompleted).
				SetVar(academy.Enrollment_Score).
				Where(academy.Enrollment_CourseID.EQVar(), academy.Enrollment_DeletedAt.EQVal(0)).
				Exec(ctx, rt, int64(88), int64(1))
			if err != nil {
				t.Fatalf("bulk update: %v", err)
			}

			if affected != 2 {
				t.Fatalf("expected 2 rows updated, got %d", affected)
			}

			reloaded, err := academy.QueryEnrollmentByUID.GetOrErr(ctx, rt, rows[0].UID)
			if err != nil {
				t.Fatalf("reload enrollment: %v", err)
			}

			if reloaded.Score != 88 || reloaded.Status != academy.EnrollmentStatusCompleted {
				t.Fatalf("bulk update did not apply: %+v", reloaded)
			}

			if reloaded.Version != stale.Version+1 {
				t.Fatalf("expected bulk update to advance version from %d, got %d", stale.Version, reloaded.Version)
			}

			stale.Score = 1
			if err := stale.Update(ctx, rt); !tsq.IsOptimisticLockError(err) {
				t.Fatalf("expected the pre-bulk row to conflict, got %v", err)
			}

			affected, err = tsq.DeleteFrom[academy.Enrollment]().
				Where(academy.Enrollment_UID.InVar()).
				Exec(ctx, rt, []int64{rows[1].UID, rows[2].UID})
			if err != nil {
				t.Fatalf("bulk delete: %v", err)
			}

			if affected != 2 {
				t.Fatalf("expected 2 rows deleted, got %d", affected)
			}

			remaining, err := tsq.Select(academy.Enrollment_UID).From(academy.TableEnrollment).MustBuild().Count(ctx, rt)
			if err != nil {
				t.Fatalf("count enrollments: %v", err)
			}

			if remaining != 1 {
				t.Fatalf("expected 1 enrollment left, got %d", remaining)
			}
		})
	}
}
