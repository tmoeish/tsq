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
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	null "gopkg.in/nullbio/null.v6"
	_ "modernc.org/sqlite"

	"github.com/tmoeish/tsq/v5"
	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
	"github.com/tmoeish/tsq/v5/examples/academy"
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

func openWithPolicy(t *testing.T, target integrationTarget, tables []tsq.Table, policy tsq.SchemaPolicy) (*tsq.Runtime, *ddlRecorder) {
	t.Helper()

	recorder := &ddlRecorder{}

	rt, err := tsq.Open(context.Background(), target.driver, target.dsn, tables, tsq.WithTablePolicy(policy), tsq.WithIndexPolicy(policy), tsq.WithLogger(recorder))
	if err != nil {
		t.Fatalf("NewRuntime(%s, %s) error = %v", target.name, policy, err)
	}

	t.Cleanup(func() { _ = rt.Close() })

	return rt, recorder
}

// widenedLearner redeclares the learner table with company widened to size, the
// way a changed struct tag would.
func widenedLearner(t *testing.T, size int) []tsq.Table {
	t.Helper()

	schema := academy.TableLearner.Schema()
	widened := false

	for i := range schema {
		if schema[i].Name == "company" {
			schema[i].Type.Size = size
			widened = true
		}
	}

	if !widened {
		t.Fatal("learner.company column not found")
	}

	h := tsq.NewTable[academy.Learner]("learner")
	id := tsq.NewColumn(h, "id", "id", func(r *academy.Learner) *int64 { return &r.ID })
	created := tsq.NewColumn(h, "created_at", "created_at", func(r *academy.Learner) *null.Time { return &r.CreatedAt })
	name := tsq.NewColumn(h, "name", "name", func(r *academy.Learner) *string { return &r.Name })
	email := tsq.NewColumn(h, "email", "email", func(r *academy.Learner) *string { return &r.Email })
	company := tsq.NewColumn(h, "company", "company", func(r *academy.Learner) *string { return &r.Company })

	learner := h.Define(tsq.TableSpec[academy.Learner]{
		Columns:       []tsq.BoundColumn[academy.Learner]{id, created, name, email, company},
		PrimaryKey:    id,
		AutoIncrement: true,
		CreatedAt:     created,
		Schema:        schema,
		Indexes:       academy.TableLearner.Indexes(),
	})

	tables := []tsq.Table{learner}
	for _, table := range academy.TSQTables() {
		if table.Name() != "learner" {
			tables = append(tables, table)
		}
	}

	return tables
}

func TestIntegrationManagedSchemaBootstrapIsIdempotent(t *testing.T) {
	for _, target := range integrationTargets(t) {
		t.Run(target.name, func(t *testing.T) {
			dropAcademyTables(t, target)

			_, first := openWithPolicy(t, target, academy.TSQTables(), tsq.SchemaPolicyReconcile)
			if first.count() == 0 {
				t.Fatal("expected first managed bootstrap to create tables and indexes")
			}

			// The second bootstrap must find nothing to do. Any statement here is a
			// type round-trip that does not close (v4.2.0 shipped several of those),
			// and it would run on every process start forever.
			_, second := openWithPolicy(t, target, academy.TSQTables(), tsq.SchemaPolicyReconcile)
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
			openWithPolicy(t, target, academy.TSQTables(), tsq.SchemaPolicyReconcile)

			widened := widenedLearner(t, 200)

			rt, recorder := openWithPolicy(t, target, widened, tsq.SchemaPolicyReconcile)
			if rt.Dialect().AlterMode() != tsqdialect.AlterRebuild && recorder.count() != 1 {
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

			_, converged := openWithPolicy(t, target, widened, tsq.SchemaPolicyReconcile)
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
			rt, _ := openWithPolicy(t, target, academy.TSQTables(), tsq.SchemaPolicyReconcile)

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

			// Duplicate-key detection is per driver error type; WithSkipDuplicates is the
			// public path that depends on it.
			learners := []*academy.Learner{
				{Name: "Grace", Email: "grace@example.com", Company: "Navy"},
				{Name: "Grace again", Email: "grace@example.com", Company: "Navy"},
			}

			err = academy.TableLearner.BatchInsert(ctx, rt, learners, tsq.WithBatchSize(1), tsq.WithSkipDuplicates())
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
			rt, _ := openWithPolicy(t, target, academy.TSQTables(), tsq.SchemaPolicyReconcile)

			track := &academy.Track{Name: "locks", Description: "lock probe", SkillItems: []byte(`[]`)}
			if err := track.Insert(ctx, rt); err != nil {
				t.Fatalf("insert track: %v", err)
			}

			holder, err := rt.DB().BeginTx(ctx, nil)
			if err != nil {
				t.Fatalf("begin holder tx: %v", err)
			}
			defer holder.Rollback() //nolint:errcheck // best-effort cleanup

			lockSQL := fmt.Sprintf("SELECT id FROM track WHERE id = %s FOR UPDATE", rt.Dialect().Placeholder(0))
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

			if !tsq.IsTxConflictError(err) {
				t.Fatalf("expected %T to be classified as a retryable conflict: %v", err, err)
			}

			if !tsq.IsRetryableTxError(err) {
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
			rt, _ := openWithPolicy(t, target, academy.TSQTables(), tsq.SchemaPolicyReconcile)

			learner := &academy.Learner{Name: "Linus", Email: "linus@example.com", Company: "Kernel"}
			if err := learner.Insert(ctx, rt); err != nil {
				t.Fatalf("insert learner: %v", err)
			}

			dialect := rt.Dialect()

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
				if _, ok := errors.AsType[*tsqdialect.UnsupportedCapabilityError](err); !ok {
					t.Fatalf("expected UnsupportedCapabilityError for FULL JOIN on %s, got %v", target.name, err)
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
			rt, _ := openWithPolicy(t, target, academy.TSQTables(), tsq.SchemaPolicyReconcile)

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
				resp, err := academy.QueryLearner.Page(ctx, rt, tsq.Paging{
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
			resp, err := academy.QueryLearner.Page(ctx, rt, tsq.Paging{Page: 1, Size: 10, Keyword: "Wildcard"})
			if err != nil {
				t.Fatalf("substring keyword on %s: %v", target.name, err)
			}

			if resp.Total != 2 {
				t.Fatalf("substring keyword on %s matched %d rows, want 2", target.name, resp.Total)
			}
		})
	}
}

// TestIntegrationBatchInsertIgnoresDuplicatesInsideTransaction is the gate for the
// in-transaction ignore-duplicates path, and it only means anything against a real
// PostgreSQL server.
//
// PostgreSQL aborts a transaction as soon as any statement in it fails and rejects
// every later statement with 25P02 until the transaction unwinds. "Catch the duplicate
// and keep going" therefore fails on PostgreSQL alone: the ignored duplicate poisons
// the rest of the batch, and the call comes back with an error that is not even a
// duplicate-key error. SQLite and MySQL keep the transaction usable, so the unit suite
// cannot see this.
func TestIntegrationBatchInsertIgnoresDuplicatesInsideTransaction(t *testing.T) {
	for _, target := range integrationTargets(t) {
		t.Run(target.name, func(t *testing.T) {
			ctx := context.Background()

			dropAcademyTables(t, target)
			rt, _ := openWithPolicy(t, target, academy.TSQTables(), tsq.SchemaPolicyReconcile)

			learners := []*academy.Learner{
				{Name: "Ada", Email: "ada@example.test", Company: "Analytical"},
				{Name: "Ada again", Email: "ada@example.test", Company: "Analytical"},
				{Name: "Grace", Email: "grace@example.test", Company: "Navy"},
			}

			err := rt.WithTx(ctx, nil, func(ctx context.Context, txExec tsq.Executor) error {
				if err := academy.TableLearner.BatchInsert(ctx, txExec, learners, tsq.WithBatchSize(10), tsq.WithSkipDuplicates()); err != nil {
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
				t.Fatalf("batch insert with WithSkipDuplicates inside a transaction on %s: %v", target.name, err)
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

// TestIntegrationSchemaPolicyNeverDropsUndeclaredTables runs the isolation rule
// against every configured server. TSQ only ever adds, so a runtime that declares
// nothing must leave every existing table alone; the SQLite unit test covers the
// same property, but only a real server proves the inspection half of it.
func TestIntegrationSchemaPolicyNeverDropsUndeclaredTables(t *testing.T) {
	for _, target := range integrationTargets(t) {
		t.Run(target.name, func(t *testing.T) {
			ctx := context.Background()

			dropAcademyTables(t, target)

			// One runtime manages the academy tables under its own owner.
			academyRT, err := tsq.Open(ctx, target.driver, target.dsn, academy.TSQTables(),
				tsq.WithTablePolicy(tsq.SchemaPolicyReconcile), tsq.WithIndexPolicy(tsq.SchemaPolicyReconcile))
			if err != nil {
				t.Fatalf("bootstrap academy runtime on %s: %v", target.name, err)
			}

			t.Cleanup(func() { _ = academyRT.Close() })

			// A second runtime declares nothing at all. It used to wipe every academy
			// table on the way through, because it recorded its own empty view into a
			// registry shared by the whole database.
			otherRT, err := tsq.Open(ctx, target.driver, target.dsn, nil,
				tsq.WithTablePolicy(tsq.SchemaPolicyReconcile), tsq.WithIndexPolicy(tsq.SchemaPolicyReconcile))
			if err != nil {
				t.Fatalf("bootstrap second runtime on %s: %v", target.name, err)
			}

			if err := otherRT.Close(); err != nil {
				t.Fatalf("close second runtime: %v", err)
			}

			for _, name := range []string{"learner", "course", "enrollment"} {
				_, found, err := academyRT.Dialect().InspectColumns(ctx, academyRT.DB(), name)
				if err != nil {
					t.Fatalf("inspect %s on %s: %v", name, target.name, err)
				}

				if !found {
					t.Fatalf("table %s was dropped by a runtime that never declared it on %s", name, target.name)
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
			rt, _ := openWithPolicy(t, target, academy.TSQTables(), tsq.SchemaPolicyReconcile)

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

			score := tsq.NewParam[int64]("score")

			affected, err := tsq.UpdateTable(academy.TableEnrollment).
				SetVal(academy.Enrollment_Status, academy.EnrollmentStatusCompleted).
				Set(academy.Enrollment_Score, score).
				Where(academy.Enrollment_CourseID.EQ(academy.Enrollment_CourseID.Param())).
				Exec(ctx, rt, score.Bind(88), academy.Enrollment_CourseID.Bind(1))
			if err != nil {
				t.Fatalf("bulk update: %v", err)
			}

			if affected != 2 {
				t.Fatalf("expected 2 rows updated, got %d", affected)
			}

			reloaded, err := academy.QueryEnrollmentByUID.Get(ctx, rt, academy.Enrollment_UID.Bind(rows[0].UID))
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

			beforeSoftDelete, err := academy.QueryEnrollmentByUID.Get(ctx, rt, academy.Enrollment_UID.Bind(rows[1].UID))
			if err != nil {
				t.Fatalf("reload enrollment before soft delete: %v", err)
			}

			// Enrollment declares deleted_at, so DeleteFrom renders an UPDATE that
			// stamps the tombstone. The rows stay in the table and leave every
			// generated query.
			affected, err = tsq.DeleteFrom(academy.TableEnrollment).
				Where(academy.Enrollment_UID.In(academy.Enrollment_UID.ListParam())).
				Exec(ctx, rt, academy.Enrollment_UID.BindList(rows[1].UID, rows[2].UID))
			if err != nil {
				t.Fatalf("bulk soft delete: %v", err)
			}

			if affected != 2 {
				t.Fatalf("expected 2 rows soft-deleted, got %d", affected)
			}

			active, err := academy.QueryEnrollment.Count(ctx, rt)
			if err != nil {
				t.Fatalf("count active enrollments: %v", err)
			}

			if active != 1 {
				t.Fatalf("expected 1 active enrollment left, got %d", active)
			}

			stored, err := tsq.Select(academy.Enrollment_UID).From(academy.TableEnrollment.WithDeleted()).MustBuild().Count(ctx, rt)
			if err != nil {
				t.Fatalf("count stored enrollments: %v", err)
			}

			if stored != 3 {
				t.Fatalf("expected soft delete to keep all 3 rows stored, got %d", stored)
			}

			// The soft-deleted rows carry a tombstone and an advanced version.
			tombstoned, err := tsq.Select(academy.Enrollment__Cols...).
				From(academy.TableEnrollment.WithDeleted()).
				Where(academy.Enrollment_UID.EQ(academy.Enrollment_UID.Param())).
				MustBuild().
				Get(ctx, rt, academy.Enrollment_UID.Bind(rows[1].UID))
			if err != nil {
				t.Fatalf("reload soft-deleted enrollment: %v", err)
			}

			if tombstoned.DeletedAt == 0 {
				t.Fatal("expected soft delete to stamp deleted_at")
			}

			if tombstoned.Version != beforeSoftDelete.Version+1 {
				t.Fatalf("expected soft delete to advance version from %d, got %d", beforeSoftDelete.Version, tombstoned.Version)
			}

			// HardDeleteFrom ignores deleted_at and removes the rows.
			affected, err = tsq.HardDeleteFrom(academy.TableEnrollment).
				Where(academy.Enrollment_UID.In(academy.Enrollment_UID.ListParam())).
				Exec(ctx, rt, academy.Enrollment_UID.BindList(rows[1].UID, rows[2].UID))
			if err != nil {
				t.Fatalf("bulk hard delete: %v", err)
			}

			if affected != 2 {
				t.Fatalf("expected 2 rows hard-deleted, got %d", affected)
			}

			stored, err = tsq.Select(academy.Enrollment_UID).From(academy.TableEnrollment.WithDeleted()).MustBuild().Count(ctx, rt)
			if err != nil {
				t.Fatalf("count stored enrollments: %v", err)
			}

			if stored != 1 {
				t.Fatalf("expected 1 enrollment left after hard delete, got %d", stored)
			}
		})
	}
}

// TestIntegrationSoftDeleteScopeJoins checks the soft-delete scope in every join
// position on real engines. A RIGHT or FULL JOIN renders the scoped table as a
// derived table, which is the spelling most likely to differ between dialects.
func TestIntegrationSoftDeleteScopeJoins(t *testing.T) {
	for _, target := range integrationTargets(t) {
		t.Run(target.name, func(t *testing.T) {
			ctx := context.Background()

			dropAcademyTables(t, target)
			rt, _ := openWithPolicy(t, target, academy.TSQTables(), tsq.SchemaPolicyReconcile)

			learners := []*academy.Learner{
				{Name: "Live", Email: "live@example.com"},
				{Name: "Gone", Email: "gone@example.com"},
			}
			if err := academy.TableLearner.BatchInsert(ctx, rt, learners); err != nil {
				t.Fatal(err)
			}

			enrollments := []*academy.Enrollment{
				{LearnerID: learners[0].ID, CourseID: 1},
				{LearnerID: learners[1].ID, CourseID: 1},
			}
			for _, e := range enrollments {
				if err := e.Insert(ctx, rt); err != nil {
					t.Fatal(err)
				}
			}

			if err := enrollments[1].Delete(ctx, rt); err != nil {
				t.Fatal(err)
			}

			on := academy.Enrollment_LearnerID.EQ(academy.Learner_ID)
			count := func(name string, stage tsq.QueryStage[academy.Learner], want int64) {
				t.Helper()

				n, err := stage.Count(ctx, rt)
				if err != nil {
					t.Fatalf("%s: %v", name, err)
				}

				if n != want {
					t.Errorf("%s = %d, want %d", name, n, want)
				}
			}

			from := func() tsq.JoinStage[academy.Learner] {
				return tsq.Select(academy.Learner_ID).From(academy.TableLearner)
			}

			count("inner join", from().Join(academy.TableEnrollment, on), 1)
			count("left join without a live enrollment", from().LeftJoin(academy.TableEnrollment, on).Where(academy.Enrollment_UID.IsNull()), 1)
			count("right join", from().RightJoin(academy.TableEnrollment, on), 1)
			count("inner join with deleted", from().Join(academy.TableEnrollment.WithDeleted(), on), 2)

			if rt.Dialect().SupportsCapability(tsqdialect.CapabilityFullOuterJoin) {
				count("full join", from().FullJoin(academy.TableEnrollment, on), 2)
			}
		})
	}
}

// TestIntegrationColumnFunctionsArePortable runs every column function on every
// target and checks the value. Functions are spelled differently per dialect (MySQL's
// LENGTH counts bytes, PostgreSQL rounds only NUMERIC, SQLite has no YEAR), so a
// rendered-string assertion proves nothing here.
func TestIntegrationColumnFunctionsArePortable(t *testing.T) {
	for _, target := range integrationTargets(t) {
		t.Run(target.name, func(t *testing.T) {
			ctx := context.Background()

			dropAcademyTables(t, target)
			rt, _ := openWithPolicy(t, target, academy.TSQTables(), tsq.SchemaPolicyReconcile)

			learner := &academy.Learner{Name: "  Ünïcödé  ", Email: "u@example.com", Company: "ACME"}
			if err := learner.Insert(ctx, rt); err != nil {
				t.Fatal(err)
			}

			at := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
			for _, score := range []int64{-7, 3, 3, 10} {
				e := &academy.Enrollment{LearnerID: learner.ID, CourseID: 1, Score: score}
				e.CreatedAt = at
				if err := e.Insert(ctx, rt); err != nil {
					t.Fatal(err)
				}
			}

			name := academy.Learner_Name
			str := func(col tsq.Column[academy.Learner, string], want string) {
				t.Helper()

				got, err := tsq.Select(col).From(academy.TableLearner).MustBuild().Scalar(ctx, rt, col)
				if err != nil || got != want {
					t.Errorf("%v = %q, %v; want %q", col, got, err, want)
				}
			}

			str(tsq.Trim(name), "Ünïcödé")
			// SQLite's UPPER and LOWER fold ASCII only, so they are checked on ASCII text.
			str(tsq.Lower(academy.Learner_Company), "acme")
			str(tsq.Upper(tsq.Lower(academy.Learner_Company)), "ACME")
			str(tsq.Substring(tsq.Trim(name), 2, 3), "nïc")
			str(tsq.NullIfVal(name, "x"), "  Ünïcödé  ")
			str(tsq.CoalesceVal(academy.Learner_Company, "none"), "ACME")

			length := tsq.Length(tsq.Trim(name))
			if n, err := tsq.Select(length).From(academy.TableLearner).MustBuild().Scalar(ctx, rt, length); err != nil || n != 7 {
				t.Errorf("Length() = %d, %v; want 7 characters", n, err)
			}

			score := academy.Enrollment_Score
			num := func(col tsq.Column[academy.Enrollment, int64], want int64) {
				t.Helper()

				got, err := tsq.Select(col).From(academy.TableEnrollment).MustBuild().Scalar(ctx, rt, col)
				if err != nil || got != want {
					t.Errorf("%v = %d, %v; want %d", col, got, err, want)
				}
			}

			created := academy.Enrollment_CreatedAt
			num(tsq.Sum(score), 9)
			num(tsq.Max(score), 10)
			num(tsq.Min(score), -7)
			num(tsq.Count(score), 4)
			num(tsq.CountDistinct(score), 3)
			num(tsq.Max(tsq.Year(created)), 2026)
			num(tsq.Max(tsq.Month(created)), 3)
			num(tsq.Max(tsq.Day(created)), 4)
			num(tsq.Abs(tsq.Min(score)), 7)

			dec := func(col tsq.Column[academy.Enrollment, float64], want float64) {
				t.Helper()

				got, err := tsq.Select(col).From(academy.TableEnrollment).MustBuild().Scalar(ctx, rt, col)
				if err != nil || got != want {
					t.Errorf("%v = %v, %v; want %v", col, got, err, want)
				}
			}

			dec(tsq.Avg(score), 2.25)
			dec(tsq.Round(tsq.Avg(score), 1), 2.3)
			dec(tsq.Ceil(tsq.Avg(score)), 3)
			dec(tsq.Floor(tsq.Avg(score)), 2)

			day := tsq.Max(tsq.Date(created))
			if got, err := tsq.Select(day).From(academy.TableEnrollment).MustBuild().Scalar(ctx, rt, day); err != nil || got != "2026-03-04" {
				t.Errorf("Date() = %q, %v", got, err)
			}

			distinct, err := tsq.SelectDistinct(score).From(academy.TableEnrollment).MustBuild().Count(ctx, rt)
			if err != nil || distinct != 3 {
				t.Errorf("SelectDistinct count = %d, %v; want 3", distinct, err)
			}
		})
	}
}
