package integration_test

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
	"encoding/json"
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

	"github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"

	"github.com/tmoeish/tsq/v5"
	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
	"github.com/tmoeish/tsq/v5/internal/integration/academy"
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

func (l *ddlRecorder) reset() {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.applied = nil
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

	schema := academy.TableLearner.ColumnSpecs()
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

	h := tsq.NewTable[academy.Learner, int64]("learner")
	id := tsq.NewColumn(h, "id", "id", func(r *academy.Learner) *int64 { return &r.ID })
	created := tsq.NewNullColumn[time.Time](h, "created_at", "created_at", func(r *academy.Learner) *sql.Null[time.Time] { return &r.CreatedAt })
	name := tsq.NewColumn(h, "name", "name", func(r *academy.Learner) *string { return &r.Name })
	email := tsq.NewColumn(h, "email", "email", func(r *academy.Learner) *string { return &r.Email })
	company := tsq.NewColumn(h, "company", "company", func(r *academy.Learner) *string { return &r.Company })

	learner := h.Define(tsq.TableSpec[academy.Learner, int64]{
		Columns:       []tsq.BoundColumn[academy.Learner]{id, created, name, email, company},
		PrimaryKey:    id,
		AutoIncrement: true,
		CreatedAt:     created,
		ColumnSpecs:   schema,
		Indexes:       academy.TableLearner.Indexes(),
	})

	tables := []tsq.Table{learner}
	for _, table := range academy.TSQTables() {
		if table.TableName() != "learner" {
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
			// SQLite rebuilds the table instead of altering the column.
			if rt.Dialect() != tsqdialect.SQLite && recorder.count() != 1 {
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

// TestIntegrationCreateMissingAddsAMissingColumn drops a declared column and
// starts again under CreateMissing, which is documented to add it back and used to
// refuse to start instead.
func TestIntegrationCreateMissingAddsAMissingColumn(t *testing.T) {
	for _, target := range integrationTargets(t) {
		t.Run(target.name, func(t *testing.T) {
			dropAcademyTables(t, target)
			rt, _ := openWithPolicy(t, target, academy.TSQTables(), tsq.SchemaPolicyReconcile)

			if _, err := rt.ExecContext(context.Background(), "ALTER TABLE track DROP COLUMN skill_items"); err != nil {
				t.Fatalf("drop column: %v", err)
			}

			// SQLite adds a NOT NULL column without a default by rebuilding the table
			// with it: it has no DROP DEFAULT to take the fill value away again.
			_, recorder := openWithPolicy(t, target, academy.TSQTables(), tsq.SchemaPolicyCreateMissing)
			if !slices.ContainsFunc(recorder.statements(), func(ddl string) bool {
				return strings.Contains(ddl, "ADD COLUMN") || (target.name == "sqlite" && strings.Contains(ddl, `"skill_items"`))
			}) {
				t.Fatalf("expected the column to be added, got:\n  %s", strings.Join(recorder.statements(), "\n  "))
			}

			_, again := openWithPolicy(t, target, academy.TSQTables(), tsq.SchemaPolicyReconcile)
			if again.count() != 0 {
				t.Fatalf("the added column differs from the declared one:\n  %s", strings.Join(again.statements(), "\n  "))
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

			count, err := tsq.Select(academy.TableLearner.ID).From(academy.TableLearner).MustBuild().Count(ctx, rt)
			if err != nil {
				t.Fatalf("count learners: %v", err)
			}

			if count != 1 {
				t.Fatalf("expected exactly one learner after ignored duplicate, got %d", count)
			}

			if err := learners[0].HardDelete(ctx, rt); err != nil {
				t.Fatalf("delete learner: %v", err)
			}
		})
	}
}

// TestIntegrationLockConflictsAreRetryable provokes a real lock-wait failure and
// checks that the driver's error is recognised as a retryable conflict. This is the
// path that silently broke for pgx v5 when the matcher was tied to pgx v4's type.
// TestIntegrationFetchByFollowsTheCollation checks the lookup against the
// database's own string comparison. MySQL's default collation is case-insensitive,
// so "ada@example.test" finds the row holding "Ada@Example.test", and FetchBy must
// return it rather than report it missing after comparing bytes in Go. PostgreSQL
// and SQLite compare case-sensitively and find nothing.
func TestIntegrationFetchByFollowsTheCollation(t *testing.T) {
	for _, target := range integrationTargets(t) {
		t.Run(target.name, func(t *testing.T) {
			ctx := context.Background()

			dropAcademyTables(t, target)
			rt, _ := openWithPolicy(t, target, academy.TSQTables(), tsq.SchemaPolicyReconcile)

			ada := &academy.Learner{Name: "Ada", Email: "Ada@Example.test", Company: "Analytical"}
			if err := ada.Insert(ctx, rt); err != nil {
				t.Fatalf("insert learner: %v", err)
			}

			fetched, fetchErr := academy.TableLearner.FetchByEmail(ctx, rt, "ada@example.test")
			got, getErr := academy.TableLearner.GetByEmail(ctx, rt, "ada@example.test")

			if target.driver == "mysql" {
				if fetchErr != nil || len(fetched) != 1 || fetched[0].ID != ada.ID {
					t.Fatalf("FetchByEmail under a case-insensitive collation = %v, %v", fetched, fetchErr)
				}

				if getErr != nil || got.ID != ada.ID {
					t.Fatalf("GetByEmail under a case-insensitive collation = %v, %v", got, getErr)
				}

				return
			}

			if !errors.Is(fetchErr, sql.ErrNoRows) || !errors.Is(getErr, sql.ErrNoRows) {
				t.Fatalf("case-sensitive %s found a row: FetchByEmail %v, GetByEmail %v", target.name, fetchErr, getErr)
			}
		})
	}
}

// TestIntegrationPredicatesMatchTheSameRows runs every predicate family on each
// engine and checks the rows matched, not the SQL text: the unit suite renders
// them, but only SQLite had ever executed the negations, the custom expressions
// or the empty-list forms. An empty NotIn once rendered NOT IN (SELECT 1 WHERE 1 = 0),
// which passed on integer columns and failed on PostgreSQL for text ones
// (varchar = integer), so the empty forms run on a text column too.
func TestIntegrationPredicatesMatchTheSameRows(t *testing.T) {
	for _, target := range integrationTargets(t) {
		t.Run(target.name, func(t *testing.T) {
			ctx := context.Background()

			dropAcademyTables(t, target)
			rt, _ := openWithPolicy(t, target, academy.TSQTables(), tsq.SchemaPolicyReconcile)

			learners := []*academy.Learner{
				{Name: "Ada", Email: "ada@x.test", Company: "A_Co"},
				{Name: "Bob", Email: "bob@x.test", Company: "AbCo"},
				{Name: "Cyd", Email: "cyd@x.test", Company: "50%"},
			}
			if err := academy.TableLearner.BatchInsert(ctx, rt, learners); err != nil {
				t.Fatalf("insert learners: %v", err)
			}

			enrolled := &academy.Enrollment{LearnerID: learners[0].ID, CourseID: 1, Status: academy.EnrollmentStatusActive}
			if err := enrolled.Insert(ctx, rt); err != nil {
				t.Fatalf("insert enrollment: %v", err)
			}

			l := academy.TableLearner
			e := academy.TableEnrollment
			first, last := learners[0].ID, learners[2].ID

			for _, tt := range []struct {
				name string
				cond tsq.Condition
				args []tsq.Arg
				want []string
			}{
				{"In over no values", l.ID.In(tsq.Vals[int64]()), nil, nil},
				{"NotIn over no values", l.ID.NotIn(tsq.Vals[int64]()), nil, []string{"Ada", "Bob", "Cyd"}},
				{"In over an empty list param", l.ID.In(l.ID.ListParam()), []tsq.Arg{l.ID.BindList()}, nil},
				{"NotIn over an empty list param", l.ID.NotIn(l.ID.ListParam()), []tsq.Arg{l.ID.BindList()}, []string{"Ada", "Bob", "Cyd"}},
				{"NotIn over no text values", l.Name.NotIn(tsq.Vals[string]()), nil, []string{"Ada", "Bob", "Cyd"}},
				{"NotIn over an empty text list param", l.Name.NotIn(l.Name.ListParam()), []tsq.Arg{l.Name.BindList()}, []string{"Ada", "Bob", "Cyd"}},
				{"NotIn over a text list param", l.Name.NotIn(l.Name.ListParam()), []tsq.Arg{l.Name.BindList("Bob")}, []string{"Ada", "Cyd"}},
				{"In over no text values", l.Name.In(tsq.Vals[string]()), nil, nil},
				{"LTE", l.ID.LTE(tsq.Val(first)), nil, []string{"Ada"}},
				{"NotBetween", l.ID.NotBetween(tsq.Val(first), tsq.Val(first)), nil, []string{"Bob", "Cyd"}},
				{"Like as written", tsq.Like(l.Name, tsq.Val("_d_")), nil, []string{"Ada"}},
				{"NotLike", tsq.NotLike(l.Name, tsq.Val("B%")), nil, []string{"Ada", "Cyd"}},
				{"NotStartsWith escapes _", tsq.NotStartsWith(l.Company, tsq.Val("A_")), nil, []string{"Bob", "Cyd"}},
				{"NotEndsWith escapes %", tsq.NotEndsWith(l.Company, tsq.Val("0%")), nil, []string{"Ada", "Bob"}},
				{"NotContains", tsq.NotContains(l.Name, tsq.Val("o")), nil, []string{"Ada", "Cyd"}},
				{"Not", tsq.Not(l.Name.EQ(tsq.Val("Bob"))), nil, []string{"Ada", "Cyd"}},
				{"Expr", l.Name.Expr("LOWER(%s)").EQ(tsq.Val("cyd")), nil, []string{"Cyd"}},
				{"Exprf", l.ID.Exprf("%s + %s", tsq.Val(int64(1))).GT(tsq.Val(last)), nil, []string{"Cyd"}},
				{"In a limited subquery", l.ID.In(tsq.SelectValue(l.ID).From(l).OrderBy(l.ID.Asc()).Limit(1)), nil, []string{"Ada"}},
				{"NotExists", tsq.NotExists(tsq.SelectValue(e.UID).From(e).Correlate(l).Where(e.LearnerID.EQ(l.ID))), nil, []string{"Bob", "Cyd"}},
			} {
				names, err := tsq.SelectValue(l.Name).From(l).Where(tt.cond).OrderBy(l.Name.Asc()).List(ctx, rt, tt.args...)
				if err != nil {
					t.Errorf("%s: %v", tt.name, err)
					continue
				}

				got := make([]string, 0, len(names))
				for _, name := range names {
					got = append(got, *name)
				}

				if strings.Join(got, ",") != strings.Join(tt.want, ",") {
					t.Errorf("%s matched %v, want %v", tt.name, got, tt.want)
				}
			}

			// A chain reads left to right on every engine: (Ada, Bob ∪ Cyd) ∩ (Ada, Cyd).
			// Written flat, MySQL and PostgreSQL bind INTERSECT first and add Bob.
			named := func(names ...string) tsq.WhereStage[string] {
				return tsq.SelectValue(l.Name).From(l).Where(l.Name.In(tsq.Vals(names...)))
			}

			chain, err := named("Ada", "Bob").Union(named("Cyd")).Intersect(named("Ada", "Cyd")).OrderBy(l.Name.Asc()).MustBuild().List(ctx, rt)
			if err != nil {
				t.Fatalf("set operation chain: %v", err)
			}

			if len(chain) != 2 || *chain[0] != "Ada" || *chain[1] != "Cyd" {
				t.Errorf("set operation chain matched %d rows, want Ada and Cyd", len(chain))
			}
		})
	}
}

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

			lockSQL := fmt.Sprintf("SELECT id FROM track WHERE id = %s FOR UPDATE", placeholder(rt))
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

// TestIntegrationASwallowedDeadlockCannotCommit covers a callback that catches a
// deadlock and goes on. InnoDB rolled the transaction back and left the session
// autocommitting, so the statements after it ran on their own and the COMMIT
// committed nothing: the first writes were lost and the later ones kept, with no
// error. The executor now refuses every statement after such an error, WithTx
// refuses to commit and reports the deadlock, and WithRetry runs the body again.
func TestIntegrationASwallowedDeadlockCannotCommit(t *testing.T) {
	targets := integrationTargets(t)
	requireExternalTargets(t, targets)

	for _, target := range targets {
		if target.name != "mysql" {
			continue
		}

		t.Run(target.name, func(t *testing.T) {
			ctx := context.Background()

			dropAcademyTables(t, target)
			rt, _ := openWithPolicy(t, target, academy.TSQTables(), tsq.SchemaPolicyReconcile)

			tracks := []*academy.Track{
				{Name: "one", Description: "d", SkillItems: []byte(`[]`)},
				{Name: "two", Description: "d", SkillItems: []byte(`[]`)},
			}
			if err := academy.TableTrack.BatchInsert(ctx, rt, tracks); err != nil {
				t.Fatalf("insert tracks: %v", err)
			}

			var calls int

			// deadlock runs a body that locks first then second, after writing a
			// learner, against a transaction holding the locks the other way round.
			// The body catches the deadlock, writes another learner, and returns nil.
			deadlock := func(options ...tsq.TxOption) (err error, bodies int, refused error) {
				other, err := rt.DB().BeginTx(ctx, nil)
				if err != nil {
					t.Fatalf("begin: %v", err)
				}

				defer other.Rollback() //nolint:errcheck // best-effort cleanup

				// The other transaction changes more rows than the body will, so
				// that InnoDB, which rolls back the smaller transaction, picks the body.
				calls++

				for i := range 3 {
					if _, err := other.ExecContext(ctx, "INSERT INTO learner (name, email, company) VALUES (?, ?, '')", "other", fmt.Sprintf("o%d-%d@x", calls, i)); err != nil {
						t.Fatalf("the other's rows: %v", err)
					}
				}

				lock := "SELECT id FROM track WHERE id = ? FOR UPDATE"
				if _, err := other.ExecContext(ctx, lock, tracks[1].ID); err != nil {
					t.Fatalf("lock two: %v", err)
				}

				ready := make(chan struct{})
				done := make(chan error, 1)

				go func() {
					<-ready

					time.Sleep(200 * time.Millisecond)

					_, err := other.ExecContext(ctx, lock, tracks[0].ID)
					// Let go of the locks: a retried body must be able to take them.
					_ = other.Rollback()
					done <- err
				}()

				err = rt.WithTx(ctx, func(ctx context.Context, tx tsq.Executor) error {
					bodies++

					defer func() {
						// Whatever the body did, the helper must not wait forever.
						select {
						case <-ready:
						default:
							close(ready)
						}
					}()

					if err := academy.TableLearner.Insert(ctx, tx, &academy.Learner{Name: "before", Email: fmt.Sprintf("b%d-%d@x", calls, bodies)}); err != nil {
						return fmt.Errorf("insert before: %w", err)
					}

					if _, err := tsq.Select(academy.TableTrack.ID).From(academy.TableTrack).Where(academy.TableTrack.ID.EQ(tsq.Val(tracks[0].ID))).ForUpdate().MustBuild().List(ctx, tx); err != nil {
						return fmt.Errorf("lock one: %w", err)
					}

					if bodies == 1 {
						close(ready)
					}

					// Waits for the other's lock; one of the two is the victim.
					if _, err := tsq.Select(academy.TableTrack.ID).From(academy.TableTrack).Where(academy.TableTrack.ID.EQ(tsq.Val(tracks[1].ID))).ForUpdate().MustBuild().List(ctx, tx); err != nil {
						if !tsq.IsTxConflictError(err) {
							return err
						}
						// Swallowed: the mistake under test.
					}

					refused = academy.TableLearner.Insert(ctx, tx, &academy.Learner{Name: "after", Email: fmt.Sprintf("a%d-%d@x", calls, bodies)})

					return nil
				}, options...)

				if otherErr := <-done; otherErr != nil && !tsq.IsTxConflictError(otherErr) {
					t.Fatalf("the other transaction: %v", otherErr)
				} else if otherErr != nil {
					// The other was the victim: the body saw no deadlock. Rare, so retried
					// by the caller.
					return errOtherWasTheVictim, bodies, nil
				}

				return err, bodies, refused
			}

			var err, refused error

			var bodies int

			for attempt := range 5 {
				err, bodies, refused = deadlock()
				if !errors.Is(err, errOtherWasTheVictim) {
					break
				}

				if attempt == 4 {
					t.Skip("the other transaction was the deadlock victim five times")
				}
			}

			if err == nil || !tsq.IsTxConflictError(err) || !strings.Contains(err.Error(), "nothing was committed") {
				t.Fatalf("WithTx after a swallowed deadlock = %v; want the deadlock reported, nothing committed", err)
			}

			if refused == nil || !strings.Contains(refused.Error(), "cannot go on") {
				t.Fatalf("the statement after the deadlock = %v; want it refused", refused)
			}

			n, err := academy.TableLearner.Query().Count(ctx, rt)
			if err != nil || n != 0 {
				t.Fatalf("learners after the rolled-back transaction = %d, %v; want none", n, err)
			}

			// With a retry the body runs again and commits whole.
			for attempt := range 5 {
				err, bodies, _ = deadlock(tsq.WithRetry(tsq.IsTxConflictError))
				if !errors.Is(err, errOtherWasTheVictim) {
					break
				}

				if attempt == 4 {
					t.Skip("the other transaction was the deadlock victim five times")
				}
			}

			if err != nil || bodies != 2 {
				t.Fatalf("with retry = %v after %d bodies; want nil after two", err, bodies)
			}

			n, err = academy.TableLearner.Query().Count(ctx, rt)
			if err != nil || n != 2 {
				t.Fatalf("learners after the retried transaction = %d, %v; want the two of the second body", n, err)
			}
		})
	}
}

var errOtherWasTheVictim = errors.New("the other transaction was the victim")

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

			if tsqdialect.Supports(rt.Dialect(), tsqdialect.CapabilityCTE) {
				recent := tsq.CTE("recent_learners",
					tsq.Select(academy.TableLearner.ID).From(academy.TableLearner).Where(academy.TableLearner.ID.GT(tsq.Val(int64(0)))))
				recentID := academy.TableLearner.ID.WithTable(recent)

				rows, err := tsq.Select(recentID).From(recent).MustBuild().List(ctx, rt)
				if err != nil {
					t.Fatalf("CTE advertised but failed on %s: %v", target.name, err)
				}

				if len(rows) != 1 {
					t.Fatalf("expected one row through the CTE, got %d", len(rows))
				}
			}

			if tsqdialect.Supports(rt.Dialect(), tsqdialect.CapabilityIntersect) {
				query := tsq.Select(academy.TableLearner.ID).From(academy.TableLearner).
					Intersect(tsq.Select(academy.TableLearner.ID).From(academy.TableLearner)).
					MustBuild()

				rows, err := query.List(ctx, rt)
				if err != nil {
					t.Fatalf("INTERSECT advertised but failed on %s: %v", target.name, err)
				}

				if len(rows) != 1 {
					t.Fatalf("expected one row from INTERSECT, got %d", len(rows))
				}
			}

			for _, capability := range []tsqdialect.Capability{tsqdialect.CapabilityIntersectAll, tsqdialect.CapabilityExceptAll} {
				if !tsqdialect.Supports(rt.Dialect(), capability) {
					continue
				}

				ids := tsq.SelectValue(academy.TableLearner.ID).From(academy.TableLearner)

				query := ids.IntersectAll(ids)
				if capability == tsqdialect.CapabilityExceptAll {
					query = ids.ExceptAll(ids)
				}

				if _, err := query.MustBuild().List(ctx, rt); err != nil {
					t.Fatalf("%s advertised but failed on %s: %v", capability, target.name, err)
				}
			}

			if tsqdialect.Supports(rt.Dialect(), tsqdialect.CapabilityExcept) {
				query := tsq.Select(academy.TableLearner.ID).From(academy.TableLearner).
					Except(tsq.Select(academy.TableLearner.ID).From(academy.TableLearner)).
					MustBuild()

				rows, err := query.List(ctx, rt)
				if err != nil {
					t.Fatalf("EXCEPT advertised but failed on %s: %v", target.name, err)
				}

				if len(rows) != 0 {
					t.Fatalf("expected no rows from EXCEPT, got %d", len(rows))
				}
			}

			if tsqdialect.Supports(rt.Dialect(), tsqdialect.CapabilityFullJoin) {
				// Both sides of a FULL JOIN can be NULL, so the key is coalesced.
				query := tsq.SelectValue(tsq.Coalesce(academy.TableLearner.ID, tsq.Val(int64(0)))).From(academy.TableLearner).
					FullJoin(academy.TableEnrollment, academy.TableLearner.ID.EQ(academy.TableEnrollment.LearnerID)).
					MustBuild()

				rows, err := query.List(ctx, rt)
				if err != nil {
					t.Fatalf("FULL JOIN advertised but failed on %s: %v", target.name, err)
				}

				if len(rows) != 1 {
					t.Fatalf("expected one row from FULL JOIN, got %d", len(rows))
				}
			} else {
				// Both sides of a FULL JOIN can be NULL, so the key is coalesced.
				query := tsq.SelectValue(tsq.Coalesce(academy.TableLearner.ID, tsq.Val(int64(0)))).From(academy.TableLearner).
					FullJoin(academy.TableEnrollment, academy.TableLearner.ID.EQ(academy.TableEnrollment.LearnerID)).
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
				resp, err := academy.TableLearner.Query().Page(ctx, rt, tsq.Paging{Page: 1, Size: 10}, tsq.Keyword(tc.keyword))
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
			resp, err := academy.TableLearner.Query().Page(ctx, rt, tsq.Paging{Page: 1, Size: 10}, tsq.Keyword("Wildcard"))
			if err != nil {
				t.Fatalf("substring keyword on %s: %v", target.name, err)
			}

			if resp.Total != 2 {
				t.Fatalf("substring keyword on %s matched %d rows, want 2", target.name, resp.Total)
			}

			// The pattern functions escape the same way, with a Val or a Param.
			prefix := tsq.NewParam[string]("prefix")
			for _, tc := range []struct {
				name string
				cond tsq.Condition
				args []tsq.Arg
			}{
				{"StartsWith(Val)", tsq.StartsWith(academy.TableLearner.Name, tsq.Val("100%")), nil},
				{"EndsWith(Val)", tsq.EndsWith(academy.TableLearner.Name, tsq.Val("_b")), nil},
				{"Contains(Val)", tsq.Contains(academy.TableLearner.Name, tsq.Val("~")), nil},
				{"StartsWith(Param)", tsq.StartsWith(academy.TableLearner.Name, prefix), []tsq.Arg{prefix.Bind("a_")}},
			} {
				n, err := tsq.Select(academy.TableLearner.ID).From(academy.TableLearner).Where(tc.cond).MustBuild().
					Count(ctx, rt, tc.args...)
				if err != nil {
					t.Fatalf("%s on %s: %v", tc.name, target.name, err)
				}

				if n != 1 {
					t.Errorf("%s on %s matched %d rows, want 1", tc.name, target.name, n)
				}
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

			err := rt.WithTx(ctx, func(ctx context.Context, txExec tsq.Executor) error {
				if err := academy.TableLearner.BatchInsert(ctx, txExec, learners, tsq.WithBatchSize(10), tsq.WithSkipDuplicates()); err != nil {
					return err
				}

				// The transaction must still be usable after an ignored duplicate;
				// this is the statement PostgreSQL rejects with 25P02 when it is not.
				_, err := tsq.Select(academy.TableLearner.ID).From(academy.TableLearner).
					MustBuild().
					Count(ctx, txExec)

				return err
			})
			if err != nil {
				t.Fatalf("batch insert with WithSkipDuplicates inside a transaction on %s: %v", target.name, err)
			}

			count, err := tsq.Select(academy.TableLearner.ID).From(academy.TableLearner).MustBuild().Count(ctx, rt)
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
				// Selecting nothing from a table fails only when the table is gone.
				var one int
				if err := academyRT.DB().QueryRowContext(ctx, "SELECT 1 FROM "+name+" WHERE 1 = 0").Scan(&one); !errors.Is(err, sql.ErrNoRows) {
					t.Fatalf("table %s was dropped by a runtime that never declared it on %s: %v", name, target.name, err)
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
				Set(academy.TableEnrollment.Status, tsq.Val(academy.EnrollmentStatusCompleted)).
				Set(academy.TableEnrollment.Score, score).
				Where(academy.TableEnrollment.CourseID.EQ(academy.TableEnrollment.CourseID.Param())).
				Exec(ctx, rt, score.Bind(88), academy.TableEnrollment.CourseID.Bind(1))
			if err != nil {
				t.Fatalf("bulk update: %v", err)
			}

			if affected != 2 {
				t.Fatalf("expected 2 rows updated, got %d", affected)
			}

			reloaded, err := academy.TableEnrollment.Get(ctx, rt, rows[0].UID)
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

			beforeSoftDelete, err := academy.TableEnrollment.Get(ctx, rt, rows[1].UID)
			if err != nil {
				t.Fatalf("reload enrollment before soft delete: %v", err)
			}

			// Enrollment declares deleted_at, so DeleteFrom renders an UPDATE that
			// stamps the tombstone. The rows stay in the table and leave every
			// generated query.
			affected, err = tsq.DeleteFrom(academy.TableEnrollment).
				Where(academy.TableEnrollment.UID.In(academy.TableEnrollment.UID.ListParam())).
				Exec(ctx, rt, academy.TableEnrollment.UID.BindList(rows[1].UID, rows[2].UID))
			if err != nil {
				t.Fatalf("bulk soft delete: %v", err)
			}

			if affected != 2 {
				t.Fatalf("expected 2 rows soft-deleted, got %d", affected)
			}

			active, err := academy.TableEnrollment.Query().Count(ctx, rt)
			if err != nil {
				t.Fatalf("count active enrollments: %v", err)
			}

			if active != 1 {
				t.Fatalf("expected 1 active enrollment left, got %d", active)
			}

			stored, err := tsq.Select(academy.TableEnrollment.UID).From(academy.TableEnrollment.WithDeleted()).MustBuild().Count(ctx, rt)
			if err != nil {
				t.Fatalf("count stored enrollments: %v", err)
			}

			if stored != 3 {
				t.Fatalf("expected soft delete to keep all 3 rows stored, got %d", stored)
			}

			// The soft-deleted rows carry a tombstone and an advanced version.
			tombstoned, err := tsq.Select(academy.TableEnrollment.Columns()...).
				From(academy.TableEnrollment.WithDeleted()).
				Where(academy.TableEnrollment.UID.EQ(academy.TableEnrollment.UID.Param())).
				MustBuild().
				Get(ctx, rt, academy.TableEnrollment.UID.Bind(rows[1].UID))
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
				Where(academy.TableEnrollment.UID.In(academy.TableEnrollment.UID.ListParam())).
				Exec(ctx, rt, academy.TableEnrollment.UID.BindList(rows[1].UID, rows[2].UID))
			if err != nil {
				t.Fatalf("bulk hard delete: %v", err)
			}

			if affected != 2 {
				t.Fatalf("expected 2 rows hard-deleted, got %d", affected)
			}

			stored, err = tsq.Select(academy.TableEnrollment.UID).From(academy.TableEnrollment.WithDeleted()).MustBuild().Count(ctx, rt)
			if err != nil {
				t.Fatalf("count stored enrollments: %v", err)
			}

			if stored != 1 {
				t.Fatalf("expected 1 enrollment left after hard delete, got %d", stored)
			}
		})
	}
}

// TestIntegrationUpsert runs Upsert and BatchUpsert on every dialect: the three
// spell it differently, and MySQL reports the key of an updated row only through
// LAST_INSERT_ID(expr).
func TestIntegrationUpsert(t *testing.T) {
	for _, target := range integrationTargets(t) {
		t.Run(target.name, func(t *testing.T) {
			ctx := context.Background()

			dropAcademyTables(t, target)
			rt, _ := openWithPolicy(t, target, academy.TSQTables(), tsq.SchemaPolicyReconcile)
			mysql := rt.Dialect() == tsqdialect.MySQL

			// By a unique index: insert, then update the same learner.
			first := &academy.Learner{Name: "Ada", Email: "ada@example.test", Company: "A"}
			if err := academy.TableLearner.Upsert(ctx, rt, first, tsq.OnConflict(academy.TableLearner.Email)); err != nil {
				t.Fatal(err)
			}

			if first.ID == 0 {
				t.Fatal("expected the generated key to be written back")
			}

			// created_at is kept on update and read back over the value passed in.
			ancient := time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC)
			again := &academy.Learner{Name: "Ada L.", Email: "ada@example.test", Company: "B"}
			again.CreatedAt = sql.Null[time.Time]{V: ancient, Valid: true}

			if err := academy.TableLearner.Upsert(ctx, rt, again, tsq.OnConflict(academy.TableLearner.Email)); err != nil {
				t.Fatal(err)
			}

			if again.ID != first.ID || !again.CreatedAt.Valid || again.CreatedAt.V.Year() == 2001 {
				t.Fatalf("updated row reads back id %d created %v; want id %d and the original time", again.ID, again.CreatedAt, first.ID)
			}

			// Unchanged values still report the key.
			same := *again
			same.ID = 0
			if err := academy.TableLearner.Upsert(ctx, rt, &same, tsq.OnConflict(academy.TableLearner.Email)); err != nil || same.ID != first.ID {
				t.Fatalf("no-op upsert = id %d, %v; want %d", same.ID, err, first.ID)
			}

			stored, err := academy.TableLearner.Get(ctx, rt, first.ID)
			if err != nil || stored.Name != "Ada L." || stored.Company != "B" {
				t.Fatalf("stored = %+v, %v", stored, err)
			}

			// Fetch splits a key list larger than the server binds in one statement.
			// SQLite is covered by the unit suite, where it is not this slow.
			if target.driver != "sqlite" {
				keys := make([]int64, 0, 70001)
				for i := range int64(70000) {
					keys = append(keys, 1_000_000+i)
				}

				if _, err := academy.TableLearner.Fetch(ctx, rt, append(keys, first.ID)...); !errors.Is(err, sql.ErrNoRows) {
					t.Fatalf("fetch with missing keys = %v; want sql.ErrNoRows", err)
				}

				byID := tsq.Select(academy.TableLearner.Columns()...).From(academy.TableLearner).
					Where(academy.TableLearner.ID.In(academy.TableLearner.ID.ListParam())).MustBuild()

				found, err := byID.ListIn(ctx, rt, academy.TableLearner.ID.ListParam(), append(keys, first.ID))
				if err != nil || len(found) != 1 || found[0].ID != first.ID {
					t.Fatalf("ListIn over 70001 keys = %d rows, %v", len(found), err)
				}
			}

			// A known primary key could hit a second unique key; only MySQL cares.
			explicit := &academy.Learner{Name: "Ada", Email: "ada@example.test"}
			explicit.ID = first.ID
			err = academy.TableLearner.Upsert(ctx, rt, explicit, tsq.OnConflict(academy.TableLearner.Email))
			if mysql != (err != nil) {
				t.Fatalf("upsert with a key set on %s: %v", target.name, err)
			}

			// BatchUpsert: one existing, one new.
			batch := []*academy.Learner{
				{Name: "Ada 3", Email: "ada@example.test"},
				{Name: "Bob", Email: "bob@example.test"},
			}
			if err := academy.TableLearner.BatchUpsert(ctx, rt, batch, tsq.OnConflict(academy.TableLearner.Email)); err != nil {
				t.Fatal(err)
			}

			if n, err := academy.TableLearner.Query().Count(ctx, rt); err != nil || n != 2 {
				t.Fatalf("learners = %d, %v; want 2", n, err)
			}

			dup := []*academy.Learner{{Email: "x@example.test"}, {Email: "x@example.test"}}
			if err := academy.TableLearner.BatchUpsert(ctx, rt, dup, tsq.OnConflict(academy.TableLearner.Email)); err == nil {
				t.Fatal("expected two rows with one key to be refused")
			}

			if err := academy.TableLearner.Upsert(ctx, rt, &academy.Learner{Email: "y@example.test"}, tsq.OnConflict(academy.TableLearner.Company)); err == nil {
				t.Fatal("expected a non-unique key to be refused")
			}

			// By primary key on a managed table: version and timestamps follow.
			enrollment := &academy.Enrollment{LearnerID: first.ID, CourseID: 1, Score: 10}
			if err := academy.TableEnrollment.Upsert(ctx, rt, enrollment); err != nil {
				t.Fatal(err)
			}

			version := enrollment.Version
			enrollment.Score = 20

			if err := academy.TableEnrollment.Upsert(ctx, rt, enrollment); err != nil {
				t.Fatal(err)
			}

			if enrollment.Version != version+1 {
				t.Fatalf("version = %d after an update from %d", enrollment.Version, version)
			}

			enrollment.Score = 30
			if err := enrollment.Update(ctx, rt); err != nil {
				t.Fatalf("Update after upsert: %v", err)
			}

			// Writing deleted_at back to zero restores a deleted row.
			if err := enrollment.Delete(ctx, rt); err != nil {
				t.Fatal(err)
			}

			if err := academy.TableEnrollment.Upsert(ctx, rt, enrollment); err != nil {
				t.Fatal(err)
			}

			restored, err := academy.TableEnrollment.Get(ctx, rt, enrollment.UID)
			if err != nil || restored.Score != 30 {
				t.Fatalf("restored = %+v, %v", restored, err)
			}

			// Delete and Restore write only the managed columns.
			restored.Score = 99
			if err := restored.Delete(ctx, rt); err != nil {
				t.Fatal(err)
			}

			if err := restored.Restore(ctx, rt); err != nil {
				t.Fatal(err)
			}

			if err := restored.Restore(ctx, rt); !isRowState(err) {
				t.Fatalf("restoring a live row = %v", err)
			}

			back, err := academy.TableEnrollment.Get(ctx, rt, enrollment.UID)
			if err != nil || back.Score != 30 || back.Version != restored.Version {
				t.Fatalf("after delete and restore = %+v, %v; want score 30 and version %d", back, err, restored.Version)
			}
		})
	}
}

// TestIntegrationPageKeysetOverTimestamps walks enrollments by created_at and key:
// the cursor carries a time value, which each driver binds and compares its way.
func TestIntegrationPageKeysetOverTimestamps(t *testing.T) {
	for _, target := range integrationTargets(t) {
		t.Run(target.name, func(t *testing.T) {
			ctx := context.Background()

			dropAcademyTables(t, target)
			rt, _ := openWithPolicy(t, target, academy.TSQTables(), tsq.SchemaPolicyReconcile)

			base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

			var rows []*academy.Enrollment

			for i := range 7 {
				e := &academy.Enrollment{LearnerID: int64(i), CourseID: 1}
				// Three rows share a timestamp, so the key breaks ties.
				e.CreatedAt = base.Add(time.Duration(min(i, 4)) * time.Hour)
				rows = append(rows, e)
			}

			if err := academy.TableEnrollment.BatchInsert(ctx, rt, rows); err != nil {
				t.Fatal(err)
			}

			order := []tsq.OrderBy{academy.TableEnrollment.CreatedAt.Desc(), academy.TableEnrollment.UID.Desc()}

			want, err := tsq.Select(academy.TableEnrollment.Columns()...).From(academy.TableEnrollment).OrderBy(order[0], order[1:]...).MustBuild().List(ctx, rt)
			if err != nil {
				t.Fatal(err)
			}

			var walked []*academy.Enrollment

			k := tsq.Keyset{Size: 2, OrderBy: order}

			for {
				page, err := academy.TableEnrollment.Query().PageKeyset(ctx, rt, k)
				if err != nil {
					t.Fatal(err)
				}

				walked = append(walked, page.Data...)

				if !page.HasNext() {
					break
				}

				k.After = page.Next
			}

			if len(walked) != len(want) {
				t.Fatalf("walked %d rows, want %d", len(walked), len(want))
			}

			for i := range want {
				if walked[i].UID != want[i].UID {
					t.Fatalf("row %d is %d, want %d", i, walked[i].UID, want[i].UID)
				}
			}
		})
	}
}

// TestIntegrationNullableColumns writes and reads NULL through NullColumns on
// every dialect: comparisons take the value type, SetNull writes NULL, and a
// NULL aggregate comes back through ScalarNull.
func TestIntegrationNullableColumns(t *testing.T) {
	for _, target := range integrationTargets(t) {
		t.Run(target.name, func(t *testing.T) {
			ctx := context.Background()

			dropAcademyTables(t, target)
			rt, _ := openWithPolicy(t, target, academy.TSQTables(), tsq.SchemaPolicyReconcile)

			rows := []*academy.Enrollment{{LearnerID: 1, CourseID: 1}, {LearnerID: 2, CourseID: 1}}
			if err := academy.TableEnrollment.BatchInsert(ctx, rt, rows); err != nil {
				t.Fatal(err)
			}

			updated := academy.TableEnrollment.UpdatedAt
			cleared, err := tsq.UpdateTable(academy.TableEnrollment).SetNull(updated).
				Where(academy.TableEnrollment.UID.EQ(tsq.Val(rows[0].UID))).Exec(ctx, rt)
			if err != nil || cleared != 1 {
				t.Fatalf("SetNull = %d, %v", cleared, err)
			}

			base := tsq.Select(academy.TableEnrollment.UID).From(academy.TableEnrollment)

			if n, err := base.Where(updated.IsNull()).MustBuild().Count(ctx, rt); err != nil || n != 1 {
				t.Fatalf("IsNull = %d, %v", n, err)
			}

			since := time.Now().Add(-time.Hour)
			if n, err := base.Where(updated.GT(tsq.Val(since))).MustBuild().Count(ctx, rt); err != nil || n != 1 {
				t.Fatalf("comparison with a time value = %d, %v", n, err)
			}

			stored, err := academy.TableEnrollment.Get(ctx, rt, rows[0].UID)
			if err != nil || stored.UpdatedAt.Valid {
				t.Fatalf("stored = %+v, %v; want a NULL updated_at", stored, err)
			}

			latest := tsq.Max(updated)
			none, err := tsq.SelectNullValue(latest).From(academy.TableEnrollment).
				Where(academy.TableEnrollment.UID.LT(tsq.Val(int64(0)))).MustBuild().Get(ctx, rt)
			if err != nil || none.Valid {
				t.Fatalf("MAX over no rows = %v, %v", none, err)
			}
		})
	}
}

// TestIntegrationNullOrderingAgrees sorts a nullable column on every dialect: the
// default and each explicit placement must give the same order everywhere.
func TestIntegrationNullOrderingAgrees(t *testing.T) {
	for _, target := range integrationTargets(t) {
		t.Run(target.name, func(t *testing.T) {
			ctx := context.Background()

			dropAcademyTables(t, target)
			rt, _ := openWithPolicy(t, target, academy.TSQTables(), tsq.SchemaPolicyReconcile)

			rows := []*academy.Enrollment{{LearnerID: 1, CourseID: 1}, {LearnerID: 2, CourseID: 1}, {LearnerID: 3, CourseID: 1}}
			if err := academy.TableEnrollment.BatchInsert(ctx, rt, rows); err != nil {
				t.Fatal(err)
			}

			// rows[1] has no updated_at; rows[0] is older than rows[2].
			base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			for i, at := range map[int]time.Time{0: base, 2: base.Add(time.Hour)} {
				if _, err := tsq.UpdateTable(academy.TableEnrollment).Set(academy.TableEnrollment.UpdatedAt, tsq.Val(at)).
					Where(academy.TableEnrollment.UID.EQ(tsq.Val(rows[i].UID))).Exec(ctx, rt); err != nil {
					t.Fatal(err)
				}
			}

			if _, err := tsq.UpdateTable(academy.TableEnrollment).SetNull(academy.TableEnrollment.UpdatedAt).
				Where(academy.TableEnrollment.UID.EQ(tsq.Val(rows[1].UID))).Exec(ctx, rt); err != nil {
				t.Fatal(err)
			}

			updated := academy.TableEnrollment.UpdatedAt
			for name, tc := range map[string]struct {
				order tsq.OrderBy
				want  []int
			}{
				"asc":              {updated.Asc(), []int{1, 0, 2}},
				"desc":             {updated.Desc(), []int{2, 0, 1}},
				"asc nulls last":   {updated.Asc().NullsLast(), []int{0, 2, 1}},
				"desc nulls first": {updated.Desc().NullsFirst(), []int{1, 2, 0}},
			} {
				got, err := academy.TableEnrollment.Query().Page(ctx, rt, tsq.Paging{Size: 10, OrderBy: []tsq.OrderBy{tc.order}})
				if err != nil {
					t.Fatalf("%s: %v", name, err)
				}

				for i, idx := range tc.want {
					if got.Data[i].UID != rows[idx].UID {
						t.Errorf("%s on %s: position %d is %d, want %d", name, target.name, i, got.Data[i].UID, rows[idx].UID)
					}
				}
			}
		})
	}
}

// TestIntegrationDatabaseFilledColumns covers a column with a DEFAULT and a
// generated column on every dialect: the three spell generated columns the same
// way but reject writes to them differently, and the values come back from the
// database.
func TestIntegrationDatabaseFilledColumns(t *testing.T) {
	for _, target := range integrationTargets(t) {
		t.Run(target.name, func(t *testing.T) {
			ctx := context.Background()

			dropAcademyTables(t, target)
			rt, recorder := openWithPolicy(t, target, academy.TSQTables(), tsq.SchemaPolicyReconcile)

			course := &academy.Course{TrackID: 1, InstructorID: 1, Title: "Filled", Summary: "s", ListPriceCents: 1}
			if err := course.Insert(ctx, rt); err != nil {
				t.Fatal(err)
			}

			if deref(course.Currency) != "USD" || course.Slug != "filled" {
				t.Fatalf("inserted course = %+v; want the database values read back", course)
			}

			// An explicit value wins over the DEFAULT.
			explicit := &academy.Course{TrackID: 1, InstructorID: 1, Title: "Euro", Summary: "s", Currency: new("EUR")}
			if err := explicit.Insert(ctx, rt); err != nil {
				t.Fatal(err)
			}

			if deref(explicit.Currency) != "EUR" || explicit.Slug != "euro" {
				t.Fatalf("explicit currency = %+v", explicit)
			}

			// Update never writes the generated column, whatever the struct holds.
			course.Title = "Renamed"
			course.Slug = "ignored"

			if err := course.Update(ctx, rt); err != nil {
				t.Fatal(err)
			}

			stored, err := academy.TableCourse.Get(ctx, rt, course.ID)
			if err != nil || stored.Slug != "renamed" {
				t.Fatalf("stored = %+v, %v; want the slug recomputed", stored, err)
			}

			// A batch insert leaves the columns to the database without reading back.
			batch := []*academy.Course{
				{TrackID: 1, InstructorID: 1, Title: "Batch A", Summary: "s"},
				{TrackID: 1, InstructorID: 1, Title: "Batch B", Summary: "s", Currency: new("GBP")},
			}
			if err := academy.TableCourse.BatchInsert(ctx, rt, batch); err != nil {
				t.Fatal(err)
			}

			rows, err := academy.TableCourse.Fetch(ctx, rt, batch[0].ID, batch[1].ID)
			if err != nil || deref(rows[0].Currency) != "USD" || deref(rows[1].Currency) != "GBP" || rows[0].Slug != "batch a" {
				t.Fatalf("batch rows = %+v, %v", rows, err)
			}

			// Upsert writes the columns Insert does: never the generated slug, and the
			// defaulted currency only when the row sets it. It used to write both, so a
			// table with a generated column could not be upserted at all.
			up := &academy.Course{TrackID: 1, InstructorID: 1, Title: "Upserted", Summary: "s"}
			if err := academy.TableCourse.Upsert(ctx, rt, up, tsq.OnConflict(academy.TableCourse.Title)); err != nil {
				t.Fatal(err)
			}

			if up.ID == 0 || deref(up.Currency) != "USD" || up.Slug != "upserted" {
				t.Fatalf("upserted course = %+v; want the database values read back", up)
			}

			euro := &academy.Course{TrackID: 1, InstructorID: 1, Title: "Upserted", Summary: "s", Currency: new("EUR")}
			if err := academy.TableCourse.Upsert(ctx, rt, euro, tsq.OnConflict(academy.TableCourse.Title)); err != nil {
				t.Fatal(err)
			}

			// An unset default keeps the stored value on update rather than writing "".
			unset := &academy.Course{TrackID: 1, InstructorID: 1, Title: "Upserted", Summary: "again"}
			if err := academy.TableCourse.Upsert(ctx, rt, unset, tsq.OnConflict(academy.TableCourse.Title)); err != nil {
				t.Fatal(err)
			}

			if unset.ID != up.ID || deref(unset.Currency) != "EUR" {
				t.Fatalf("second update = %+v; want id %d keeping EUR", unset, up.ID)
			}

			// A batch groups rows by the columns they write.
			upserts := []*academy.Course{
				{TrackID: 1, InstructorID: 1, Title: "Batch Up A", Summary: "s"},
				{TrackID: 1, InstructorID: 1, Title: "Batch Up B", Summary: "s", Currency: new("GBP")},
			}
			if err := academy.TableCourse.BatchUpsert(ctx, rt, upserts, tsq.OnConflict(academy.TableCourse.Title)); err != nil {
				t.Fatal(err)
			}

			upserted, err := academy.TableCourse.FetchByTitle(ctx, rt, "Batch Up A", "Batch Up B")
			if err != nil || deref(upserted[0].Currency) != "USD" || deref(upserted[1].Currency) != "GBP" || upserted[0].Slug != "batch up a" {
				t.Fatalf("batch upserted = %+v, %v", upserted, err)
			}

			// Reconcile must not keep altering the generated column.
			recorder.reset()

			if _, err := tsq.Open(ctx, target.driver, target.dsn, academy.TSQTables(),
				tsq.WithSchemaPolicy(tsq.SchemaPolicyReconcile), tsq.WithLogger(recorder)); err != nil {
				t.Fatal(err)
			}

			if applied := recorder.statements(); len(applied) != 0 {
				t.Fatalf("second boot applied %d statements: %v", len(applied), applied)
			}
		})
	}
}

// TestIntegrationBatchWritesMatchByVersion runs the multi-row key match of a table
// with a version column on every dialect: pk IN (...) AND CASE pk WHEN ? THEN
// version = ? ... END. It replaced one OR per row, which SQLite refused beyond 998
// rows, under the default batch size of 1000.
func TestIntegrationBatchWritesMatchByVersion(t *testing.T) {
	for _, target := range integrationTargets(t) {
		t.Run(target.name, func(t *testing.T) {
			ctx := context.Background()

			dropAcademyTables(t, target)
			rt, _ := openWithPolicy(t, target, academy.TSQTables(), tsq.SchemaPolicyReconcile)

			rows := make([]*academy.Enrollment, 3)
			for i := range rows {
				rows[i] = &academy.Enrollment{LearnerID: int64(i + 1), CourseID: 1}
			}

			if err := academy.TableEnrollment.BatchInsert(ctx, rt, rows); err != nil {
				t.Fatal(err)
			}

			for _, row := range rows {
				row.Score = 70
			}

			if err := academy.TableEnrollment.BatchUpdate(ctx, rt, rows); err != nil {
				t.Fatal(err)
			}

			// One stale row fails the whole statement's count.
			stale := *rows[1]
			stale.Version--

			if err := academy.TableEnrollment.BatchUpdate(ctx, rt, []*academy.Enrollment{rows[0], &stale}); !tsq.IsOptimisticLockError(err) {
				t.Fatalf("stale batch update = %v; want OptimisticLockError", err)
			}

			// The failed statement changed nothing; reload the versions it left.
			fresh, err := academy.TableEnrollment.Fetch(ctx, rt, rows[0].UID, rows[1].UID, rows[2].UID)
			if err != nil {
				t.Fatal(err)
			}

			if err := academy.TableEnrollment.BatchDelete(ctx, rt, fresh[:2]); err != nil {
				t.Fatal(err)
			}

			if err := academy.TableEnrollment.BatchHardDelete(ctx, rt, fresh); err != nil {
				t.Fatalf("hard delete of live and deleted rows = %v", err)
			}

			left, err := tsq.Select(academy.TableEnrollment.UID).From(academy.TableEnrollment.WithDeleted()).MustBuild().Count(ctx, rt)
			if err != nil || left != 0 {
				t.Fatalf("rows left = %d, %v; want none", left, err)
			}
		})
	}
}

// courseTotal is a row of a grouped CTE over enrollments.
type courseTotal struct {
	CourseID int64
	Fees     int64
}

// TestIntegrationDerivedColumnsAreNamed runs, on every dialect, the SQL that names
// output columns. A select item that is not a column reference is written with AS
// its name, which is what a CTE column and a set operation's ORDER BY find it by;
// each dialect used to name it differently (the expression's text, "sum"). A
// combined operand of a set operation is a derived table, since SQLite has no
// parenthesized compound SELECT.
func TestIntegrationDerivedColumnsAreNamed(t *testing.T) {
	for _, target := range integrationTargets(t) {
		t.Run(target.name, func(t *testing.T) {
			ctx := context.Background()

			dropAcademyTables(t, target)
			rt, _ := openWithPolicy(t, target, academy.TSQTables(), tsq.SchemaPolicyReconcile)

			e := academy.TableEnrollment
			rows := []*academy.Enrollment{
				{LearnerID: 1, CourseID: 1, FeeCents: 100},
				{LearnerID: 2, CourseID: 1, FeeCents: 50},
				{LearnerID: 3, CourseID: 2, FeeCents: 70},
			}
			if err := e.BatchInsert(ctx, rt, rows); err != nil {
				t.Fatal(err)
			}

			// A CTE column that is an aggregate, read through the CTE by name.
			totals := tsq.CTE("totals", tsq.Select(
				tsq.MapInto(e.CourseID, func(r *courseTotal) *int64 { return &r.CourseID }),
				tsq.MapInto(tsq.Sum(e.FeeCents), func(r *courseTotal) *int64 { return &r.Fees }),
			).From(e).GroupBy(e.CourseID))

			fees := e.FeeCents.WithTable(totals)
			got, err := tsq.Select(
				tsq.MapInto(e.CourseID.WithTable(totals), func(r *courseTotal) *int64 { return &r.CourseID }),
				tsq.MapInto(fees, func(r *courseTotal) *int64 { return &r.Fees }),
			).From(totals).Where(fees.GT(tsq.Val(int64(100)))).MustBuild().List(ctx, rt)
			if err != nil || len(got) != 1 || got[0].CourseID != 1 || got[0].Fees != 150 {
				t.Fatalf("courses over 100 = %+v, %v; want course 1 with 150", got, err)
			}

			// A set operation ordered by a selected expression.
			l := academy.TableLearner
			for _, name := range []string{"bo", "al", "cy"} {
				if err := l.Insert(ctx, rt, &academy.Learner{Name: name, Email: name + "@example.test"}); err != nil {
					t.Fatal(err)
				}
			}

			upper := tsq.MapInto(tsq.Upper(l.Name), func(r *string) *string { return r })

			ordered, err := tsq.Select[string](upper).From(l).Where(l.Name.EQ(tsq.Val("bo"))).
				Union(tsq.Select[string](upper).From(l).Where(l.Name.NE(tsq.Val("bo")))).
				OrderBy(upper.Desc()).MustBuild().List(ctx, rt)
			if err != nil || len(ordered) != 3 || *ordered[0] != "CY" || *ordered[2] != "AL" {
				t.Fatalf("ordered union = %v, %v; want CY, BO, AL", ordered, err)
			}

			// A set operation with a combined operand.
			id := func(name string) tsq.WhereStage[int64] {
				return tsq.SelectValue(l.ID).From(l).Where(l.Name.EQ(tsq.Val(name)))
			}

			ids, err := id("al").Union(id("bo").UnionAll(id("cy"))).MustBuild().List(ctx, rt)
			if err != nil || len(ids) != 3 {
				t.Fatalf("nested union = %v, %v; want three ids", ids, err)
			}
		})
	}
}

// fkChild is a table whose parent_id is a foreign key.
type fkChild struct {
	ID       int64
	ParentID int64
}

// fkChildTable declares fk_child with idx_fk_child_parent over (parent_id, id), a
// different definition from the (parent_id) index the database holds.
func fkChildTable() tsq.Table {
	h := tsq.NewTable[fkChild, int64]("fk_child")
	id := tsq.NewColumn(h, "id", "id", func(r *fkChild) *int64 { return &r.ID })
	parent := tsq.NewColumn(h, "parent_id", "parent_id", func(r *fkChild) *int64 { return &r.ParentID })

	return h.Define(tsq.TableSpec[fkChild, int64]{
		Columns:    []tsq.BoundColumn[fkChild]{id, parent},
		PrimaryKey: id,
		ColumnSpecs: []tsqdialect.ColumnSpec{
			{Name: "id", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64}, PrimaryKey: true},
			{Name: "parent_id", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64}},
		},
		Indexes: []tsq.IndexSpec{{Name: "idx_fk_child_parent", Columns: []string{"parent_id", "id"}}},
	})
}

// TestIntegrationIndexAForeignKeyNeedsIsNotRebuilt covers Index.Constraint on
// MySQL, which never set it: an index a foreign key uses cannot be dropped there
// (error 1553), and Reconcile went ahead and hit that. It is now refused with the
// reason. PostgreSQL and SQLite need no index for a foreign key, so there the
// index is rebuilt to its declaration.
func TestIntegrationIndexAForeignKeyNeedsIsNotRebuilt(t *testing.T) {
	for _, target := range integrationTargets(t) {
		t.Run(target.name, func(t *testing.T) {
			ctx := context.Background()

			db, err := sql.Open(target.driver, target.dsn)
			if err != nil {
				t.Fatal(err)
			}

			t.Cleanup(func() { _ = db.Close() })

			setup := []string{
				"DROP TABLE IF EXISTS fk_child",
				"DROP TABLE IF EXISTS fk_parent",
				"CREATE TABLE fk_parent (id BIGINT PRIMARY KEY)",
			}
			if target.name == "mysql" {
				setup = append(setup, "CREATE TABLE fk_child (id BIGINT PRIMARY KEY, parent_id BIGINT NOT NULL, "+
					"INDEX idx_fk_child_parent (parent_id), CONSTRAINT fk_child_parent FOREIGN KEY (parent_id) REFERENCES fk_parent (id))")
			} else {
				setup = append(setup,
					"CREATE TABLE fk_child (id BIGINT PRIMARY KEY, parent_id BIGINT NOT NULL REFERENCES fk_parent (id))",
					"CREATE INDEX idx_fk_child_parent ON fk_child (parent_id)")
			}

			for _, statement := range setup {
				if _, err := db.ExecContext(ctx, statement); err != nil {
					t.Fatalf("%s: %v", statement, err)
				}
			}

			t.Cleanup(func() {
				_, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS fk_child")
				_, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS fk_parent")
			})

			rt, err := tsq.Open(ctx, target.driver, target.dsn, []tsq.Table{fkChildTable()},
				tsq.WithTablePolicy(tsq.SchemaPolicyManual), tsq.WithIndexPolicy(tsq.SchemaPolicyReconcile))

			if target.name == "mysql" {
				if err == nil || !strings.Contains(err.Error(), "backed by a primary key or constraint") {
					t.Fatalf("Open = %v; want the rebuild refused naming the constraint", err)
				}

				return
			}

			if err != nil {
				t.Fatalf("Open = %v; want the index rebuilt", err)
			}

			_ = rt.Close()
		})
	}
}

// TestIntegrationFullTextSearch searches the declared full-text index on every
// dialect. MySQL and PostgreSQL use their own index; SQLite has none TSQ manages,
// so the same predicate matches substrings, which is why the assertions only cover
// what all three agree on.
func TestIntegrationFullTextSearch(t *testing.T) {
	for _, target := range integrationTargets(t) {
		t.Run(target.name, func(t *testing.T) {
			ctx := context.Background()

			dropAcademyTables(t, target)
			rt, _ := openWithPolicy(t, target, academy.TSQTables(), tsq.SchemaPolicyReconcile)

			courses := []*academy.Course{
				{TrackID: 1, InstructorID: 1, Title: "Query Planning", Summary: "sqlite explains its plans"},
				{TrackID: 1, InstructorID: 1, Title: "Sqlite Internals", Summary: "pages and journals"},
				{TrackID: 1, InstructorID: 1, Title: "Kafka Streams", Summary: "topics and partitions"},
			}
			if err := academy.TableCourse.BatchInsert(ctx, rt, courses); err != nil {
				t.Fatal(err)
			}

			term := tsq.NewParam[string]("term")
			search := tsq.Select(academy.TableCourse.Columns()...).From(academy.TableCourse).
				Where(tsq.Matches(academy.TableCourse.FullTextTitleAndSummary(), term)).
				OrderBy(academy.TableCourse.Title.Asc()).MustBuild()

			found, err := search.List(ctx, rt, term.Bind("sqlite"))
			if err != nil {
				t.Fatal(err)
			}

			titles := make([]string, 0, len(found))
			for _, course := range found {
				titles = append(titles, course.Title)
			}

			if !slices.Equal(titles, []string{"Query Planning", "Sqlite Internals"}) {
				t.Fatalf("matches on %s = %v", target.name, titles)
			}

			if n, err := search.Count(ctx, rt, term.Bind("kafka")); err != nil || n != 1 {
				t.Fatalf("count = %d, %v", n, err)
			}

			if n, err := search.Count(ctx, rt, term.Bind("cassandra")); err != nil || n != 0 {
				t.Fatalf("no match = %d, %v", n, err)
			}

			// A Val term works the same way, and the index is only created once.
			byValue := tsq.Select(academy.TableCourse.ID).From(academy.TableCourse).
				Where(tsq.Matches(academy.TableCourse.FullTextTitleAndSummary(), tsq.Val("journals"))).MustBuild()

			if n, err := byValue.Count(ctx, rt); err != nil || n != 1 {
				t.Fatalf("value term = %d, %v", n, err)
			}

			// A term is words on every engine: operator characters typed into a
			// search box are not a syntax error anywhere (InnoDB refuses a lone or
			// phrase-trailing '*' in natural language mode unless it is removed).
			for _, operators := range []string{"*", " * ", "\"*", "\"query planning\"*", "+kafka -streams", "kafka*", "(", "~<>@", `"`} {
				if _, err := search.Count(ctx, rt, term.Bind(operators)); err != nil {
					t.Fatalf("term %q on %s: %v", operators, target.name, err)
				}
			}

			native := tsqdialect.Supports(rt.Dialect(), tsqdialect.CapabilityFullTextSearch)

			// The words around the operators are still searched where there is an
			// index (the substring fallback looks for the asterisk itself).
			if n, err := search.Count(ctx, rt, term.Bind("kafka*")); err != nil || native && n != 1 {
				t.Fatalf("kafka* = %d, %v", n, err)
			}

			if native != (target.driver != "sqlite") {
				t.Fatalf("%s reports full-text support %v", target.name, native)
			}
		})
	}
}

// TestIntegrationAttachLoadsChildrenInOneQuery checks eager loading on every
// dialect: the children come back in one statement, grouped by the key.
func TestIntegrationAttachLoadsChildrenInOneQuery(t *testing.T) {
	for _, target := range integrationTargets(t) {
		t.Run(target.name, func(t *testing.T) {
			ctx := context.Background()

			dropAcademyTables(t, target)
			rt, _ := openWithPolicy(t, target, academy.TSQTables(), tsq.SchemaPolicyReconcile)

			learners := []*academy.Learner{
				{Name: "One", Email: "one@example.test"},
				{Name: "Two", Email: "two@example.test"},
				{Name: "None", Email: "none@example.test"},
			}
			if err := academy.TableLearner.BatchInsert(ctx, rt, learners); err != nil {
				t.Fatal(err)
			}

			enrollments := []*academy.Enrollment{
				{LearnerID: learners[0].ID, CourseID: 1},
				{LearnerID: learners[0].ID, CourseID: 2},
				{LearnerID: learners[1].ID, CourseID: 1},
			}
			for _, e := range enrollments {
				if err := e.Insert(ctx, rt); err != nil {
					t.Fatal(err)
				}
			}

			// A deleted enrollment is out of scope for the child query too.
			if err := enrollments[1].Delete(ctx, rt); err != nil {
				t.Fatal(err)
			}

			children := tsq.Select(academy.TableEnrollment.Columns()...).From(academy.TableEnrollment).
				Where(academy.TableEnrollment.LearnerID.In(academy.TableEnrollment.LearnerID.ListParam())).MustBuild()

			counts := map[string]int{}

			err := tsq.AttachMany(ctx, rt, learners, academy.TableLearner.ID, children, academy.TableEnrollment.LearnerID,
				func(l *academy.Learner, es []*academy.Enrollment) { counts[l.Name] = len(es) })
			if err != nil {
				t.Fatal(err)
			}

			if counts["One"] != 1 || counts["Two"] != 1 || counts["None"] != 0 {
				t.Fatalf("counts on %s = %v", target.name, counts)
			}

			// AttachOne follows a foreign key back to its row.
			parents := tsq.Select(academy.TableLearner.Columns()...).From(academy.TableLearner).
				Where(academy.TableLearner.ID.In(academy.TableLearner.ID.ListParam())).MustBuild()

			live, err := academy.TableEnrollment.Query().List(ctx, rt)
			if err != nil {
				t.Fatal(err)
			}

			names := map[int64]string{}

			err = tsq.AttachOne(ctx, rt, live, academy.TableEnrollment.LearnerID, parents, academy.TableLearner.ID,
				func(e *academy.Enrollment, l *academy.Learner) { names[e.UID] = l.Name })
			if err != nil {
				t.Fatal(err)
			}

			if len(names) != 2 {
				t.Fatalf("attached parents = %v", names)
			}
		})
	}
}

// writeBeforeList runs write once, when the runtime logs the list statement of a
// Page: after the count has run and before the rows are read.
type writeBeforeList struct {
	once  sync.Once
	write func()
}

func (l *writeBeforeList) Enabled(context.Context, slog.Level) bool { return true }

func (l *writeBeforeList) LogAttrs(_ context.Context, _ slog.Level, msg string, _ ...slog.Attr) {
	if msg == "page" {
		l.once.Do(l.write)
	}
}

// TestIntegrationPageReadsOneSnapshot inserts a row from another connection
// between the count and the list statement of a Page. Both must still describe
// the same rows.
func TestIntegrationPageReadsOneSnapshot(t *testing.T) {
	for _, target := range integrationTargets(t) {
		t.Run(target.name, func(t *testing.T) {
			ctx := context.Background()

			dropAcademyTables(t, target)
			setup, _ := openWithPolicy(t, target, academy.TSQTables(), tsq.SchemaPolicyReconcile)

			other, err := sql.Open(target.driver, target.dsn)
			if err != nil {
				t.Fatal(err)
			}

			t.Cleanup(func() { _ = other.Close() })

			if target.driver == "sqlite" {
				// Without WAL a writer cannot commit while a reader holds the file.
				if _, err := other.ExecContext(ctx, "PRAGMA journal_mode=WAL"); err != nil {
					t.Fatal(err)
				}
			}

			for _, name := range []string{"a", "b"} {
				if err := (&academy.Learner{Name: name, Email: name + "@example.test"}).Insert(ctx, setup); err != nil {
					t.Fatal(err)
				}
			}

			var writeErr error

			hook := &writeBeforeList{write: func() {
				late := &academy.Learner{Name: "late", Email: "late@example.test"}
				exec, err := tsq.WrapExecutor(other, setup.Dialect())
				if err != nil {
					writeErr = err
					return
				}

				writeErr = academy.TableLearner.Insert(ctx, exec, late)
			}}

			rt, err := tsq.Open(ctx, target.driver, target.dsn, academy.TSQTables(), tsq.WithLogger(hook), tsq.WithSQLLogging())
			if err != nil {
				t.Fatal(err)
			}

			t.Cleanup(func() { _ = rt.Close() })

			page, err := academy.TableLearner.Query().Page(ctx, rt, tsq.Paging{Size: 10})
			if err != nil {
				t.Fatal(err)
			}

			if writeErr != nil {
				t.Fatalf("concurrent insert: %v", writeErr)
			}

			if page.Total != int64(len(page.Data)) || page.Total != 2 {
				t.Fatalf("Total = %d with %d rows; want both 2 from the snapshot", page.Total, len(page.Data))
			}

			if n, err := academy.TableLearner.Query().Count(ctx, rt); err != nil || n != 3 {
				t.Fatalf("count after the page = %d, %v; want the concurrent insert visible", n, err)
			}
		})
	}
}

// TestIntegrationSoftDeleteScopeJoins checks the soft-delete scope in every join
// position on real engines. A RIGHT or FULL JOIN renders the scoped table as a
// derived table, which is the spelling most likely to differ between dialects.
// TestIntegrationDeleteByPKNamesMissingKeys covers the keys BatchDeleteByPK and
// BatchHardDeleteByPK did not delete, read through RETURNING on PostgreSQL and
// SQLite and by the stamp or a prior read on MySQL.
func TestIntegrationDeleteByPKNamesMissingKeys(t *testing.T) {
	for _, target := range integrationTargets(t) {
		t.Run(target.name, func(t *testing.T) {
			ctx := context.Background()

			dropAcademyTables(t, target)
			rt, _ := openWithPolicy(t, target, academy.TSQTables(), tsq.SchemaPolicyReconcile)

			rows := []*academy.Enrollment{{CourseID: 1}, {CourseID: 1}, {CourseID: 1}}
			for _, e := range rows {
				if err := e.Insert(ctx, rt); err != nil {
					t.Fatal(err)
				}
			}

			missing := func(err error, want ...int64) {
				t.Helper()

				state, ok := errors.AsType[*tsq.RowStateError](err)
				if !ok || len(state.Keys) != len(want) {
					t.Fatalf("err = %v; want a RowStateError naming %v", err, want)
				}

				for i, key := range want {
					if fmt.Sprint(state.Keys[i]) != fmt.Sprint(key) {
						t.Fatalf("keys = %v; want %v", state.Keys, want)
					}
				}
			}

			if err := academy.TableEnrollment.BatchDeleteByPK(ctx, rt, []int64{rows[0].UID}); err != nil {
				t.Fatal(err)
			}

			// Already deleted, and never there.
			missing(academy.TableEnrollment.BatchDeleteByPK(ctx, rt, []int64{rows[0].UID, rows[1].UID, 9999}), rows[0].UID, 9999)
			missing(academy.TableEnrollment.BatchHardDeleteByPK(ctx, rt, []int64{rows[2].UID, 9999}), 9999)

			if n, err := tsq.Select(academy.TableEnrollment.Columns()...).From(academy.TableEnrollment.WithDeleted()).Count(ctx, rt); err != nil || n != 2 {
				t.Fatalf("rows left = %d, %v; want the two soft-deleted ones", n, err)
			}
		})
	}
}

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

			on := academy.TableEnrollment.LearnerID.EQ(academy.TableLearner.ID)
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
				return tsq.Select(academy.TableLearner.ID).From(academy.TableLearner)
			}

			count("inner join", from().InnerJoin(academy.TableEnrollment, on), 1)
			count("left join without a live enrollment", from().LeftJoin(academy.TableEnrollment, on).Where(academy.TableEnrollment.UID.IsNull()), 1)
			count("right join", from().RightJoin(academy.TableEnrollment, on), 1)
			count("inner join with deleted", from().InnerJoin(academy.TableEnrollment.WithDeleted(), on), 2)

			if tsqdialect.Supports(rt.Dialect(), tsqdialect.CapabilityFullJoin) {
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

			name := academy.TableLearner.Name
			str := func(col tsq.Expression[string], want string) {
				t.Helper()

				got, err := tsq.SelectNullValue(col).From(academy.TableLearner).MustBuild().Get(ctx, rt)
				if err != nil || !got.Valid || got.V != want {
					t.Errorf("%v = %v, %v; want %q", col, got, err, want)
				}
			}

			str(tsq.Trim(name), "Ünïcödé")
			// SQLite's UPPER and LOWER fold ASCII only, so they are checked on ASCII text.
			str(tsq.Lower(academy.TableLearner.Company), "acme")
			str(tsq.Upper(tsq.Lower(academy.TableLearner.Company)), "ACME")
			str(tsq.Substring(tsq.Trim(name), 2, 3), "nïc")
			str(tsq.NullIf(name, tsq.Val("x")), "  Ünïcödé  ")
			str(tsq.Coalesce(academy.TableLearner.Company, tsq.Val("none")), "ACME")

			length := tsq.Length(tsq.Trim(name))
			if n, err := tsq.SelectValue(length).From(academy.TableLearner).MustBuild().Get(ctx, rt); err != nil || *n != 7 {
				t.Errorf("Length() = %v, %v; want 7 characters", n, err)
			}

			score := academy.TableEnrollment.Score
			num := func(col tsq.Expression[int64], want int64) {
				t.Helper()

				got, err := tsq.SelectNullValue(col).From(academy.TableEnrollment).MustBuild().Get(ctx, rt)
				if err != nil || !got.Valid || got.V != want {
					t.Errorf("%v = %v, %v; want %d", col, got, err, want)
				}
			}

			created := academy.TableEnrollment.CreatedAt
			num(tsq.Sum(score), 9)
			num(tsq.Max(score), 10)
			num(tsq.Min(score), -7)
			num(tsq.Count(score), 4)
			num(tsq.CountDistinct(score), 3)
			num(tsq.Max(tsq.Year(created)), 2026)
			num(tsq.Max(tsq.Month(created)), 3)
			num(tsq.Max(tsq.Day(created)), 4)
			num(tsq.Abs(tsq.Min(score)), 7)
			num(tsq.Add(tsq.Max(score), tsq.Val(int64(5))), 15)
			num(tsq.Sub(tsq.Min(score), tsq.Val(int64(3))), -10)
			num(tsq.Mul(tsq.Max(score), tsq.Val(int64(3))), 30)
			// Integer division truncates toward zero on every dialect (DIV on MySQL).
			num(tsq.Div(tsq.Max(score), tsq.Val(int64(4))), 2)
			num(tsq.Div(tsq.Min(score), tsq.Val(int64(2))), -3)
			// SUM of an integer column is NUMERIC on PostgreSQL and DECIMAL on MySQL.
			num(tsq.Div(tsq.Sum(score), tsq.Val(int64(2))), 4)

			dec := func(col tsq.Expression[float64], want float64) {
				t.Helper()

				got, err := tsq.SelectNullValue(col).From(academy.TableEnrollment).MustBuild().Get(ctx, rt)
				if err != nil || !got.Valid || got.V != want {
					t.Errorf("%v = %v, %v; want %v", col, got, err, want)
				}
			}

			dec(tsq.Avg(score), 2.25)
			dec(tsq.Round(tsq.Avg(score), 1), 2.3)
			dec(tsq.Ceil(tsq.Avg(score)), 3)
			dec(tsq.Floor(tsq.Avg(score)), 2)
			dec(tsq.Div(tsq.Avg(score), tsq.Val(2.0)), 1.125)

			day := tsq.Max(tsq.Date(created))
			if got, err := tsq.SelectNullValue(day).From(academy.TableEnrollment).MustBuild().Get(ctx, rt); err != nil || got.V != "2026-03-04" {
				t.Errorf("Date() = %v, %v", got, err)
			}

			// SelectValue refuses what SelectNullValue reads.
			if _, err := tsq.SelectValue(day).From(academy.TableEnrollment).MustBuild().Get(ctx, rt); err == nil {
				t.Error("expected SelectValue to refuse an aggregate without GROUP BY")
			}

			distinct, err := tsq.SelectDistinct(score).From(academy.TableEnrollment).MustBuild().Count(ctx, rt)
			if err != nil || distinct != 3 {
				t.Errorf("SelectDistinct count = %d, %v; want 3", distinct, err)
			}
		})
	}
}

// TestMySQLErrorsAreClassifiedWithoutImportingTheDriver checks the reflection the
// root package uses to read *mysql.MySQLError, which it cannot import without
// adding the driver to every user's module graph.
func TestMySQLErrorsAreClassifiedWithoutImportingTheDriver(t *testing.T) {
	deadlock := fmt.Errorf("commit: %w", &mysql.MySQLError{Number: 1213, Message: "deadlock"})
	if !tsq.IsTxConflictError(deadlock) || !tsq.IsRetryableTxError(deadlock) {
		t.Fatal("expected a wrapped deadlock to be a transaction conflict")
	}

	joined := errors.Join(errors.New("other"), &mysql.MySQLError{Number: 1205})
	if !tsq.IsTxConflictError(joined) {
		t.Fatal("expected a lock wait timeout inside errors.Join to be a transaction conflict")
	}

	// NOWAIT finding the row locked: PostgreSQL's 55P03 was retried, MySQL's was not.
	if !tsq.IsTxConflictError(&mysql.MySQLError{Number: 3572}) {
		t.Fatal("expected a NOWAIT lock failure to be a transaction conflict")
	}

	if tsq.IsTxConflictError(&mysql.MySQLError{Number: 1062}) {
		t.Fatal("a duplicate key is not a transaction conflict")
	}
}

// placeholder is the first bind placeholder of rt's dialect, for raw SQL.
func placeholder(rt *tsq.Runtime) string {
	if rt.Dialect() == tsqdialect.Postgres {
		return "$1"
	}

	return "?"
}

// isRowState reports a *RowStateError, which callers match with errors.AsType.
func isRowState(err error) bool {
	_, ok := errors.AsType[*tsq.RowStateError](err)

	return ok
}

// TestIntegrationBatchInsertKeysFollowTheAutoIncrementStep backfills the keys of a
// multi-row INSERT under auto_increment_increment = 2, as a multi-primary MySQL
// setup runs. The keys used to be assumed consecutive, so every row but the first
// got another row's key.
func TestIntegrationBatchInsertKeysFollowTheAutoIncrementStep(t *testing.T) {
	for _, target := range integrationTargets(t) {
		t.Run(target.name, func(t *testing.T) {
			ctx := context.Background()

			dropAcademyTables(t, target)
			rt, _ := openWithPolicy(t, target, academy.TSQTables(), tsq.SchemaPolicyReconcile)

			learners := []*academy.Learner{
				{Name: "Ada", Email: "ada@step.test", Company: "A"},
				{Name: "Bob", Email: "bob@step.test", Company: "B"},
				{Name: "Cyd", Email: "cyd@step.test", Company: "C"},
			}

			// The session variable lives on one connection, so the insert runs in a
			// transaction on it.
			err := rt.WithTx(ctx, func(ctx context.Context, tx tsq.Executor) error {
				if target.name == "mysql" {
					if _, err := tx.ExecContext(ctx, "SET SESSION auto_increment_increment = 2"); err != nil {
						return err
					}
				}

				return academy.TableLearner.BatchInsert(ctx, tx, learners)
			})
			if err != nil {
				t.Fatalf("batch insert: %v", err)
			}

			for _, learner := range learners {
				stored, err := academy.TableLearner.Get(ctx, rt, learner.ID)
				if err != nil || stored.Email != learner.Email {
					t.Fatalf("key %d of %s reads back %v, %v", learner.ID, learner.Email, stored, err)
				}
			}
		})
	}
}

// TestIntegrationUpdateWithoutAVersionTellsUnchangedFromMissing updates a table
// with no version and no updated_at. MySQL counts only the rows a statement
// changes, so writing a row's own values reports none affected, which must not
// read as a missing row; a row that is really gone must.
func TestIntegrationUpdateWithoutAVersionTellsUnchangedFromMissing(t *testing.T) {
	for _, target := range integrationTargets(t) {
		t.Run(target.name, func(t *testing.T) {
			ctx := context.Background()

			dropAcademyTables(t, target)
			rt, _ := openWithPolicy(t, target, academy.TSQTables(), tsq.SchemaPolicyReconcile)

			instructor := &academy.Instructor{Name: "Ada", Email: "ada@unchanged.test"}
			if err := instructor.Insert(ctx, rt); err != nil {
				t.Fatalf("insert: %v", err)
			}

			if err := academy.TableInstructor.Update(ctx, rt, instructor); err != nil {
				t.Fatalf("update with unchanged values: %v", err)
			}

			gone := *instructor
			gone.ID += 1000

			if err := academy.TableInstructor.Update(ctx, rt, &gone); !errors.As(err, new(*tsq.RowStateError)) {
				t.Fatalf("update of a missing row = %v; want a RowStateError", err)
			}
		})
	}
}

type keyOnlyRow struct{ ID int64 }

// TestIntegrationStampsAndKeysRoundTrip covers two writes each dialect spelled
// differently: a table whose only column is its generated key, which rendered
// "INSERT INTO t () VALUES ()" (MySQL only), and a stamped time, which MySQL's
// DATETIME rounded to the second so the row in memory disagreed with the row read
// back.
func TestIntegrationStampsAndKeysRoundTrip(t *testing.T) {
	h := tsq.NewTable[keyOnlyRow, int64]("tsq_key_only")
	id := tsq.NewColumn(h, "id", "id", func(r *keyOnlyRow) *int64 { return &r.ID })
	keyOnly := h.Define(tsq.TableSpec[keyOnlyRow, int64]{
		Columns:       []tsq.BoundColumn[keyOnlyRow]{id},
		PrimaryKey:    id,
		AutoIncrement: true,
		ColumnSpecs: []tsqdialect.ColumnSpec{
			{Name: "id", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64}, PrimaryKey: true, AutoIncrement: true},
		},
	})

	for _, target := range integrationTargets(t) {
		t.Run(target.name, func(t *testing.T) {
			ctx := context.Background()

			dropAcademyTables(t, target)

			db, err := sql.Open(target.driver, target.dsn)
			if err != nil {
				t.Fatal(err)
			}

			_, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS tsq_key_only")
			_ = db.Close()

			rt, _ := openWithPolicy(t, target, append(academy.TSQTables(), keyOnly), tsq.SchemaPolicyReconcile)

			one := &keyOnlyRow{}
			if err := keyOnly.Insert(ctx, rt, one); err != nil || one.ID == 0 {
				t.Fatalf("Insert = %+v, %v", one, err)
			}

			batch := []*keyOnlyRow{{}, {}}
			if err := keyOnly.BatchInsert(ctx, rt, batch); err != nil {
				t.Fatal(err)
			}

			if n, err := tsq.Select(id).From(keyOnly).MustBuild().Count(ctx, rt); err != nil || n != 3 {
				t.Fatalf("rows = %d, %v", n, err)
			}

			learner := &academy.Learner{Name: "Stamp", Email: "stamp@example.com"}
			if err := learner.Insert(ctx, rt); err != nil {
				t.Fatal(err)
			}

			stored, err := academy.TableLearner.Get(ctx, rt, learner.ID)
			if err != nil || !stored.CreatedAt.V.Equal(learner.CreatedAt.V) {
				t.Fatalf("stored created_at = %v, in memory %v (%v)", stored.CreatedAt.V, learner.CreatedAt.V, err)
			}
		})
	}
}

//go:fix inline
func deref(s *string) string {
	if s == nil {
		return "<nil>"
	}

	return *s
}

// parcel is a row with a string key, a version and the values a list of bound
// parameters loses its types over: bytes that are not text, JSON, an integer
// above the signed range, a named bool, a time and a string with a length.
type parcel struct {
	Code    string
	Version int64
	Blob    []byte
	Doc     json.RawMessage
	Big     uint64
	On      flag
	At      time.Time
	Note    sql.Null[string]
}

var parcels = func() *tsq.TableOf[parcel, string] {
	h := tsq.NewTable[parcel, string]("parcels")
	code := tsq.NewColumn(h, "code", "code", func(r *parcel) *string { return &r.Code })
	version := tsq.NewColumn(h, "version", "version", func(r *parcel) *int64 { return &r.Version })

	return h.Define(tsq.TableSpec[parcel, string]{
		Columns: []tsq.BoundColumn[parcel]{
			code, version,
			tsq.NewColumn(h, "blob_value", "blob_value", func(r *parcel) *[]byte { return &r.Blob }),
			tsq.NewColumn(h, "doc", "doc", func(r *parcel) *json.RawMessage { return &r.Doc }),
			tsq.NewColumn(h, "big", "big", func(r *parcel) *uint64 { return &r.Big }),
			tsq.NewColumn(h, "is_on", "is_on", func(r *parcel) *flag { return &r.On }),
			tsq.NewColumn(h, "at", "at", func(r *parcel) *time.Time { return &r.At }),
			tsq.NewNullColumn[string](h, "note", "note", func(r *parcel) *sql.Null[string] { return &r.Note }),
		},
		PrimaryKey: code,
		Version:    version,
		ColumnSpecs: []tsqdialect.ColumnSpec{
			{Name: "code", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindString, Size: 20}, PrimaryKey: true},
			{Name: "version", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64}, Default: "1"},
			{Name: "blob_value", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindBytes}},
			{Name: "doc", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindBytes, RawType: "JSON"}},
			{Name: "big", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64, Unsigned: true}},
			{Name: "is_on", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindBool}},
			{Name: "at", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindTime}},
			{Name: "note", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindString, Size: 5, Nullable: true}},
		},
	})
}()

// TestIntegrationBatchUpdateCarriesEveryValue covers the statement a batch update
// of several rows is: the table joined to the list of the rows' values. A list of
// parameters has no types, and each engine went wrong over it in its own way when
// the list was written plainly: PostgreSQL read every value as text, and MySQL
// refused bytes that are not text. The values must arrive as a single-row update
// writes them, a string longer than its column must be refused and not cut, and a
// stale row must still be told from the rows that were written.
//
// On MySQL it runs again over a table of another character set than the
// connection's, which is what a schema older than utf8mb4 is: the server refuses
// to give the list a type there ("Illegal mix of collations ... for operation
// 'UNION'"), and the rows are written one by one.
func TestIntegrationBatchUpdateCarriesEveryValue(t *testing.T) {
	var targets []integrationTarget

	for _, target := range integrationTargets(t) {
		targets = append(targets, target)

		if target.name == "mysql" {
			targets = append(targets, target)
		}
	}

	for i, target := range targets {
		latin1 := i > 0 && targets[i-1].name == target.name

		name := target.name
		if latin1 {
			name += " over a latin1 table"
		}

		t.Run(name, func(t *testing.T) {
			ctx := context.Background()

			dropTables(t, target, "parcels")

			rt, _, err := openQuietly(target, tsq.SchemaPolicyCreateMissing, parcels)
			if err != nil {
				t.Fatalf("open: %v", err)
			}

			defer func() { _ = rt.Close() }()

			if latin1 {
				if _, err := rt.ExecContext(ctx, "ALTER TABLE parcels CONVERT TO CHARACTER SET latin1"); err != nil {
					t.Fatalf("convert the table: %v", err)
				}
			}

			// SQLite's integers are signed: database/sql refuses a larger one there.
			big := uint64(1<<63) + 5
			if target.name == "sqlite" {
				big = 1 << 62
			}

			rows := make([]*parcel, 4)
			for i := range rows {
				rows[i] = &parcel{Code: fmt.Sprintf("p%d", i), Blob: []byte{1}, Doc: json.RawMessage(`{}`), At: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)}
			}

			if err := parcels.BatchInsert(ctx, rt, rows); err != nil {
				t.Fatalf("insert: %v", err)
			}

			at := time.Date(2025, 2, 3, 4, 5, 6, 789012000, time.UTC)

			for i, row := range rows {
				row.Blob = []byte{0xff, 0x00, 0x80, byte(i)}
				// A document MySQL stores in a form of its own (keys sorted, spaces
				// added): the rows of a batch with a stale row are told apart by
				// reading them back, and that must compare the document, not its text.
				row.Doc = json.RawMessage(fmt.Sprintf(`{"z":1,"n":[%d,"it's"]}`, i))
				row.Big = big + uint64(i)
				row.On = i%2 == 0
				row.At = at.Add(time.Duration(i) * time.Microsecond)
				row.Note = sql.Null[string]{}
				if i%2 == 1 {
					row.Note = sql.Null[string]{V: "abc", Valid: true}
				}
			}

			if err := parcels.BatchUpdate(ctx, rt, rows); err != nil {
				t.Fatalf("batch update: %v", err)
			}

			stored, err := parcels.Fetch(ctx, rt, "p0", "p1", "p2", "p3")
			if err != nil {
				t.Fatalf("fetch: %v", err)
			}

			for i, got := range stored {
				want := rows[i]

				var doc, wantDoc any
				if err := json.Unmarshal(got.Doc, &doc); err != nil {
					t.Fatalf("row %d holds %q as its document: %v", i, got.Doc, err)
				}

				_ = json.Unmarshal(want.Doc, &wantDoc)

				if got.Version != 2 || want.Version != 2 || !slices.Equal(got.Blob, want.Blob) || fmt.Sprint(doc) != fmt.Sprint(wantDoc) ||
					got.Big != want.Big || got.On != want.On || !got.At.Equal(want.At) || got.Note != want.Note {
					t.Fatalf("row %d was written as %+v, want %+v", i, *got, *want)
				}
			}

			// A stale row is named; the rows beside it are written.
			rows[1].Version = 1
			for _, row := range rows {
				row.Big++
			}

			err = parcels.BatchUpdate(ctx, rt, rows)

			var conflict *tsq.OptimisticLockError
			if !errors.As(err, &conflict) || len(conflict.Keys) != 1 || conflict.Keys[0] != "p1" {
				t.Fatalf("a batch with one stale row: %v", err)
			}

			if got, err := parcels.Get(ctx, rt, "p2"); err != nil || got.Big != rows[2].Big || got.Version != 3 || rows[2].Version != 3 {
				t.Fatalf("the row beside the stale one: %+v (in memory version %d), %v", got, rows[2].Version, err)
			}

			if got, err := parcels.Get(ctx, rt, "p1"); err != nil || got.Version != 2 || got.Big != big+1 {
				t.Fatalf("the stale row was written: %+v, %v", got, err)
			}

			// A string longer than its column is refused where a length is enforced.
			if target.name != "sqlite" {
				fresh, err := parcels.Fetch(ctx, rt, "p0", "p3")
				if err != nil {
					t.Fatalf("fetch: %v", err)
				}

				for _, row := range fresh {
					row.Note = sql.Null[string]{V: "abcdefghij", Valid: true}
				}

				if err := parcels.BatchUpdate(ctx, rt, fresh); err == nil {
					got, _ := parcels.Get(ctx, rt, "p3")
					t.Fatalf("a ten-character note went into a column of five; it now holds %q", got.Note.V)
				}

				if got, err := parcels.Get(ctx, rt, "p3"); err != nil || got.Note.V != "abc" {
					t.Fatalf("the refused update changed the note: %+v, %v", got, err)
				}
			}
		})
	}
}

// TestIntegrationAnUnsetRawMessageIsTheJSONNull covers a json.RawMessage field
// that was never set. A nil byte slice is bound as empty bytes, which is not
// JSON: MySQL and PostgreSQL refused the row for a JSON column. It is the JSON
// null, as encoding/json writes a nil RawMessage.
func TestIntegrationAnUnsetRawMessageIsTheJSONNull(t *testing.T) {
	// A driver that writes its parameters into the statement spells a byte slice
	// as binary, which a JSON column refuses: the null must still read as JSON.
	inline := map[string]string{"mysql": "&interpolateParams=true", "postgres": "&default_query_exec_mode=simple_protocol"}

	var targets []integrationTarget

	for _, target := range integrationTargets(t) {
		targets = append(targets, target)

		if param, ok := inline[target.name]; ok {
			target.name, target.dsn = target.name+" with inline parameters", target.dsn+param
			targets = append(targets, target)
		}
	}

	for _, target := range targets {
		t.Run(target.name, func(t *testing.T) {
			ctx := context.Background()

			dropTables(t, target, "parcels")

			rt, _, err := openQuietly(target, tsq.SchemaPolicyCreateMissing, parcels)
			if err != nil {
				t.Fatalf("open: %v", err)
			}

			defer func() { _ = rt.Close() }()

			rows := []*parcel{{Code: "a"}, {Code: "b", Doc: json.RawMessage{}}, {Code: "c", Doc: json.RawMessage(`[1]`)}}
			if err := parcels.Insert(ctx, rt, rows[0]); err != nil {
				t.Fatalf("insert a row whose document was never set: %v", err)
			}

			if err := parcels.BatchInsert(ctx, rt, rows[1:]); err != nil {
				t.Fatalf("batch insert: %v", err)
			}

			rows[2].Doc = nil
			if err := parcels.BatchUpdate(ctx, rt, rows[1:]); err != nil {
				t.Fatalf("batch update to an unset document: %v", err)
			}

			stored, err := parcels.Fetch(ctx, rt, "a", "b", "c")
			if err != nil {
				t.Fatalf("fetch: %v", err)
			}

			for _, row := range stored {
				if strings.TrimSpace(string(row.Doc)) != "null" {
					t.Errorf("row %s holds %q, want the JSON null", row.Code, row.Doc)
				}
			}
		})
	}
}

type held struct {
	ID   int64
	Note string
	Doc  json.RawMessage
	Text string
}

type heldTable struct {
	*tsq.TableOf[held, int64]

	ID   tsq.Column[held, int64]
	Note tsq.Column[held, string]
	Doc  tsq.Column[held, json.RawMessage]
	Text tsq.Column[held, string]
}

var heldCols = func() heldTable {
	h := tsq.NewTable[held, int64]("held")
	t := heldTable{
		TableOf: h,
		ID:      tsq.NewColumn(h, "id", "id", func(r *held) *int64 { return &r.ID }),
		Note:    tsq.NewColumn(h, "note", "note", func(r *held) *string { return &r.Note }),
		Doc:     tsq.NewColumn(h, "doc", "doc", func(r *held) *json.RawMessage { return &r.Doc }),
		Text:    tsq.NewColumn(h, "text", "text", func(r *held) *string { return &r.Text }),
	}

	h.Define(tsq.TableSpec[held, int64]{
		Columns:       []tsq.BoundColumn[held]{t.ID, t.Note, t.Doc, t.Text},
		PrimaryKey:    t.ID,
		AutoIncrement: true,
		ColumnSpecs: []tsqdialect.ColumnSpec{
			{Name: "id", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64}, PrimaryKey: true, AutoIncrement: true},
			{Name: "note", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindString, Size: 5}},
			{Name: "doc", Type: tsqdialect.ColumnType{RawType: "JSON"}},
			{Name: "text", Type: tsqdialect.ColumnType{RawType: "TEXT"}},
		},
	})

	return t
}()

// TestIntegrationValuesAreHeldToTheColumnOnEveryEngine covers a value SQLite
// takes and the other engines refuse: a string longer than its VARCHAR, which
// SQLite stores whole since it enforces no length, and a json.RawMessage that is
// not JSON, which it stores as text having no JSON type. Both passed on a SQLite
// development database and failed in production. They are refused before the
// statement runs, by every write that names the column; a comparison is not held.
func TestIntegrationValuesAreHeldToTheColumnOnEveryEngine(t *testing.T) {
	ctx := context.Background()
	h := heldCols
	long := "abcdef"
	accents := strings.Repeat("é", 5) // five characters, ten bytes

	for _, target := range integrationTargets(t) {
		t.Run(target.name, func(t *testing.T) {
			dropTables(t, target, "held")

			rt, _, err := openQuietly(target, tsq.SchemaPolicyCreateMissing, h)
			if err != nil {
				t.Fatalf("create: %v", err)
			}

			defer func() { _ = rt.Close() }()

			kept := &held{Note: accents, Doc: json.RawMessage(`{"ok":true}`), Text: strings.Repeat("x", 1000)}
			if err := h.Insert(ctx, rt, kept); err != nil {
				t.Fatalf("a value of five characters in ten bytes, and a thousand in a TEXT: %v", err)
			}

			param := h.Note.Param()

			for name, write := range map[string]func() error{
				"insert": func() error { return h.Insert(ctx, rt, &held{Note: long, Doc: json.RawMessage(`{}`)}) },
				"batch insert": func() error {
					return h.BatchInsert(ctx, rt, []*held{{Note: "ok", Doc: json.RawMessage(`{}`)}, {Note: long, Doc: json.RawMessage(`{}`)}})
				},
				"update": func() error { c := *kept; c.Note = long; return h.Update(ctx, rt, &c) },
				"upsert": func() error { c := *kept; c.Note = long; return h.Upsert(ctx, rt, &c) },
				"set a value": func() error {
					_, err := tsq.UpdateTable(h).Set(h.Note, tsq.Val(long)).Where(h.ID.EQ(tsq.Val(kept.ID))).MustBuild().Exec(ctx, rt)
					return err
				},
				"set a parameter": func() error {
					_, err := tsq.UpdateTable(h).Set(h.Note, param).Where(h.ID.EQ(tsq.Val(kept.ID))).MustBuild().Exec(ctx, rt, param.Bind(long))
					return err
				},
				"insert bad json": func() error { return h.Insert(ctx, rt, &held{Note: "ok", Doc: json.RawMessage(`{not json`)}) },
				"set bad json": func() error {
					_, err := tsq.UpdateTable(h).Set(h.Doc, tsq.Val(json.RawMessage(`[1,`))).Where(h.ID.EQ(tsq.Val(kept.ID))).MustBuild().Exec(ctx, rt)
					return err
				},
				"six accents": func() error { c := *kept; c.Note = accents + "é"; return h.Update(ctx, rt, &c) },
			} {
				err := write()
				if err == nil {
					t.Errorf("%s: the value was taken", name)

					continue
				}

				if target.name == "sqlite" && strings.Contains(name, "json") && !strings.Contains(err.Error(), "not valid JSON") {
					t.Errorf("%s: %v; want the value named as not JSON", name, err)
				}

				if target.name == "sqlite" && !strings.Contains(name, "json") && !strings.Contains(err.Error(), "the column holds 5") {
					t.Errorf("%s: %v; want the column's length named", name, err)
				}
			}

			// A comparison is not held to the column: a longer value matches nothing.
			rows, err := tsq.Select(h.Columns()...).From(h).Where(h.Note.EQ(tsq.Val("abcdefgh"))).List(ctx, rt)
			if err != nil || len(rows) != 0 {
				t.Errorf("a comparison with a longer value: %d rows, %v", len(rows), err)
			}

			// Nothing of the refused writes reached the table.
			got, err := h.Get(ctx, rt, kept.ID)
			if err != nil || got.Note != accents || !json.Valid(got.Doc) {
				t.Fatalf("the row after the refused writes: %+v, %v", got, err)
			}

			if n, err := tsq.Select(h.ID).From(h).Count(ctx, rt); err != nil || n != 1 {
				t.Fatalf("rows after the refused writes: %d, %v", n, err)
			}
		})
	}
}

// keyedRow holds a key as its bytes in every shape the generator accepts for one:
// an array, a named array, and their nullable forms.
type keyedRow struct {
	ID    int64
	Key   [16]byte
	Named keyBytes
	Maybe sql.Null[[16]byte]
	Ptr   *keyBytes
}

type keyBytes [8]byte

type keyedTable struct {
	*tsq.TableOf[keyedRow, int64]

	ID    tsq.Column[keyedRow, int64]
	Key   tsq.Column[keyedRow, [16]byte]
	Named tsq.Column[keyedRow, keyBytes]
	Maybe tsq.NullColumn[keyedRow, [16]byte]
	Ptr   tsq.NullColumn[keyedRow, keyBytes]
}

var keyedCols = func() keyedTable {
	k := tsq.NewTable[keyedRow, int64]("keyed")
	t := keyedTable{
		TableOf: k,
		ID:      tsq.NewColumn(k, "id", "id", func(r *keyedRow) *int64 { return &r.ID }),
		Key:     tsq.NewColumn(k, "key", "key", func(r *keyedRow) *[16]byte { return &r.Key }),
		Named:   tsq.NewColumn(k, "named", "named", func(r *keyedRow) *keyBytes { return &r.Named }),
		Maybe:   tsq.NewNullColumn[[16]byte](k, "maybe", "maybe", func(r *keyedRow) *sql.Null[[16]byte] { return &r.Maybe }),
		Ptr:     tsq.NewNullColumn[keyBytes](k, "ptr", "ptr", func(r *keyedRow) **keyBytes { return &r.Ptr }),
	}

	k.Define(tsq.TableSpec[keyedRow, int64]{
		Columns:       []tsq.BoundColumn[keyedRow]{t.ID, t.Key, t.Named, t.Maybe, t.Ptr},
		PrimaryKey:    t.ID,
		AutoIncrement: true,
		ColumnSpecs: []tsqdialect.ColumnSpec{
			{Name: "id", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64}, PrimaryKey: true, AutoIncrement: true},
			{Name: "key", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindBytes, Size: 16}},
			{Name: "named", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindBytes, Size: 8}},
			{Name: "maybe", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindBytes, Size: 16, Nullable: true}},
			{Name: "ptr", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindBytes, Size: 8, Nullable: true}},
		},
	})

	return t
}()

// TestIntegrationByteArraysAreBoundAndReadAsBytes covers a [N]byte field, the
// shape a UUID or a hash is kept in: database/sql binds and scans slices of
// bytes and refuses arrays ("unsupported type [16]uint8, a array"), so a model
// the generator accepted could neither insert nor read its row. The array and
// its nullable forms now go to the driver as a slice and come back into the
// array, on every engine; a value of another length is a scan error.
func TestIntegrationByteArraysAreBoundAndReadAsBytes(t *testing.T) {
	ctx := context.Background()
	k := keyedCols
	key := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	named := keyBytes{8, 7, 6, 5, 4, 3, 2, 1}

	for _, target := range integrationTargets(t) {
		t.Run(target.name, func(t *testing.T) {
			dropTables(t, target, "keyed")

			rt, _, err := openQuietly(target, tsq.SchemaPolicyCreateMissing, k)
			if err != nil {
				t.Fatalf("create: %v", err)
			}

			defer func() { _ = rt.Close() }()

			row := &keyedRow{Key: key, Named: named}
			if err := k.Insert(ctx, rt, row); err != nil {
				t.Fatalf("insert: %v", err)
			}

			full := &keyedRow{Key: key, Named: named, Maybe: sql.Null[[16]byte]{V: key, Valid: true}, Ptr: &named}
			if err := k.Insert(ctx, rt, full); err != nil {
				t.Fatalf("insert with the nullable forms set: %v", err)
			}

			got, err := k.Get(ctx, rt, row.ID)
			if err != nil {
				t.Fatalf("get: %v", err)
			}

			if got.Key != key || got.Named != named || got.Maybe.Valid || got.Ptr != nil {
				t.Fatalf("read back %+v", got)
			}

			got, err = k.Get(ctx, rt, full.ID)
			if err != nil {
				t.Fatalf("get the full row: %v", err)
			}

			if got.Key != key || got.Named != named || !got.Maybe.Valid || got.Maybe.V != key || got.Ptr == nil || *got.Ptr != named {
				t.Fatalf("read back the full row %+v", got)
			}

			// The array compares and binds as a value, a parameter and a list.
			n, err := tsq.Select(k.ID).From(k).Where(k.Key.EQ(tsq.Val(key)), k.Maybe.EQ(tsq.Val(key))).MustBuild().Count(ctx, rt)
			if err != nil || n != 1 {
				t.Fatalf("compare = %d, %v", n, err)
			}

			param := k.Named.Param()

			rows, err := tsq.Select(k.Columns()...).From(k).Where(k.Named.EQ(param)).MustBuild().List(ctx, rt, param.Bind(named))
			if err != nil || len(rows) != 2 {
				t.Fatalf("parameter = %d rows, %v", len(rows), err)
			}

			found, err := tsq.Select(k.ID).From(k).Where(k.Key.In(tsq.Vals(key, [16]byte{}))).MustBuild().List(ctx, rt)
			if err != nil || len(found) != 2 {
				t.Fatalf("in a list = %d rows, %v", len(found), err)
			}

			// Updates and batch writes bind the same way, and the read-back after a
			// batch compares the arrays by value.
			got.Named = keyBytes{1}
			got.Maybe = sql.Null[[16]byte]{}

			if err := k.Update(ctx, rt, got); err != nil {
				t.Fatalf("update: %v", err)
			}

			if err := k.BatchUpdate(ctx, rt, []*keyedRow{row, got}); err != nil {
				t.Fatalf("batch update: %v", err)
			}

			if _, err := tsq.UpdateTable(k).Set(k.Key, tsq.Val([16]byte{9})).SetNull(k.Ptr).Where(k.ID.EQ(tsq.Val(full.ID))).MustBuild().Exec(ctx, rt); err != nil {
				t.Fatalf("set: %v", err)
			}

			got, err = k.Get(ctx, rt, full.ID)
			if err != nil || got.Key != [16]byte{9} || got.Named != (keyBytes{1}) || got.Maybe.Valid || got.Ptr != nil {
				t.Fatalf("after the updates %+v, %v", got, err)
			}

			// Bytes of another length, written by someone else, do not fit the array.
			db, err := sql.Open(target.driver, target.dsn)
			if err != nil {
				t.Fatal(err)
			}

			defer func() { _ = db.Close() }()

			second := "?"
			if rt.Dialect() == tsqdialect.Postgres {
				second = "$2"
			}

			if _, err := db.ExecContext(ctx, "UPDATE keyed SET named = "+placeholder(rt)+" WHERE id = "+second, []byte{1, 2}, row.ID); err != nil {
				t.Fatalf("two bytes: %v", err)
			}

			if short, err := k.Get(ctx, rt, row.ID); err == nil || !strings.Contains(err.Error(), "holds 8") {
				t.Fatalf("two bytes into eight = %+v, %v", short, err)
			}
		})
	}
}
