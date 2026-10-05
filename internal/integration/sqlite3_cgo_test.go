//go:build cgo

package integration_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"github.com/tmoeish/tsq/v5"
	"github.com/tmoeish/tsq/v5/internal/integration/academy"
)

// TestSQLite3DriverIsSupported runs TSQ over github.com/mattn/go-sqlite3, the CGO
// SQLite driver, which registers itself as "sqlite3". The queries were always
// portable; what needed the driver itself is error classification, because it
// reports SQLite result codes in struct fields rather than through a method, so
// duplicate keys and busy databases would otherwise go unrecognized.
//
// It lives here, not in the root package, so the CGO driver stays out of the
// module graph of anyone who only imports the library.
func TestSQLite3DriverIsSupported(t *testing.T) {
	ctx := context.Background()

	rt, err := tsq.Open(ctx, "sqlite3", filepath.Join(t.TempDir(), "cgo.db"), academy.TSQTables(),
		tsq.WithSchemaPolicy(tsq.SchemaPolicyReconcile))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = rt.Close() })

	learner := &academy.Learner{Name: "Cgo", Email: "cgo@example.test"}
	if err := learner.Insert(ctx, rt); err != nil {
		t.Fatal(err)
	}

	// The unique index is what proves the error type is understood.
	duplicate := &academy.Learner{Name: "Cgo again", Email: "cgo@example.test"}
	if err := duplicate.Insert(ctx, rt); !tsq.IsDuplicateKeyError(err) {
		t.Fatalf("duplicate insert = %v; want a duplicate key error", err)
	}

	if err := academy.TableLearner.BatchInsert(ctx, rt, []*academy.Learner{
		{Name: "Third", Email: "cgo@example.test"},
		{Name: "Fourth", Email: "fourth@example.test"},
	}, tsq.WithSkipDuplicates()); err != nil {
		t.Fatalf("skip duplicates on the cgo driver: %v", err)
	}

	rows, err := academy.TableLearner.Query().Count(ctx, rt)
	if err != nil || rows != 2 {
		t.Fatalf("learners = %d, %v; want the duplicate skipped", rows, err)
	}

	// This driver is built without SQLite's math functions unless asked: CEIL and
	// FLOOR are "no such function" there, and TSQ spells them without. An integer
	// is not rounded at all, so the value is a floating-point one: 2.5, whatever
	// the keys are.
	mean := tsq.Avg(academy.TableLearner.ID)
	half := tsq.Add(tsq.Sub(mean, mean), tsq.Val(2.5))

	up, err := tsq.SelectNullValue(tsq.Ceil(half)).From(academy.TableLearner).Get(ctx, rt)
	if err != nil || up.V != 3 {
		t.Fatalf("Ceil on the cgo driver: %+v, %v; want 3", up, err)
	}

	down, err := tsq.SelectNullValue(tsq.Floor(half)).From(academy.TableLearner).Get(ctx, rt)
	if err != nil || down.V != 2 {
		t.Fatalf("Floor on the cgo driver: %+v, %v; want 2", down, err)
	}

	stored, err := academy.TableLearner.FetchByEmail(ctx, rt, "cgo@example.test")
	if err != nil || len(stored) != 1 || !stored[0].CreatedAt.Valid {
		t.Fatalf("stored = %+v, %v", stored, err)
	}

	if _, err := academy.TableLearner.GetByEmail(ctx, rt, "missing@example.test"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("missing row = %v", err)
	}
}
