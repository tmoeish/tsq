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
	"github.com/tmoeish/tsq/v5/examples/academy"
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

	rows, err := academy.QueryLearner.Count(ctx, rt)
	if err != nil || rows != 2 {
		t.Fatalf("learners = %d, %v; want the duplicate skipped", rows, err)
	}

	stored, err := academy.FetchLearnerByEmail(ctx, rt, "cgo@example.test")
	if err != nil || len(stored) != 1 || !stored[0].CreatedAt.Valid {
		t.Fatalf("stored = %+v, %v", stored, err)
	}

	if _, err := academy.QueryLearnerByEmail.Get(ctx, rt, academy.Learner_Email.Bind("missing@example.test")); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("missing row = %v", err)
	}
}
