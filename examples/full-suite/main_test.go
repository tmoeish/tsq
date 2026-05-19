package main

import (
	"context"
	"testing"

	"github.com/tmoeish/tsq/v4/examples/academy"
)

func TestFullSuite(t *testing.T) {
	engine, cleanup, err := academy.OpenSQLiteExampleDB()
	if err != nil {
		t.Fatalf("open example db: %v", err)
	}
	t.Cleanup(cleanup)

	summary, err := academy.RunFullSuite(context.Background(), engine)
	if err != nil {
		t.Fatalf("run full suite: %v", err)
	}

	if !summary.Quickstart.TrackCRUD.DeletedSuccessfully {
		t.Fatal("expected quickstart CRUD demo to succeed")
	}

	if len(summary.Advanced.SetOps.UnionTitles) == 0 {
		t.Fatal("expected advanced demos to populate set ops output")
	}

	if summary.Comprehensive.Total == 0 || summary.Comprehensive.First == nil {
		t.Fatal("expected comprehensive demo to return result rows")
	}
}
