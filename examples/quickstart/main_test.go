package main

import (
	"context"
	"testing"

	"github.com/tmoeish/tsq/v4/examples/academy"
)

func TestQuickstart(t *testing.T) {
	rt, cleanup, err := academy.OpenSQLiteExampleDB()
	if err != nil {
		t.Fatalf("open example db: %v", err)
	}
	t.Cleanup(cleanup)

	summary, err := academy.RunQuickstart(context.Background(), rt)
	if err != nil {
		t.Fatalf("run quickstart: %v", err)
	}

	if !summary.TrackCRUD.DeletedSuccessfully {
		t.Fatal("expected CRUD demo to delete the inserted track")
	}

	if len(summary.TrackCRUD.UpdatedSkillItems) == 0 {
		t.Fatal("expected CRUD demo to round-trip custom JSON skill items")
	}

	if summary.CatalogSearch.Total == 0 || len(summary.CatalogSearch.Titles) == 0 {
		t.Fatal("expected catalog search to return rows")
	}

	if len(summary.BackendCatalog.Titles) == 0 {
		t.Fatal("expected backend catalog to return rows")
	}
}
