package main

import (
	"context"
	"testing"

	"github.com/tmoeish/tsq"
	"github.com/tmoeish/tsq/examples/database"
)

func newExampleDBMap(t *testing.T) *tsq.DbMap {
	t.Helper()

	dbmap, cleanup, err := openExampleDB()
	if err != nil {
		t.Fatalf("open example db: %v", err)
	}

	t.Cleanup(cleanup)

	return dbmap
}

func TestRunAllExamples(t *testing.T) {
	dbmap := newExampleDBMap(t)

	summary, err := runAllExamples(context.Background(), dbmap)
	if err != nil {
		t.Fatalf("run all examples: %v", err)
	}

	if !summary.CRUD.DeletedSuccessfully {
		t.Fatal("expected CRUD demo to delete the inserted category")
	}

	if summary.DTO.Total == 0 || summary.DTO.First == nil {
		t.Fatal("expected DTO demo to return rows")
	}

	if len(summary.InVar.ItemNames) == 0 {
		t.Fatal("expected InVar demo to return items")
	}

	if summary.Chunked.Before != summary.Chunked.After {
		t.Fatalf("expected chunked demo to leave user count unchanged, got before=%d after=%d", summary.Chunked.Before, summary.Chunked.After)
	}
}

func TestPageUserOrderSmoke(t *testing.T) {
	dbmap := newExampleDBMap(t)
	pageReq := &tsq.PageReq{
		Page:    1,
		Size:    5,
		OrderBy: "user_id,order_id",
		Order:   "asc,asc",
	}

	if err := pageReq.ValidateStrict(); err != nil {
		t.Fatalf("validate page request: %v", err)
	}

	resp, err := database.PageUserOrder(context.Background(), dbmap, pageReq, 1, "图书", "视频")
	if err != nil {
		t.Fatalf("page user order: %v", err)
	}

	if resp.Total == 0 || len(resp.Data) == 0 {
		t.Fatal("expected DTO page query to return rows")
	}

	if resp.Data[0].UserID != 1 {
		t.Fatalf("expected DTO page to be filtered by user_id=1, got %#v", resp.Data[0])
	}
}
