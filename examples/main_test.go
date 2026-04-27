package main

import (
	"context"
	"database/sql"
	"os"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/tmoeish/tsq"
	"github.com/tmoeish/tsq/examples/database"
)

func newExampleDBMap(t *testing.T) *tsq.DbMap {
	t.Helper()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}

	t.Cleanup(func() {
		_ = db.Close()
	})

	dbmap := &tsq.DbMap{Db: db, Dialect: tsq.SqliteDialect{}}
	if err := tsq.Init(dbmap, true, true); err != nil {
		t.Fatalf("init tsq: %v", err)
	}

	mockSQL, err := os.ReadFile("database/mock.sql")
	if err != nil {
		t.Fatalf("read mock.sql: %v", err)
	}

	if _, err := db.Exec(string(mockSQL)); err != nil {
		t.Fatalf("seed mock.sql: %v", err)
	}

	return dbmap
}

func TestPageUserOrderSmoke(t *testing.T) {
	dbmap := newExampleDBMap(t)

	resp, err := database.PageUserOrder(
		context.Background(),
		dbmap,
		&tsq.PageReq{
			Page:    1,
			Size:    10,
			Order:   "asc,desc",
			OrderBy: "user_id,order_id",
		},
		1,
		"图书",
		"视频",
		`杂fds""了''志`,
	)
	if err != nil {
		t.Fatalf("PageUserOrder returned error: %v", err)
	}

	if resp.Total != 2 || len(resp.Data) != 2 {
		t.Fatalf("expected 2 paged rows, got total=%d len=%d", resp.Total, len(resp.Data))
	}
}

func TestChunkedInsertDuplicateSmoke(t *testing.T) {
	dbmap := newExampleDBMap(t)
	ctx := context.Background()

	users := createTestUsers(3)
	if err := tsq.ChunkedInsert(ctx, dbmap, users); err != nil {
		t.Fatalf("ChunkedInsert returned error: %v", err)
	}

	duplicateUsers := createTestUsers(3)
	if err := tsq.ChunkedInsert(ctx, dbmap, duplicateUsers, &tsq.ChunkedInsertOptions{
		ChunkSize:    2,
		IgnoreErrors: true,
	}); err != nil {
		t.Fatalf("ChunkedInsert ignore duplicates returned error: %v", err)
	}

	count, err := dbmap.SelectInt("SELECT COUNT(*) FROM user WHERE name LIKE 'demo_user_%'")
	if err != nil {
		t.Fatalf("count users: %v", err)
	}

	if count != 3 {
		t.Fatalf("expected duplicate insert to preserve 3 demo users, got %d", count)
	}
}
