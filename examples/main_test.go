package main

import (
	"database/sql"
	"os"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/tmoeish/tsq"
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

	// Load mock.sql first (creates tables)
	mockSQL, err := os.ReadFile("database/mock.sql")
	if err != nil {
		t.Fatalf("read mock.sql: %v", err)
	}

	if _, err := db.Exec(string(mockSQL)); err != nil {
		t.Fatalf("seed mock.sql: %v", err)
	}

	// Then initialize tsq (tables already exist)
	dbmap := &tsq.DbMap{Db: db, Dialect: tsq.SqliteDialect{}}
	if err := tsq.Init(dbmap, false, true); err != nil {
		t.Fatalf("init tsq: %v", err)
	}

	return dbmap
}
