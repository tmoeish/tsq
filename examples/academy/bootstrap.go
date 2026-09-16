package academy

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"

	"github.com/tmoeish/tsq/v5"
	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
)

//go:embed mock.sql
var mockSQL string

// OpenSQLiteExampleDB opens the in-memory Academy example database and seeds it.
func OpenSQLiteExampleDB() (*tsq.Runtime, func(), error) {
	dir, err := os.MkdirTemp("", "tsq-academy-*")
	if err != nil {
		return nil, nil, err
	}
	dsn := filepath.Join(dir, "academy.db")

	baseCleanup := func() {
		_ = os.RemoveAll(dir)
	}

	// The pool is opened here because the schema is seeded before TSQ sees it,
	// which is what NewRuntimeFromDB is for: the caller keeps the pool it set up
	// and still gets tracers, SQL logging and the page-size cap.
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		baseCleanup()

		return nil, nil, err
	}

	cleanup := func() {
		_ = db.Close()

		baseCleanup()
	}

	if _, err := db.ExecContext(context.Background(), mockSQL); err != nil {
		cleanup()

		return nil, nil, fmt.Errorf("%s: %w", "seed mock.sql", err)
	}

	runtime, err := tsq.NewRuntimeFromDB(
		context.Background(),
		db,
		tsqdialect.SQLiteDialect{},
		TSQTables(),
		tsq.WithIndexPolicy(tsq.SchemaPolicyCreateMissing),
	)
	if err != nil {
		cleanup()

		return nil, nil, fmt.Errorf("%s: %w", "init tsq runtime", err)
	}

	return runtime, cleanup, nil
}
