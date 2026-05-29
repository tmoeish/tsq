package academy

import (
	_ "embed"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"

	"github.com/tmoeish/tsq/v4"
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

	db, err := tsq.NewRuntime("sqlite", dsn, nil)
	if err != nil {
		baseCleanup()
		return nil, nil, err
	}

	if _, err := db.DB().Exec(mockSQL); err != nil {
		baseCleanup()
		return nil, nil, fmt.Errorf("%s: %w", "seed mock.sql", err)
	}
	_ = db.DB().Close()

	runtime, err := tsq.NewRuntime("sqlite", dsn, TSQTables(), &tsq.RuntimeOptions{IndexPolicy: tsq.SchemaPolicyCreateMissing})
	if err != nil {
		baseCleanup()
		return nil, nil, fmt.Errorf("%s: %w", "init tsq runtime", err)
	}

	cleanup := func() {
		_ = runtime.DB().Close()

		baseCleanup()
	}

	return runtime, cleanup, nil
}
