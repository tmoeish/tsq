package academy

import (
	"database/sql"
	_ "embed"
	"fmt"

	_ "github.com/mattn/go-sqlite3"

	"github.com/tmoeish/tsq/v4"
	tsqdialect "github.com/tmoeish/tsq/v4/dialect"
)

//go:embed mock.sql
var mockSQL string

// OpenSQLiteExampleDB opens the in-memory Academy example database and seeds it.
func OpenSQLiteExampleDB() (*tsq.Runtime, func(), error) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		return nil, nil, err
	}

	cleanup := func() {
		_ = db.Close()
	}

	if _, err := db.Exec(mockSQL); err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("%s: %w", "seed mock.sql", err)
	}

	if err := tsq.Init(db, tsqdialect.SQLiteDialect{}, &tsq.InitOptions{UpsertIndexes: true}); err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("%s: %w", "init tsq", err)
	}

	return tsq.DefaultRuntime(), cleanup, nil
}
