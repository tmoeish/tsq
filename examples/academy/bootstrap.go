package academy

import (
	"database/sql"
	_ "embed"
	"fmt"

	_ "github.com/mattn/go-sqlite3"

	"github.com/tmoeish/tsq/v4"
)

//go:embed mock.sql
var mockSQL string

func OpenSQLiteExampleDB() (*tsq.Engine, func(), error) {
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

	engine := &tsq.Engine{DB: db, Dialect: tsq.SQLiteDialect{}}
	if err := tsq.Init(engine, &tsq.InitOptions{UpsertIndexes: true}); err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("%s: %w", "init tsq", err)
	}

	return engine, cleanup, nil
}
