package academy

import (
	"database/sql"
	_ "embed"

	"github.com/juju/errors"
	_ "github.com/mattn/go-sqlite3"

	"github.com/tmoeish/tsq"
)

//go:embed mock.sql
var mockSQL string

func OpenSQLiteExampleDB() (*tsq.Engine, func(), error) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	cleanup := func() {
		_ = db.Close()
	}

	if _, err := db.Exec(mockSQL); err != nil {
		cleanup()
		return nil, nil, errors.Annotate(err, "seed mock.sql")
	}

	engine := &tsq.Engine{DB: db, Dialect: tsq.SQLiteDialect{}}
	if err := tsq.Init(engine, true); err != nil {
		cleanup()
		return nil, nil, errors.Annotate(err, "init tsq")
	}

	return engine, cleanup, nil
}
