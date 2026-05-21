package tsq

import (
	"database/sql"

	tsqdialect "github.com/tmoeish/tsq/v4/dialect"
)

type engine struct {
	db            *sql.DB
	dialect       tsqdialect.Dialect
	indexInitMode IndexInitMode
}

func newEngine(db *sql.DB, sqlDialect tsqdialect.Dialect) *engine {
	return &engine{
		db:      db,
		dialect: sqlDialect,
	}
}
