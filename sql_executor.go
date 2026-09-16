package tsq

import (
	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
)

// Executor is the shared query execution surface implemented by
// database/sql entry points such as *sql.DB and *sql.Tx, and by *Runtime.
//
// It is an alias rather than a second declaration of the same three methods:
// the dialect package needs the identical surface to run its schema inspection,
// and two structurally identical interfaces stay interchangeable only until one
// of them grows a method.
type Executor = tsqdialect.Executor
