package tsq

import (
	"context"
	"database/sql"
)

// SQLExecutor defines the shared query execution surface implemented by
// database/sql entry points such as *sql.DB and *sql.Tx.
// The standard library does not provide this exact interface, so tsq defines
// the minimal Context-based method set it needs.
type SQLExecutor interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}
