package tsq

import (
	"context"
	"database/sql"
	"errors"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
	sqld "github.com/tmoeish/tsq/v5/internal/sqldialect"
)

// Executor runs TSQ statements. It is a *Runtime, the transaction executor
// Runtime.WithTx passes to its callback, or the result of WrapExecutor.
//
// A bare *sql.DB or *sql.Tx is not an Executor: TSQ has to know the dialect to
// render a statement, and a database/sql handle does not say which one it talks
// to. The method that makes the difference is unexported, so passing one is a
// compile error rather than a statement rendered for the wrong database.
type Executor interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)

	// needsRuntimeOrWrapExecutor is what the compiler reports for a bare *sql.DB
	// or *sql.Tx; it sorts before scope, so it is the method named.
	needsRuntimeOrWrapExecutor()
	scope() execScope
}

// execScope is what an Executor knows beyond database/sql.
type execScope struct {
	dialect sqld.Dialect
	runtime *Runtime
	// tx reports that statements run inside a transaction.
	tx bool
}

// DBTX is what *sql.DB, *sql.Tx and *sql.Conn have in common: the database/sql
// handle WrapExecutor turns into an Executor.
type DBTX interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

type boundExecutor struct {
	DBTX
	s execScope
}

func (b boundExecutor) scope() execScope { return b.s }

func (boundExecutor) needsRuntimeOrWrapExecutor() {}

// WrapExecutor makes an Executor of a database/sql handle TSQ did not open, such as
// a *sql.Tx begun elsewhere, talking to engine. Statements run without the logging,
// tracing and page size cap a Runtime provides. It returns nil when db is nil or
// engine is not one of the dialect package's names.
func WrapExecutor(db DBTX, engine tsqdialect.Name) Executor {
	exec, err := wrapExecutor(db, engine)
	if err != nil {
		return nil
	}

	return exec
}

func wrapExecutor(db DBTX, engine tsqdialect.Name) (Executor, error) {
	if isNilValue(db) {
		return nil, errors.New("db cannot be nil")
	}

	dialect, err := sqld.For(engine)
	if err != nil {
		return nil, err
	}

	if b, ok := db.(boundExecutor); ok {
		b.s.dialect = dialect
		return b, nil
	}

	_, isTx := db.(*sql.Tx)

	return boundExecutor{DBTX: db, s: execScope{dialect: dialect, tx: isTx}}, nil
}

// executorScope validates exec and returns its scope.
func executorScope(exec Executor) (execScope, error) {
	if isNilValue(exec) {
		return execScope{}, errors.New("executor cannot be nil")
	}

	s := exec.scope()
	if isNilValue(s.dialect) {
		return execScope{}, errors.New("executor has no dialect")
	}

	return s, nil
}

func runtimeForExecutor(exec Executor) *Runtime {
	if isNilValue(exec) {
		return nil
	}

	return exec.scope().runtime
}
