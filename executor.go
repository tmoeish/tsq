package tsq

import (
	"context"
	"database/sql"
	"errors"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
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

	scope() execScope
}

// execScope is what an Executor knows beyond database/sql.
type execScope struct {
	dialect tsqdialect.Dialect
	runtime *Runtime
	// tx reports that statements run inside a transaction.
	tx bool
}

type boundExecutor struct {
	tsqdialect.Executor
	s execScope
}

func (b boundExecutor) scope() execScope { return b.s }

// WrapExecutor makes an Executor of a database/sql handle TSQ did not open, such as
// a *sql.Tx begun elsewhere. Statements run without the logging, tracing and page
// size cap a Runtime provides.
func WrapExecutor(exec tsqdialect.Executor, dialect tsqdialect.Dialect) Executor {
	if isNilValue(exec) || isNilValue(dialect) {
		return nil
	}

	if b, ok := exec.(boundExecutor); ok {
		b.s.dialect = dialect
		return b
	}

	_, isTx := exec.(*sql.Tx)

	return boundExecutor{Executor: exec, s: execScope{dialect: dialect, tx: isTx}}
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
