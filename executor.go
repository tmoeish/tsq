package tsq

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

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

// DialectOf returns the dialect db renders for: a Runtime's, or the one given to
// WrapExecutor, including inside a WithTx callback. Use it with
// dialect.Supports to pick a query shape before running it.
func DialectOf(db Executor) tsqdialect.Name {
	if isNilValue(db) {
		return ""
	}

	if d := db.scope().dialect; d != nil {
		return d.Name()
	}

	return ""
}

// execScope is what an Executor knows beyond database/sql.
type execScope struct {
	dialect sqld.Dialect
	runtime *Runtime
	// tx reports that statements run inside a transaction.
	tx bool
}

// bindParams is the number of parameters TSQ puts in one statement it splits:
// the engine's limit, or the runtime's lower budget for its driver.
func (s execScope) bindParams() int {
	limit := sqld.MaxBindParams(s.dialect)
	if s.runtime != nil && s.runtime.bindBudget > 0 && (limit <= 0 || s.runtime.bindBudget < limit) {
		return s.runtime.bindBudget
	}

	return limit
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
	// tx is the state of the transaction the executor runs in; nil outside
	// Runtime.WithTx.
	tx *txState
}

func (b boundExecutor) scope() execScope { return b.s }

func (boundExecutor) needsRuntimeOrWrapExecutor() {}

func (b boundExecutor) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if err := b.tx.usable(); err != nil {
		return nil, err
	}

	result, err := b.DBTX.ExecContext(ctx, query, args...)

	return result, b.tx.noted(b.s.dialect, err)
}

func (b boundExecutor) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if err := b.tx.usable(); err != nil {
		return nil, err
	}

	rows, err := b.DBTX.QueryContext(ctx, query, args...)

	return rows, b.tx.noted(b.s.dialect, err)
}

func (b boundExecutor) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if err := b.tx.usable(); err != nil {
		return queryRowWithError(ctx, err, query, args...)
	}

	row := b.DBTX.QueryRowContext(ctx, query, args...)
	if row != nil {
		_ = b.tx.noted(b.s.dialect, row.Err())
	}

	return row
}

// txState is what a transaction's executor remembers between statements: the
// error after which the engine rolled the transaction back on its own. MySQL
// does that for a deadlock and a full lock table, and the session is then out of
// the transaction: a statement that follows runs and commits by itself, and the
// COMMIT at the end commits nothing, so a callback that caught the error and went
// on lost its earlier writes and kept its later ones. PostgreSQL refuses every
// statement after a failure until the rollback, and its driver reports a COMMIT
// that became a ROLLBACK; SQLite keeps the transaction.
type txState struct {
	mu      sync.Mutex
	aborted error
	// iterating counts the Iter loops open over the transaction's connection,
	// which carries their rows: a statement run on it meanwhile breaks both on
	// MySQL and PostgreSQL.
	iterating int
	// done is set once the transaction committed or rolled back: a context that
	// still carries it (kept past the callback) then carries nothing.
	done bool
}

// finish marks the transaction over.
func (t *txState) finish() {
	if t == nil {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	t.done = true
}

// live reports a transaction not yet committed or rolled back.
func (t *txState) live() bool {
	if t == nil {
		return false
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	return !t.done
}

// holdRows marks an Iter open over the transaction; releaseRows undoes it.
func (t *txState) holdRows() {
	if t == nil {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	t.iterating++
}

func (t *txState) releaseRows() {
	if t == nil {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	t.iterating--
}

// rowsOpen reports an Iter open over the transaction.
func (t *txState) rowsOpen() bool {
	if t == nil {
		return false
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	return t.iterating > 0
}

// noted records err when it ended the transaction, and returns err.
func (t *txState) noted(d sqld.Dialect, err error) error {
	if t == nil || err == nil || !rolledBackByEngine(d, err) {
		return err
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.aborted == nil {
		t.aborted = err
	}

	return err
}

// usable is the error a statement gets once the engine rolled the transaction
// back: running it would commit it on its own.
func (t *txState) usable() error {
	if err := t.abortedBy(); err != nil {
		return fmt.Errorf("the transaction was rolled back by the database and cannot go on; a statement now would commit on its own: %w", err)
	}

	return nil
}

func (t *txState) abortedBy() error {
	if t == nil {
		return nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	return t.aborted
}

// WrapExecutor makes an Executor of a database/sql handle TSQ did not open, such as
// a *sql.Tx begun elsewhere, talking to dialect. Statements run without the logging
// and tracing a Runtime provides, and pages are capped at DefaultMaxPageSize
// rather than a runtime's WithMaxPageSize. It fails when db is nil or dialect is
// not one of the dialect package's names.
func WrapExecutor(db DBTX, dialect tsqdialect.Name) (Executor, error) {
	if isNilValue(db) {
		return nil, errors.New("db cannot be nil")
	}

	d, err := sqld.For(dialect)
	if err != nil {
		return nil, err
	}

	if b, ok := db.(boundExecutor); ok {
		b.s.dialect = d
		return b, nil
	}

	// A transaction begun elsewhere gets the same memory of an error after which
	// the engine rolled it back (txState): the statements that follow are refused
	// rather than run on their own. Its commit is the caller's, and commits
	// nothing then.
	var state *txState

	_, isTx := db.(interface {
		Commit() error
		Rollback() error
	})
	if isTx {
		state = &txState{}
	}

	return boundExecutor{DBTX: db, s: execScope{dialect: d, tx: isTx}, tx: state}, nil
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
