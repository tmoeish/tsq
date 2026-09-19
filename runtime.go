package tsq

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
	sqld "github.com/tmoeish/tsq/v5/internal/sqldialect"
)

// Runtime owns the initialized TSQ process state used for execution, index setup,
// identifier validation, and tracing.
type Runtime struct {
	tables      []*registeredTable
	tracers     []Tracer
	db          *sql.DB
	dialect     sqld.Dialect
	tablePolicy SchemaPolicy
	indexPolicy SchemaPolicy
	logger      Logger
	maxPageSize int
	logSQL      bool
	ownsDB      bool
}

// Open opens a database connection, resolves the SQL dialect from
// driverName, and constructs an initialized runtime for the provided tables.
//
// ctx bounds the connection ping and the schema policy application, which may
// execute DDL. The runtime owns the pool it opened, so Close closes it.
func Open(
	ctx context.Context,
	driverName string,
	dsn string,
	tables []Table,
	options ...RuntimeOption,
) (*Runtime, error) {
	if ctx == nil {
		return nil, errors.New("context cannot be nil")
	}

	if driverName == "" {
		return nil, errors.New("driver name cannot be empty")
	}

	if dsn == "" {
		return nil, errors.New("dsn cannot be empty")
	}

	sqlDialect, err := resolveRuntimeDialect(driverName)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, err
	}

	runtime, err := newRuntime(ctx, db, sqlDialect, tables, true, options)
	if err != nil {
		_ = db.Close()

		return nil, err
	}

	return runtime, nil
}

// NewRuntime constructs a runtime over a pool the caller already owns,
// which is the way to keep an instrumented or specially configured connection
// while still getting SQL logging, tracers and the page-size cap.
//
// The pool is not closed by Close: whoever opened it decides when it goes away.
func NewRuntime(
	ctx context.Context,
	db *sql.DB,
	engine tsqdialect.Name,
	tables []Table,
	options ...RuntimeOption,
) (*Runtime, error) {
	if ctx == nil {
		return nil, errors.New("context cannot be nil")
	}

	if db == nil {
		return nil, errors.New("db cannot be nil")
	}

	sqlDialect, err := sqld.For(engine)
	if err != nil {
		return nil, err
	}

	return newRuntime(ctx, db, sqlDialect, tables, false, options)
}

func newRuntime(
	ctx context.Context,
	db *sql.DB,
	sqlDialect sqld.Dialect,
	tables []Table,
	ownsDB bool,
	options []RuntimeOption,
) (*Runtime, error) {
	registeredTables, err := registerTables(tables)
	if err != nil {
		return nil, err
	}

	cfg, err := newRuntimeConfig(options)
	if err != nil {
		return nil, err
	}

	if err := db.PingContext(ctx); err != nil {
		return nil, err
	}

	runtime := &Runtime{
		tables:      registeredTables,
		tracers:     cfg.tracers,
		db:          db,
		dialect:     sqlDialect,
		tablePolicy: cfg.tablePolicy,
		indexPolicy: cfg.indexPolicy,
		logger:      cfg.logger,
		maxPageSize: cfg.maxPageSize,
		logSQL:      cfg.logSQL,
		ownsDB:      ownsDB,
	}

	// Identifiers are checked before any DDL runs: a name the dialect will
	// truncate produces objects that do not match what the queries reference.
	if err := runtime.validateRegisteredTableIdentifiers(); err != nil {
		return nil, err
	}

	if err := runtime.applySchemaPolicies(ctx); err != nil {
		return nil, err
	}

	return runtime, nil
}

var _ Executor = (*Runtime)(nil)

// Close releases the underlying database connection pool. It is safe to call on a
// nil runtime.
// Close releases the connection pool, but only the one this runtime opened.
// A pool handed to NewRuntime belongs to its caller.
func (r *Runtime) Close() error {
	if r == nil || r.db == nil || !r.ownsDB {
		return nil
	}

	return r.db.Close()
}

// MaxPageSize returns the page-size cap applied to paged queries on this runtime.
func (r *Runtime) MaxPageSize() int {
	if r == nil || r.maxPageSize <= 0 {
		return DefaultMaxPageSize
	}

	return r.maxPageSize
}

func (r *Runtime) scope() execScope {
	if r == nil {
		return execScope{}
	}

	return execScope{dialect: r.dialect, runtime: r}
}

// DB returns the current *sql.DB.
func (r *Runtime) DB() *sql.DB {
	if r == nil {
		return nil
	}

	return r.db
}

// Dialect returns the SQL engine this runtime talks to.
func (r *Runtime) Dialect() tsqdialect.Name {
	if r == nil || r.dialect == nil {
		return ""
	}

	return r.dialect.Name()
}

// QueryContext executes a query against the runtime database.
func (r *Runtime) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	db, err := r.sqlDB()
	if err != nil {
		return nil, err
	}

	return db.QueryContext(ctx, query, args...)
}

// QueryRowContext executes a query expected to return at most one row.
func (r *Runtime) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	db, err := r.sqlDB()
	if err != nil {
		return queryRowWithError(ctx, err, query, args...)
	}

	return db.QueryRowContext(ctx, query, args...)
}

// ExecContext executes a statement against the runtime database.
func (r *Runtime) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	db, err := r.sqlDB()
	if err != nil {
		return nil, err
	}

	return db.ExecContext(ctx, query, args...)
}

// WithTx starts a transaction on the runtime database and passes a dialect-aware executor to fn.
// It manages BeginTx, Commit, and Rollback automatically.
func (r *Runtime) WithTx(
	ctx context.Context,
	options *TxOptions,
	fn func(context.Context, Executor) error,
) error {
	if fn == nil {
		return errors.New("transaction function cannot be nil")
	}

	_, err := r.withTxResult(ctx, options, func(ctx context.Context, txExec Executor) (struct{}, error) {
		return struct{}{}, fn(ctx, txExec)
	})

	return err
}

func (r *Runtime) sqlDB() (*sql.DB, error) {
	if err := validateTxRuntime(r); err != nil {
		return nil, err
	}

	return r.db, nil
}

// runtimeErrorKey carries the error that made a runtime unusable into the shared
// error-only *sql.DB below.
type runtimeErrorKey struct{}

// errRuntimeUnusable is the fallback for connection attempts that reach the error-only
// pool without a caller error attached, which database/sql can do from its background
// connection opener.
var errRuntimeUnusable = errors.New("tsq runtime is not usable")

// errorDB is a single process-wide *sql.DB on which every connection attempt fails
// with the error the caller placed in the context.
//
// QueryRowContext must return a *sql.Row and has no other channel for reporting that
// the runtime is unusable, and a *sql.Row carrying an error cannot be constructed from
// outside database/sql. Opening a throwaway pool per call used to be the answer, but
// sql.OpenDB starts a connection-opener goroutine that only Close stops, so every call
// against a nil or half-built runtime leaked a *sql.DB and a goroutine. One shared pool
// costs one goroutine for the life of the process no matter how often callers hit it.
var errorDB = sync.OnceValue(func() *sql.DB {
	return sql.OpenDB(runtimeErrorConnector{})
})

// queryRowWithError returns a *sql.Row whose Scan reports err.
func queryRowWithError(ctx context.Context, err error, query string, args ...any) *sql.Row {
	return errorDB().QueryRowContext(context.WithValue(ctx, runtimeErrorKey{}, err), query, args...)
}

type runtimeErrorConnector struct{}

func (runtimeErrorConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if err, ok := ctx.Value(runtimeErrorKey{}).(error); ok && err != nil {
		return nil, err
	}

	return nil, errRuntimeUnusable
}

func (c runtimeErrorConnector) Driver() driver.Driver {
	return runtimeErrorDriver{}
}

type runtimeErrorDriver struct{}

func (runtimeErrorDriver) Open(string) (driver.Conn, error) {
	return nil, errRuntimeUnusable
}

func (r *Runtime) validateRegisteredTableIdentifiers() error {
	for _, table := range r.tables {
		if err := validateIdentifierForDialect(table.name, r.dialect); err != nil {
			return fmt.Errorf("table %s: %w", table.name, err)
		}

		for _, col := range table.columns {
			if err := validateIdentifierForDialect(col.name, r.dialect); err != nil {
				return fmt.Errorf("column %s.%s: %w", table.name, col.name, err)
			}
		}

		for _, index := range table.Indexes {
			if err := validateIdentifierForDialect(index.Name, r.dialect); err != nil {
				return fmt.Errorf("index %s on %s: %w", index.Name, table.name, err)
			}
		}
	}

	return nil
}
