package tsq

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"
)

type errorRowContextKey struct{}

type errorRowDriver struct{}

type errorRowConn struct{}

var (
	errorRowDBOnce sync.Once
	errorRowDB     *sql.DB
)

// Open returns the single-purpose driver connection used to surface QueryRow errors.
func (errorRowDriver) Open(string) (driver.Conn, error) {
	return errorRowConn{}, nil
}

// Prepare rejects prepared statements because the error-row driver is QueryRow-only.
func (errorRowConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare is not supported")
}

// Close is a no-op because the error-row driver holds no external resources.
func (errorRowConn) Close() error {
	return nil
}

// Begin rejects transactions because the error-row driver only simulates QueryRow failures.
func (errorRowConn) Begin() (driver.Tx, error) {
	return nil, errors.New("transactions are not supported")
}

// QueryContext returns the injected error stored on the context.
func (errorRowConn) QueryContext(ctx context.Context, _ string, _ []driver.NamedValue) (driver.Rows, error) {
	if err, ok := ctx.Value(errorRowContextKey{}).(error); ok && err != nil {
		return nil, err
	}

	return nil, errors.New("missing query row error")
}

// CheckNamedValue accepts all argument values because they are ignored by the driver.
func (errorRowConn) CheckNamedValue(*driver.NamedValue) error {
	return nil
}

func errorQueryRow(ctx context.Context, err error) *sql.Row {
	errorRowDBOnce.Do(func() {
		sql.Register("tsq-error-row", errorRowDriver{})

		db, openErr := sql.Open("tsq-error-row", "")
		if openErr != nil {
			panic(openErr)
		}
		errorRowDB = db
	})

	if ctx == nil {
		ctx = context.Background()
	}

	return errorRowDB.QueryRowContext(context.WithValue(ctx, errorRowContextKey{}, err), "SELECT 1")
}

func engineExecutionError(engine *Engine) error {
	if engine == nil {
		return errEngineNil
	}

	if engine.DB == nil {
		return errEngineDatabaseNil
	}

	return nil
}

// Engine couples a *sql.DB with the dialect rules tsq should use for it.
type Engine struct {
	DB             *sql.DB // DB is the underlying database handle used for execution.
	Dialect        Dialect // Dialect controls SQL rendering and identifier rules for DB.
	schemaConfigMu sync.RWMutex
	schemaConfig   dbSchemaConfig
}

type dbSchemaConfig struct {
	indexInitMode      IndexInitMode
	schemaEventHandler func(SchemaEvent)
}

func defaultDBSchemaConfig() dbSchemaConfig {
	return dbSchemaConfig{indexInitMode: IndexInitUpsert}
}

func loadDBSchemaConfig(db *Engine) dbSchemaConfig {
	if db == nil {
		return defaultDBSchemaConfig()
	}

	db.schemaConfigMu.RLock()
	cfg := db.schemaConfig
	db.schemaConfigMu.RUnlock()

	if cfg.indexInitMode != "" || cfg.schemaEventHandler != nil {
		return cfg
	}

	return defaultDBSchemaConfig()
}

func storeDBSchemaConfig(db *Engine, cfg dbSchemaConfig) {
	if db == nil {
		return
	}

	if cfg.indexInitMode == "" {
		cfg.indexInitMode = IndexInitUpsert
	}

	if cfg.indexInitMode == IndexInitUpsert && cfg.schemaEventHandler == nil {
		db.schemaConfigMu.Lock()
		db.schemaConfig = dbSchemaConfig{}
		db.schemaConfigMu.Unlock()

		return
	}

	db.schemaConfigMu.Lock()
	db.schemaConfig = cfg
	db.schemaConfigMu.Unlock()
}

// TSQDialect exposes the Engine dialect for SQL rendering and validation.
func (e *Engine) TSQDialect() Dialect {
	if e == nil {
		return nil
	}

	return e.Dialect
}

// QueryContext executes a query and returns rows.
func (e *Engine) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if err := engineExecutionError(e); err != nil {
		return nil, err
	}

	rows, err := e.DB.QueryContext(ctx, query, args...)

	return rows, err
}

// QueryRowContext executes a query that returns a single row.
func (e *Engine) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if err := engineExecutionError(e); err != nil {
		return errorQueryRow(ctx, err)
	}

	return e.DB.QueryRowContext(ctx, query, args...)
}

// ExecContext executes a query without returning rows.
func (e *Engine) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if err := engineExecutionError(e); err != nil {
		return nil, err
	}

	res, err := e.DB.ExecContext(ctx, query, args...)

	return res, err
}
