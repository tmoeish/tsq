package tsq

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	tsqdialect "github.com/tmoeish/tsq/v4/dialect"
)

// Runtime owns the initialized TSQ process state used for execution, index setup,
// identifier validation, and tracing.
type Runtime struct {
	tables        []*registeredTable
	traceManager  *traceManager
	db            *sql.DB
	dialect       tsqdialect.Dialect
	indexInitMode IndexInitMode
}

// NewRuntime constructs an initialized runtime for one database connection and
// the provided table metadata.
func NewRuntime(
	db *sql.DB,
	sqlDialect tsqdialect.Dialect,
	tables []TableRegistration,
	options ...*InitOptions,
) (*Runtime, error) {
	if db == nil {
		return nil, errors.New("database connection cannot be nil")
	}

	if sqlDialect == nil {
		return nil, errors.New("dialect cannot be nil")
	}

	registeredTables, err := buildRegisteredTables(tables)
	if err != nil {
		return nil, err
	}

	var opts *InitOptions
	if len(options) > 0 {
		opts = options[0]
	}

	if opts == nil {
		opts = &InitOptions{}
	}

	indexMode := resolveIndexInitMode(opts)
	if err := validateIndexInitMode(indexMode); err != nil {
		return nil, err
	}

	runtime := &Runtime{
		tables:        registeredTables,
		traceManager:  newTraceManager(),
		db:            db,
		dialect:       sqlDialect,
		indexInitMode: indexMode,
	}
	runtime.traceManager.AddUnique(opts.Tracers...)

	if opts.IdentifierValidationMode != "skip" {
		if err := runtime.validateRegisteredTableIdentifiers(opts.IdentifierValidationMode); err != nil {
			if opts.IdentifierValidationMode == "strict" {
				return nil, err
			}

			slog.Warn("identifier validation warning during runtime bootstrap", "error", err)
		}
	}

	if indexMode != IndexInitSkip {
		for _, table := range runtime.tables {
			tableName := physicalTableName(table.Table)
			for _, index := range table.Indexes {
				if err := upsertIndex(runtime.db, runtime.dialect, runtime.indexInitMode, tableName, index.Unique, index.Name, index.Fields); err != nil {
					return nil, fmt.Errorf("failed to initialize index %s on table %s: %w", index.Name, tableName, err)
				}
			}
		}
	}

	return runtime, nil
}

var _ SQLExecutor = (*Runtime)(nil)

func (r *Runtime) tsqDialect() tsqdialect.Dialect {
	return r.SQLDialect()
}

func (r *Runtime) tsqTraceManager() *traceManager {
	if r == nil {
		return nil
	}

	return r.traceManager
}

// DB returns the current *sql.DB.
func (r *Runtime) DB() *sql.DB {
	if r == nil {
		return nil
	}

	return r.db
}

// SQLDialect returns the concrete SQL dialect bound to this runtime.
func (r *Runtime) SQLDialect() tsqdialect.Dialect {
	if r == nil {
		return nil
	}

	return r.dialect
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
		return sql.OpenDB(runtimeErrorConnector{err: err}).QueryRowContext(ctx, query, args...)
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
	fn func(context.Context, SQLExecutor) error,
) error {
	if fn == nil {
		return errors.New("transaction function cannot be nil")
	}

	_, err := withTxRuntime1(r, ctx, options, func(ctx context.Context, txExec SQLExecutor) (struct{}, error) {
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

type runtimeErrorConnector struct {
	err error
}

func (c runtimeErrorConnector) Connect(context.Context) (driver.Conn, error) {
	return nil, c.err
}

func (c runtimeErrorConnector) Driver() driver.Driver {
	return runtimeErrorDriver(c)
}

type runtimeErrorDriver struct {
	err error
}

func (d runtimeErrorDriver) Open(string) (driver.Conn, error) {
	return nil, d.err
}

// AddTracer adds a tracer to this runtime.
func (r *Runtime) AddTracer(tracer Tracer) {
	if r == nil {
		return
	}

	r.traceManager.Add(tracer)
}

// ClearTracers removes all tracers from this runtime.
func (r *Runtime) ClearTracers() {
	if r == nil {
		return
	}

	r.traceManager.Clear()
}

// GetTracers returns a snapshot of this runtime's tracers.
func (r *Runtime) GetTracers() []Tracer {
	if r == nil {
		return nil
	}

	return r.traceManager.Get()
}

// Trace executes fn with this runtime's tracers applied.
func (r *Runtime) Trace(ctx context.Context, fn func(ctx context.Context) error) error {
	if r == nil {
		return errors.New("runtime cannot be nil")
	}

	return r.traceManager.Trace(ctx, fn)
}

func trace1WithRuntime[T any](r *Runtime, ctx context.Context, fn func(ctx context.Context) (T, error)) (T, error) {
	if r == nil {
		var zero T
		return zero, errors.New("runtime cannot be nil")
	}

	return traceManagerTrace1(r.traceManager, ctx, fn)
}

// ValidateIdentifiersForDialect validates all configured table and column identifiers against the current database dialect.
func (r *Runtime) ValidateIdentifiersForDialect() error {
	if r == nil {
		return errors.New("runtime cannot be nil")
	}

	if r.db == nil || r.dialect == nil {
		return errors.New("runtime is not initialized; construct it with NewRuntime")
	}

	if r.SQLDialect() == nil {
		return errors.New("unable to determine current database dialect")
	}

	return r.validateRegisteredTableIdentifiers("strict")
}

func (r *Runtime) validateRegisteredTableIdentifiers(mode string) error {
	if r == nil {
		return errors.New("runtime cannot be nil")
	}

	dialect := r.SQLDialect()
	if dialect == nil {
		return nil
	}

	var validationErrors []string

	for _, table := range r.tables {
		if table.Table == nil {
			continue
		}

		tableName := physicalTableName(table.Table)
		if err := validateIdentifierLength(tableName, r.dialect); err != nil {
			if mode == "strict" {
				return fmt.Errorf("table %s identifier validation failed: %w", tableName, err)
			}

			validationErrors = append(validationErrors, err.Error())
		}

		if err := validateColumnIdentifiersForDialect(tableName, table.Cols(), r.dialect, mode, &validationErrors); err != nil {
			return err
		}

		if err := validateColumnIdentifiersForDialect(tableName, searchColumnsAsSQLColumns(table.SearchColumns()), r.dialect, mode, &validationErrors); err != nil {
			return err
		}

		if err := validateIndexIdentifiersForDialect(tableName, table.Indexes, r.dialect, mode, &validationErrors); err != nil {
			return err
		}
	}

	if len(validationErrors) > 0 && mode == "warn" {
		return errors.New("identifier validation warnings: " + strings.Join(validationErrors, "; "))
	}

	return nil
}

func validateIndexIdentifiersForDialect(
	tableName string,
	indexes []TableIndex,
	dialect tsqdialect.Dialect,
	mode string,
	validationErrors *[]string,
) error {
	for _, index := range indexes {
		if err := validateIdentifierLength(index.Name, dialect); err != nil {
			if mode == "strict" {
				return fmt.Errorf("index %s on table %s identifier validation failed: %w", index.Name, tableName, err)
			}

			*validationErrors = append(*validationErrors, err.Error())
		}
	}

	return nil
}

func validateColumnIdentifiersForDialect(
	tableName string,
	cols []SQLColumn,
	dialect tsqdialect.Dialect,
	mode string,
	validationErrors *[]string,
) error {
	seen := make(map[string]struct{}, len(cols))
	for _, col := range cols {
		if col == nil {
			continue
		}

		colName := col.OutputName()
		if _, ok := seen[colName]; ok {
			continue
		}
		seen[colName] = struct{}{}

		if err := validateIdentifierLength(colName, dialect); err != nil {
			if mode == "strict" {
				return fmt.Errorf("column %s.%s identifier validation failed: %w", tableName, colName, err)
			}

			*validationErrors = append(*validationErrors, err.Error())
		}
	}

	return nil
}

func searchColumnsAsSQLColumns(cols []SearchColumn) []SQLColumn {
	result := make([]SQLColumn, 0, len(cols))
	for _, col := range cols {
		result = append(result, col)
	}

	return result
}
