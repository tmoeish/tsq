package tsq

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"

	tsqdialect "github.com/tmoeish/tsq/v4/dialect"
)

// Runtime owns the initialized TSQ process state used for execution, index setup,
// identifier validation, and tracing.
type Runtime struct {
	tables      []*registeredTable
	tracers     []Tracer
	db          *sql.DB
	dialect     tsqdialect.Dialect
	tablePolicy SchemaPolicy
	indexPolicy SchemaPolicy
	logger      Logger
	maxPageSize int
}

// NewRuntime opens a database connection, resolves the SQL dialect from driverName,
// and constructs an initialized runtime for the provided table metadata.
// It is NewRuntimeContext with context.Background(); prefer NewRuntimeContext when
// schema bootstrap must honor a deadline or cancellation.
func NewRuntime(
	driverName string,
	dsn string,
	tables []TableRegistration,
	options ...*RuntimeOptions,
) (*Runtime, error) {
	return NewRuntimeContext(context.Background(), driverName, dsn, tables, options...)
}

// NewRuntimeContext is NewRuntime with a context that bounds the connection ping,
// identifier validation, and schema policy application (which may execute DDL).
func NewRuntimeContext(
	ctx context.Context,
	driverName string,
	dsn string,
	tables []TableRegistration,
	options ...*RuntimeOptions,
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

	registeredTables, err := buildRegisteredTables(tables)
	if err != nil {
		return nil, err
	}

	var opts *RuntimeOptions
	if len(options) > 0 {
		opts = options[0]
	}

	if opts == nil {
		opts = &RuntimeOptions{}
	}

	tablePolicy := resolveSchemaPolicy(opts.TablePolicy)
	if err := validateSchemaPolicy(tablePolicy); err != nil {
		return nil, err
	}

	indexPolicy := resolveSchemaPolicy(opts.IndexPolicy)

	if err := validateSchemaPolicy(indexPolicy); err != nil {
		return nil, err
	}

	identifierMode, err := resolveIdentifierValidationMode(opts.IdentifierValidationMode)
	if err != nil {
		return nil, err
	}

	if opts.MaxPageSize < 0 {
		return nil, fmt.Errorf("invalid max page size: %d", opts.MaxPageSize)
	}

	db, sqlDialect, err := openRuntimeDB(ctx, driverName, dsn)
	if err != nil {
		return nil, err
	}

	cleanup := true

	defer func() {
		if cleanup {
			_ = db.Close()
		}
	}()

	runtime := &Runtime{
		tables:      registeredTables,
		tracers:     appendTracers(nil, opts.Tracers...),
		db:          db,
		dialect:     sqlDialect,
		tablePolicy: tablePolicy,
		indexPolicy: indexPolicy,
		logger:      resolveRuntimeLogger(opts),
		maxPageSize: opts.MaxPageSize,
	}

	if identifierMode != IdentifierValidationSkip {
		if err := runtime.validateRegisteredTableIdentifiers(identifierMode); err != nil {
			if identifierMode == IdentifierValidationStrict {
				return nil, err
			}

			runtime.warn("identifier validation warning during runtime bootstrap", "error", err)
		}
	}

	if err := runtime.applySchemaPolicies(ctx); err != nil {
		return nil, err
	}

	cleanup = false

	return runtime, nil
}

var _ SQLExecutor = (*Runtime)(nil)

func resolveIdentifierValidationMode(mode IdentifierValidationMode) (IdentifierValidationMode, error) {
	switch mode {
	case "":
		return IdentifierValidationStrict, nil
	case IdentifierValidationStrict, IdentifierValidationWarn, IdentifierValidationSkip:
		return mode, nil
	default:
		return "", fmt.Errorf("invalid identifier validation mode %q", mode)
	}
}

// Close releases the underlying database connection pool. It is safe to call on a
// nil runtime.
func (r *Runtime) Close() error {
	if r == nil || r.db == nil {
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

func (r *Runtime) tsqDialect() tsqdialect.Dialect {
	return r.SQLDialect()
}

func (r *Runtime) tsqRuntime() *Runtime {
	return r
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

	_, err := r.withTxResult(ctx, options, func(ctx context.Context, txExec SQLExecutor) (struct{}, error) {
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

	return r.validateRegisteredTableIdentifiers(IdentifierValidationStrict)
}

func (r *Runtime) validateRegisteredTableIdentifiers(mode IdentifierValidationMode) error {
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
			if mode == IdentifierValidationStrict {
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

	if len(validationErrors) > 0 {
		return errors.New("identifier validation warnings: " + strings.Join(validationErrors, "; "))
	}

	return nil
}

func validateIndexIdentifiersForDialect(
	tableName string,
	indexes []TableIndex,
	dialect tsqdialect.Dialect,
	mode IdentifierValidationMode,
	validationErrors *[]string,
) error {
	for _, index := range indexes {
		if err := validateIdentifierLength(index.Name, dialect); err != nil {
			if mode == IdentifierValidationStrict {
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
	mode IdentifierValidationMode,
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
			if mode == IdentifierValidationStrict {
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
