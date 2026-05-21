package tsq

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	tsqdialect "github.com/tmoeish/tsq/v4/dialect"
)

// Runtime owns the mutable TSQ process state used for table registration,
// initialization, and tracing. Applications that need isolation can create a
// dedicated Runtime instead of relying on the package-level defaults.
type Runtime struct {
	registry     *registry
	traceManager *traceManager
	initMu       sync.Mutex
	engine       *engine // Stored after Init for runtime-scoped DB+dialect access.
}

// NewRuntime creates an isolated runtime with its own registrations and tracers.
func NewRuntime() *Runtime {
	return &Runtime{
		registry:     newRegistry(),
		traceManager: newTraceManager(),
	}
}

var defaultRuntime = NewRuntime()

// DB returns the current *sql.DB if Init has been called.
func (r *Runtime) DB() *sql.DB {
	if r == nil {
		return nil
	}

	if r.engine == nil {
		return nil
	}

	return r.engine.db
}

// SQLDialect returns the concrete SQL dialect bound to this runtime.
func (r *Runtime) SQLDialect() tsqdialect.Dialect {
	if r == nil || r.engine == nil {
		return nil
	}

	return r.engine.dialect
}

// Executor returns the runtime's initialized SQL executor with dialect metadata attached.
// It returns nil until Init has been called successfully.
func (r *Runtime) Executor() SQLExecutor {
	if r == nil || r.engine == nil {
		return nil
	}

	return WrapExecutor(r.engine.db, r.engine.dialect)
}

// RegisterTable registers a table and its declared indexes on this runtime.
func (r *Runtime) RegisterTable(
	table Table,
	indexes ...TableIndex,
) error {
	if r == nil {
		return &RegistrationError{Type: RegistrationErrorNilRuntime, Message: "runtime cannot be nil"}
	}

	return r.registry.Register(table, indexes...)
}

func (r *Runtime) snapshotRegisteredTables() []*registeredTable {
	if r == nil {
		return nil
	}

	return r.registry.Snapshot()
}

// Init initializes indexes and runtime state using optional explicit options.
func (r *Runtime) Init(db *sql.DB, sqlDialect tsqdialect.Dialect, options ...*InitOptions) error {
	if r == nil {
		return errors.New("runtime cannot be nil")
	}

	if db == nil {
		return errors.New("database connection cannot be nil")
	}

	if sqlDialect == nil {
		return errors.New("dialect cannot be nil")
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
		return err
	}

	r.initMu.Lock()
	defer r.initMu.Unlock()

	engine := newEngine(db, sqlDialect)
	engine.indexInitMode = indexMode
	prevEngine := r.engine
	r.engine = engine

	rollbackTracers := r.traceManager.snapshot()
	committed := false

	defer func() {
		if committed {
			return
		}

		r.engine = prevEngine
		r.traceManager.restore(rollbackTracers)
	}()

	r.traceManager.AddUnique(opts.Tracers...)

	registeredTables := r.registry.Snapshot()

	// Validate identifiers if configured (after db is stored so we can get current dialect)
	if opts.IdentifierValidationMode != "skip" {
		if err := r.validateRegisteredTableIdentifiers(opts.IdentifierValidationMode); err != nil {
			if opts.IdentifierValidationMode == "strict" {
				return err
			}
			// For "warn" mode, just log the error but continue
			slog.Warn("identifier validation warning during init", "error", err)
		}
	}

	if indexMode != IndexInitSkip {
		for _, table := range registeredTables {
			tableName := physicalTableName(table.Table)
			for _, index := range table.Indexes {
				if err := upsertIndex(engine, tableName, index.Unique, index.Name, index.Fields); err != nil {
					return fmt.Errorf("failed to initialize index %s on table %s: %w", index.Name, tableName, err)
				}
			}
		}
	}

	committed = true

	return nil
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

// validateRegisteredTableIdentifiers checks registered table and column identifiers
// against the current dialect-specific length limits.
// mode should be "strict" (fail on violation), "warn" (log warning), or "skip" (no validation).
func (r *Runtime) validateRegisteredTableIdentifiers(mode string) error {
	if r == nil {
		return errors.New("runtime cannot be nil")
	}

	dialect := r.SQLDialect()
	if dialect == nil {
		// Unknown dialect, skip validation
		return nil
	}

	registeredTables := r.registry.Snapshot()
	var validationErrors []string

	for _, table := range registeredTables {
		if table.Table == nil {
			continue
		}

		tableName := physicalTableName(table.Table)
		if err := validateIdentifierLength(tableName, r.engine.dialect); err != nil {
			if mode == "strict" {
				return fmt.Errorf("table %s identifier validation failed"+": %w", tableName, err)
			}

			validationErrors = append(validationErrors, err.Error())
		}

		if err := validateColumnIdentifiersForDialect(tableName, table.Cols(), r.engine.dialect, mode, &validationErrors); err != nil {
			return err
		}

		// Also validate keyword search columns if present
		if err := validateColumnIdentifiersForDialect(tableName, searchColumnsAsSQLColumns(table.SearchColumns()), r.engine.dialect, mode, &validationErrors); err != nil {
			return err
		}

		if err := validateIndexIdentifiersForDialect(tableName, table.Indexes, r.engine.dialect, mode, &validationErrors); err != nil {
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

// ValidateIdentifiersForDialect validates all registered table and column identifiers against the current database dialect.
// It is useful for pre-deployment validation.
// It returns nil when all identifiers are valid for the current dialect.
// It returns an error if validation fails or Init has not been called.
func (r *Runtime) ValidateIdentifiersForDialect() error {
	if r == nil {
		return errors.New("runtime cannot be nil")
	}

	if r.engine == nil {
		return errors.New("database not initialized; call Init first")
	}

	dialect := r.SQLDialect()
	if dialect == nil {
		return errors.New("unable to determine current database dialect")
	}

	// Use strict mode for explicit validation call
	return r.validateRegisteredTableIdentifiers("strict")
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
				return fmt.Errorf("column %s.%s identifier validation failed"+": %w", tableName, colName, err)
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
