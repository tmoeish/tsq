package tsq

import (
	"context"
	"log/slog"
	"strings"
	"sync"

	"github.com/juju/errors"
)

// Runtime owns the mutable TSQ process state used for table registration,
// initialization, and tracing. Applications that need isolation can create a
// dedicated Runtime instead of relying on the package-level defaults.
type Runtime struct {
	registry     *registry
	traceManager *traceManager
	initMu       sync.Mutex
	engine       *Engine // Stored after InitWithOptions for dialect access
}

// NewRuntime creates an isolated runtime with its own registrations and tracers.
func NewRuntime() *Runtime {
	return &Runtime{
		registry:     newRegistry(),
		traceManager: newTraceManager(),
	}
}

var defaultRuntime = NewRuntime()

// Engine returns the current Engine if Init has been called.
// Returns nil if Init has not been called or if runtime is nil.
func (r *Runtime) Engine() *Engine {
	if r == nil {
		return nil
	}

	return r.engine
}

// Dialect returns the SQL dialect of the current database if Init has been called.
// Returns empty string if Init has not been called or runtime has no initialized dialect.
func (r *Runtime) Dialect() DialectName {
	if r == nil || r.engine == nil || r.engine.Dialect == nil {
		return ""
	}

	return r.engine.Dialect.Name()
}

// RegisterTable registers a table and its index-initialization hook on this runtime.
func (r *Runtime) RegisterTable(
	table Table,
	initFunc func(db *Engine) error,
) error {
	if r == nil {
		return errors.Trace(&RegistrationError{Type: RegistrationErrorNilRuntime, Message: "runtime cannot be nil"})
	}

	return errors.Trace(r.registry.Register(table, initFunc))
}

func (r *Runtime) snapshotRegisteredTables() []*registeredTable {
	if r == nil {
		return nil
	}

	return r.registry.Snapshot()
}

// Init initializes indexes and tracers for the runtime using the convenience options.
func (r *Runtime) Init(db *Engine, upsertIndexes bool, tracers ...Tracer) error {
	return errors.Trace(r.InitWithOptions(db, &InitOptions{
		UpsertIndexes: upsertIndexes,
		Tracers:       tracers,
	}))
}

// InitWithOptions initializes indexes and runtime state using explicit options.
func (r *Runtime) InitWithOptions(db *Engine, options *InitOptions) error {
	if r == nil {
		return errors.New("runtime cannot be nil")
	}

	if db == nil {
		return errors.New("engine cannot be nil")
	}

	if db.DB == nil {
		return errors.New("engine database cannot be nil")
	}

	if db.Dialect == nil {
		return errors.New("engine dialect cannot be nil")
	}

	if options == nil {
		options = &InitOptions{}
	}

	indexMode := resolveIndexInitMode(options)
	if err := validateIndexInitMode(indexMode); err != nil {
		return errors.Trace(err)
	}

	r.initMu.Lock()
	defer r.initMu.Unlock()

	// Store the Engine for later dialect access.
	r.engine = db
	storeDBSchemaConfig(db, dbSchemaConfig{
		indexInitMode:      indexMode,
		schemaEventHandler: options.SchemaEventHandler,
	})

	rollbackTracers := r.traceManager.snapshot()
	r.traceManager.AddUnique(options.Tracers...)

	registeredTables := r.registry.Snapshot()

	// Validate identifiers if configured (after db is stored so we can get current dialect)
	if options.IdentifierValidationMode != "skip" {
		if err := r.validateRegisteredTableIdentifiers(options.IdentifierValidationMode); err != nil {
			if options.IdentifierValidationMode == "strict" {
				r.traceManager.restore(rollbackTracers)
				return errors.Trace(err)
			}
			// For "warn" mode, just log the error but continue
			slog.Warn("identifier validation warning during init", "error", err)
		}
	}

	if indexMode != IndexInitSkip {
		for _, table := range registeredTables {
			if err := table.InitFunc(db); err != nil {
				r.traceManager.restore(rollbackTracers)
				return errors.Annotatef(err, "failed to initialize table %s", table.Table.Table())
			}
		}
	}

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

// validateRegisteredTableIdentifiers checks if all registered table names conform to dialect-specific length limits.
// mode should be "strict" (fail on violation), "warn" (log warning), or "skip" (no validation).
func (r *Runtime) validateRegisteredTableIdentifiers(mode string) error {
	if r == nil {
		return errors.New("runtime cannot be nil")
	}

	dialect := r.Dialect()
	if dialect == "" {
		// Unknown dialect, skip validation
		return nil
	}

	registeredTables := r.registry.Snapshot()
	var validationErrors []string

	for _, table := range registeredTables {
		if table.Table == nil {
			continue
		}

		tableName := table.Table.Table()
		if err := ValidateIdentifierLength(tableName, r.engine.Dialect); err != nil {
			if mode == "strict" {
				return errors.Annotatef(err, "table %s identifier validation failed", tableName)
			}

			validationErrors = append(validationErrors, err.Error())
		}

		// Also validate keyword search columns if present
		if kwCols := table.SearchColumns(); kwCols != nil {
			for _, col := range kwCols {
				if col == nil {
					continue
				}

				colName := col.OutputName()
				if err := ValidateIdentifierLength(colName, r.engine.Dialect); err != nil {
					if mode == "strict" {
						return errors.Annotatef(err, "column %s.%s identifier validation failed", tableName, colName)
					}

					validationErrors = append(validationErrors, err.Error())
				}
			}
		}
	}

	if len(validationErrors) > 0 && mode == "warn" {
		return errors.New("identifier validation warnings: " + strings.Join(validationErrors, "; "))
	}

	return nil
}

// ValidateIdentifiersForDialect validates all registered table and column identifiers
// against the current database dialect. This is useful for pre-deployment validation.
// Returns nil if all identifiers are valid for the current dialect, otherwise returns an error.
// If no database has been initialized (Init not called), returns an error.
func (r *Runtime) ValidateIdentifiersForDialect() error {
	if r == nil {
		return errors.New("runtime cannot be nil")
	}

	if r.engine == nil {
		return errors.New("database not initialized; call Init or InitWithOptions first")
	}

	dialect := r.Dialect()
	if dialect == "" {
		return errors.New("unable to determine current database dialect")
	}

	// Use strict mode for explicit validation call
	return r.validateRegisteredTableIdentifiers("strict")
}
