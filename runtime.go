package tsq

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/juju/errors"
)

// Runtime owns the mutable TSQ process state used for table registration,
// initialization, and tracing. Applications that need isolation can create a
// dedicated Runtime instead of relying on the package-level defaults.
type Runtime struct {
	registry     *Registry
	traceManager *TraceManager
	initMu       sync.Mutex
	db           *DbMap // Stored after InitWithOptions for dialect access
}

func NewRuntime() *Runtime {
	return &Runtime{
		registry:     NewRegistry(),
		traceManager: NewTraceManager(),
	}
}

var defaultRuntime = NewRuntime()

func DefaultRuntime() *Runtime {
	return defaultRuntime
}

func (r *Runtime) Registry() *Registry {
	if r == nil {
		return nil
	}

	return r.registry
}

func (r *Runtime) TraceManager() *TraceManager {
	if r == nil {
		return nil
	}

	return r.traceManager
}

// CurrentDB returns the current database map if Init has been called.
// Returns nil if Init has not been called or if runtime is nil.
func (r *Runtime) CurrentDB() *DbMap {
	if r == nil {
		return nil
	}

	return r.db
}

// CurrentDialect returns the SQL dialect of the current database if Init has been called.
// Returns empty string if Init has not been called, runtime is nil, or dialect cannot be determined.
func (r *Runtime) CurrentDialect() string {
	if r == nil || r.db == nil || r.db.Dialect == nil {
		return ""
	}

	// Get the type name of the dialect to determine which database is being used
	dialectType := fmt.Sprintf("%T", r.db.Dialect)

	// Map gorp dialect types to our standard names
	switch {
	case strings.Contains(strings.ToLower(dialectType), "mysql"):
		return "mysql"
	case strings.Contains(strings.ToLower(dialectType), "postgre"):
		return "postgres"
	case strings.Contains(strings.ToLower(dialectType), "oracle"):
		return "oracle"
	case strings.Contains(strings.ToLower(dialectType), "sqlite"):
		return "sqlite"
	default:
		return ""
	}
}

func contains(s, substr string) bool {
	return len(s) > 0 && len(substr) > 0 && (s == substr || len(s) > len(substr) &&
		(s[:len(substr)] == substr || s[len(s)-len(substr):] == substr))
}

func (r *Runtime) RegisterTable(
	table Table,
	addTableFunc func(db *DbMap),
	initFunc func(db *DbMap) error,
) error {
	if r == nil {
		return &RegistrationError{
			Type:    RegistrationErrorNilRuntime,
			Message: "runtime cannot be nil",
		}
	}

	return r.registry.Register(table, addTableFunc, initFunc)
}

func (r *Runtime) snapshotRegisteredTables() []*RegisteredTable {
	if r == nil {
		return nil
	}

	return r.registry.Snapshot()
}

func (r *Runtime) Init(
	db *DbMap,
	autoCreateTable bool,
	upsertIndexies bool,
	tracer ...Tracer,
) error {
	return r.InitWithOptions(db, &InitOptions{
		AutoCreateTables: autoCreateTable,
		UpsertIndexes:    upsertIndexies,
		Tracers:          tracer,
	})
}

func (r *Runtime) InitWithOptions(db *DbMap, options *InitOptions) error {
	if r == nil {
		return errors.New("runtime cannot be nil")
	}

	if db == nil {
		return errors.New("db map cannot be nil")
	}

	if db.Db == nil {
		return errors.New("db map database cannot be nil")
	}

	if db.Dialect == nil {
		return errors.New("db map dialect cannot be nil")
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

	// Store the database map for later dialect access
	r.db = db
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
				return err
			}
			// For "warn" mode, just log the error but continue
			slog.Warn("identifier validation warning during init", "error", err)
		}
	}

	for _, table := range registeredTables {
		table.AddTableFunc(db)
	}

	if options.AutoCreateTables {
		if err := db.CreateTablesIfNotExists(); err != nil {
			r.traceManager.restore(rollbackTracers)
			return errors.Annotate(err, "failed to create tables")
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

func (r *Runtime) AddTracer(tracer Tracer) {
	if r == nil {
		return
	}

	r.traceManager.Add(tracer)
}

func (r *Runtime) ClearTracers() {
	if r == nil {
		return
	}

	r.traceManager.Clear()
}

func (r *Runtime) GetTracers() []Tracer {
	if r == nil {
		return nil
	}

	return r.traceManager.Get()
}

func (r *Runtime) Trace(ctx context.Context, fn func(ctx context.Context) error) error {
	if r == nil {
		return errors.New("runtime cannot be nil")
	}

	return r.traceManager.Trace(ctx, fn)
}

func Trace1WithRuntime[T any](r *Runtime, ctx context.Context, fn func(ctx context.Context) (T, error)) (T, error) {
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

	dialect := r.CurrentDialect()
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
		if err := ValidateIdentifierLength(tableName, dialect); err != nil {
			if mode == "strict" {
				return errors.Annotatef(err, "table %s identifier validation failed", tableName)
			}

			validationErrors = append(validationErrors, err.Error())
		}

		// Also validate keyword search columns if present
		if kwCols := table.KwList(); kwCols != nil {
			for _, col := range kwCols {
				if col == nil {
					continue
				}

				colName := col.Name()
				if err := ValidateIdentifierLength(colName, dialect); err != nil {
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

	if r.db == nil {
		return errors.New("database not initialized; call Init or InitWithOptions first")
	}

	dialect := r.CurrentDialect()
	if dialect == "" {
		return errors.New("unable to determine current database dialect")
	}

	// Use strict mode for explicit validation call
	return r.validateRegisteredTableIdentifiers("strict")
}
