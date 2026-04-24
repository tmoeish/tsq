package tsq

import (
	"context"
	"sync"

	"github.com/juju/errors"
	"gopkg.in/gorp.v2"
)

// Runtime owns the mutable TSQ process state used for table registration,
// initialization, and tracing. Applications that need isolation can create a
// dedicated Runtime instead of relying on the package-level defaults.
type Runtime struct {
	registry     *Registry
	traceManager *TraceManager
	initMu       sync.Mutex
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

func (r *Runtime) RegisterTable(
	table Table,
	addTableFunc func(db *gorp.DbMap),
	initFunc func(db *gorp.DbMap) error,
) {
	if r == nil {
		panic("runtime cannot be nil")
	}

	r.registry.Register(table, addTableFunc, initFunc)
}

func (r *Runtime) snapshotRegisteredTables() []*RegisteredTable {
	if r == nil {
		return nil
	}

	return r.registry.Snapshot()
}

func (r *Runtime) Init(
	db *gorp.DbMap,
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

func (r *Runtime) InitWithOptions(db *gorp.DbMap, options *InitOptions) error {
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

	r.initMu.Lock()
	defer r.initMu.Unlock()

	rollbackTracers := r.traceManager.snapshot()
	r.traceManager.AddUnique(options.Tracers...)

	registeredTables := r.registry.Snapshot()

	for _, table := range registeredTables {
		table.AddTableFunc(db)
	}

	if options.AutoCreateTables {
		if err := db.CreateTablesIfNotExists(); err != nil {
			r.traceManager.restore(rollbackTracers)
			return errors.Annotate(err, "failed to create tables")
		}
	}

	if options.UpsertIndexes {
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
