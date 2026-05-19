package tsq

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"reflect"
	"sync"
	"time"
)

// MaxTracers bounds the number of tracers retained by a runtime.
const MaxTracers = 100

// TraceFn is the traced function signature used by Tracer.
type TraceFn func(ctx context.Context) error

// Tracer wraps a function call with tracing behavior.
type Tracer func(next TraceFn) TraceFn

type traceManager struct {
	mu        sync.RWMutex
	restoreMu sync.Mutex
	tracers   []Tracer
}

func newTraceManager() *traceManager {
	return &traceManager{}
}

// AddTracer adds a tracer to the default trace manager.
func AddTracer(tracer Tracer) {
	defaultRuntime.AddTracer(tracer)
}

// ClearTracers clears the default trace manager.
func ClearTracers() {
	defaultRuntime.ClearTracers()
}

// GetTracers returns all tracers from the default trace manager.
func GetTracers() []Tracer {
	return defaultRuntime.GetTracers()
}

// Add appends a tracer when capacity allows.
func (m *traceManager) Add(tracer Tracer) {
	if tracer == nil {
		return
	}

	m.restoreMu.Lock()
	defer m.restoreMu.Unlock()

	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.tracers) >= MaxTracers {
		slog.Warn("maximum tracer limit reached", "limit", MaxTracers)
		return
	}

	m.tracers = append(m.tracers, tracer)
}

// AddUnique appends only tracers that are not already registered.
func (m *traceManager) AddUnique(tracers ...Tracer) {
	m.restoreMu.Lock()
	defer m.restoreMu.Unlock()

	m.mu.Lock()
	defer m.mu.Unlock()

	for _, tracer := range tracers {
		if tracer == nil {
			continue
		}

		if len(m.tracers) >= MaxTracers {
			slog.Warn("maximum tracer limit reached", "limit", MaxTracers)
			return
		}

		duplicated := false

		for _, current := range m.tracers {
			if sameTracer(current, tracer) {
				duplicated = true
				break
			}
		}

		if !duplicated {
			m.tracers = append(m.tracers, tracer)
		}
	}
}

// Clear removes all registered tracers.
func (m *traceManager) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.tracers = nil
}

// Get returns a defensive copy of the registered tracers.
func (m *traceManager) Get() []Tracer {
	return m.snapshot()
}

func (m *traceManager) snapshot() []Tracer {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]Tracer, len(m.tracers))
	copy(result, m.tracers)

	return result
}

func (m *traceManager) restore(snapshot []Tracer) {
	m.restoreMu.Lock()
	defer m.restoreMu.Unlock()

	m.mu.Lock()
	defer m.mu.Unlock()

	m.tracers = append([]Tracer(nil), snapshot...)
}

// Trace executes fn through the registered tracer chain.
func (m *traceManager) Trace(ctx context.Context, fn func(ctx context.Context) error) error {
	if fn == nil {
		return errors.New("trace function cannot be nil")
	}

	if ctx == nil {
		return errors.New("context cannot be nil")
	}

	tracers := m.snapshot()
	wrappedFn := fn

	for i := len(tracers) - 1; i >= 0; i-- {
		wrappedFn = tracers[i](wrappedFn)
	}

	return wrappedFn(ctx)
}

func traceManagerTrace1[T any](m *traceManager, ctx context.Context, fn func(ctx context.Context) (T, error)) (T, error) {
	if fn == nil {
		var zero T
		return zero, errors.New("trace function cannot be nil")
	}

	if ctx == nil {
		var zero T
		return zero, errors.New("context cannot be nil")
	}

	tracers := m.snapshot()

	var result T

	wrappedFn := func(ctx context.Context) error {
		var err error

		result, err = fn(ctx)
		if err != nil {
			return err
		}

		return nil
	}

	for i := len(tracers) - 1; i >= 0; i-- {
		wrappedFn = tracers[i](wrappedFn)
	}

	return result, wrappedFn(ctx)
}

// Trace executes a function with all registered tracers applied.
func Trace(ctx context.Context, fn func(ctx context.Context) error) error {
	return defaultRuntime.Trace(ctx, fn)
}

// Trace1 executes fn with all registered tracers applied and returns its typed result.
func Trace1[T any](ctx context.Context, fn func(ctx context.Context) (T, error)) (T, error) {
	result, err := trace1WithRuntime(defaultRuntime, ctx, fn)
	return result, err
}

func appendUniqueTracers(existing []Tracer, newTracers ...Tracer) []Tracer {
	result := existing

	for _, tracer := range newTracers {
		if tracer == nil {
			continue
		}

		duplicated := false

		for _, current := range result {
			if sameTracer(current, tracer) {
				duplicated = true
				break
			}
		}

		if !duplicated {
			result = append(result, tracer)
		}
	}

	return result
}

func sameTracer(left, right Tracer) bool {
	if left == nil {
		return right == nil
	}

	if right == nil {
		return false
	}

	return tracerIdentity(left) == tracerIdentity(right)
}

func tracerIdentity(tracer Tracer) uintptr {
	return reflect.ValueOf(tracer).Pointer()
}

// PrintCost logs how long the wrapped call took.
func PrintCost(next TraceFn) TraceFn {
	return func(ctx context.Context) error {
		start := time.Now()
		err := next(ctx)

		duration := time.Since(start)
		if err != nil {
			slog.Info("cost", "duration", duration, "error", err)
		} else {
			slog.Info("cost", "duration", duration)
		}

		return err
	}
}

// PrintError logs the wrapped error, if any.
func PrintError(next TraceFn) TraceFn {
	return func(ctx context.Context) error {
		err := next(ctx)
		if err != nil {
			slog.Error("error", "error", err)
		}

		return err
	}
}

type contextKey string

const (
	printSQL contextKey = "printSQL"
)

// PrintSQL marks the context so query helpers log SQL and args.
func PrintSQL(next TraceFn) TraceFn {
	return func(ctx context.Context) error {
		return next(context.WithValue(ctx, printSQL, true))
	}
}

// PrettyJSON returns indented JSON string of obj.
func PrettyJSON(obj any) string {
	bs, err := json.MarshalIndent(obj, "", "    ")
	if err != nil {
		return ""
	}

	return string(bs)
}

// CompactJSON returns compact JSON string of obj.
func CompactJSON(obj any) string {
	bs, err := json.Marshal(obj)
	if err != nil {
		return ""
	}

	return string(bs)
}
