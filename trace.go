package tsq

import (
	"context"
	"encoding/json"
	"log/slog"
	"reflect"
	"sync"
	"time"

	"github.com/juju/errors"
)

const MaxTracers = 100

// Fn represents a function that can be traced.
type Fn func(ctx context.Context) error

// Tracer is a middleware function that wraps another function for tracing/monitoring.
type Tracer func(next Fn) Fn

// TraceManager stores and executes tracers.
type TraceManager struct {
	mu        sync.RWMutex
	restoreMu sync.Mutex
	tracers   []Tracer
}

func NewTraceManager() *TraceManager {
	return &TraceManager{}
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

func (m *TraceManager) Add(tracer Tracer) {
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

func (m *TraceManager) AddUnique(tracers ...Tracer) {
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

func (m *TraceManager) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.tracers = nil
}

func (m *TraceManager) Get() []Tracer {
	return m.snapshot()
}

func (m *TraceManager) snapshot() []Tracer {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]Tracer, len(m.tracers))
	copy(result, m.tracers)

	return result
}

func (m *TraceManager) restore(snapshot []Tracer) {
	m.restoreMu.Lock()
	defer m.restoreMu.Unlock()

	m.mu.Lock()
	defer m.mu.Unlock()

	m.tracers = append([]Tracer(nil), snapshot...)
}

func (m *TraceManager) Trace(ctx context.Context, fn func(ctx context.Context) error) error {
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

func traceManagerTrace1[T any](m *TraceManager, ctx context.Context, fn func(ctx context.Context) (T, error)) (T, error) {
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
			return errors.Trace(err)
		}

		return nil
	}

	for i := len(tracers) - 1; i >= 0; i-- {
		wrappedFn = tracers[i](wrappedFn)
	}

	return result, errors.Trace(wrappedFn(ctx))
}

// Trace executes a function with all registered tracers applied.
func Trace(ctx context.Context, fn func(ctx context.Context) error) error {
	return errors.Trace(defaultRuntime.Trace(ctx, fn))
}

func Trace1[T any](ctx context.Context, fn func(ctx context.Context) (T, error)) (T, error) {
	result, err := Trace1WithRuntime(defaultRuntime, ctx, fn)
	return result, errors.Trace(err)
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

func PrintCost(next Fn) Fn {
	return func(ctx context.Context) error {
		start := time.Now()
		err := next(ctx)

		duration := time.Since(start)
		if err != nil {
			slog.Info("cost", "duration", duration, "error", errors.ErrorStack(err))
		} else {
			slog.Info("cost", "duration", duration)
		}

		return errors.Trace(err)
	}
}

func PrintError(next Fn) Fn {
	return func(ctx context.Context) error {
		err := next(ctx)
		if err != nil {
			slog.Error("error", "error", errors.ErrorStack(err))
		}

		return errors.Trace(err)
	}
}

type contextKey string

const (
	printSQL contextKey = "printSQL"
)

func PrintSQL(next Fn) Fn {
	return func(ctx context.Context) error {
		return errors.Trace(next(context.WithValue(ctx, printSQL, true)))
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
