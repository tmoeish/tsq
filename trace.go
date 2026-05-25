package tsq

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"reflect"
	"time"
)

const maxTracers = 100

// Tracer wraps a function call with tracing behavior.
// Configure tracers via InitOptions.Tracers when constructing a Runtime.
type Tracer func(next func(ctx context.Context) error) func(ctx context.Context) error

type traceProvider interface {
	tsqRuntime() *Runtime
}

func (r *Runtime) trace(ctx context.Context, fn func(ctx context.Context) error) error {
	if fn == nil {
		return errors.New("trace function cannot be nil")
	}

	if ctx == nil {
		return errors.New("context cannot be nil")
	}

	wrappedFn := fn

	for i := len(r.tracers) - 1; i >= 0; i-- {
		wrappedFn = r.tracers[i](wrappedFn)
	}

	return wrappedFn(ctx)
}

func traceRuntime1[T any](r *Runtime, ctx context.Context, fn func(ctx context.Context) (T, error)) (T, error) {
	if fn == nil {
		var zero T
		return zero, errors.New("trace function cannot be nil")
	}

	if ctx == nil {
		var zero T
		return zero, errors.New("context cannot be nil")
	}

	var result T

	wrappedFn := func(ctx context.Context) error {
		var err error

		result, err = fn(ctx)
		if err != nil {
			return err
		}

		return nil
	}

	for i := len(r.tracers) - 1; i >= 0; i-- {
		wrappedFn = r.tracers[i](wrappedFn)
	}

	return result, wrappedFn(ctx)
}

func traceExecutor(ctx context.Context, exec SQLExecutor, fn func(ctx context.Context) error) error {
	if provider, ok := exec.(traceProvider); ok && provider.tsqRuntime() != nil {
		return provider.tsqRuntime().trace(ctx, fn)
	}

	if fn == nil {
		return errors.New("trace function cannot be nil")
	}

	if ctx == nil {
		return errors.New("context cannot be nil")
	}

	return fn(ctx)
}

func traceExecutor1[T any](ctx context.Context, exec SQLExecutor, fn func(ctx context.Context) (T, error)) (T, error) {
	if provider, ok := exec.(traceProvider); ok && provider.tsqRuntime() != nil {
		return traceRuntime1(provider.tsqRuntime(), ctx, fn)
	}

	if fn == nil {
		var zero T
		return zero, errors.New("trace function cannot be nil")
	}

	if ctx == nil {
		var zero T
		return zero, errors.New("context cannot be nil")
	}

	return fn(ctx)
}

func appendUniqueTracers(existing []Tracer, newTracers ...Tracer) []Tracer {
	result := append([]Tracer(nil), existing...)

	for _, tracer := range newTracers {
		if tracer == nil {
			continue
		}

		if len(result) >= maxTracers {
			slog.Warn("maximum tracer limit reached", "limit", maxTracers)
			return result
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

func printCost(next func(ctx context.Context) error) func(ctx context.Context) error {
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

func printError(next func(ctx context.Context) error) func(ctx context.Context) error {
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

func printSQLTracer(next func(ctx context.Context) error) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		return next(context.WithValue(ctx, printSQL, true))
	}
}

func compactJSON(obj any) string {
	bs, err := json.Marshal(obj)
	if err != nil {
		return ""
	}

	return string(bs)
}
