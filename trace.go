package tsq

import (
	"context"
	"errors"
	"log/slog"
	"slices"
)

const maxTracers = 100

// Tracer wraps a function call with tracing behavior.
// Configure tracers via RuntimeOptions.Tracers when constructing a Runtime.
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

	for _, v := range slices.Backward(r.tracers) {
		wrappedFn = v(wrappedFn)
	}

	return wrappedFn(ctx)
}

func (r *Runtime) trace1[T any](ctx context.Context, fn func(ctx context.Context) (T, error)) (T, error) {
	if r == nil {
		var zero T
		return zero, errors.New("runtime cannot be nil")
	}

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

	for _, v := range slices.Backward(r.tracers) {
		wrappedFn = v(wrappedFn)
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
		return provider.tsqRuntime().trace1(ctx, fn)
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

func appendTracers(existing []Tracer, newTracers ...Tracer) []Tracer {
	result := append([]Tracer(nil), existing...)

	for _, tracer := range newTracers {
		if tracer == nil {
			continue
		}

		if len(result) >= maxTracers {
			// appendTracers runs while NewRuntime is still assembling the Runtime, so
			// RuntimeOptions.Logger is not reachable from here yet.
			slog.Default().Warn("maximum tracer limit reached", "limit", maxTracers)

			return result
		}

		result = append(result, tracer)
	}

	return result
}
