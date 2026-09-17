package tsq

import (
	"context"
	"errors"
	"log/slog"
	"slices"
)

const maxTracers = 100

// Tracer wraps a function call with tracing behavior.
// Configure tracers via WithTracers when constructing a Runtime.
// TraceOp names the kind of work a traced call performs.
type TraceOp string

// The operations TSQ traces. They match the labels the SQL log uses.
const (
	TraceOpInsert TraceOp = "insert"
	TraceOpUpdate TraceOp = "update"
	TraceOpDelete TraceOp = "delete"
	TraceOpGet    TraceOp = "get"
	TraceOpList   TraceOp = "list"
	TraceOpPage   TraceOp = "page"
	TraceOpCount  TraceOp = "count"
	TraceOpScalar TraceOp = "scalar"
	TraceOpExec   TraceOp = "exec"
	TraceOpTx     TraceOp = "tx"
)

// Tracer wraps one traced operation. Call next to run it, and return its error.
//
// op says what is being run, which is what makes a tracer useful: the previous
// signature passed only the continuation, so a tracer could time a call without
// being able to say what it had timed. The rendered SQL is not available here
// because tracing brackets the whole operation, including argument binding and
// dialect rendering; RuntimeOption WithSQLLogging reports statements instead.
type Tracer func(ctx context.Context, op TraceOp, next func(ctx context.Context) error) error

func (r *Runtime) trace(ctx context.Context, op TraceOp, fn func(ctx context.Context) error) error {
	if fn == nil {
		return errors.New("trace function cannot be nil")
	}

	if ctx == nil {
		return errors.New("context cannot be nil")
	}

	wrappedFn := fn

	for _, tracer := range slices.Backward(r.tracers) {
		next := wrappedFn

		wrappedFn = func(ctx context.Context) error {
			return tracer(ctx, op, next)
		}
	}

	return wrappedFn(ctx)
}

func (r *Runtime) trace1[T any](ctx context.Context, op TraceOp, fn func(ctx context.Context) (T, error)) (T, error) {
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

	for _, tracer := range slices.Backward(r.tracers) {
		next := wrappedFn

		wrappedFn = func(ctx context.Context) error {
			return tracer(ctx, op, next)
		}
	}

	return result, wrappedFn(ctx)
}

func traceExecutor(ctx context.Context, exec Executor, op TraceOp, fn func(ctx context.Context) error) error {
	if rt := runtimeForExecutor(exec); rt != nil {
		return rt.trace(ctx, op, fn)
	}

	if fn == nil {
		return errors.New("trace function cannot be nil")
	}

	if ctx == nil {
		return errors.New("context cannot be nil")
	}

	return fn(ctx)
}

func traceExecutor1[T any](ctx context.Context, exec Executor, op TraceOp, fn func(ctx context.Context) (T, error)) (T, error) {
	if rt := runtimeForExecutor(exec); rt != nil {
		return rt.trace1(ctx, op, fn)
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
			// WithLogger is not reachable from here yet.
			slog.Default().Warn("maximum tracer limit reached", "limit", maxTracers)

			return result
		}

		result = append(result, tracer)
	}

	return result
}
