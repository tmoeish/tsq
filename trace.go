package tsq

import (
	"context"
	"errors"
	"log/slog"
	"slices"
)

const maxTracers = 100

// TraceOp names the kind of work a traced call performs.
type TraceOp string

// The operations TSQ traces. They match the labels the SQL log uses.
const (
	TraceOpInsert TraceOp = "insert"
	TraceOpUpsert TraceOp = "upsert"
	TraceOpUpdate TraceOp = "update"
	TraceOpDelete TraceOp = "delete"
	TraceOpGet    TraceOp = "get"
	TraceOpList   TraceOp = "list"
	TraceOpIter   TraceOp = "iter"
	TraceOpPage   TraceOp = "page"
	TraceOpCount  TraceOp = "count"
	TraceOpTx     TraceOp = "tx"
)

// TraceInfo describes a traced operation.
type TraceInfo struct {
	// Op is what runs.
	Op TraceOp
	// Table is the table written, or the FROM table of a query; empty for a
	// transaction.
	Table string
}

// Tracer wraps one traced operation: call next to run it, and return its error.
// Configure tracers with WithTracers. A tracer brackets the whole operation,
// argument binding and rendering included, so the statement is not known when it
// starts; WithSQLLogging reports statements.
type Tracer func(ctx context.Context, info TraceInfo, next func(ctx context.Context) error) error

func (r *Runtime) trace(ctx context.Context, info TraceInfo, fn func(ctx context.Context) error) error {
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
			return tracer(ctx, info, next)
		}
	}

	return wrappedFn(ctx)
}

func (r *Runtime) trace1[T any](ctx context.Context, info TraceInfo, fn func(ctx context.Context) (T, error)) (T, error) {
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
			return tracer(ctx, info, next)
		}
	}

	return result, wrappedFn(ctx)
}

func traceExecutor(ctx context.Context, exec Executor, info TraceInfo, fn func(ctx context.Context) error) error {
	if rt := runtimeForExecutor(exec); rt != nil {
		return rt.trace(ctx, info, fn)
	}

	if fn == nil {
		return errors.New("trace function cannot be nil")
	}

	if ctx == nil {
		return errors.New("context cannot be nil")
	}

	return fn(ctx)
}

func traceExecutor1[T any](ctx context.Context, exec Executor, info TraceInfo, fn func(ctx context.Context) (T, error)) (T, error) {
	if rt := runtimeForExecutor(exec); rt != nil {
		return rt.trace1(ctx, info, fn)
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
