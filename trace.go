package tsq

import (
	"context"
	"errors"
	"fmt"
	"slices"
)

// TraceOp names the kind of work a traced call performs.
type TraceOp string

// The operations TSQ traces. They match the labels the SQL log uses.
const (
	TraceOpInsert TraceOp = "insert"
	TraceOpUpsert TraceOp = "upsert"
	TraceOpUpdate TraceOp = "update"
	// TraceOpDelete is a soft delete: it stamps deleted_at.
	TraceOpDelete TraceOp = "delete"
	// TraceOpHardDelete removes rows.
	TraceOpHardDelete TraceOp = "hard_delete"
	// TraceOpRestore clears the tombstone of soft-deleted rows.
	TraceOpRestore TraceOp = "restore"
	TraceOpGet     TraceOp = "get"
	TraceOpExists  TraceOp = "exists"
	TraceOpList    TraceOp = "list"
	TraceOpIter    TraceOp = "iter"
	TraceOpPage    TraceOp = "page"
	TraceOpCount   TraceOp = "count"
	TraceOpTx      TraceOp = "tx"
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

	return r.traced(info, fn)(ctx)
}

// traced wraps fn in the runtime's tracers and holds them to what a Tracer must
// do: run the operation once, with a context. One that returned nil without
// calling next made the operation a success that never ran (an Insert that
// inserted nothing, a Get that returned a nil row and no error); one that called
// next twice ran the statement twice; and one that passed a nil context made
// database/sql panic with a lock held, after which Close never returned.
func (r *Runtime) traced(info TraceInfo, fn func(ctx context.Context) error) func(ctx context.Context) error {
	if len(r.tracers) == 0 {
		return fn
	}

	ran := 0

	wrapped := func(ctx context.Context) error {
		if ctx == nil {
			return fmt.Errorf("a tracer passed a nil context to the %s operation", info.Op)
		}

		ran++
		if ran > 1 {
			return fmt.Errorf("a tracer called next more than once for the %s operation; it ran once", info.Op)
		}

		return fn(ctx)
	}

	for _, tracer := range slices.Backward(r.tracers) {
		next := wrapped
		wrapped = func(ctx context.Context) error {
			return tracer(ctx, info, next)
		}
	}

	return func(ctx context.Context) error {
		err := wrapped(ctx)
		if err == nil && ran == 0 {
			return fmt.Errorf("a tracer returned without calling next: the %s operation did not run", info.Op)
		}

		return err
	}
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

	if err := r.traced(info, wrappedFn)(ctx); err != nil {
		var zero T

		return zero, err
	}

	return result, nil
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
