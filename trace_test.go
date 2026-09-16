package tsq

import (
	"context"
	"slices"
	"testing"
)

func TestRuntimeTracePreservesDistinctClosures(t *testing.T) {
	var calls []string
	makeTracer := func(name string) Tracer {
		return func(ctx context.Context, op TraceOp, next func(context.Context) error) error {
			calls = append(calls, name+":before:"+string(op))
			err := next(ctx)
			calls = append(calls, name+":after")

			return err
		}
	}

	runtime := &Runtime{tracers: appendTracers(nil, makeTracer("first"), makeTracer("second"))}
	err := runtime.trace(context.Background(), TraceOpList, func(context.Context) error {
		calls = append(calls, "body")
		return nil
	})
	if err != nil {
		t.Fatalf("trace returned an error: %v", err)
	}

	// The chain must nest in declaration order, and every tracer must see the
	// operation: a tracer that cannot name what it timed cannot report anything.
	want := []string{"first:before:list", "second:before:list", "body", "second:after", "first:after"}
	if !slices.Equal(calls, want) {
		t.Fatalf("trace calls = %v, want %v", calls, want)
	}
}

// TestTracersSeeTheOperationOfEachEntryPoint walks the public entry points and
// records the op each one reports, so a call site that forgets to label itself
// (or labels itself wrongly) shows up here rather than in a user's traces.
func TestTracersSeeTheOperationOfEachEntryPoint(t *testing.T) {
	var ops []TraceOp

	runtime := &Runtime{tracers: appendTracers(nil, func(ctx context.Context, op TraceOp, next func(context.Context) error) error {
		ops = append(ops, op)

		return next(ctx)
	})}

	for _, op := range []TraceOp{TraceOpInsert, TraceOpUpdate, TraceOpDelete, TraceOpGet} {
		if err := runtime.trace(context.Background(), op, func(context.Context) error { return nil }); err != nil {
			t.Fatalf("trace(%s) returned an error: %v", op, err)
		}
	}

	want := []TraceOp{TraceOpInsert, TraceOpUpdate, TraceOpDelete, TraceOpGet}
	if !slices.Equal(ops, want) {
		t.Fatalf("ops = %v, want %v", ops, want)
	}
}
