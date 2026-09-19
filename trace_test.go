package tsq

import (
	"context"
	"slices"
	"testing"
)

func TestRuntimeTracePreservesDistinctClosures(t *testing.T) {
	var calls []string
	makeTracer := func(name string) Tracer {
		return func(ctx context.Context, info TraceInfo, next func(context.Context) error) error {
			calls = append(calls, name+":before:"+string(info.Op))
			err := next(ctx)
			calls = append(calls, name+":after")

			return err
		}
	}

	runtime := &Runtime{tracers: appendTracers(nil, makeTracer("first"), makeTracer("second"))}
	err := runtime.trace(context.Background(), TraceInfo{Op: TraceOpList}, func(context.Context) error {
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

	runtime := &Runtime{tracers: appendTracers(nil, func(ctx context.Context, info TraceInfo, next func(context.Context) error) error {
		ops = append(ops, info.Op)

		return next(ctx)
	})}

	for _, op := range []TraceOp{TraceOpInsert, TraceOpUpdate, TraceOpDelete, TraceOpGet} {
		if err := runtime.trace(context.Background(), TraceInfo{Op: op}, func(context.Context) error { return nil }); err != nil {
			t.Fatalf("trace(%s) returned an error: %v", op, err)
		}
	}

	want := []TraceOp{TraceOpInsert, TraceOpUpdate, TraceOpDelete, TraceOpGet}
	if !slices.Equal(ops, want) {
		t.Fatalf("ops = %v, want %v", ops, want)
	}
}

// TestTracersSeeTheTable is what makes a span name useful: the table a write
// changes, or the FROM table of a query.
func TestTracersSeeTheTable(t *testing.T) {
	ctx := context.Background()

	var seen []TraceInfo

	rt := newSQLite(t, WithTracers(func(ctx context.Context, info TraceInfo, next func(context.Context) error) error {
		seen = append(seen, info)
		return next(ctx)
	}))

	row := &user{Name: "a", Email: "a@x"}
	if err := Users.Insert(ctx, rt, row); err != nil {
		t.Fatal(err)
	}

	if _, err := Select(Order_ID).From(Orders).Join(Users, Order_UserID.EQ(User_ID)).List(ctx, rt); err != nil {
		t.Fatal(err)
	}

	if _, err := DeleteFrom(Users).Where(User_ID.EQ(Val(row.ID))).Exec(ctx, rt); err != nil {
		t.Fatal(err)
	}

	if err := rt.WithTx(ctx, func(context.Context, Executor) error { return nil }); err != nil {
		t.Fatal(err)
	}

	want := []TraceInfo{
		{Op: TraceOpInsert, Table: "users"},
		{Op: TraceOpList, Table: "orders"},
		{Op: TraceOpDelete, Table: "users"},
		{Op: TraceOpTx},
	}
	if !slices.Equal(seen, want) {
		t.Fatalf("trace infos = %+v, want %+v", seen, want)
	}
}
