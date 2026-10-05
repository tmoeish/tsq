package tsq

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"
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

	runtime := &Runtime{tracers: []Tracer{makeTracer("first"), makeTracer("second")}}
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

	runtime := &Runtime{tracers: []Tracer{func(ctx context.Context, info TraceInfo, next func(context.Context) error) error {
		ops = append(ops, info.Op)

		return next(ctx)
	}}}

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

	if _, err := Select(Order_ID).From(Orders).InnerJoin(Users, Order_UserID.EQ(User_ID)).List(ctx, rt); err != nil {
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

// TestHardDeletesAreTracedApart covers hard and soft deletes, which tracers saw
// under one name, delete: a trace could not say whether data was removed.
func TestHardDeletesAreTracedApart(t *testing.T) {
	var ops []TraceOp

	rt := newSQLite(t, WithTracers(func(ctx context.Context, info TraceInfo, next func(context.Context) error) error {
		ops = append(ops, info.Op)

		return next(ctx)
	}))

	rows := seedUsers(t, rt, "a", "b")
	ops = nil

	if err := Users.Delete(context.Background(), rt, rows[0]); err != nil {
		t.Fatal(err)
	}

	if err := Users.HardDelete(context.Background(), rt, rows[1]); err != nil {
		t.Fatal(err)
	}

	if _, err := HardDeleteFrom(Users).Where(And()).Exec(context.Background(), rt); err != nil {
		t.Fatal(err)
	}

	if len(ops) != 3 || ops[0] != TraceOpDelete || ops[1] != TraceOpHardDelete || ops[2] != TraceOpHardDelete {
		t.Fatalf("traced %v", ops)
	}
}

// TestTracersAreHeldToTheirContract covers a tracer that does not do what a
// Tracer must: run the operation once, with a context. One that returned nil
// without calling next made every operation a success that never ran (an Insert
// that inserted nothing, a Get that returned a nil row and no error, a
// transaction whose callback was skipped); one that called next twice ran the
// statement twice; one that passed a nil context made database/sql panic with a
// lock held, and Close never returned.
func TestTracersAreHeldToTheirContract(t *testing.T) {
	ctx := context.Background()

	skips := newSQLite(t, WithTracers(func(context.Context, TraceInfo, func(context.Context) error) error { return nil }))

	row := &user{Name: "a", Email: "a@example.test"}
	if err := Users.Insert(ctx, skips, row); err == nil || !strings.Contains(err.Error(), "without calling next") {
		t.Errorf("an insert whose tracer never ran it = %v", err)
	}

	if rows, err := Users.Query().List(ctx, skips); err == nil {
		t.Errorf("a list whose tracer never ran it returned %v and no error", rows)
	}

	if got, err := Users.Get(ctx, skips, 1); err == nil {
		t.Errorf("a get whose tracer never ran it returned %v and no error", got)
	}

	ran := false
	if err := skips.WithTx(ctx, func(context.Context, Executor) error { ran = true; return nil }); err == nil || ran {
		t.Errorf("a transaction whose tracer never ran it: callback ran = %v, err = %v", ran, err)
	}

	// A tracer may still refuse an operation: its error is the operation's.
	refused := errors.New("not today")
	refuses := newSQLite(t, WithTracers(func(context.Context, TraceInfo, func(context.Context) error) error { return refused }))

	if err := Users.Insert(ctx, refuses, &user{Name: "b", Email: "b@example.test"}); !errors.Is(err, refused) {
		t.Errorf("a refused insert = %v", err)
	}

	twice := newSQLite(t, WithTracers(func(ctx context.Context, _ TraceInfo, next func(context.Context) error) error {
		_ = next(ctx)

		return next(ctx)
	}))

	if err := Orders.Insert(ctx, twice, &order{UserID: 1, Amount: 5}); err == nil || !strings.Contains(err.Error(), "more than once") {
		t.Errorf("an insert whose tracer ran it twice = %v", err)
	}

	if _, err := UpdateTable(Orders).Set(Order_Amount, Add(Order_Amount, Val(int64(1)))).Where(And()).Exec(ctx, twice); err == nil {
		t.Error("an update whose tracer ran it twice returned no error")
	}

	var stored, amount int64
	if err := twice.DB().QueryRowContext(ctx, "SELECT COUNT(*), COALESCE(SUM(amount), 0) FROM orders").Scan(&stored, &amount); err != nil || stored != 1 || amount != 6 {
		t.Errorf("after two operations each run twice by their tracer: %d rows, amount %d, %v; want one row of 6", stored, amount, err)
	}

	// The nil context is the case under test.
	var none context.Context

	nilContext := newSQLite(t, WithTracers(func(_ context.Context, _ TraceInfo, next func(context.Context) error) error { return next(none) }))

	if _, err := Orders.Query().List(ctx, nilContext); err == nil || !strings.Contains(err.Error(), "nil context") {
		t.Errorf("a list whose tracer passed a nil context = %v", err)
	}

	// The pool is not left locked: the runtime still answers, and closes.
	done := make(chan error, 1)
	go func() { done <- nilContext.Close() }()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return after a tracer passed a nil context")
	}
}
