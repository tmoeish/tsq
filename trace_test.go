package tsq

import (
	"context"
	"slices"
	"testing"
)

func TestRuntimeTracePreservesDistinctClosures(t *testing.T) {
	var calls []string
	makeTracer := func(name string) Tracer {
		return func(next func(context.Context) error) func(context.Context) error {
			return func(ctx context.Context) error {
				calls = append(calls, name+":before")
				err := next(ctx)
				calls = append(calls, name+":after")

				return err
			}
		}
	}

	runtime := &Runtime{tracers: appendTracers(nil, makeTracer("first"), makeTracer("second"))}
	err := runtime.trace(context.Background(), func(context.Context) error {
		calls = append(calls, "body")
		return nil
	})
	if err != nil {
		t.Fatalf("trace returned an error: %v", err)
	}

	want := []string{"first:before", "second:before", "body", "second:after", "first:after"}
	if !slices.Equal(calls, want) {
		t.Fatalf("trace calls = %v, want %v", calls, want)
	}
}
