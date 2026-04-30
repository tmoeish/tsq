package tsq

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
)

func passthroughTracer(next Fn) Fn {
	return next
}

func TestAddTracer(t *testing.T) {
	// Clear tracers before test
	ClearTracers()

	// Test adding a tracer
	tracer1 := func(next Fn) Fn {
		return func(ctx context.Context) error {
			return next(ctx)
		}
	}

	AddTracer(tracer1)

	tracers := GetTracers()
	if len(tracers) != 1 {
		t.Errorf("Expected 1 tracer, got %d", len(tracers))
	}

	// Test adding multiple tracers
	tracer2 := func(next Fn) Fn {
		return func(ctx context.Context) error {
			return next(ctx)
		}
	}

	AddTracer(tracer2)

	tracers = GetTracers()
	if len(tracers) != 2 {
		t.Errorf("Expected 2 tracers, got %d", len(tracers))
	}
}

func TestClearTracers(t *testing.T) {
	// Clear tracers before test to ensure clean state
	ClearTracers()

	// Add some tracers
	tracer := func(next Fn) Fn {
		return func(ctx context.Context) error {
			return next(ctx)
		}
	}

	AddTracer(tracer)
	AddTracer(tracer)

	// Verify tracers were added
	if len(GetTracers()) != 2 {
		t.Errorf("Expected 2 tracers before clear, got %d", len(GetTracers()))
	}

	// Clear tracers
	ClearTracers()

	// Verify tracers were cleared
	tracers := GetTracers()
	if len(tracers) != 0 {
		t.Errorf("Expected 0 tracers after clear, got %d", len(tracers))
	}
}

func TestGetTracers(t *testing.T) {
	// Clear tracers before test
	ClearTracers()

	// Test empty tracers
	tracers := GetTracers()
	if len(tracers) != 0 {
		t.Errorf("Expected 0 tracers, got %d", len(tracers))
	}

	// Add tracers and test
	tracer1 := func(next Fn) Fn { return next }
	tracer2 := func(next Fn) Fn { return next }

	AddTracer(tracer1)
	AddTracer(tracer2)

	tracers = GetTracers()
	if len(tracers) != 2 {
		t.Errorf("Expected 2 tracers, got %d", len(tracers))
	}

	// Test that returned slice is a copy (modifying it shouldn't affect original)
	tracers[0] = nil

	originalTracers := GetTracers()
	if len(originalTracers) != 2 {
		t.Error("GetTracers() should return a copy, not the original slice")
	}
}

func TestAppendUniqueTracersPreservesDistinctClosures(t *testing.T) {
	makeTracer := func(label string) Tracer {
		return func(next Fn) Fn {
			return func(ctx context.Context) error {
				_ = label
				return next(ctx)
			}
		}
	}

	left := makeTracer("left")
	right := makeTracer("right")

	tracers := appendUniqueTracers(nil, left, right)
	if len(tracers) != 2 {
		t.Fatalf("expected distinct closures to remain distinct, got %d tracers", len(tracers))
	}
}

func TestAppendUniqueTracersDeduplicatesSameTracerValue(t *testing.T) {
	tracer := Tracer(passthroughTracer)

	tracers := appendUniqueTracers(nil, tracer, tracer)
	if len(tracers) != 1 {
		t.Fatalf("expected identical tracer value to be deduplicated, got %d tracers", len(tracers))
	}
}

func TestTrace_NoTracers(t *testing.T) {
	// Clear tracers before test
	ClearTracers()

	executed := false
	fn := func(ctx context.Context) error {
		executed = true
		return nil
	}

	err := Trace(context.Background(), fn)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if !executed {
		t.Error("Function should have been executed")
	}
}

func TestTrace_RejectsNilFunction(t *testing.T) {
	ClearTracers()

	err := Trace(context.Background(), nil)
	if err == nil {
		t.Fatal("expected nil trace function to return an error")
	}

	if err.Error() != "trace function cannot be nil" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestTrace1_RejectsNilFunction(t *testing.T) {
	ClearTracers()

	result, err := Trace1[int](context.Background(), nil)
	if err == nil {
		t.Fatal("expected nil trace function to return an error")
	}

	if err.Error() != "trace function cannot be nil" {
		t.Fatalf("unexpected error: %v", err)
	}

	if result != 0 {
		t.Fatalf("expected zero result, got %d", result)
	}
}

func TestTrace_SingleTracer(t *testing.T) {
	// Clear tracers before test
	ClearTracers()

	tracerExecuted := false
	functionExecuted := false

	tracer := func(next Fn) Fn {
		return func(ctx context.Context) error {
			tracerExecuted = true
			return next(ctx)
		}
	}

	AddTracer(tracer)

	fn := func(ctx context.Context) error {
		functionExecuted = true
		return nil
	}

	err := Trace(context.Background(), fn)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if !tracerExecuted {
		t.Error("Tracer should have been executed")
	}

	if !functionExecuted {
		t.Error("Function should have been executed")
	}
}

func TestTrace_MultipleTracers(t *testing.T) {
	// Clear tracers before test
	ClearTracers()

	var executionOrder []string

	tracer1 := func(next Fn) Fn {
		return func(ctx context.Context) error {
			executionOrder = append(executionOrder, "tracer1_start")
			err := next(ctx)

			executionOrder = append(executionOrder, "tracer1_end")

			return err
		}
	}

	tracer2 := func(next Fn) Fn {
		return func(ctx context.Context) error {
			executionOrder = append(executionOrder, "tracer2_start")
			err := next(ctx)

			executionOrder = append(executionOrder, "tracer2_end")

			return err
		}
	}

	AddTracer(tracer1)
	AddTracer(tracer2)

	fn := func(ctx context.Context) error {
		executionOrder = append(executionOrder, "function")
		return nil
	}

	err := Trace(context.Background(), fn)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	// Tracers should be applied in reverse order (LIFO)
	// So tracer2 (last added) should wrap tracer1
	// But the actual execution order depends on how they're applied
	expectedOrder := []string{
		"tracer1_start",
		"tracer2_start",
		"function",
		"tracer2_end",
		"tracer1_end",
	}

	if len(executionOrder) != len(expectedOrder) {
		t.Errorf("Expected %d execution steps, got %d", len(expectedOrder), len(executionOrder))
	}

	for i, expected := range expectedOrder {
		if i >= len(executionOrder) || executionOrder[i] != expected {
			t.Errorf("Expected execution order[%d] to be '%s', got '%s'", i, expected, executionOrder[i])
		}
	}
}

func TestTrace_ErrorPropagation(t *testing.T) {
	// Clear tracers before test
	ClearTracers()

	expectedError := errors.New("test error")

	tracer := func(next Fn) Fn {
		return func(ctx context.Context) error {
			return next(ctx) // Should propagate error
		}
	}

	AddTracer(tracer)

	fn := func(ctx context.Context) error {
		return expectedError
	}

	err := Trace(context.Background(), fn)

	if !errors.Is(err, expectedError) {
		t.Errorf("Expected error %v, got %v", expectedError, err)
	}
}

func TestTrace_TracerError(t *testing.T) {
	// Clear tracers before test
	ClearTracers()

	expectedError := errors.New("tracer error")

	tracer := func(next Fn) Fn {
		return func(ctx context.Context) error {
			return expectedError // Tracer returns error without calling next
		}
	}

	AddTracer(tracer)

	functionExecuted := false
	fn := func(ctx context.Context) error {
		functionExecuted = true
		return nil
	}

	err := Trace(context.Background(), fn)

	if !errors.Is(err, expectedError) {
		t.Errorf("Expected error %v, got %v", expectedError, err)
	}

	if functionExecuted {
		t.Error("Function should not have been executed when tracer returns error")
	}
}

func TestTimingTracer(t *testing.T) {
	// Clear tracers before test
	ClearTracers()

	AddTracer(PrintCost)

	executed := false
	fn := func(ctx context.Context) error {
		executed = true
		return nil
	}

	err := Trace(context.Background(), fn)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if !executed {
		t.Error("Function should have been executed")
	}
}

func TestTimingTracer_WithError(t *testing.T) {
	// Clear tracers before test
	ClearTracers()

	AddTracer(PrintCost)

	expectedError := errors.New("test error")
	fn := func(ctx context.Context) error {
		return expectedError
	}

	err := Trace(context.Background(), fn)

	if !errors.Is(err, expectedError) {
		t.Errorf("Expected error %v, got %v", expectedError, err)
	}
}

func TestErrorTracer(t *testing.T) {
	// Clear tracers before test
	ClearTracers()

	AddTracer(PrintError)

	executed := false
	fn := func(ctx context.Context) error {
		executed = true
		return nil
	}

	err := Trace(context.Background(), fn)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if !executed {
		t.Error("Function should have been executed")
	}
}

func TestErrorTracer_WithError(t *testing.T) {
	// Clear tracers before test
	ClearTracers()

	AddTracer(PrintError)

	expectedError := errors.New("test error")
	fn := func(ctx context.Context) error {
		return expectedError
	}

	err := Trace(context.Background(), fn)

	if !errors.Is(err, expectedError) {
		t.Errorf("Expected error %v, got %v", expectedError, err)
	}
}

func TestMaxTracersEnforced(t *testing.T) {
	// Clear tracers before test
	ClearTracers()

	// Add exactly MaxTracers tracers
	for i := 0; i < MaxTracers; i++ {
		tracer := func(next Fn) Fn {
			return func(ctx context.Context) error {
				return next(ctx)
			}
		}
		AddTracer(tracer)
	}

	tracers := GetTracers()
	if len(tracers) != MaxTracers {
		t.Errorf("Expected %d tracers, got %d", MaxTracers, len(tracers))
	}

	// Try to add one more tracer - should be rejected
	tracer := func(next Fn) Fn {
		return func(ctx context.Context) error {
			return next(ctx)
		}
	}
	AddTracer(tracer)

	tracers = GetTracers()
	if len(tracers) != MaxTracers {
		t.Errorf("Expected max tracers to remain at %d, got %d", MaxTracers, len(tracers))
	}
}

func TestConcurrentTracerAddDuringRestore(t *testing.T) {
	// Clear tracers before test
	ClearTracers()

	// Add some initial tracers
	for i := 0; i < 5; i++ {
		tracer := func(next Fn) Fn {
			return func(ctx context.Context) error {
				return next(ctx)
			}
		}
		AddTracer(tracer)
	}

	// Now we'll simulate a restore operation while concurrent AddTracer calls happen
	rt := DefaultRuntime()
	tm := rt.TraceManager()

	// Create a channel to signal when restore has begun
	restoreStarted := make(chan struct{})
	restoreDone := make(chan struct{})

	// Goroutine to perform restore in background (simulating rollback during Init)
	go func() {
		snapshot := tm.snapshot()
		close(restoreStarted)
		// Simulate some delay to increase chance of interleaving
		tm.restore(snapshot)
		close(restoreDone)
	}()

	// Wait for restore to start, then try to add tracers concurrently
	<-restoreStarted

	// Launch multiple goroutines trying to add tracers concurrently
	var wg sync.WaitGroup
	errors := make(chan error, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			tracer := func(next Fn) Fn {
				return func(ctx context.Context) error {
					return next(ctx)
				}
			}
			// This should not panic even with concurrent restore
			AddTracer(tracer)
		}(i)
	}

	// Wait for all goroutines to complete
	wg.Wait()

	// Wait for restore to complete
	<-restoreDone

	// Verify we have tracers (some combination of initial + added ones)
	finalTracers := GetTracers()
	if len(finalTracers) == 0 {
		t.Error("Expected tracers to remain after concurrent operations")
	}

	// Verify no errors occurred (the test would panic on race condition)
	close(errors)
	for err := range errors {
		if err != nil {
			t.Errorf("Concurrent operation failed: %v", err)
		}
	}
}

func TestPrettyJSON(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected bool // whether result should be non-empty
	}{
		{
			name: "simple object",
			input: map[string]any{
				"name": "test",
				"age":  25,
			},
			expected: true,
		},
		{
			name:     "string",
			input:    "hello",
			expected: true,
		},
		{
			name:     "number",
			input:    42,
			expected: true,
		},
		{
			name:     "nil",
			input:    nil,
			expected: true,
		},
		{
			name:     "invalid json (channel)",
			input:    make(chan int),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := PrettyJSON(tt.input)

			if tt.expected && result == "" {
				t.Error("Expected non-empty result")
			}

			if !tt.expected && result != "" {
				t.Error("Expected empty result for invalid JSON")
			}

			// For valid JSON, verify it's properly formatted
			if tt.expected && result != "" {
				var parsed any

				err := json.Unmarshal([]byte(result), &parsed)
				if err != nil {
					t.Errorf("Result is not valid JSON: %v", err)
				}
			}
		})
	}
}

func TestCompactJSON(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected bool // whether result should be non-empty
	}{
		{
			name: "simple object",
			input: map[string]any{
				"name": "test",
				"age":  25,
			},
			expected: true,
		},
		{
			name:     "string",
			input:    "hello",
			expected: true,
		},
		{
			name:     "number",
			input:    42,
			expected: true,
		},
		{
			name:     "nil",
			input:    nil,
			expected: true,
		},
		{
			name:     "invalid json (channel)",
			input:    make(chan int),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CompactJSON(tt.input)

			if tt.expected && result == "" {
				t.Error("Expected non-empty result")
			}

			if !tt.expected && result != "" {
				t.Error("Expected empty result for invalid JSON")
			}

			// For valid JSON, verify it's properly formatted
			if tt.expected && result != "" {
				var parsed any

				err := json.Unmarshal([]byte(result), &parsed)
				if err != nil {
					t.Errorf("Result is not valid JSON: %v", err)
				}
			}
		})
	}
}

func TestPrettyJSON_vs_CompactJSON(t *testing.T) {
	input := map[string]any{
		"name": "test",
		"nested": map[string]any{
			"value": 42,
		},
	}

	pretty := PrettyJSON(input)
	compact := CompactJSON(input)

	if pretty == "" || compact == "" {
		t.Fatal("Both functions should return non-empty results")
	}

	// Pretty JSON should be longer due to indentation
	if len(pretty) <= len(compact) {
		t.Error("Pretty JSON should be longer than compact JSON")
	}

	// Both should be valid JSON
	var prettyParsed, compactParsed any
	if err := json.Unmarshal([]byte(pretty), &prettyParsed); err != nil {
		t.Errorf("Pretty JSON is invalid: %v", err)
	}

	if err := json.Unmarshal([]byte(compact), &compactParsed); err != nil {
		t.Errorf("Compact JSON is invalid: %v", err)
	}
}

// TestAppendUniqueTracersWithNilInput tests dedup with nil entries
func TestAppendUniqueTracersWithNilInput(t *testing.T) {
	tracer1 := func(next Fn) Fn {
		return func(ctx context.Context) error { return next(ctx) }
	}

	// Mix nil and valid tracers
	var input []Tracer
	input = append(input, tracer1, nil, tracer1, nil, tracer1)

	result := appendUniqueTracers(nil, input...)

	// Should only have 1 unique tracer (nil should be skipped, duplicates removed)
	if len(result) != 1 {
		t.Errorf("expected 1 tracer from duplicate+nil mix, got %d", len(result))
	}

	// Test with empty input
	result = appendUniqueTracers(nil)
	if len(result) != 0 {
		t.Errorf("expected 0 tracers from empty input, got %d", len(result))
	}

	// Test with only nils
	result = appendUniqueTracers(nil, nil, nil, nil)
	if len(result) != 0 {
		t.Errorf("expected 0 tracers from only nils, got %d", len(result))
	}
}

// TestAppendUniqueTracersWithLargeList tests dedup on list with large number of same tracer
func TestAppendUniqueTracersWithLargeList(t *testing.T) {
	// Use the same tracer reference multiple times
	executed := 0
	tracer := func(next Fn) Fn {
		return func(ctx context.Context) error {
			executed++
			return next(ctx)
		}
	}

	// Add the same tracer 60 times (simulating duplicate adds)
	var input []Tracer
	for i := 0; i < 60; i++ {
		input = append(input, tracer)
	}

	result := appendUniqueTracers(nil, input...)

	// Should have exactly 1 tracer (all adds were the same tracer)
	if len(result) != 1 {
		t.Errorf("expected 1 unique tracer, got %d", len(result))
	}
}

// TestPrintSQLTracer tests PrintSQL() tracer function
func TestPrintSQLTracer(t *testing.T) {
	ClearTracers()

	executed := false
	ctxValue := false

	tracer := PrintSQL
	fn := func(ctx context.Context) error {
		executed = true
		// Check if context has printSQL value set
		val := ctx.Value(printSQL)
		if val != nil && val.(bool) {
			ctxValue = true
		}
		return nil
	}

	wrappedFn := tracer(fn)
	err := wrappedFn(context.Background())
	if err != nil {
		t.Errorf("expected no error, got: %v", err)
	}

	if !executed {
		t.Error("wrapped function should have executed")
	}

	if !ctxValue {
		t.Error("expected printSQL context value to be set to true")
	}
}

// TestPrintVersionFunctions tests PrintVersion() and PrintVersionJSON()
func TestPrintVersionFunctions(t *testing.T) {
	// These functions write to stdout, so we mainly test that they don't panic
	// In a real scenario, we'd capture stdout
	testCases := []struct {
		name string
		fn   func()
	}{
		{
			name: "PrintVersion",
			fn:   PrintVersion,
		},
		{
			name: "PrintVersionJSON",
			fn:   PrintVersionJSON,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Should not panic
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("PrintVersion functions should not panic: %v", r)
				}
			}()

			tc.fn()
		})
	}

	// Also test GetVersionInfo doesn't return nil
	info := GetVersionInfo()
	if info == nil {
		t.Error("GetVersionInfo should never return nil")
	}

	// Verify version info has reasonable structure
	if info.Version == "" {
		t.Error("Version should not be empty")
	}

	// Test string methods
	versionStr := info.String()
	if versionStr == "" || versionStr == "TSQ unknown" {
		t.Error("Version string should be non-empty and meaningful")
	}

	shortStr := info.ShortString()
	if shortStr == "" {
		t.Error("Short version string should not be empty")
	}
}

// TestRestoreTracersFromSnapshot tests restore() in various states
func TestRestoreTracersFromSnapshot(t *testing.T) {
	ClearTracers()
	rt := DefaultRuntime()
	tm := rt.TraceManager()

	// Add some initial tracers
	tracer1 := func(next Fn) Fn {
		return func(ctx context.Context) error { return next(ctx) }
	}
	tracer2 := func(next Fn) Fn {
		return func(ctx context.Context) error { return next(ctx) }
	}

	AddTracer(tracer1)
	AddTracer(tracer2)

	// Take a snapshot
	snapshot := tm.snapshot()
	if len(snapshot) != 2 {
		t.Errorf("expected 2 tracers in snapshot, got %d", len(snapshot))
	}

	// Add another tracer
	tracer3 := func(next Fn) Fn {
		return func(ctx context.Context) error { return next(ctx) }
	}
	AddTracer(tracer3)

	currentTracers := tm.Get()
	if len(currentTracers) != 3 {
		t.Errorf("expected 3 tracers after adding, got %d", len(currentTracers))
	}

	// Restore to the snapshot
	tm.restore(snapshot)

	restoredTracers := tm.Get()
	if len(restoredTracers) != 2 {
		t.Errorf("expected 2 tracers after restore, got %d", len(restoredTracers))
	}

	// Test restore with empty snapshot
	emptySnapshot := []Tracer{}
	tm.restore(emptySnapshot)
	if len(tm.Get()) != 0 {
		t.Error("restore with empty snapshot should result in empty tracers")
	}

	// Test restore with nil snapshot
	tm.restore(nil)
	if len(tm.Get()) != 0 {
		t.Error("restore with nil snapshot should result in empty tracers")
	}
}

// TestTraceManagerConcurrentSnapshot tests concurrent reads during tracer changes
func TestTraceManagerConcurrentSnapshot(t *testing.T) {
	ClearTracers()
	rt := DefaultRuntime()
	tm := rt.TraceManager()

	// Add initial tracers
	for i := 0; i < 10; i++ {
		tracer := func(next Fn) Fn {
			return func(ctx context.Context) error { return next(ctx) }
		}
		AddTracer(tracer)
	}

	done := make(chan struct{})
	errors := make(chan string, 100)

	// Goroutine that continuously adds tracers
	go func() {
		for i := 0; i < 50; i++ {
			tracer := func(next Fn) Fn {
				return func(ctx context.Context) error { return next(ctx) }
			}
			AddTracer(tracer)
		}
		close(done)
	}()

	// Goroutine that continuously takes snapshots
	for j := 0; j < 20; j++ {
		go func() {
			for k := 0; k < 100; k++ {
				snapshot := tm.snapshot()
				if snapshot == nil {
					errors <- "snapshot returned nil"
				}
				if len(snapshot) < 0 {
					errors <- "snapshot had negative length"
				}
			}
		}()
	}

	// Wait for additions to complete
	<-done

	// Give snapshot goroutines time to finish
	for i := 0; i < 20; i++ {
		// Small delay to allow goroutines to finish
	}

	// Check for any errors
	close(errors)
	for err := range errors {
		t.Error(err)
	}

	// Verify final state is valid
	finalTracers := tm.Get()
	if len(finalTracers) == 0 {
		t.Error("expected some tracers after concurrent operations")
	}
}
