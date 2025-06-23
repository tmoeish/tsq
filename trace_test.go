package tsq

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

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

	if err != expectedError {
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

	if err != expectedError {
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

	if err != expectedError {
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

	if err != expectedError {
		t.Errorf("Expected error %v, got %v", expectedError, err)
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
