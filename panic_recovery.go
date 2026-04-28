package tsq

import (
	"fmt"
	"runtime/debug"
)

// PanicContext captures context information when a panic occurs
type PanicContext struct {
	Operation  string // The operation that panicked (e.g., "Query.Build", "Query.Scan")
	Details    string // Additional context details
	Recovered  any    // The panic value
	StackTrace string // Stack trace at panic point
}

// PanicRecoveryError wraps a panic with context
type PanicRecoveryError struct {
	Context PanicContext
	Message string
}

func NewPanicRecoveryError(operation, details string, recovered any) *PanicRecoveryError {
	stackTrace := string(debug.Stack())

	return &PanicRecoveryError{
		Context: PanicContext{
			Operation:  operation,
			Details:    details,
			Recovered:  recovered,
			StackTrace: stackTrace,
		},
		Message: fmt.Sprintf("panic in %s: %v", operation, recovered),
	}
}

func (e *PanicRecoveryError) Error() string {
	return e.Message
}

// Unwrap returns the recovered panic value for errors.Is/As checks
func (e *PanicRecoveryError) Unwrap() error {
	if err, ok := e.Context.Recovered.(error); ok {
		return err
	}

	return nil
}

// GetContext returns the panic context for debugging
func (e *PanicRecoveryError) GetContext() PanicContext {
	return e.Context
}

// SafeOperation wraps an operation with panic recovery and context
// If the operation panics, returns PanicRecoveryError with context
func SafeOperation(operation string, fn func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = NewPanicRecoveryError(operation, "", r)
		}
	}()

	return fn()
}

// SafeOperationWithContext wraps an operation and returns context on panic
func SafeOperationWithContext(operation, details string, fn func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = NewPanicRecoveryError(operation, details, r)
		}
	}()

	return fn()
}

// PanicDocumentation documents which operations can panic in TSQ

// Operations that may panic:
// 1. Query.Scan() - When result scanning encounters type mismatches or field pointer errors
// 2. Column transformations - When functions receive unexpected input types
// 3. Condition evaluation - When comparing incompatible types
// 4. Join operations - When tables or columns are in invalid state
// 5. Field pointer calls - When the pointer function panics
//
// Best practices:
// - Always validate input before operations
// - Use proper error handling with errors.Is/As
// - Provide field pointers that handle edge cases
// - Test with edge case data
