package tsq

import (
	"runtime/debug"

	"github.com/juju/errors"
)

type panicContext struct {
	Operation  string // The operation that panicked (e.g., "Query.Build", "Query.Scan")
	Details    string // Additional context details
	Recovered  any    // The panic value
	StackTrace string // Stack trace at panic point
}

// PanicRecoveryError wraps a panic with diagnostic context.
type PanicRecoveryError struct {
	context panicContext
	message string
}

func newPanicRecoveryError(operation, details string, recovered any) *PanicRecoveryError {
	stackTrace := string(debug.Stack())

	return &PanicRecoveryError{
		context: panicContext{
			Operation:  operation,
			Details:    details,
			Recovered:  recovered,
			StackTrace: stackTrace,
		},
		message: errors.Errorf("panic in %s: %v", operation, recovered).Error(),
	}
}

func (e *PanicRecoveryError) Error() string {
	if e == nil {
		return ""
	}

	return e.message
}

// Unwrap returns the recovered panic value for errors.Is/As checks
func (e *PanicRecoveryError) Unwrap() error {
	if e == nil {
		return nil
	}

	if err, ok := e.context.Recovered.(error); ok {
		return err
	}

	return nil
}

func (e *PanicRecoveryError) Operation() string {
	if e == nil {
		return ""
	}

	return e.context.Operation
}

func (e *PanicRecoveryError) Details() string {
	if e == nil {
		return ""
	}

	return e.context.Details
}

func (e *PanicRecoveryError) Recovered() any {
	if e == nil {
		return nil
	}

	return e.context.Recovered
}

func (e *PanicRecoveryError) StackTrace() string {
	if e == nil {
		return ""
	}

	return e.context.StackTrace
}

// SafeOperation wraps an operation with panic recovery.
func SafeOperation(operation string, fn func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = newPanicRecoveryError(operation, "", r)
		}
	}()

	return errors.Trace(fn())
}
