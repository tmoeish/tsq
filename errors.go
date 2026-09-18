package tsq

import (
	"errors"
	"fmt"
)

// RowStateError reports that a write needed the row in a state it was not in: a
// Delete of a row already deleted, or a Restore of one that is not. Unlike
// OptimisticLockError it is not a concurrency conflict, so retrying cannot help;
// the row is either in the other state already or gone.
type RowStateError struct {
	Table    string
	Op       string
	Need     string
	Expected int
	Actual   int64
}

func (e *RowStateError) Error() string {
	return fmt.Sprintf("%s on %s needs %s: expected %d row(s) to match, matched %d",
		e.Op, e.Table, e.Need, e.Expected, e.Actual)
}

// IsRowStateError reports whether err wraps a RowStateError.
func IsRowStateError(err error) bool {
	_, ok := errors.AsType[*RowStateError](err)

	return ok
}

// OptimisticLockError reports that a version-guarded write matched fewer rows than
// it was given: another writer changed or deleted one of them first. It is a
// business outcome to handle, typically by reloading and retrying; see
// TxOptions.RetryIf and IsOptimisticLockError.
type OptimisticLockError struct {
	Table    string
	Expected int
	Actual   int64
}

func (e *OptimisticLockError) Error() string {
	return fmt.Sprintf("optimistic lock conflict on %s: expected %d row(s) to match, matched %d",
		e.Table, e.Expected, e.Actual)
}
