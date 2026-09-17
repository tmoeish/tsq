package tsq

import "fmt"

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
