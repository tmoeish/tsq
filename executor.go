package tsq

import (
	"errors"
	"fmt"
)

var (
	errSQLExecutorNil              = errors.New("sql executor cannot be nil")
	errInsertRequiresColumn        = errors.New("insert requires at least one column")
	errInsertLayoutMismatch        = errors.New("batch insert requires matching column layouts")
	errUpdateRequiresMutableColumn = errors.New("update requires at least one mutable column")
	errUpdateRequiresPrimaryKey    = errors.New("update requires a non-zero primary key")
	errUpdateLayoutMismatch        = errors.New("batch update requires matching column layouts")
	errDeleteRequiresPrimaryKey    = errors.New("delete requires a non-zero primary key")
	errMutationItemNil             = errors.New("mutation item cannot be nil")
	errMutationItemPointer         = errors.New("mutation item must be a non-nil pointer")
	errMutationItemStructPointer   = errors.New("mutation item must point to a struct")
	errMutationItemNoTaggedFields  = errors.New("mutation item has no db-tagged fields")
)

// ErrOptimisticLockConflict reports that a version-guarded mutation matched fewer
// rows than expected.
type ErrOptimisticLockConflict struct {
	table    string
	expected int
	actual   int64
}

// Error implements error.
func (e *ErrOptimisticLockConflict) Error() string {
	if e == nil {
		return "optimistic lock conflict"
	}

	if e.table == "" {
		return fmt.Sprintf(
			"optimistic lock conflict: expected %d row(s) to match, updated %d",
			e.expected,
			e.actual,
		)
	}

	return fmt.Sprintf(
		"optimistic lock conflict on %s: expected %d row(s) to match, updated %d",
		e.table,
		e.expected,
		e.actual,
	)
}

// Is reports whether target is an optimistic lock conflict.
func (e *ErrOptimisticLockConflict) Is(target error) bool {
	_, ok := target.(*ErrOptimisticLockConflict)
	return ok
}
