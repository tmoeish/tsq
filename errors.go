package tsq

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
)

// RowStateError reports that a write needed the row in a state it was not in: a
// Delete of a row already deleted, a Restore of one that is not, or, on a table
// without a version column, an Update of a row that is gone. Unlike
// OptimisticLockError it is not a concurrency conflict, so retrying cannot help.
// On a table with a version column a row that is gone is an OptimisticLockError:
// its version no longer matches.
type RowStateError struct {
	Table string
	// Op is the write: TraceOpUpdate, TraceOpDelete (a soft delete) or
	// TraceOpRestore.
	Op TraceOp
	// Need is the state the write needed the rows in.
	Need     RowState
	Expected int64
	Actual   int64
	// Keys are the primary keys of the rows in the wrong state, when the write
	// could tell.
	Keys []any
}

func (e *RowStateError) Error() string {
	return fmt.Sprintf("%s on %s needs %s rows: expected %d row(s) to match, matched %d%s",
		e.Op, e.Table, e.Need, e.Expected, e.Actual, keysSuffix(e.Keys))
}

// RowState is the state a write needs its rows in.
type RowState uint8

const (
	// RowExists is a row still in the table: an Update on a table without
	// deleted_at.
	RowExists RowState = iota + 1
	// RowLive is a row not soft-deleted: an Update or Delete on a soft-delete table.
	RowLive
	// RowDeleted is a soft-deleted row: a Restore.
	RowDeleted
)

func (s RowState) String() string {
	switch s {
	case RowExists:
		return "existing"
	case RowLive:
		return "live"
	case RowDeleted:
		return "deleted"
	default:
		return fmt.Sprintf("RowState(%d)", uint8(s))
	}
}

// keysSuffix names the rows an error is about. Only keys are printed: the rest of
// a row may carry data that must not reach logs.
func keysSuffix(keys []any) string {
	if len(keys) == 0 {
		return ""
	}

	return fmt.Sprintf(" (keys %v)", keys)
}

// OptimisticLockError reports that a version-guarded write matched fewer rows than
// it was given: another writer changed or deleted one of them first. It is a
// business outcome to handle, typically by reloading and retrying; see
// WithRetry and IsOptimisticLockError.
type OptimisticLockError struct {
	Table    string
	Expected int64
	Actual   int64
	// Keys are the primary keys of the rows that were not written, when the write
	// could tell (BatchUpdate reads the rows back to find them). The other rows
	// of the batch were written and carry their new version.
	Keys []any
}

func (e *OptimisticLockError) Error() string {
	return fmt.Sprintf("optimistic lock conflict on %s: expected %d row(s) to match, matched %d%s",
		e.Table, e.Expected, e.Actual, keysSuffix(e.Keys))
}

// IsOptimisticLockError reports whether err wraps an OptimisticLockError.
func IsOptimisticLockError(err error) bool {
	_, ok := errors.AsType[*OptimisticLockError](err)

	return ok
}

// IsRetryableNetworkError reports whether err looks like a transient connection failure.
func IsRetryableNetworkError(err error) bool {
	if err == nil {
		return false
	}

	// A bare io.EOF is not one: a callback that reads a file returns it, and
	// retrying reran the whole transaction. Drivers report a dropped connection
	// as ErrBadConn or io.ErrUnexpectedEOF.
	if errors.Is(err, driver.ErrBadConn) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ECONNABORTED) ||
		errors.Is(err, syscall.ETIMEDOUT) {
		return true
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	var netErr net.Error

	return errors.As(err, &netErr) && netErr.Timeout()
}

// IsTxConflictError reports whether err is a conflict that running the whole
// transaction again can resolve: a deadlock, a serialization failure, or a lock
// that could not be taken (a wait timeout, or NOWAIT). It is the only class TSQ
// retries after a failed COMMIT, because a COMMIT failing with one of them did
// not commit; a network failure at commit time leaves that unknown.
func IsTxConflictError(err error) bool {
	if err == nil {
		return false
	}

	if number, ok := mysqlErrorNumber(err); ok {
		// 1205 lock wait timeout, 1213 deadlock, 3572 a NOWAIT lock that was taken:
		// the last is PostgreSQL's 55P03, which is retried there too.
		return number == 1205 || number == 1213 || number == 3572
	}

	if isPostgresRetryableTransactionConflict(err) {
		return true
	}

	return isSQLiteRetryableTransactionConflict(err)
}

// IsRetryableTxError reports whether err is any of the conditions TSQ knows how
// to retry: an optimistic-lock conflict, a retryable network failure, or a
// transaction conflict. It is the predicate to pass to WithRetry unless
// the caller wants a narrower rule.
func IsRetryableTxError(err error) bool {
	return IsOptimisticLockError(err) ||
		IsRetryableNetworkError(err) ||
		IsTxConflictError(err)
}

// IsDuplicateKeyError reports whether err is the database refusing a row because a
// primary key or unique index already holds its value. Each driver reports it its
// own way, which is what this hides: on MySQL an error number, on PostgreSQL a
// SQLSTATE, on SQLite a result code that one driver exposes as a method and another
// as a struct field.
func IsDuplicateKeyError(err error) bool { return isDuplicateKeyError(err) }

func isDuplicateKeyError(err error) bool {
	if err == nil {
		return false
	}

	if number, ok := mysqlErrorNumber(err); ok {
		return number == 1062
	}

	return isSQLiteDuplicateKeyError(err) || isPostgresDuplicateKeyError(err)
}
