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
// Delete of a row already deleted, or a Restore of one that is not. Unlike
// OptimisticLockError it is not a concurrency conflict, so retrying cannot help;
// the row is either in the other state already or gone.
type RowStateError struct {
	Table    string
	Op       string
	Need     string
	Expected int64
	Actual   int64
}

func (e *RowStateError) Error() string {
	return fmt.Sprintf("%s on %s needs %s: expected %d row(s) to match, matched %d",
		e.Op, e.Table, e.Need, e.Expected, e.Actual)
}

// OptimisticLockError reports that a version-guarded write matched fewer rows than
// it was given: another writer changed or deleted one of them first. It is a
// business outcome to handle, typically by reloading and retrying; see
// WithRetry and IsOptimisticLockError.
type OptimisticLockError struct {
	Table    string
	Expected int64
	Actual   int64
}

func (e *OptimisticLockError) Error() string {
	return fmt.Sprintf("optimistic lock conflict on %s: expected %d row(s) to match, matched %d",
		e.Table, e.Expected, e.Actual)
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

	if errors.Is(err, driver.ErrBadConn) ||
		errors.Is(err, io.EOF) ||
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

// IsTxConflictError reports whether err is a deadlock or serialization failure
// the database has already rolled back, which is the only class TSQ retries
// after a failed COMMIT: those codes guarantee the transaction is gone, while a
// network failure at commit time leaves it unknown whether the commit landed.
func IsTxConflictError(err error) bool {
	if err == nil {
		return false
	}

	if number, ok := mysqlErrorNumber(err); ok {
		return number == 1205 || number == 1213
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
