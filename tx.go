package tsq

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgconn"
	"github.com/lib/pq"
	"github.com/mattn/go-sqlite3"
)

const (
	defaultTxRetryMaxAttempts       = 3
	defaultTxRetryInitialBackoff    = 5 * time.Millisecond
	defaultTxRetryMaxBackoff        = 25 * time.Millisecond
	defaultTxRetryBackoffMultiplier = 2.0
)

type txRetryStage uint8

const (
	txRetryStageBegin txRetryStage = iota + 1
	txRetryStageBody
	txRetryStageCommit
)

// TxRetryPredicate reports whether err should retry the whole transaction callback.
type TxRetryPredicate func(err error) bool

// TxOptions configures Runtime.WithTx.
type TxOptions struct {
	// SQL passes through to database/sql BeginTx.
	SQL *sql.TxOptions
	// Retry decides whether a failed transaction attempt should be retried.
	// When set, RetryConfig defaults are applied automatically unless overridden.
	Retry TxRetryPredicate
	// RetryConfig customizes retry timing and attempt limits when Retry is set.
	RetryConfig *TxRetryConfig
}

// TxRetryConfig configures retry timing and attempt limits for Runtime.WithTx.
type TxRetryConfig struct {
	// MaxAttempts is the total number of attempts, including the first try.
	MaxAttempts int
	// InitialBackoff is the delay after the first retryable failure.
	InitialBackoff time.Duration
	// MaxBackoff caps exponential backoff. Zero means no cap.
	MaxBackoff time.Duration
	// BackoffMultiplier grows the delay after each retryable failure.
	BackoffMultiplier float64
}

// DefaultTxRetryConfig returns the default retry timing used when Retry is set.
func DefaultTxRetryConfig() *TxRetryConfig {
	return &TxRetryConfig{
		MaxAttempts:       defaultTxRetryMaxAttempts,
		InitialBackoff:    defaultTxRetryInitialBackoff,
		MaxBackoff:        defaultTxRetryMaxBackoff,
		BackoffMultiplier: defaultTxRetryBackoffMultiplier,
	}
}

// IsOptimisticLockError reports whether err is an ErrOptimisticLockConflict.
func IsOptimisticLockError(err error) bool {
	return errors.Is(err, &ErrOptimisticLockConflict{})
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

// IsRetryableTransactionConflictError reports whether err looks like a transient database concurrency failure.
func IsRetryableTransactionConflictError(err error) bool {
	if err == nil {
		return false
	}

	if mysqlErr, ok := errors.AsType[*mysql.MySQLError](err); ok {
		return mysqlErr.Number == 1205 || mysqlErr.Number == 1213
	}

	if pqErr, ok := errors.AsType[*pq.Error](err); ok {
		switch string(pqErr.Code) {
		case "40001", "40P01", "55P03":
			return true
		}
	}

	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		switch pgErr.Code {
		case "40001", "40P01", "55P03":
			return true
		}
	}

	if sqliteErr, ok := errors.AsType[sqlite3.Error](err); ok {
		return errors.Is(sqliteErr.Code, sqlite3.ErrBusy) || errors.Is(sqliteErr.Code, sqlite3.ErrLocked)
	}

	return false
}

// IsCommonTransactionRetryableError reports whether err matches any built-in transaction retry helper.
func IsCommonTransactionRetryableError(err error) bool {
	return IsOptimisticLockError(err) ||
		IsRetryableNetworkError(err) ||
		IsRetryableTransactionConflictError(err)
}

type normalizedTxOptions struct {
	sqlOptions  *sql.TxOptions
	retry       TxRetryPredicate
	retryConfig *TxRetryConfig
}

func normalizeTxOptions(options *TxOptions) (*normalizedTxOptions, error) {
	normalized := &normalizedTxOptions{}
	if options == nil {
		return normalized, nil
	}

	if options.SQL != nil {
		normalized.sqlOptions = new(*options.SQL)
	}

	if options.Retry == nil {
		if options.RetryConfig != nil {
			return nil, errors.New("transaction retry config requires a retry predicate")
		}

		return normalized, nil
	}

	retryConfig := DefaultTxRetryConfig()
	if options.RetryConfig != nil {
		retryConfig = &TxRetryConfig{
			MaxAttempts:       options.RetryConfig.MaxAttempts,
			InitialBackoff:    options.RetryConfig.InitialBackoff,
			MaxBackoff:        options.RetryConfig.MaxBackoff,
			BackoffMultiplier: options.RetryConfig.BackoffMultiplier,
		}
	}

	if err := validateTxRetryConfig(retryConfig); err != nil {
		return nil, err
	}

	normalized.retry = options.Retry
	normalized.retryConfig = retryConfig

	return normalized, nil
}

func validateTxRetryConfig(options *TxRetryConfig) error {
	if options == nil {
		return nil
	}

	if options.MaxAttempts < 1 {
		return fmt.Errorf("invalid transaction retry max attempts: %d", options.MaxAttempts)
	}

	if options.InitialBackoff < 0 {
		return fmt.Errorf("invalid transaction retry initial backoff: %s", options.InitialBackoff)
	}

	if options.MaxBackoff < 0 {
		return fmt.Errorf("invalid transaction retry max backoff: %s", options.MaxBackoff)
	}

	if options.MaxBackoff > 0 && options.MaxBackoff < options.InitialBackoff {
		return fmt.Errorf(
			"invalid transaction retry backoff range: max backoff %s is smaller than initial backoff %s",
			options.MaxBackoff,
			options.InitialBackoff,
		)
	}

	if options.BackoffMultiplier < 1 {
		return fmt.Errorf("invalid transaction retry backoff multiplier: %v", options.BackoffMultiplier)
	}

	return nil
}

func validateTxRuntime(r *Runtime) error {
	if r == nil {
		return errors.New("runtime cannot be nil")
	}

	if r.db == nil || r.dialect == nil {
		return errors.New("runtime is not initialized; construct it with NewRuntime")
	}

	return nil
}

func shouldRetryTx(err error, stage txRetryStage, options *normalizedTxOptions, attempt int) bool {
	return options != nil &&
		options.retry != nil &&
		options.retryConfig != nil &&
		attempt < options.retryConfig.MaxAttempts &&
		stage != txRetryStageCommit &&
		options.retry(err)
}

func txRetryDelay(options *TxRetryConfig, attempt int) time.Duration {
	if options == nil || attempt < 1 {
		return 0
	}

	delay := options.InitialBackoff
	for i := 1; i < attempt; i++ {
		delay = time.Duration(float64(delay) * options.BackoffMultiplier)
		if options.MaxBackoff > 0 && delay >= options.MaxBackoff {
			return options.MaxBackoff
		}
	}

	if options.MaxBackoff > 0 && delay > options.MaxBackoff {
		return options.MaxBackoff
	}

	return delay
}

func waitTxRetry(ctx context.Context, options *TxRetryConfig, attempt int) error {
	delay := txRetryDelay(options, attempt)
	if delay <= 0 {
		return nil
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func executeTxAttempt1[T any](
	ctx context.Context,
	r *Runtime,
	options *normalizedTxOptions,
	fn func(context.Context, SQLExecutor) (T, error),
) (_ T, stage txRetryStage, err error) {
	tx, err := r.db.BeginTx(ctx, options.sqlOptions)
	if err != nil {
		var zero T
		return zero, txRetryStageBegin, fmt.Errorf("begin transaction: %w", err)
	}

	committed := false

	defer func() {
		if committed {
			return
		}

		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			wrapped := fmt.Errorf("rollback transaction: %w", rollbackErr)
			if err == nil {
				err = wrapped
				return
			}

			err = errors.Join(err, wrapped)
		}
	}()

	result, err := fn(ctx, wrapExecutor(tx, r.dialect, r))
	if err != nil {
		var zero T
		return zero, txRetryStageBody, err
	}

	if err := tx.Commit(); err != nil {
		var zero T
		return zero, txRetryStageCommit, fmt.Errorf("commit transaction: %w", err)
	}

	committed = true

	return result, 0, nil
}

func withTxRuntime1[T any](
	r *Runtime,
	ctx context.Context,
	options *TxOptions,
	fn func(context.Context, SQLExecutor) (T, error),
) (T, error) {
	var zero T

	if err := validateTxRuntime(r); err != nil {
		return zero, err
	}

	if fn == nil {
		return zero, errors.New("transaction function cannot be nil")
	}

	normalized, err := normalizeTxOptions(options)
	if err != nil {
		return zero, err
	}

	return trace1WithRuntime(r, ctx, func(ctx context.Context) (T, error) {
		for attempt := 1; ; attempt++ {
			result, phase, err := executeTxAttempt1(ctx, r, normalized, fn)
			if err == nil {
				return result, nil
			}

			if !shouldRetryTx(err, phase, normalized, attempt) {
				return zero, err
			}

			if waitErr := waitTxRetry(ctx, normalized.retryConfig, attempt); waitErr != nil {
				return zero, errors.Join(err, waitErr)
			}
		}
	})
}

// WithTx1 runs fn in a transaction and returns one result value plus an error.
// The runtime is an explicit parameter because Go does not support generic methods on Runtime.
func WithTx1[T any](
	r *Runtime,
	ctx context.Context,
	options *TxOptions,
	fn func(context.Context, SQLExecutor) (T, error),
) (T, error) {
	return withTxRuntime1(r, ctx, options, fn)
}

// WithTx2 runs fn in a transaction and returns two result values plus an error.
// The runtime is an explicit parameter because Go does not support generic methods on Runtime.
func WithTx2[T1, T2 any](
	r *Runtime,
	ctx context.Context,
	options *TxOptions,
	fn func(context.Context, SQLExecutor) (T1, T2, error),
) (T1, T2, error) {
	type pair struct {
		first  T1
		second T2
	}

	result, err := withTxRuntime1(r, ctx, options, func(ctx context.Context, txExec SQLExecutor) (pair, error) {
		first, second, err := fn(ctx, txExec)
		if err != nil {
			return pair{}, err
		}

		return pair{first: first, second: second}, nil
	})
	if err != nil {
		var zero1 T1
		var zero2 T2

		return zero1, zero2, err
	}

	return result.first, result.second, nil
}
