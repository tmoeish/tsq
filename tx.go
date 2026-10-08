package tsq

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"
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

// TxOption configures Runtime.WithTx and Runtime.WithTxResult.
type TxOption func(*txConfig)

type txConfig struct {
	sql      sql.TxOptions
	retryIf  func(err error) bool
	retrySet bool
	policy   *RetryPolicy
}

// WithIsolation sets the transaction's isolation level.
func WithIsolation(level sql.IsolationLevel) TxOption {
	return func(c *txConfig) { c.sql.Isolation = level }
}

// WithReadOnly makes the transaction read-only.
func WithReadOnly() TxOption {
	return func(c *txConfig) { c.sql.ReadOnly = true }
}

// WithRetry runs the whole transaction again when an attempt fails with an error
// retryIf accepts, under DefaultRetryPolicy unless WithRetryPolicy sets another.
// A failed COMMIT is retried only for IsTxConflictError, whatever retryIf says:
// any other commit failure may have committed. IsRetryableTxError is the usual
// retryIf.
func WithRetry(retryIf func(err error) bool) TxOption {
	return func(c *txConfig) {
		c.retryIf = retryIf
		c.retrySet = true
	}
}

// WithRetryPolicy sets the attempt limit and backoff of WithRetry.
func WithRetryPolicy(policy RetryPolicy) TxOption {
	return func(c *txConfig) { c.policy = &policy }
}

// RetryPolicy is the attempt limit and backoff of a transaction run WithRetry.
// Each wait is drawn between half the backoff and the whole of it, so that two
// transactions that collided do not retry in step.
type RetryPolicy struct {
	// MaxAttempts is the total number of attempts, including the first try.
	MaxAttempts int
	// InitialBackoff is the delay after the first retryable failure.
	InitialBackoff time.Duration
	// MaxBackoff caps exponential backoff. Zero means no cap.
	MaxBackoff time.Duration
	// Multiplier grows the delay after each retryable failure.
	Multiplier float64
}

// DefaultRetryPolicy returns the policy WithRetry uses unless WithRetryPolicy
// sets another: three attempts, 5ms backoff doubling up to 25ms.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts:    defaultTxRetryMaxAttempts,
		InitialBackoff: defaultTxRetryInitialBackoff,
		MaxBackoff:     defaultTxRetryMaxBackoff,
		Multiplier:     defaultTxRetryBackoffMultiplier,
	}
}

type normalizedTxOptions struct {
	sqlOptions  *sql.TxOptions
	retryIf     func(err error) bool
	retryPolicy *RetryPolicy
}

func normalizeTxOptions(options []TxOption) (*normalizedTxOptions, error) {
	var cfg txConfig

	for _, option := range options {
		if option == nil {
			return nil, errors.New("transaction option cannot be nil")
		}

		option(&cfg)
	}

	normalized := &normalizedTxOptions{}
	if cfg.sql != (sql.TxOptions{}) {
		normalized.sqlOptions = new(cfg.sql)
	}

	if !cfg.retrySet {
		if cfg.policy != nil {
			return nil, errors.New("WithRetryPolicy requires WithRetry")
		}

		return normalized, nil
	}

	if cfg.retryIf == nil {
		return nil, errors.New("WithRetry needs a function that decides which errors to retry")
	}

	policy := DefaultRetryPolicy()
	if cfg.policy != nil {
		policy = *cfg.policy
	}

	if err := validateRetryPolicy(&policy); err != nil {
		return nil, err
	}

	normalized.retryIf = cfg.retryIf
	normalized.retryPolicy = &policy

	return normalized, nil
}

func validateRetryPolicy(options *RetryPolicy) error {
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

	if options.Multiplier < 1 {
		return fmt.Errorf("invalid transaction retry backoff multiplier: %v", options.Multiplier)
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

// shouldRetryTx decides whether a failed attempt runs again. Commit-stage failures
// are only retried when the database reported a definite transaction conflict
// (serialization failure, deadlock, lock timeout): those codes guarantee the
// transaction was rolled back, and PostgreSQL commonly raises 40001 at COMMIT.
// Any other commit failure is ambiguous (the commit may have succeeded) and is
// never replayed.
func shouldRetryTx(err error, stage txRetryStage, options *normalizedTxOptions, attempt int) bool {
	if options == nil || options.retryIf == nil || options.retryPolicy == nil {
		return false
	}

	if attempt >= options.retryPolicy.MaxAttempts {
		return false
	}

	if stage == txRetryStageCommit && !IsTxConflictError(err) {
		return false
	}

	return options.retryIf(err)
}

func txRetryDelay(options *RetryPolicy, attempt int) time.Duration {
	if options == nil || attempt < 1 {
		return 0
	}

	delay := options.InitialBackoff
	for i := 1; i < attempt; i++ {
		delay = time.Duration(float64(delay) * options.Multiplier)
		if options.MaxBackoff > 0 && delay >= options.MaxBackoff {
			return options.MaxBackoff
		}
	}

	if options.MaxBackoff > 0 && delay > options.MaxBackoff {
		return options.MaxBackoff
	}

	return delay
}

func waitTxRetry(ctx context.Context, options *RetryPolicy, attempt int) error {
	delay := txRetryDelay(options, attempt)
	if delay <= 0 {
		return nil
	}

	// Two transactions that deadlocked fail at the same moment, and with the same
	// backoff they met again at the next attempt: each waits somewhere between
	// half the backoff and the whole of it. The draw spreads retries and guards
	// nothing, so it needs no cryptographic source.
	delay = delay/2 + rand.N(delay/2+1) // #nosec G404 -- backoff jitter, not a secret

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (r *Runtime) executeTxAttempt[T any](
	ctx context.Context,
	options *normalizedTxOptions,
	fn func(context.Context, Executor) (T, error),
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

	state := &txState{}
	exec := boundExecutor{DBTX: tx, s: execScope{dialect: r.dialect, runtime: r, tx: true}, tx: state}

	result, err := fn(context.WithValue(ctx, txContextKey{}, exec), exec)
	if err != nil {
		var zero T
		return zero, txRetryStageBody, err
	}

	// The callback returned nil after the engine rolled the transaction back:
	// COMMIT would succeed and commit nothing. Reported as the body's failure, so
	// that WithRetry runs the transaction again as it would had the error been
	// returned.
	if aborted := state.abortedBy(); aborted != nil {
		var zero T
		return zero, txRetryStageBody, fmt.Errorf("the transaction was rolled back by the database and nothing was committed: %w", aborted)
	}

	if err := tx.Commit(); err != nil {
		var zero T
		return zero, txRetryStageCommit, fmt.Errorf("commit transaction: %w", err)
	}

	committed = true

	return result, 0, nil
}

func (r *Runtime) withTxResult[T any](
	ctx context.Context,
	fn func(context.Context, Executor) (T, error),
	options []TxOption,
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

	// Inside its own callback, WithTx joins the transaction there is: a second
	// one would run on another connection, see nothing the first wrote, and wait
	// for a connection a pool of one has not got. The callback runs under a
	// savepoint, so that its failure undoes its own writes and leaves the outer
	// transaction to go on; the options are the outer transaction's.
	if outer, ok := ctx.Value(txContextKey{}).(boundExecutor); ok && outer.s.runtime == r {
		// The rows of an Iter travel on the transaction's connection: a statement
		// on it meanwhile breaks the rows and the transaction (MySQL "busy
		// buffer", PostgreSQL "bad connection"), so the join is refused here.
		if outer.tx.rowsOpen() {
			return zero, errors.New("WithTx inside a WithTx callback while an Iter over its transaction is open: the connection carries the rows; finish or break the iteration first, or List the rows and loop over them")
		}

		return joinTx(ctx, outer, fn)
	}

	return r.trace1(ctx, TraceInfo{Op: TraceOpTx}, func(ctx context.Context) (T, error) {
		for attempt := 1; ; attempt++ {
			result, phase, err := r.executeTxAttempt(ctx, normalized, fn)
			if err == nil {
				return result, nil
			}

			if !shouldRetryTx(err, phase, normalized, attempt) {
				return zero, err
			}

			if waitErr := waitTxRetry(ctx, normalized.retryPolicy, attempt); waitErr != nil {
				return zero, errors.Join(err, waitErr)
			}
		}
	})
}

// WithTxResult is WithTx for a callback that returns a value; return a small
// struct when several values come back.
func (r *Runtime) WithTxResult[T any](
	ctx context.Context,
	fn func(context.Context, Executor) (T, error),
	options ...TxOption,
) (T, error) {
	return r.withTxResult(ctx, fn, options)
}

// txContextKey carries the executor of a WithTx callback in its context, so that
// a WithTx called inside the callback joins the transaction instead of opening
// another.
type txContextKey struct{}

// txDepthKey is how many WithTx callbacks the context is inside, which names
// the savepoint of the next one.
type txDepthKey struct{}

// joinTx runs fn on the transaction of the enclosing WithTx callback, under a
// savepoint of its own: an error rolls back to it and is returned, so that the
// outer callback decides; a nil error releases it.
func joinTx[T any](ctx context.Context, outer boundExecutor, fn func(context.Context, Executor) (T, error)) (T, error) {
	var zero T

	depth, _ := ctx.Value(txDepthKey{}).(int)
	depth++

	savepoint := fmt.Sprintf("tsq_nested_%d", depth)

	if _, err := outer.ExecContext(ctx, "SAVEPOINT "+savepoint); err != nil {
		return zero, fmt.Errorf("join the enclosing transaction: %w", err)
	}

	result, err := fn(context.WithValue(ctx, txDepthKey{}, depth), outer)
	if err != nil {
		if _, rollbackErr := outer.ExecContext(ctx, "ROLLBACK TO SAVEPOINT "+savepoint); rollbackErr != nil {
			return zero, errors.Join(err, fmt.Errorf("roll back to the savepoint of the nested transaction: %w", rollbackErr))
		}

		return zero, err
	}

	if _, err := outer.ExecContext(ctx, "RELEASE SAVEPOINT "+savepoint); err != nil {
		return zero, fmt.Errorf("release the savepoint of the nested transaction: %w", err)
	}

	return result, nil
}
