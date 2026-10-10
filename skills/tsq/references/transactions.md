# Transactions

`Runtime.WithTx` / `WithTxResult`, their options (isolation, read-only, retry), nesting, what a
rollback undoes, and how each engine treats a failed statement inside a transaction. Row locks are
in `advanced-queries.md`; the error predicates used for retry in `errors.md`.

## `WithTx`


```go
err := runtime.WithTx(ctx, func(ctx context.Context, txExec tsq.Executor) error {
	...
})
```

Options follow the callback: `tsq.WithIsolation(sql.LevelSerializable)`, `tsq.WithReadOnly()`,
`tsq.WithRetry(tsq.IsRetryableTxError)` and `tsq.WithRetryPolicy(policy)`. A read-only transaction
refuses writes on every engine; on SQLite, whose drivers ignore the flag, TSQ runs it on a connection
of its own with `PRAGMA query_only` set and clears it afterwards. SQLite transactions are serializable
whatever level is asked for. The wait before a retry is
drawn between half the policy's backoff and the whole of it, so two transactions that deadlocked do
not meet again at the next attempt.

Use transaction helpers when:

- several TSQ writes must be atomic
- row-locking queries must share the same transaction
- a batch helper must be atomic as one unit

## `WithTxResult`

When the callback returns a value, use the generic method:

```go
result, err := runtime.WithTxResult(ctx, func(ctx context.Context, txExec tsq.Executor) (*Result, error) {
	return loadAndUpdate(ctx, txExec)
}, opts...)
```

Return a small result struct when several related values come back; `WithTxResult` is the only typed transaction helper.

## Nested `WithTx`

`WithTx` called inside its own callback — a helper that opens a transaction, called from another —
joins the transaction there is instead of opening a second one (which ran on another connection,
saw nothing the first wrote, and on a pool of one connection waited for it forever). The inner
callback runs under a savepoint of its own: its writes are the outer transaction's and its reads see
them; its error rolls back to the savepoint and is returned, so the outer callback decides whether
to go on; a nil return releases the savepoint. The options are the outer transaction's: an inner
`WithRetry`, `WithIsolation` or `WithReadOnly` does nothing, since there is no transaction of its own
to retry or set, and no engine makes a savepoint read-only: inside a read-write transaction a
read-only callback can write. Open a read-only transaction at the outermost call to have the engine
refuse writes. The context handed to the callback is what carries the transaction: pass it on; kept past the
callback, it carries a transaction that is over, and a `WithTx` given it opens its own. `Page`, which
runs its count and its rows in a read-only transaction when given the runtime, joins the enclosing
transaction the same way from inside a callback, and sees what the callback wrote. While an `Iter`
over the transaction is open, a nested `WithTx` is refused instead: the connection carries the
rows, and a statement on it meanwhile would break both (MySQL and PostgreSQL). Finish or break the
iteration first, or `List` the rows and loop over them.

## What a rollback does and does not undo

A rollback undoes the database, not memory: a row an `Insert` or `Update` inside the callback stamped
(key, `created_at`, `updated_at`, `version`) keeps those values after a rollback, and a retry runs
the callback again with them. Load or build the rows a transaction writes inside its callback.

## A failed statement inside the callback

A statement that fails inside the callback does not end the transaction on MySQL and SQLite (a
duplicate key can be caught and the callback can go on; `BatchInsert` with `WithSkipDuplicates`
does that behind a savepoint), and ends it on PostgreSQL, which refuses every statement until the
rollback. The exception is an error after which the engine rolled the **whole** transaction back on
its own — a deadlock, or a full lock table, on MySQL (`tsq.IsTxConflictError`): the session is then
out of the transaction, so a callback that caught the error and went on would run its later
statements on their own and `COMMIT` nothing. The executor refuses every statement after such an
error, `WithTx` refuses to commit and returns the error that ended the transaction, and
`WithRetry` runs the callback again as it would had the error been returned.

## Rules and retry

- transaction boundaries stay explicit
- the `Batch*` writes do not silently create outer transactions
- `tsq.WithRetry(predicate)` reruns the whole callback while the predicate accepts the error; `tsq.WithRetryPolicy(p)` (attempts and backoff) defaults to `tsq.DefaultRetryPolicy()` and is rejected without `WithRetry`. `tsq.IsRetryableTxError` covers every condition TSQ knows how to retry; the narrower `tsq.IsOptimisticLockError`, `tsq.IsRetryableNetworkError` and `tsq.IsTxConflictError` are there when a caller wants one class and not the others
- after a failed `COMMIT` only `tsq.IsTxConflictError` conditions are retried, whatever the predicate says: those codes guarantee the transaction was rolled back, while a network failure at commit time leaves it unknown whether the commit landed

```go
err := runtime.WithTx(ctx, func(ctx context.Context, tx tsq.Executor) error {
	u, err := database.TableUser.Get(ctx, tx, id)
	if err != nil {
		return err
	}
	u.Balance -= amount
	return u.Update(ctx, tx) // OptimisticLockError when someone else won: the whole callback reruns
}, tsq.WithRetry(tsq.IsRetryableTxError), tsq.WithRetryPolicy(tsq.RetryPolicy{
	MaxAttempts:    5,                      // attempts in total, the first one included
	InitialBackoff: 10 * time.Millisecond,  // wait after the first retryable failure
	MaxBackoff:     200 * time.Millisecond, // cap on the wait; 0 means no cap
	Multiplier:     2,                      // growth of the wait per failure
}))
```

`tsq.DefaultRetryPolicy()` is 3 attempts, 5ms initial backoff, 25ms cap, multiplier 2. The options
are `tsq.TxOption` values: `tsq.WithIsolation(level)`, `tsq.WithReadOnly()`, `tsq.WithRetry(pred)`,
`tsq.WithRetryPolicy(p)`; a nil option is an error.
