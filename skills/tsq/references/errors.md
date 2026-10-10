# Errors

What each error TSQ returns means and what to do about it: the typed errors, the predicates, the
errors that wrap `sql.ErrNoRows`, the usual build and execution errors, and the compile errors
that name their own fix. Match typed errors with `errors.AsType`, as in
`errors.AsType[*tsq.OptimisticLockError](err)` (or `errors.As`); every TSQ error wraps its cause, so `errors.Is` works through it.

## Typed errors

| error | returned by | means | do |
| --- | --- | --- | --- |
| `*tsq.OptimisticLockError` `{Table, Expected, Actual, Keys}` | `Update`, soft `Delete`, `Restore` and their `Batch*` forms on a table with `version` | another writer changed (or deleted) the row since it was loaded; `Keys` names the stale rows of a batch | a business outcome: reload and retry, or report a conflict. `tsq.WithRetry(tsq.IsOptimisticLockError)` reruns a transaction (`transactions.md`) |
| `*tsq.RowStateError` `{Table, Op, Need, Expected, Actual, Keys}` | soft `Delete` of a deleted row, `Restore` of a live one, `HardDelete*` of a row that is gone, `BatchDeleteByPK` of a key with no live row, `Update` of a gone row on a table without `version` | the row was not in the state the write needs: `Need` is `tsq.RowExisting`, `tsq.RowLive` or `tsq.RowDeleted`; `Op` the `tsq.TraceOp` of the write | not a race: retrying cannot help. Treat as "already done" or "not found" as the caller decides; `tsq.DeleteFrom` / `HardDeleteFrom` delete whatever matches without this check |
| `*tsq.PageRequestError` `{Field, Reason}` | `PageRequest.Paging` / `Keyset` | the client's page request is invalid (negative page or size, page past `tsq.MaxPageNumber`, unknown or ambiguous sort field, bad direction, NUL in the keyword, bad cursor) | answer 400 with `Error()` |
| `*dialect.UnsupportedCapabilityError` `{Capability, Dialect}` | executing a query that needs a capability the engine lacks | the query built but this engine cannot run it (`dialects.md`) | gate with `dialect.Supports` and build another variant |
| `*tsq.MissingTableError` `{Table}` | `Open` / `NewRuntime` under `SchemaPolicyValidate` | a declared table does not exist | run the migration, or start with `CreateMissing` / `Reconcile` |
| `*tsq.MissingIndexError` `{Table, IndexSpec}` | the same | a declared index does not exist | the same |
| `*tsq.SchemaMismatchError` `{Table, Changes}` | `Validate`, or `CreateMissing` over a column that differs | the live columns differ from the declaration; `Changes` says how, one entry per column | write the migration (`tsq gen` writes one into the `.sql` files), or `Reconcile` in development |
| `*tsq.DuplicateRowsError` `{Table, Index, Columns, Values, Rows}` | `CreateMissing` / `Reconcile` about to create a unique index | rows already share values the new unique index would forbid; nothing was changed | remove the duplicates, or drop the index from the declaration |

Every typed error's `Error()` names rows only by primary key, never by their other values, so
column contents do not reach logs.

## Predicates

| predicate | true for |
| --- | --- |
| `tsq.IsOptimisticLockError(err)` | an `*OptimisticLockError` anywhere in the chain |
| `tsq.IsTxConflictError(err)` | a deadlock, a serialization failure, a lock wait timeout or `NOWAIT` failure, SQLite's busy database: a conflict rerunning the transaction can resolve |
| `tsq.IsRetryableNetworkError(err)` | a dropped connection (`driver.ErrBadConn`, `io.ErrUnexpectedEOF`, connection resets); a bare `io.EOF` is not |
| `tsq.IsRetryableTxError(err)` | any of the three above: the usual `WithRetry` predicate |
| `tsq.IsDuplicateKeyError(err)` | the database refusing a row because a primary key or unique index already holds its value, on every engine and driver |

```go
if err := u.Insert(ctx, runtime); tsq.IsDuplicateKeyError(err) {
	return ErrEmailTaken
}
```

## Not found

`Get`, `GetBy`, `GetByX`, `Fetch`, `FetchBy` and `query.Get` return an error wrapping
`sql.ErrNoRows` when a row (or, for `Fetch`, any of the keys) is missing: test it with
`errors.Is(err, sql.ErrNoRows)`. The `Find` forms return `nil, nil` instead.

## Errors at `Build()` or the first execution

Structure errors come back from `Build()` (or from the read or `Exec` of an unbuilt stage). The
common ones and their fixes:

| message mentions | fix |
| --- | --- |
| `table X is referenced but is not in this query's FROM/JOIN` | join the table, or `Correlate(X)` in a subquery of a query over X (`advanced-queries.md`) |
| a value that can be NULL read into a field that cannot hold it | inner join, `tsq.Coalesce`, `tsq.MapIntoNull` or `tsq.SelectNullValue` (`expressions.md` § Nullable columns) |
| a column neither grouped nor aggregated | add it to `GroupBy`, or aggregate it |
| combining builder `OrderBy` / `Limit` with `Page` | drop the builder's, pass `tsq.Paging` |
| `list in: ... must be used once` / too many bound values | use `ListIn` with one `col.In(param)` (`queries.md`) |
| a missing, unused or duplicated argument | bind every parameter the query uses, once |
| partial row saved whole | `row.Update(ctx, db, cols...)` with the columns it was read with |
| a definition error from every method of a table (`TableXxx.Err()`) | the generated files are stale or the hand-written `TableSpec` is wrong: run `tsq gen` |

## Compile errors that name their fix

TSQ's interfaces carry marker methods chosen so that the compiler's message says what to write:
`missing method needsTsqVal` (wrap a literal in `tsq.Val`), `needsTsqVals` (wrap a slice in
`tsq.Vals`), `needsRuntimeOrWrapExecutor` (pass the runtime or a wrapped executor, not a
`*sql.DB`), `needsDeletedAtOrHardDeleteFrom` (`DeleteFrom` on a table without `deleted_at`: use
`HardDeleteFrom`), `searchable` (wrap the column in `tsq.Searchable`), `wrong type for method
valueOfType` (give the value the column's type). The full table is in `expressions.md` § Values
fixed in the code.

## Generator errors

`tsq gen` exits 1 with the struct's `file:line:column` and the reason: an unknown directive or
option, a field type database/sql cannot carry, a codec type without `type:`, a name clash with a
generated method, a name too long for an engine. It exits 2 under `--check` when files are out of
date. Both are in `cli.md` and `annotations.md`.
