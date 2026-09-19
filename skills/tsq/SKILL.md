---
name: tsq
description: Use this skill when working in a Go project that wants to adopt TSQ, annotate structs with //tsq: directives, run tsq gen, initialize tsq.Runtime, or build typed SQL queries, CRUD flows, bulk UPDATE / DELETE by condition, pagination, search, subqueries, CASE, CTE, set operations, and transactions with TSQ.
license: MIT
compatibility: Intended for GitHub Copilot, Claude Code, and Gemini CLI in Go repositories where the agent can inspect files and optionally run Go or tsq commands.
metadata:
  source-repository: github.com/tmoeish/tsq
  module: github.com/tmoeish/tsq/v5
---

# TSQ skill

Use this skill for **using TSQ in another Go project**, not for developing the TSQ repository itself.

If you are working inside the TSQ repository itself, load the `tsq-dev` skill there
(`.agents/skills/tsq-dev`) instead; it carries the architecture, code map and release
process for the generator and library. This skill describes the contract, that one
describes the implementation.

## Activate this skill when

- the user wants to add TSQ to a Go service or library
- the task involves `//tsq:` directives or `tsq gen`
- the task involves generated `*.tsq.go` files
- the user wants typed query building instead of handwritten SQL helpers
- the task involves TSQ paging, search, CRUD helpers, bulk `UPDATE` / `DELETE` by condition, transactions, aliases, subqueries, `CASE`, CTEs, set operations, or optimistic locking

## Primary goals

1. Put TSQ in the target project's normal model/query package layout.
2. Keep source-of-truth in handwritten structs and annotations.
3. Generate code with `tsq gen`.
4. Use the current Build-based API and current runtime API.
5. Preserve dialect correctness and transaction boundaries.

## Working rules

- Prefer `//tsq:table` / `//tsq:result` directives over handwritten metadata layers.
- There is no formatting step: `//tsq:` directives survive gofmt.
- Treat generated `*.tsq.go` and `*.result.tsq.go` as outputs; do not hand-edit them unless the user is explicitly debugging generation output.
- Prefer the current Build-based query flow:
  `tsq.Select(...).From(...).Where(...).Build()`
- Pass `runtime` (or the `WithTx` executor) where a `tsq.Executor` is needed; wrap a pool TSQ did not open with `tsq.WrapExecutor(db, dialect.MySQL)` (the `dialect` package names the three engines).
- Values known only at execution are parameters: `col.EQ(col.Param())` in the query and `col.Bind(v)` when running it, or `tsq.NewParam[T]("name")` when a column needs two values. Arguments are `tsq.Arg` values matched by parameter, never positional.
- Row writes go through the generated row methods (`row.Insert(ctx, db)`) or the table descriptor (`TableXxx.BatchInsert(ctx, db, rows)`). A row read with a partial `Select` is saved with the columns it holds, `row.Update(ctx, db, TableXxx.Title)`; a plain `Update` of it is refused, since it would zero the unread columns.
- Use `TableXxx.Upsert(ctx, db, &row, key...)` / `BatchUpsert` for insert-or-update by the primary key or a unique index; on MySQL it is refused while the row could hit another unique key.
- Use `Runtime.WithTx(...)` when several TSQ operations must share one transaction.
- Use `Runtime.WithTxResult(...)` when that transaction callback returns a typed value; return a small struct when several values come back.
- Functions of a column return `tsq.Expression[T]`, which `Select` does not take: project it with `tsq.MapInto`, or read it alone with `tsq.SelectValue` (`tsq.SelectNullValue` when it can be NULL), whose rows are the values. A `SelectValue` stage is itself a typed subquery: `col.In(tsq.SelectValue(other).From(t).Where(...))`, no `Build` needed.
- Use `tsq.UpdateTable(TableXxx)` / `tsq.DeleteFrom(TableXxx)` for `UPDATE ... WHERE` / `DELETE ... WHERE` over rows the caller does not hold. They skip the optimistic-lock check but still increment `version` and require exactly one `Where(...)`. `UpdateTable` also refreshes `updated_at` unless you `Set` it; `DeleteFrom` soft-deletes when the table declares `deleted_at`.
- Remember that on a table declaring `deleted_at`, `Delete` stamps a tombstone and `HardDelete` removes the row, and deleted rows are out of scope for every query and `UpdateTable` / `DeleteFrom` naming the table, joins included; never add a `deleted_at` filter by hand. `TableXxx.WithDeleted()` includes them. Without `deleted_at` the two deletes are the same operation.
- Nullable fields are `tsq.NullColumn[Xxx, T]` compared by their value type; write NULL with `SetNull`. A query refuses to read a value that can be NULL (nullable column, outer-joined table, aggregate without GROUP BY) into a non-nullable field: prefer an inner join or `tsq.Coalesce`, else `tsq.MapIntoNull` into a nullable field; use `ScalarNull` for a scalar that can be NULL.
- Column functions are package-level and type-constrained: `tsq.Upper(col)`, `tsq.Sum(col)`, `tsq.Count(col)`. Do not look for them as column methods.
- `query.Page` takes a typed `tsq.Paging`; convert an HTTP `tsq.PageRequest` with `req.Paging(sortableCols...)`, which is also where it is validated and which carries the request's keyword into `Page`. A size above the runtime cap is served capped, not rejected.
- Transaction options follow the callback: `runtime.WithTx(ctx, fn, tsq.WithRetry(tsq.IsRetryableTxError))`.
- Columns are fields of the generated table: `TableXxx.Name`, and `TableXxx.Columns()` for `Select`. An alias is `TableXxx.As("x")`, whose fields are the columns bound to it.
- Read by primary key with `TableXxx.Get(ctx, db, id)` / `Find` / `Fetch(ctx, db, ids...)`, and by a unique index with the generated `TableXxx.GetByEmail` / `FetchByEmail`. Write lookups on plain indexes with the builder.
- Do not assume this skill ships management scripts; install or upgrade TSQ with explicit `go install .../cmd/tsq@version` commands, and run `tsq gen` directly against the chosen package.
- The builder is stage-based: `Where(...)` and `Search(...)` each appear at most once per chain, enforced by the Go type system at compile time. Pass all filter conditions to the single `Where(...)` call; use `tsq.Or(...)` for OR groups. Both clauses can coexist in either order.
- Remember that `In` over an empty list parameter matches nothing and `NotIn` matches everything; the filter is never dropped.
- Predicate naming: `Op(rhs)` takes a column, `Param`, `tsq.Val(v)` or subquery (`tsq.Vals(vs...)` for `In`); an untyped numeric constant needs its column's type, `tsq.Val(int64(90))`; negations are `Not*` (`NotIn`, `NotLike`); pattern sugar is `tsq.StartsWith(col, tsq.Val(s))` or `tsq.StartsWith(col, param)` and escapes wildcards, while `Like` takes a pattern as written.
- Remember that `Build()` validates query structure, while execution validates dialect capabilities.
- Do not assume a custom `driver.Valuer` / `sql.Scanner` type implies a DDL column type; use an explicit `db:"...,type:JSON"` / `type:TEXT` / `type:JSONB"` override when the Go type is not directly mappable.

## What to inspect in the target project first

- `go.mod` for module path and existing TSQ version
- packages that own DB structs, persistence models, and result models
- DB bootstrap code where dialect and runtime should be initialized
- handwritten SQL helpers that TSQ should replace
- tests that cover the persistence path being changed

## Recommended operating sequence

1. Choose the target package for table structs and result structs.
2. Add or update the `//tsq:` directives.
3. Run `tsq gen`.
4. Wire `tsq.Open(ctx, driverName, dsn, package.TSQTables(), opts...)` (or `tsq.NewRuntime(ctx, db, dialect.Postgres, ...)` over an existing pool) in the existing DB bootstrap path.
5. Replace one query or CRUD path at a time.
6. Keep the change aligned with the target project's existing tests and transaction model.

## Do not do these things

- do not hand-maintain generated column metadata or CRUD helpers
- do not try to call `Where(...)` or `Search(...)` more than once per chain; the stage-based type system makes this a compile error — put all conditions in the single call
- do not assume every built query runs on every dialect
- do not treat an empty list parameter as “ignore this filter”
- do not pass values positionally to `List` / `Get` / `Exec`; bind them to parameters
- do not move transaction boundaries into hidden helper behavior
- do not emulate `UPDATE ... WHERE` by listing rows and calling `Update(...)` per row; use `tsq.UpdateTable(TableXxx)`

## Reference map

- `references/QUICKSTART.md` — shortest end-to-end setup in a fresh Go project
- `references/CONCEPTS.md` — mental model for annotations, generated files, tables and rows, parameters, runtime, and execution
- `references/REFERENCE.md` — TSQ DSL, features, query patterns, runtime patterns, and important edge cases
