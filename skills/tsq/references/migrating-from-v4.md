# Migrating a project from TSQ v4 to v5

v5 is a redesign with no compatibility layer: no aliases, no migration command, no reader for the
old annotation syntax. Moving a project means changing the import path, rewriting the annotations,
regenerating, and fixing what the compiler then reports. Most v4 mistakes are now compile errors,
so the compiler leads the way; this file maps what it reports to the v5 form.

## Steps

1. `go get github.com/tmoeish/tsq/v5@latest`, then replace every `github.com/tmoeish/tsq/v4` import
   with `github.com/tmoeish/tsq/v5` (and `.../v5/dialect`). Install the v5 CLI (`cli.md`).
2. Rewrite the annotations as `//tsq:` directive lines above each struct (`annotations.md`): the
   v4 `@TABLE` / `@RESULT` comment blocks are not read at all.
3. Delete the old generated `*.tsq.go` files if `tsq gen` refuses to overwrite them, and run
   `tsq gen <package>`.
4. Fix the build with the table below, package by package.
5. Replace the runtime bootstrap with `tsq.Open` / `tsq.NewRuntime` (`runtime.md`).
6. Review the first migration section `tsq gen` wrote: derived index names changed (below).

## v4 → v5

| v4 | v5 |
| --- | --- |
| `@TABLE` / `@RESULT` annotation comments, `tsq fmt` | `//tsq:table` / `//tsq:result` / `//tsq:managed` / `//tsq:unique` / `//tsq:index` / `//tsq:search` / `//tsq:fulltext` lines; no formatting step |
| package-level columns `Course_Title`, `Course__Cols` | fields of the table value: `TableCourse.Title`, `TableCourse.Columns()` |
| row methods for metadata, `tsq.Owner` / `tsq.Result` interfaces, `TableRegistration` | the `TableXxx` descriptor (`*tsq.TableOf[R, K]`); `TSQTables()` returns `[]tsq.Table` |
| `tsq.NewRuntime(driver, dsn, tables, *RuntimeOptions)`, `NewRuntimeContext(ctx, driver, dsn, ...)` | `tsq.Open(ctx, driver, dsn, tables, opts...)`. **v5's `tsq.NewRuntime(ctx, db, dialect.X, tables, opts...)` is a different function**: it takes a pool you opened |
| `RuntimeOptions{...}` struct | functional options: `tsq.WithSchemaPolicy(...)`, `tsq.WithLogger(...)`, ... |
| `runtime.WithTx(ctx, &tsq.TxOptions{SQL, Retry, RetryConfig}, fn)`, `TxRetryConfig`, `SQLExecutor` | `runtime.WithTx(ctx, fn, tsq.WithIsolation(...), tsq.WithReadOnly(), tsq.WithRetry(pred), tsq.WithRetryPolicy(tsq.RetryPolicy{...}))`; the callback takes a `tsq.Executor` |
| passing `*sql.DB` as the executor | `*tsq.Runtime`, the `WithTx` executor, or `tsq.WrapExecutor(db, dialect.X)` |
| positional `args ...any`, `EQVar()`, `SetVar`, `BindSlice` | parameters: `col.EQ(col.Param())` + `col.Bind(v)`; `col.In(col.ListParam())` + `col.BindList(vs...)`; `tsq.NewParam[T]` |
| `EQVal(v)`, `InVal`, `BetweenVal`, `SetVal`, `WhenVal`, `CoalesceVal`, ... | `EQ(tsq.Val(v))`, `In(tsq.Vals(vs...))`, `Set(col, tsq.Val(v))`, ... |
| `NIn`, `col.Like(...)`, `col.StartsWith(...)` | `NotIn`, `tsq.Like(col, p)` / `tsq.NotLike`, `tsq.StartsWith(col, tsq.Val(s))`, ... |
| column methods `col.Upper()`, `col.Sum()`, `col.Distinct()` | package functions `tsq.Upper(col)`, `tsq.Sum(col)`; `tsq.CountDistinct(col)`, `tsq.SelectDistinct(...)` |
| `col.ExistsSub(...)`, `tsq.BuildSubquery`, `Query.AsSubquery` | `tsq.Exists(stage)` / `tsq.NotExists`; any stage is a subquery, `tsq.SelectValue(col).From(...)` for a value list |
| `tsq.From[O](t).Select(...)` | `tsq.Select(...).From(t)` |
| `Join(t, on)` | `InnerJoin(t, on, more...)`; `CrossJoin(t)` without a condition |
| several `Where(...)` calls | one `Where(cond, more...)`; `tsq.Or` / `tsq.And` to group |
| `Select(tsq.Date(col))`, `Query.Scalar` / `ScalarNull` | `tsq.MapInto(expr, fieldPtr)`, `tsq.SelectValue` / `tsq.SelectNullValue` |
| `Column.As(...)`, `tsq.AliasTable`, `col.WithTable(t)` | `TableXxx.As("alias")`; `col.Rebind(t)`, `tsq.RebindNull(col, t)` |
| `Table.Name()` | `TableName()` |
| `Query.String()`, `ListSQL`, `CountSQL`, `Mutation.SQL()` | `query.SQL(dialect.X, args...)`, `mutation.SQL(dialect.X, args...)` |
| `tsq.Insert` / `tsq.Update` / `tsq.Delete` / `tsq.Batch*` functions | methods on the table: `TableXxx.Insert(ctx, db, &row)`, `BatchInsert`, ...; generated `row.Insert(ctx, db)` |
| `Delete` on any table, `row.Active()` | `Delete` only on a table with `deleted_at` (soft); `HardDelete*` / `tsq.HardDeleteFrom` remove rows; `row.IsDeleted()` |
| `Paging.Offset()`, `SortError`, `PagedStage` | `query.Page(ctx, db, tsq.Paging{...})`; `*tsq.PageRequestError`; `OrderedStage` |
| `TableIndex{Fields}`, `TableSpec.Schema` | `tsq.IndexSpec{Columns}`, `TableSpec.ColumnSpecs` / `TableOf.ColumnSpecs()` |
| `dialect.Dialect` interface, `MySQLDialect`, ... | `dialect.Name` (`dialect.MySQL`, `dialect.Postgres`, `dialect.SQLite`) |
| `CapabilityFullOuterJoin`, `CapabilitySelectFor*` | `CapabilityFullJoin`, `CapabilityForUpdate` / `ForShare` / `NoWait` / `SkipLocked` |
| `TraceOpScalar`, `TraceOpExec` | `TraceOpGet`, `TraceOpExists`, `TraceOpUpdate` / `TraceOpDelete` / `TraceOpHardDelete` for statements |
| `tsq gen --tpl` / `--resulttpl` | removed; the templates are fixed |
| composite primary keys | not supported: a single-column key plus `//tsq:unique A,B` |

## Behavior that changed without a compile error

- **derived index names come from column names**: `//tsq:unique SKU` over column `sku` is now
  `ux_products_sku` (v4: `ux_products_s_k_u`). The next `tsq gen` writes a migration that drops
  the old index and creates the new one; add `name=` to the directive to keep an old name
- `Update` no longer writes `created_at` or `deleted_at`, and on a soft-delete table matches live
  rows only; a soft delete writes only `deleted_at`, `updated_at` and `version`
- deleted rows are out of scope for **every** query naming a soft-delete table, hand-written ones
  and joins included: remove hand-written `deleted_at` filters
- times are written and read in UTC on every engine, truncated to microseconds
- a nullable value read into a non-nullable field fails before the query runs instead of at the
  first NULL row: give such fields a nullable type or `Coalesce` them
- NULLs sort as the smallest value on every engine (PostgreSQL used to sort them last ascending)
- `Year` / `Month` / `Day` return `int64`, `Date` returns `'YYYY-MM-DD'` text, `Length` counts
  characters on MySQL too
- `Exists` reads at most one row instead of counting; `Count` returns `int64`
