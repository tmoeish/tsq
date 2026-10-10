# TSQ concepts

The mental model behind every other file: what TSQ generates, what a query is, and where each kind
of mistake is caught. Read this before a first change in a TSQ project; the details live in the
files named in each section.

## Main flow

```txt
Go struct + //tsq: directives
            |
            v
        tsq gen                    (cli.md, annotations.md)
            |
            v
*.tsq.go / *.result.tsq.go / runtime.tsq.go / {mysql,postgres,sqlite}.sql / tsq.json
            |
            v
TableXxx: one column field per struct field (TableXxx.Name) + CRUD on the table   (generated-code.md)
            |
            v
tsq.Select(...).From(TableXxx).Where(TableXxx.Name.EQ(TableXxx.Name.Param())).Build()   (queries.md)
            |
            v
*tsq.Query[Row]   (rendered per dialect the first time it runs there, then cached)
            |
            v
query.List / Get / Find / Exists / Count / Iter / Page (ctx, executor, TableXxx.Name.Bind(v))
```

## Tables, rows and results

- a **row type** is your struct. TSQ adds only the generated `Insert` / `Update` / `HardDelete`
  (and `Delete` / `Restore` / `IsDeleted` on a soft-delete table), which call the table
- a **table** is a descriptor: `*tsq.TableOf[Row, Key]`, or `*tsq.SoftDeleteTableOf[Row, Key]`
  when the struct declares `deleted_at`. The generated `TableXxx` embeds it and adds one typed
  column field per struct field. Queries select from it; writes go through it
- a **result** (`//tsq:result`) is a struct a query scans into through mapped columns; it is
  never written. Any struct can be a result ad hoc with `tsq.MapInto`
- a **column** `tsq.Column[Row, T]` knows the row it scans into and the Go type it holds: mixing
  rows in one `Select` or comparing with a value of another type does not compile. A nullable
  field is a `tsq.NullColumn[Row, T]`

## Operands, parameters and arguments

The right-hand side of every comparison, `Set` and function argument is an **operand** of the
column's type: another column or expression, `tsq.Val(v)`, a parameter, or a typed subquery.
Lists (`In` / `NotIn`) take `tsq.Vals(vs...)`, a list parameter or a subquery.

A value known only when the query runs is a **parameter**: `col.Param()` in the query and
`col.Bind(v)` among the arguments of `List` / `Get` / `Exec`, or `tsq.NewParam[T]("name")` /
`tsq.NewListParam[T]("name")` when the column's own parameter is taken. Arguments (`tsq.Arg`) are
matched by parameter, not position, and a missing, unused or duplicated one is an execution error.
`tsq.Keyword(term)` is the argument of keyword search.

## Stages: the type system is the validator

Each builder call returns a different interface that offers only what may come next:
`Select → From → joins → Where / Search → GroupBy → Having → OrderBy → Limit → Offset → lock`.
`Where` and `Search` exist once each per chain, `Offset` only after `Limit`, `Having` only after
`GroupBy`, and a lock only where rows are table rows. Getting the order wrong is a compile error,
not a runtime check. Every complete stage can run (`List`, `Get`, ...) or `Build()` into an
immutable, reusable `*tsq.Query[O]`, and is itself a subquery, CTE body and set-operation operand.

## Two validation points

- `Build()` (or the first execution of an unbuilt stage) validates **structure**: every column
  belongs to a table in the query, aggregates and `GROUP BY` agree, NULLs can be read, set-operation
  operands line up
- execution validates the **dialect**: a CTE, `FULL JOIN`, row lock or `INTERSECT ALL` the engine
  lacks is a `*dialect.UnsupportedCapabilityError` when the statement runs

So one built query can be reused across engines, and "it builds" never means "it runs everywhere"
(`dialects.md`).

## Runtime and executors

`*tsq.Runtime` holds the pool, the dialect, the registered tables, the logger and tracers, and the
schema policies applied at startup (`runtime.md`). It is a `tsq.Executor`, as is the executor
`WithTx` passes to its callback (`transactions.md`) and `tsq.WrapExecutor(handle, dialect.X)` around
a pool or transaction opened elsewhere. `Executor` is sealed: TSQ must know the dialect to render.

## Semantics that never change silently

- an empty list never removes a filter: `In` over an empty list matches nothing, `NotIn` everything
- deleted rows of a soft-delete table are out of scope of every query and statement naming it
- a version-guarded write that loses a race is an `*tsq.OptimisticLockError`, a business outcome to
  handle (`writes.md`)
- `Batch*` writes and multi-statement reads never open a write transaction for you
- time values are written and read in UTC, truncated to microseconds, on every engine

## Adopting TSQ in an existing project

1. migrate one table or one query path at a time
2. reuse the existing DB bootstrap path and its pool (`tsq.NewRuntime`)
3. keep the project's package boundaries; generated files live beside the structs
4. use generated columns instead of handwritten column-name strings
5. introduce `//tsq:result` only where a result shape is stable and reused; use `tsq.MapInto` for
   one-off projections
