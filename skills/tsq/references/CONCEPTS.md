# TSQ Concepts

This file is the minimum mental model for understanding what TSQ generates and how queries execute.

## Main flow

```txt
Go struct + //tsq: directives
            |
            v
        tsq gen
            |
            v
generated *.tsq.go / *.result.tsq.go
            |
            v
table descriptor TableXxx + typed columns Xxx_Field + row methods
            |
            v
tsq.Select(...).From(TableXxx).Where(Xxx_Field.EQ(Xxx_Field.Param())).Build()
            |
            v
       *tsq.Query[Row]   (rendered per dialect when it runs, cached)
            |
            v
query.List/Get/Find/Page/Count(ctx, executor, Xxx_Field.Bind(value))
```

## `//tsq:table`

`//tsq:table` marks a Go struct as a physical table model for code generation.

It drives the generated table descriptor `TableXxx` (a `*tsq.TableOf[Xxx]` holding columns,
key, managed columns, search columns, schema and indexes), the typed columns, the row methods
and the generated queries.

## `//tsq:result`

`//tsq:result` marks a Go struct as a query result model rather than a physical table.

Use it for:

- join results
- API result shapes that do not match one physical table
- reporting and aggregate results

## Generated files

Typical generated outputs:

- `*.tsq.go` for tables
- `*.result.tsq.go` for result models
- dialect DDL files and schema snapshots in projects that use generation output for schema artifacts

Generated files should follow the handwritten struct definitions, not the other way around.

When a field uses a custom Go codec type, keep two responsibilities separate:

- `driver.Valuer` / `sql.Scanner` handle runtime read/write conversion
- `db:"...,type:SQL_TYPE"` handles DDL type override when TSQ cannot infer a column type from the Go type

## Rows, tables and results

- a **row type** is your struct; TSQ adds no interface to it
- a **table** is a `*tsq.TableOf[Row]` descriptor; it is what queries select from and what row
  writes go through
- a **result** is any struct a query scans into through `MapInto` columns; only tables can be
  written

A column `Column[Row, T]` knows both: the row it scans into and the Go type it holds. Mixing columns
of two rows in one `Select`, or comparing a column with a value of another type, does not compile.

## Parameters

A value known only when a query runs is a parameter: `col.Param()` in the query, `col.Bind(v)` at
execution (or `tsq.NewParam[T]("name")` when one column needs two values). Arguments are matched by
parameter, not position, and have the parameter's type.

## Runtime and execution

`Runtime` is the normal TSQ-managed executor.

- it holds the pool, dialect, registered tables, logger and tracers
- it is a `tsq.Executor`, as is the executor `WithTx` passes to its callback and the result of
  `tsq.WrapExecutor(handle, dialect.MySQL)`; a bare `*sql.DB` is not, because TSQ must know the dialect
- it runs queries, row writes, and `UpdateTable(table)` / `DeleteFrom(table)` statements

## Query lifecycle

1. build a query with the fluent API
2. call `Build()` to validate structure and produce a reusable query object
3. execute that query through an `Executor`

Important split:

- `Build()` validates structure
- execution validates dialect capability

## Two boundaries to remember

### `Where(...)` / `Search(...)`

The builder is **stage-based**: each call returns a different concrete type that restricts what comes next. `Where(...)` and `Search(...)` each appear at most once per chain — enforced by the Go type system at compile time. Both can coexist in either order.

### Empty lists

An empty list never removes the filter: `In` over an empty list parameter matches nothing, and
`NotIn` matches everything.

## Related files

- `QUICKSTART.md`
- `REFERENCE.md`
