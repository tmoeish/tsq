# TSQ Concepts

This file is the minimum mental model for understanding what TSQ generates and how queries execute.

## Main flow

```txt
Go struct + @TABLE / @RESULT
            |
            v
        tsq fmt
            |
            v
        tsq gen
            |
            v
generated *_tsq.go / *_result_tsq.go
            |
            v
generated columns + CRUD helpers + paging/search helpers
            |
            v
tsq.Select(...).From(...).Where(...).Build()
            |
            v
       *tsq.Query[Owner]
            |
            v
tsq.List/Get/Page/Count + tsq.SQLExecutor
```

## `@TABLE`

`@TABLE` marks a Go struct as a physical table model for code generation.

It drives:

- generated table and column metadata
- CRUD helpers
- paging and search helpers
- table registration metadata

## `@RESULT`

`@RESULT` marks a Go struct as a query result model rather than a physical table.

Use it for:

- join results
- API result shapes that do not match one physical table
- reporting and aggregate results

## Generated files

Typical generated outputs:

- `*_tsq.go` for tables
- `*_result_tsq.go` for result models
- dialect DDL files and schema snapshots in projects that use generation output for schema artifacts

Generated files should follow the handwritten struct definitions, not the other way around.

## Owner model

TSQ separates several concepts:

- `Owner`: anything that can receive scanned results
- `Table`: a physical table owner
- `Result`: a projection owner

All `Table` and `Result` values are `Owner`, but only `Table` is a mutation target.

## Runtime and execution

`Runtime` is the normal TSQ-managed executor.

- it holds DB, dialect, registry, and tracer state
- it implements `SQLExecutor` directly
- it can be passed directly to query and CRUD helpers

## Query lifecycle

1. build a query with the fluent API
2. call `Build()` to validate structure and produce a reusable query object
3. execute that query through a `SQLExecutor`

Important split:

- `Build()` validates structure
- execution validates dialect capability

## Two boundaries to remember

### `Where(...)` / `Search(...)`

They are setters, not appenders.

### `InVar()`

Empty or nil slices mean explicit no-match, not “remove the filter”.

## Related files

- `QUICKSTART.md`
- `REFERENCE.md`
