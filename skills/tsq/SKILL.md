---
name: tsq
description: Use this skill when working in a Go project that uses or adopts TSQ (github.com/tmoeish/tsq/v5) - annotating structs with //tsq: directives, running the tsq CLI (tsq gen, --check, --dry-run, tsq version), wiring tsq.Open / tsq.NewRuntime and schema policies, or writing typed queries and writes - CRUD, upsert, batch writes, bulk UPDATE / DELETE by condition, soft delete, optimistic locking, pagination, keyset paging, keyword and full-text search, subqueries, CASE, CTE, set operations, row locks, transactions with retry, tracing and dialect differences between MySQL, PostgreSQL and SQLite.
license: MIT
compatibility: Intended for GitHub Copilot, Claude Code, and Gemini CLI in Go repositories where the agent can inspect files and optionally run Go or tsq commands.
metadata:
  source-repository: github.com/tmoeish/tsq
  module: github.com/tmoeish/tsq/v5
---

# TSQ skill

TSQ is a type-safe SQL query builder and code generator for Go. You annotate structs with `//tsq:`
directives, `tsq gen` generates typed table descriptors, columns, CRUD helpers and per-dialect DDL,
and the `tsq` library builds and runs queries over MySQL, PostgreSQL or SQLite.

This skill is for **using TSQ in another Go project**. Inside the TSQ repository itself, load
`tsq-dev` (`.agents/skills/tsq-dev`) instead: that one describes the implementation, this one the
contract.

This file is a router. **Read only the reference files the task needs**; each is self-contained
and names the others it leans on. Everything an agent needs to use TSQ correctly is in these
files: there is no need to read the TSQ source or its repository docs.

## Which file to read

| the task | read |
| --- | --- |
| add TSQ to a project, first query end to end | `references/quickstart.md` |
| the mental model: tables, rows, results, parameters, stages, Build vs execution | `references/concepts.md` |
| install / pin the CLI, `tsq gen` flags and exit codes, generated `.sql` migrations, `tsq.json`, CI checks | `references/cli.md` |
| write or change `//tsq:` directives and `db` / `tsq` / `json` tags; DDL type mapping, defaults, indexes | `references/annotations.md` |
| what `tsq gen` generates (`TableXxx`, columns, `GetByX`, row methods, results, `TSQTables`), hand-written tables | `references/generated-code.md` |
| `tsq.Open` / `NewRuntime`, DSN rules, schema policies, logging, SQL logging, tracers, `WrapExecutor` | `references/runtime.md` |
| `Select ... From ... Where ...`, joins, grouping, ordering, locks, read methods, key lookups, `ListIn`, `AttachMany` | `references/queries.md` |
| predicates, `tsq.Val` / parameters, nullable columns, column functions, `CASE`, `MapInto`, `SelectValue`, compile errors | `references/expressions.md` |
| aliases and `Rebind`, subqueries, `Exists`, correlated subqueries, CTEs, `UNION` / `INTERSECT` / `EXCEPT`, results | `references/advanced-queries.md` |
| `Page`, `PageRequest`, keyset paging, keyword `Search`, full-text `Matches` | `references/paging-search.md` |
| `Insert`, `Update`, `Upsert`, `Batch*`, soft / hard delete, `Restore`, `UpdateTable` / `DeleteFrom`, managed columns, optimistic locking | `references/writes.md` |
| `WithTx` / `WithTxResult`, isolation, read-only, retry, nesting, what a rollback does | `references/transactions.md` |
| what each engine supports, capability checks, driver names, engine-specific behavior | `references/dialects.md` |
| an error TSQ returned: what it means and how to handle it | `references/errors.md` |
| upgrading a project from TSQ v4 | `references/migrating-from-v4.md` |

## Workflow in a target project

1. Inspect `go.mod` (module path, current TSQ version), the package that owns the DB structs, the
   DB bootstrap code, and the tests that cover the persistence path you change.
2. Add or change `//tsq:` directives on the structs (`annotations.md`).
3. Run `tsq gen <package>` with a CLI pinned to the `go.mod` version (`cli.md`). Never hand-edit
   `*.tsq.go`, `*.result.tsq.go`, `runtime.tsq.go`, `tsq.json` or the generated `.sql` files.
4. Wire `tsq.Open(ctx, driver, dsn, pkg.TSQTables(), opts...)` — or `tsq.NewRuntime` over an
   existing pool — in the existing bootstrap path (`runtime.md`).
5. Replace one query or write path at a time, and run the project's tests.

## Rules that are easy to get wrong

- **Stages, not strings.** `tsq.Select(cols...).From(t).Where(cond, more...).OrderBy(...).Build()`.
  `Where(...)` and `Search(...)` each appear at most once per chain — a compile-time rule. Pass all
  conditions to the one `Where` (they are ANDed); `tsq.Or(...)` / `tsq.And(...)` group them.
- **Values are operands.** A fixed value is `tsq.Val(v)` (lists `tsq.Vals(vs...)`); an untyped
  numeric constant needs the column's type, `tsq.Val(int64(1))`. A value known at execution is a
  parameter: `col.EQ(col.Param())` in the query, `col.Bind(v)` when running it, or
  `tsq.NewParam[T]("name")` when one column needs two values. Arguments match by parameter, never
  by position.
- **Executors.** Pass the `*tsq.Runtime`, the executor `WithTx` hands its callback, or
  `tsq.WrapExecutor(db, dialect.Postgres)`. A bare `*sql.DB` does not compile.
- **Expressions are not columns.** `tsq.Upper(col)`, `tsq.Sum(col)` and friends are package
  functions returning `tsq.Expression[T]`; `Select` takes columns, so project an expression with
  `tsq.MapInto(expr, fieldPtr)` or read it alone with `tsq.SelectValue` / `tsq.SelectNullValue`.
- **NULL is in the types.** Nullable fields are `tsq.NullColumn[R, T]`, compared by value type and
  written NULL with `SetNull`. A value that can be NULL (nullable column, outer-joined table,
  aggregate without `GROUP BY`) cannot be read into a non-nullable field: the query refuses before
  it runs.
- **Delete is soft, removal says Hard.** Only a table declaring `deleted_at` has `Delete`,
  `Restore`, `WithDeleted()` and `tsq.DeleteFrom`; everything that removes rows is `HardDelete*` /
  `tsq.HardDeleteFrom`. Deleted rows are out of every query's scope; never filter `deleted_at` by
  hand.
- **Partial reads save partially.** A row read with a narrow `Select` must be saved with
  `row.Update(ctx, db, TableXxx.Col, ...)`; a plain `Update` of it is refused.
- **Bulk changes are statements.** `tsq.UpdateTable(TableXxx).Set(...).Where(...)`, never "list the
  rows and `Update` each". They skip the version check but still increment `version`.
- **Empty lists never drop a filter.** `In` over an empty list matches nothing, `NotIn` everything.
- **Two validation points.** `Build()` checks structure; dialect support (CTE, `FULL JOIN`, row
  locks, `INTERSECT ALL`) is checked when the query runs, so one `*Query` serves every engine.
- **Batch writes are not transactions.** Wrap `Batch*` in `runtime.WithTx(ctx, fn, opts...)` when it
  must be all or nothing; options follow the callback.
- **Codec types need `type:`.** A field whose Go type implements `driver.Valuer` / `sql.Scanner`
  must declare its column type, `db:"meta,type:JSON"`; TSQ will not guess.
- **One TSQ version per repository.** The CLI that runs `tsq gen` must match the library in
  `go.mod`; generated headers and `tsq.json` record the version.

## Do not

- hand-maintain column metadata, CRUD helpers or generated files
- call `Where` / `Search` twice, or turn a compile error into a runtime branch
- pass values positionally to `List` / `Get` / `Exec`
- assume a query that builds runs on every engine
- hide transaction boundaries inside helpers, or expect `Batch*` to open one
- bundle install or generate scripts: `go install` and `tsq gen <package>` are one explicit
  command each, and the package path is project-specific
