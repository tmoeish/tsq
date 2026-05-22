---
name: tsq
description: Use this skill when working in a Go project that wants to adopt TSQ, annotate structs with @TABLE or @RESULT, run tsq fmt or tsq gen, initialize tsq.Runtime, or build typed SQL queries, CRUD flows, pagination, search, subqueries, CASE, CTE, set operations, and transactions with TSQ.
license: MIT
compatibility: Intended for GitHub Copilot, Claude Code, and Gemini CLI in Go repositories where the agent can inspect files and optionally run Go or tsq commands.
metadata:
  source-repository: github.com/tmoeish/tsq
  module: github.com/tmoeish/tsq/v4
---

# TSQ skill

Use this skill for **using TSQ in another Go project**, not for developing the TSQ repository itself.

## Activate this skill when

- the user wants to add TSQ to a Go service or library
- the task involves `@TABLE`, `@RESULT`, `tsq fmt`, or `tsq gen`
- the task involves generated `*_tsq.go` files
- the user wants typed query building instead of handwritten SQL helpers
- the task involves TSQ paging, search, CRUD helpers, transactions, aliases, subqueries, `CASE`, CTEs, set operations, or optimistic locking

## Primary goals

1. Put TSQ in the target project's normal model/query package layout.
2. Keep source-of-truth in handwritten structs and annotations.
3. Generate code with `tsq fmt` and `tsq gen`.
4. Use the current Build-based API and current runtime API.
5. Preserve dialect correctness and transaction boundaries.

## Working rules

- Prefer `@TABLE` / `@RESULT` annotations over handwritten metadata layers.
- Run `tsq fmt` before `tsq gen`.
- Treat generated `*_tsq.go` and `*_result_tsq.go` as outputs; do not hand-edit them unless the user is explicitly debugging generation output.
- Prefer the current Build-based query flow:
  `tsq.Select(...).From(...).Where(...).Build()`
- Pass `runtime` directly where a `tsq.SQLExecutor` is needed.
- Use `Runtime.WithTx(...)` when several TSQ operations must share one transaction.
- Do not assume this skill ships management scripts; install or upgrade TSQ with explicit `go install .../cmd/tsq@version` commands, and run `tsq fmt` / `tsq gen` directly against the chosen package.
- Remember that `Where(...)` and `Search(...)` are overwrite-style setters.
- Remember that `InVar()` with an empty or nil slice means explicit no-match.
- Remember that `Build()` validates query structure, while execution validates dialect capabilities.

## What to inspect in the target project first

- `go.mod` for module path and existing TSQ version
- packages that own DB structs, persistence models, and result models
- DB bootstrap code where dialect and runtime should be initialized
- handwritten SQL helpers that TSQ should replace
- tests that cover the persistence path being changed

## Recommended operating sequence

1. Choose the target package for table structs and result structs.
2. Add or update `@TABLE` / `@RESULT`.
3. Run `tsq fmt` and `tsq gen`.
4. Wire `tsq.Init(...)` or `Runtime.Init(...)` in the existing DB bootstrap path.
5. Replace one query or CRUD path at a time.
6. Keep the change aligned with the target project's existing tests and transaction model.

## Do not do these things

- do not hand-maintain generated column metadata or CRUD helpers
- do not call `Where(...)` twice expecting conditions to append
- do not assume every built query runs on every dialect
- do not treat `InVar(nil)` as “ignore this filter”
- do not use legacy `kw`; use `search=[...]`
- do not move transaction boundaries into hidden helper behavior

## Reference map

- `references/QUICKSTART.md` — shortest end-to-end setup in a fresh Go project
- `references/CONCEPTS.md` — mental model for annotations, generated files, owners, runtime, and execution
- `references/REFERENCE.md` — TSQ DSL, features, query patterns, runtime patterns, and important edge cases
