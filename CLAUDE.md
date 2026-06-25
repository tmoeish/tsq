# TSQ

Type-safe SQL query builder and code generator for Go. The CLI lives at `./cmd/tsq`; the library is the root package; runnable examples are at `./examples`.

- **Language**: Go 1.26.x
- **Module**: `github.com/tmoeish/tsq`
- **Task runner**: `make`
- **Local lint binary**: `./bin/golangci-lint`

## Commands

| Purpose | Command |
|---|---|
| Download modules | `make mod-download` |
| Tidy modules | `make mod-tidy` |
| Format | `make fmt` |
| Lint | `make lint` |
| Vet | `make vet` |
| Test | `make test` |
| Coverage | `make test-coverage` |
| Build CLI | `make build` |
| Regenerate examples + build example app | `make examples` |
| Full local sweep | `make all` |

Always prefer `make` targets over ad-hoc commands when an equivalent target exists.

## Required validation order

For every code change:

1. `make fmt`
2. `make lint`
3. `make test`

For cross-cutting runtime/query/concurrency work, also run `go test -race ./...`.

For generator, template, parser, or example changes:

1. `make examples`
2. `./bin/examples/full-suite`

For release/build workflow changes:

1. `make build`
2. `goreleaser check` (if GoReleaser config changed)

## Generated code rules

- Do not hand-edit `*_tsq.go`, `*_result_tsq.go`, or `examples/academy/*_tsq.go`. Change the source struct or template and regenerate instead.
- Exception: you may hand-edit generated files only when explicitly debugging generation output.
- When changing table DSL, templates, parser logic, or examples: run `make examples` and keep the generated files in `examples/academy/*_tsq.go` committed.

## Coding conventions

- Follow repo Go style; keep edits surgical.
- Use the Build-based query flow; do not reintroduce removed compatibility wrappers.
- Prefer explicit, typed APIs over string shortcuts.
- Naming vocabulary:
  - `Result`, not `DTO`
  - `GTE` / `LTE`
  - `StartsWith` / `EndsWith`
  - `Expr` / `Exprf` / `Pred` for custom column expressions and predicates
- Table DSL managed-field names: `version`, `created_at`, `updated_at`, `deleted_at`.
- Handle errors immediately after the failing call; prefer early returns; use `errors.Is` / `errors.As`.
- Comments: only document exported behavior and non-obvious constraints. No inline narration of what the code does.
- Generated examples are part of the repository contract; keep source structs, schema, generated code, and docs aligned.

## TSQ-specific gotchas

- The query builder is **stage-based**: each method call returns a different concrete type that restricts what can be called next. `Where(...)` and `Search(...)` each appear **at most once** per chain — the Go type system enforces this at compile time. Pass all filter conditions to the single `Where(...)` call (multiple args are ANDed); use `tsq.Or(...)` for OR groups. Both can coexist in either order: `Where(...).Search(...)` or `Search(...).Where(...)`.
- `InVar()` with empty/nil slice = explicit no-match (renders as `IN (NULL)`). `NInVar()` with empty/nil slice = explicit match-all. Neither silently removes the filter.
- `Build()` validates query structure only; dialect capability (CTE, `FULL JOIN`, row locks) is checked at execution time.
- `FULL JOIN` can be rendered but execution remains dialect-dependent.
- Custom codec fields (`driver.Valuer` / `sql.Scanner`) still need an explicit `db:"...,type:SQL_TYPE"` override when TSQ cannot infer the DDL column type.
- Example schema lives in `examples/academy/mock.sql`; schema changes must stay consistent with example structs and regenerated code.
- `ChunkedInsert` / `ChunkedUpdate` / `ChunkedDelete` do **not** open a transaction automatically; wrap in `runtime.WithTx(...)` when all-or-nothing behavior is needed.
- `ForUpdate()` / `ForShare()` are only meaningful inside an explicit transaction.
- Optimistic-lock conflicts (`ErrOptimisticLockConflict`) are business errors — handle them, never ignore them.

## Query and transaction best practices

- Validate external input before building a query. Use `Validate()` on `PageRequest` and other user-facing types; use `Normalize()` only for compatibility fallback.
- Never discard the `Build()` error (`query, _ := qb.Build()` is wrong).
- Pass `context.Context` as the first argument to all DB operations; set a deadline or timeout.
- Wrap errors with `%w`; branch on error types with `errors.Is` / `errors.As`.
- Build stable queries once (outside hot paths or at init time) and reuse the `*tsq.Query[Owner]`.
- Use `runtime.WithTx(ctx, opts, func(...) error { ... })` for multi-operation transactions.
- For automatic optimistic-lock retry: `&tsq.TxOptions{Retry: tsq.IsOptimisticLockError}`.
- Use `PageRequest.Offset()` instead of computing `page * size` manually (overflow protection).
- Use `tsq.Into[Target](...)` for result projection mapping; prefer `@RESULT` for stable shapes reused across callers.

## Local-only files (gitignored — do not commit)

`.claude/`, `bin/`, `coverage.out`, `/tsq` (built binary), `dist/`
