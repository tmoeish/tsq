# AGENT.md

Canonical instructions for coding agents and IDE assistants working in this repository.

## Project environment

- Repository: `github.com/tmoeish/tsq`
- Language: Go `1.24.x` (`go.mod` uses `go 1.24.2`; CI uses Go `1.24.3`)
- Main deliverables:
  - CLI generator: `./cmd/tsq`
  - Go library: repository root package
  - Runnable example app: `./examples`
- Primary task runner: `make`
- Local lint binary path: `./bin/golangci-lint`

## Core commands

- Download modules: `make mod-download`
- Tidy modules: `make mod-tidy`
- Format: `make fmt`
- Lint: `make lint`
- Vet: `make vet`
- Test: `make test`
- Coverage: `make test-coverage`
- Build CLI: `make build`
- Regenerate examples and build example app: `make examples`
- Full local sweep: `make all`

## Required workflow

- Prefer `make` targets over ad-hoc commands when an equivalent target exists.
- When changing generated code paths, table DSL, templates, parser logic, or examples:
  - run `make examples`
  - keep generated files in `examples/database/*_tsq.go` committed
- Do not hand-edit generated files unless you are explicitly debugging generation output; change the source/template and regenerate instead.
- Keep versioned release changes coherent:
  - update `version.go`
  - update `version_test.go`
  - update `CHANGELOG.md`
  - regenerate examples when the generated header version changes

## Validation order

- Default code-change validation:
  1. `make fmt`
  2. `make lint`
  3. `make test`
- For cross-cutting runtime/query/concurrency work, also run: `go test -race ./...`
- For generator, template, parser, or example changes, also run:
  1. `make examples`
  2. `./bin/examples`
- For release/build workflow changes, also run:
  1. `make build`
  2. `goreleaser check` if GoReleaser config changed

## Coding conventions

- Follow repository Go style and keep edits surgical.
- Follow mainstream Go guidance from Effective Go, Go Code Review Comments, the Google Go style guide, and Rob Pike's Go proverbs when they do not conflict with repository-specific rules.
- Use the Build-based query flow; avoid reintroducing removed compatibility wrappers.
- Prefer explicit, typed APIs over stringly shortcuts.
- Prefer clarity over cleverness: keep functions focused, keep control flow flat with early returns, and avoid over-abstracting small pieces of logic.
- Keep names short, specific, and context-aware; avoid stutter and vague abbreviations.
- Keep public naming consistent with current repo vocabulary:
  - use `Result`, not `DTO`
  - use `GTE` / `LTE`
  - use `StartsWith` / `EndsWith`
  - use `FnRaw` for raw function expressions
- Table DSL uses explicit managed-field names:
  - `version`
  - `created_at`
  - `updated_at`
  - `deleted_at`
- Handle errors immediately after the failing operation, prefer early returns, and use `errors.Is` / `errors.As` when behavior depends on wrapped error types.
- Prefer small consumer-defined interfaces; do not introduce broad interfaces where a concrete type or a tiny interface is clearer.
- Design zero values to be useful when adding new types, and prefer slices over arrays in public APIs.
- Keep comments high-signal: document exported behavior and non-obvious constraints, not line-by-line mechanics.
- Prefer table-driven tests and focused assertions over noisy ad-hoc test code.
- Generated examples are part of the repository contract; keep source structs, schema, generated code, and docs aligned.

## Repository-specific cautions

- `Where(...)` and `KwSearch(...)` are overwrite-style setters; use `And(...)` when appending conditions.
- `FULL JOIN` can be rendered but execution remains dialect-dependent.
- Example schema lives in `examples/database/mock.sql`; schema changes must stay consistent with example structs and regenerated code.
- Keep local-only files out of Git:
  - coverage outputs
  - local assistant settings such as `.claude/`
  - built binaries at repo root

## Release notes

- CI runs tests, race tests, lint, coverage, example regeneration drift checks, Docker build, and GoReleaser validation.
- Releases are tag-driven via GitHub Actions.
- If you create a release commit, create and push the tag after the repository is clean and validated.
