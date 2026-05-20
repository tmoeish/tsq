# AGENTS.md

Canonical instructions for coding agents and IDE assistants working in this repository.

## Project environment

- **Repository**: `github.com/tmoeish/tsq`
- **Language**: Go `1.24.x` (`go.mod` uses `go 1.24.2`; CI uses Go `1.24.3`)
- **Main deliverables**:
  - CLI generator: `./cmd/tsq`
  - Go library: repository root package
  - Runnable example app: `./examples`
- **Primary task runner**: `make`
- **Local lint binary path**: `./bin/golangci-lint`

## Core commands

- **Download modules**: `make mod-download`
- **Tidy modules**: `make mod-tidy`
- **Format**: `make fmt`
- **Lint**: `make lint`
- **Vet**: `make vet`
- **Test**: `make test`
- **Coverage**: `make test-coverage`
- **Build CLI**: `make build`
- **Regenerate examples and build example app**: `make examples`
- **Full local sweep**: `make all`

## Required workflow

- Prefer `make` targets over ad-hoc commands when an equivalent target exists.
- When changing generated code paths, table DSL, templates, parser logic, or examples:
  - run `make examples`
  - keep generated files in `examples/academy/*_tsq.go` committed
- Do not hand-edit generated files unless you are explicitly debugging generation output; change the source/template and regenerate instead.
- **Validation order**:
  1. `make fmt`
  2. `make lint`
  3. `make test`
- For cross-cutting runtime/query/concurrency work, also run: `go test -race ./...`
- For generator, template, parser, or example changes, also run:
  1. `make examples`
  2. `./bin/examples/full-suite`
- For release/build workflow changes, also run:
  1. `make build`
  2. `goreleaser check` if GoReleaser config changed

## Coding conventions

- Follow repository Go style and keep edits surgical.
- Use the Build-based query flow; avoid reintroducing removed compatibility wrappers.
- Prefer explicit, typed APIs over stringly shortcuts.
- Keep naming consistent with current repo vocabulary:
  - use `Result`, not `DTO`
  - use `GTE` / `LTE`
  - use `StartsWith` / `EndsWith`
  - use `FnRaw` for raw function expressions
- Table DSL uses explicit managed-field names: `version`, `created_at`, `updated_at`, `deleted_at`.
- Handle errors immediately after the failing operation, prefer early returns, and use `errors.Is` / `errors.As`.
- Keep comments high-signal: document exported behavior and non-obvious constraints.
- Generated examples are part of the repository contract; keep source structs, schema, generated code, and docs aligned.

## Release & Version Upgrade Procedure

Follow these steps strictly when performing a version upgrade or release:

1. **Check current version**: Identify the current version in `version.go`.
2. **Create release branch**: Create a new branch named `release/vX.Y.Z` (e.g., `git checkout -b release/v4.0.5`).
3. **Update version files**:
   - Update the version string in `version.go`.
   - Update `version_test.go` if necessary.
   - Update `CHANGELOG.md` with the new version and a summary of changes.
4. **Regenerate artifacts**:
   - Run `make examples` to ensure generated headers in example files reflect the new version.
   - Run `make build` to verify the CLI build.
5. **Verify everything**: Run `make all` (fmt, lint, vet, test, examples).
6. **Commit changes**: Use a clear commit message like `chore: release vX.Y.Z`.
7. **Create Tag**: After pushing the branch and confirming CI passes, create a tag: `git tag vX.Y.Z`.
8. **Push Tag**: `git push origin vX.Y.Z`.
9. **Update documentation**: Ensure `README.md` installation instructions or examples use the latest version if they are version-pinned.

## Repository-specific cautions

- `Where(...)` and `Search(...)` are overwrite-style setters; use `And(...)` when appending conditions.
- `FULL JOIN` can be rendered but execution remains dialect-dependent.
- Example schema lives in `examples/academy/mock.sql`; schema changes must stay consistent with example structs and regenerated code.
- Keep local-only files out of Git: coverage outputs, local assistant settings such as `.claude/`, built binaries at repo root.
