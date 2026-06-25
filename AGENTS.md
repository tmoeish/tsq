# AGENTS.md

Canonical instructions for coding agents and IDE assistants working in this repository.

## Project environment

- **Repository**: `github.com/tmoeish/tsq`
- **Language**: Go `1.26.x` (`go.mod` uses `go 1.26.0`; CI uses Go `1.26.0`)
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
  - use `Expr` / `Exprf` / `Pred` for custom column expressions and predicates
- Table DSL uses explicit managed-field names: `version`, `created_at`, `updated_at`, `deleted_at`.
- Handle errors immediately after the failing operation, prefer early returns, and use `errors.Is` / `errors.As`.
- Keep comments high-signal: document exported behavior and non-obvious constraints.
- Generated examples are part of the repository contract; keep source structs, schema, generated code, and docs aligned.

## Release & Version Upgrade Procedure

Follow these steps for a standard release (vX.Y.Z):

1.  **Prepare**: Identify the current version in `version.go` and create a `release/vX.Y.Z` branch.
2.  **Update**:
    *   Update version strings in `version.go` and `version_test.go`.
    *   Update `CHANGELOG.md` with version highlights.
    *   Run `make examples` and `make build` to sync generated code and verify the CLI.
3.  **Verify**: Run `make all` to ensure all tests, linting, and examples pass.
4.  **Finalize**:
    *   Commit changes: `chore: release vX.Y.Z`.
    *   Merge to `main`, then tag: `git tag vX.Y.Z`.
    *   Push: `git push origin main --tags`.

### Recent Upgrades
- **2026-05-20**: Upgraded Go to 1.26.0 and refreshed all dependencies.

## Branch Management Strategies


绝大多数 Go 项目应以 main（或 master）作为唯一的长期主干。根据项目的成熟度和维护规模，我们可以分为以下几种典型场景：

### 场景一：日常特性开发与 Bug 修复（标准路径）
这是最常见的状态。所有的新功能和非紧急 Bug 修复都在主干上持续推进。
- **策略**：从 main 切出 feature/xxx 或 fix/xxx 分支。
- **合并规则**：通过 PR 合并到 main 后，立即删除该临时分支。
- **发版**：直接在 main 分支的最新 commit 上打 Tag（如 v1.4.0）。

### 场景二：维护旧的大版本（同时存在 v1 和 v2）
当你的项目进行了重大重构（如发布了 v2.0.0），main 分支已经升级为 v2 甚至更高版本。但社区中仍有大量用户在使用 v1，你需要为他们修复严重漏洞。
- **策略**：创建一个长期的维护分支，例如 v1 或 v1-maintenance。
- **操作流**：
  1. 基于最后一个 v1 的 Tag（如 v1.9.5）切出新分支：`git checkout -b v1 v1.9.5`。
  2. 在 v1 分支上修复 Bug（或者从 main cherry-pick 修复代码）。
  3. 在 v1 分支上打出新的 Tag：`v1.9.6`。

### 场景三：修复旧次版本的紧急 Bug（Hotfix，少见但偶尔需要）
假设 main 正在热火朝天地开发 v1.5.0，包含大量未稳定的代码。此时用户报告了 v1.4.2 的线上致命问题，你不能基于现在的 main 发版。
- **策略**：基于出问题的 Tag 创建临时热修复分支。
- **操作流**：
  1. 基于 v1.4.2 切出分支：`git checkout -b hotfix-xxx v1.4.2`。
  2. 修复问题并提交。
  3. 在该分支打 Tag：`v1.4.3`。
  4. 将该分支的修复代码反向合并（Backport）回 main 分支，防止未来的 v1.5.0 再次出现同样的 Bug。
  5. 删除 hotfix-xxx 分支。

### 场景四：开发颠覆性的大型实验功能
有些新特性需要多人数周的开发，频繁合入 main 可能会影响当前版本的稳定性。
- **策略**：使用特性开关（Feature Flags）优先于长期分支。
- **Go 生态推荐**：Go 项目极度排斥长期不合并的分支（容易产生合并地狱）。尽量将代码通过接口隔离，放入主干但不暴露，或者在构建时通过 `//go:build experimental` 构建标签（Build Tags）隔离代码，确保 main 永远处于随时可发布状态。

## Tag Management Strategies

Go 代理系统（Proxy）是根据 Git Tag 缓存代码的。你的 Tag 怎么打，直接决定了用户 go get 能不能拿到代码。

### 场景一：发布预发布版本（Alpha / Beta / RC）
当新版本（如 v1.5.0）开发完毕，你想让社区先测试，但不想让默认的 go get 拉取到这个版本。
- **规范**：使用 SemVer 预发布后缀，格式如 `vX.Y.Z-后缀`。
- **示例**：`v1.5.0-alpha.1`, `v1.5.0-beta`, `v1.5.0-rc.1`。
- **Go Module 行为**：当用户执行 `go get github.com/user/repo@latest` 时，Go 会自动忽略带有这类后缀的预发布版本。用户必须精确指定 `go get github.com/user/repo@v1.5.0-rc.1` 才能拉取。

### 场景二：发布破坏性更新（Major Version 升级至 v2+）
这是 Go 标签管理中最容易踩坑的场景。如果你修改了对外暴露的函数签名，导致旧代码无法编译，你必须发布 v2.0.0。
- **强制规则（Semantic Import Versioning）**：Tag 升级到 v2，go.mod 里的模块名必须跟着变。
- **操作流**：
  1. 修改项目根目录的 go.mod，将 `module github.com/user/repo` 改为 `module github.com/user/repo/v2`。
  2. 确保项目内部的所有互相引用（import 路径）都加上 `/v2`。
  3. 提交这些修改到 main。
  4. 打上 `v2.0.0` 的 Tag 并推送。
- **注意**：如果没有修改 go.mod 就直接打 v2.0.0 标签，Go Proxy 将视其为非法版本，用户会遇到 invalid version 错误。

### 场景三：发布了严重 Bug 版本（版本撤回）
刚发布了 v1.2.3 标签并推送到 GitHub，5 分钟后发现包含一个致命的安全漏洞，或者无法编译。
- **错误做法**：在本地 `git tag -d v1.2.3`，然后重新修复再打一个相同的 v1.2.3 推送。
- **后果**：Go Proxy 是不可变的。一旦它抓取了第一个错误的 v1.2.3，它就会永久缓存。重新推送相同的 Tag 会导致全球不同用户的哈希校验失败（checksum mismatch）。
- **正确做法（Go 1.16+）**：使用 `retract` 指令并发布补丁。
  1. 在 main 分支修复该 Bug。
  2. 打开 go.mod，在末尾添加撤回声明（一定要写注释解释原因）：
     ```go
     retract v1.2.3 // contains critical security vulnerability
     ```
  3. 提交代码，并打一个新的 Tag：`v1.2.4`。

### 场景四：单仓库多模块（Monorepo）
如果你的仓库不仅在根目录有一个 go.mod，在子目录（比如 sdk/ 目录下）也有一个独立的 go.mod。
- **规范**：必须使用带有目录前缀的 Tag 才能触发子模块的版本发布。
- **示例**：
  - 要发布根目录的代码版本：执行 `git tag v1.2.0`
  - 要发布 sdk/ 目录的代码版本：执行 `git tag sdk/v1.0.0`
- **Go Module 行为**：Go 会根据斜杠前缀去寻找对应目录下的 go.mod 文件。

## Repository-specific cautions

- The query builder is **stage-based**: each method call returns a different concrete type that restricts what can be called next. `Where(...)` and `Search(...)` each appear **at most once** per chain — the Go type system enforces this at compile time; you cannot call either a second time. Pass all filter conditions to the single `Where(...)` call (multiple arguments are ANDed); use `tsq.Or(...)` for OR groups and `tsq.And(...)` to build compound sub-conditions. Both clauses can coexist in either order: `Where(...).Search(...)` or `Search(...).Where(...)`.
- `FULL JOIN` can be rendered but execution remains dialect-dependent.
- Custom codec fields that implement `driver.Valuer` / `sql.Scanner` still need an explicit `db:"...,type:SQL_TYPE"` override when TSQ cannot infer a DDL type.
- Example schema lives in `examples/academy/mock.sql`; schema changes must stay consistent with example structs and regenerated code.
- Keep local-only files out of Git: coverage outputs, local assistant settings such as `.claude/`, built binaries at repo root.
