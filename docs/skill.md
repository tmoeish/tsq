# TSQ Agent Skill

This repository publishes an installable agent skill in addition to the
`github.com/tmoeish/tsq/v4` Go module:

```txt
skills/
  tsq/
    SKILL.md
    references/
```

The skill is intended for coding agents working in other Go projects. The files under `docs/`
are for people browsing this repository; the references that are installed with the skill and
read by agents live under `skills/tsq/`.

## Recommended: install into a project

Project-level installation is the recommended and most common setup. Run this command from the
root of the Go project that uses TSQ:

```bash
gh skill install tmoeish/tsq skills/tsq --dir .agents/skills
```

The installed skill is located at:

```txt
<project-root>/.agents/skills/tsq/
  SKILL.md
  references/
```

Explicitly targeting `.agents/skills/` is recommended because:

- it is the common project-level skill directory recognized by many agents, including GitHub
  Copilot, Cursor, Codex, Gemini CLI, and others;
- one project-local copy can serve multiple agents and collaborators instead of creating
  duplicate, agent-specific copies that may drift apart;
- the TSQ guidance stays with the project that needs it and does not affect unrelated projects;
- the installation location remains predictable regardless of the CLI's default agent or an
  interactive agent selection.

The `--dir` path is relative to the current directory, so run the command from the target project's
root. The command records GitHub source metadata in the installed skill, which enables later
updates with `gh skill update`.

## Update an installed skill

Preview available updates without changing files:

```bash
gh skill update tsq --dir .agents/skills --dry-run
```

Update the project-level TSQ skill:

```bash
gh skill update tsq --dir .agents/skills
```

To update every skill installed in that directory without prompting:

```bash
gh skill update --all --dir .agents/skills
```

A skill installed with `--pin` is skipped during normal updates. Clear the pin and update it with:

```bash
gh skill update tsq --dir .agents/skills --unpin
```

`gh skill update` relies on the source metadata added by `gh skill install`. A manually copied
skill has no such metadata, so update it by copying a newer version again or reinstall it with
`gh skill install`.

## Other installation scopes and locations

`gh skill install` currently supports many agents. TSQ primarily targets GitHub Copilot, Claude
Code, and Gemini CLI, but the shared project directory above is suitable for any agent that
recognizes the Agent Skills convention.

If an agent requires its own project directory, let `gh skill install` resolve that location:

```bash
gh skill install tmoeish/tsq skills/tsq --agent github-copilot --scope project
gh skill install tmoeish/tsq skills/tsq --agent claude-code --scope project
gh skill install tmoeish/tsq skills/tsq --agent gemini-cli --scope project
```

The skill will be installed as a `tsq/` directory below that agent's project-level skill directory.
At the time of writing, GitHub Copilot and Gemini CLI resolve project scope to
`.agents/skills/tsq/`, while Claude Code resolves it to `.claude/skills/tsq/`. Explicit `--dir`
remains the clearest way to select the shared location without depending on agent-specific
resolution.

Use user scope only when you want the skill available in every project for the current user:

```bash
gh skill install tmoeish/tsq skills/tsq --agent github-copilot --scope user
gh skill install tmoeish/tsq skills/tsq --agent claude-code --scope user
gh skill install tmoeish/tsq skills/tsq --agent gemini-cli --scope user
```

Common user-level destinations include:

- `~/.copilot/skills/tsq/`
- `~/.claude/skills/tsq/`
- `~/.gemini/skills/tsq/`
- `~/.agents/skills/tsq/`

The exact user-level destination depends on the selected agent and CLI version. Use `gh skill list`
to inspect installed skills and their resolved locations.

## Preview and pin a version

Preview the skill before installing it:

```bash
gh skill preview tmoeish/tsq skills/tsq
```

Without a version, `gh skill install` resolves the latest tagged release and then falls back to the
default branch. To pin a project installation to a specific release:

```bash
gh skill install tmoeish/tsq skills/tsq@v4.1.21 --dir .agents/skills
```

The equivalent `--pin v4.1.21` form is also supported.

## Install from a local clone

If this repository is already cloned, install from its local path. Run the following from the root
of the target project:

```bash
gh skill install /path/to/tsq tsq --from-local --dir .agents/skills
```

Local files are copied rather than symlinked. The installed location is still:

```txt
<project-root>/.agents/skills/tsq/
```

## Manual installation

If you do not want to use `gh skill install`, copy the complete `skills/tsq` directory into the
target project's skill directory. The recommended destination is:

```txt
<project-root>/.agents/skills/tsq/
```

Other project-level destinations recognized by some agents include:

- `.github/skills/tsq/`
- `.claude/skills/tsq/`

After copying, reload skills using the selected agent's mechanism. For GitHub Copilot CLI, the
commands are typically:

```txt
/skills reload
/skills info tsq
```

Manual copies do not contain the GitHub source metadata required for automatic updates.

## Use the skill

After installation, the agent usually activates the skill automatically based on its `description`.
To request it explicitly, mention it in the prompt:

```txt
Use the /tsq skill to add TSQ to this Go service.
```

Tasks that should activate the skill include:

- adding TSQ to a Go project;
- adding `@TABLE` or `@RESULT` annotations to structs;
- running `tsq fmt` or `tsq gen`;
- initializing `tsq.Runtime`;
- writing Build-based queries;
- using CRUD, pagination, or search helpers; and
- handling transactions and edge cases involving `InVar()`, `NInVar()`, CTEs, or `FULL JOIN`.

The installed technical references are:

- `skills/tsq/SKILL.md`
- `skills/tsq/references/QUICKSTART.md`
- `skills/tsq/references/CONCEPTS.md`
- `skills/tsq/references/REFERENCE.md`

## Documentation ownership

The documentation is split into two layers to keep agent guidance unambiguous:

1. `docs/skill.md` explains installation, usage, and important risks to repository visitors.
2. `skills/tsq/references/*.md` contains the technical reference installed with the skill.

For DSL keys, managed-field semantics, the query DSL, transactions, and dialect boundaries, treat
`skills/tsq/references/REFERENCE.md` as the canonical skill reference.

## Important correctness constraints

Agents can easily generate incorrect code when the following details are misunderstood.

### 1. DSL keys refer to Go struct field names, not SQL column names

The following keys all refer to Go struct field names:

- `pk`
- `version`
- `created_at`
- `updated_at`
- `deleted_at`
- `ux[].fields`
- `idx[].fields`
- `search`

The SQL column name still comes from the field's `db` tag or the generator's column-naming rules.

### 2. `version` is the managed optimistic-lock field

The supported forms are:

```txt
version
version=true
version="Version"
version="CustomField"
```

Their meanings are:

- `version` or `version=true` enables optimistic locking and defaults to the Go field `Version`;
- `version="CustomField"` enables optimistic locking with the specified Go struct field; and
- `version=false` is equivalent to omitting the option.

The referenced field must exist and use a non-pointer integer type. Do not use a string, time,
nullable wrapper, slice, or array.

The generated behavior is:

- `Update(...)` and `Delete(...)` match by primary key and version;
- the version is incremented automatically after a successful update; and
- a conflict returns `ErrOptimisticLockConflict`.

### 3. `created_at`, `updated_at`, and `deleted_at` have type requirements

`created_at` and `updated_at` support:

- `time.Time`
- `*time.Time`
- `sql.NullTime`
- `null.Time`

`deleted_at` supports:

- `int64`
- `uint64`
- `*time.Time`
- `sql.NullTime`
- `null.Time`

Their behavior is:

- generated insert helpers set `created_at` to the current time;
- generated insert, update, and soft-delete helpers refresh `updated_at`; and
- `deleted_at` enables soft deletion, and list, get, and page helpers automatically filter for
  active rows.

For a table with a unique index, prefer `int64` or `uint64` tombstone semantics for `deleted_at`
over nullable time semantics.

### 4. `@RESULT` is different from `@TABLE`

Normally, `@RESULT` should use only:

- `name`
- `search`

Declare projection sources using field tags:

```go
tsq:"Struct.Field"
```

The following table-only keys should not be used as part of a normal `@RESULT` design:

- `pk`
- `version`
- `created_at`
- `updated_at`
- `deleted_at`
- `ux`
- `idx`

Agents should treat them as unsupported or no-op in normal result-model usage and must not depend
on them to provide table semantics.

### 5. `driver.Valuer` and `sql.Scanner` do not determine a DDL column type

Even when a custom Go type implements `driver.Valuer` and `sql.Scanner`, an agent cannot infer
whether its database type should be `JSON`, `TEXT`, `JSONB`, or something else. Provide an explicit
DDL type override:

```go
type SkillItems []*SkillItem

type Track struct {
    SkillItems SkillItems `db:"skill_items,type:JSON" json:"skill_items"`
}
```

In other words:

- `Valuer` and `Scanner` control runtime reads and writes;
- `db:"...,type:SQL_TYPE"` controls the DDL type override; and
- the `type:` value is written literally into each dialect's generated DDL, so reuse it only when
  that type is valid for every target dialect.

### 6. The skill intentionally contains no management scripts

The skill does not provide scripts that wrap:

- TSQ CLI installation or upgrades;
- `tsq fmt`; or
- `tsq gen`.

This is intentional. Those operations depend on the target project's module layout and package
paths. A generic script could run in the wrong directory, modify the wrong package, or encourage an
agent to execute it without inspecting the project first.

The agent should inspect the target project's package structure and then run explicit commands:

```bash
go install github.com/tmoeish/tsq/v4/cmd/tsq@latest
tsq fmt ./your/package
tsq gen ./your/package
```

## Suggested reading order

To evaluate the skill, read the files in this order:

1. `docs/skill.md` for installation, usage, and important risks.
2. `skills/tsq/SKILL.md` for activation conditions and agent workflow.
3. `skills/tsq/references/QUICKSTART.md` for the shortest integration path.
4. `skills/tsq/references/REFERENCE.md` for complete DSL and semantic details.

## Repository layout

The skill lives under `skills/tsq/`, rather than at the repository root, because:

1. it follows the Agent Skills model in which each skill is a directory;
2. `gh skill install` can discover it automatically;
3. its reference files can be packaged and installed with `SKILL.md`; and
4. the installed agent does not need repository-level `docs/` files to understand TSQ.

This repository is therefore both the TSQ source repository and a repository that publishes the
TSQ skill.
