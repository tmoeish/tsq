# TSQ Reference

This is the main reference for the installed TSQ skill. It covers the current TSQ surface that an agent should use in another Go project.

## 1. What TSQ is good at

TSQ is a Go code generator and typed query DSL for:

- generating table metadata from Go structs
- generating CRUD, paging, and search helpers
- building SQL through typed column and condition APIs instead of handwritten string concatenation
- keeping runtime execution explicit through `context.Context`, `SQLExecutor`, and `Runtime`

Built-in dialects:

- SQLite
- MySQL
- PostgreSQL

## 2. Installation and generation

### Install TSQ in a target project

```bash
go get github.com/tmoeish/tsq/v4@latest
go install github.com/tmoeish/tsq/v4/cmd/tsq@latest
```

To upgrade the CLI later, run the same `go install ...@latest` command again, or replace `latest` with a pinned version.

### Main generator commands

```bash
tsq fmt ./database
tsq gen ./database
```

Run `tsq fmt` before `tsq gen`.

`tsq gen` accepts:

- a module import path
- a relative directory
- an absolute directory

Useful generator checks:

```bash
tsq gen --dry-run ./database
tsq gen --check ./database
```

Use `--dry-run` to preview generation changes and `--check` in CI or review flows to fail when generated files are stale.

### Why this skill does not ship management scripts

This skill intentionally does **not** bundle scripts for:

- installing or upgrading the TSQ CLI
- running `tsq fmt`
- running `tsq gen`

Reasons:

- skill installation and updates are already handled by `gh skill install` / `gh skill update`
- TSQ CLI installation is already a single `go install` command
- `tsq fmt` and `tsq gen` need a **project-specific package path**, so a generic script is more likely to run in the wrong directory or mutate the wrong package than to help
- pre-approving shell scripts in a skill increases risk for very little gain here

For agents, the correct default is to run the explicit commands directly after inspecting the target project's package layout.

## 3. Annotation DSL

### `@TABLE`

Use `@TABLE` on physical table structs.

Example:

```go
// @TABLE(
//   search=["Name","Email"]
// )
type User struct {
	ID    int64  `db:"id"`
	Name  string `db:"name"`
	Email string `db:"email"`
}
```

Important points:

- use `search=[...]` for keyword-search columns
- do not use legacy `kw`
- the source struct plus annotation is the source-of-truth

#### Supported `@TABLE` keys

The current table DSL recognizes these top-level keys:

| key | type | purpose |
| --- | --- | --- |
| `name` | string | physical table name; default is the struct name converted to snake_case |
| `pk` | string | primary-key Go field, optionally with auto-increment flag |
| `version` | bool or string | optimistic-lock field |
| `created_at` | bool or string | managed created timestamp field |
| `updated_at` | bool or string | managed updated timestamp field |
| `deleted_at` | bool or string | managed soft-delete field |
| `ux` | array of objects | declared unique indexes |
| `idx` | array of objects | declared non-unique indexes |
| `search` | array of strings | Go field names used by generated keyword-search helpers |

All field references in table DSL keys are **Go struct field names**, not SQL column names.

#### `pk`

`pk` uses the form:

```txt
pk="FieldName"
pk="FieldName,true"
pk="FieldName,false"
```

Rules:

- default is `ID`
- one field only; composite primary keys are not supported
- omitted auto-increment flag means `true`
- `true` means inserts may omit a zero-valued primary key and let the database generate it
- `false` means the caller must provide the primary-key value explicitly

#### Managed fields: `version`, `created_at`, `updated_at`, `deleted_at`

These keys support two forms:

```txt
version
version=true
version="Version"

created_at
created_at=true
created_at=false
updated_at="MTime"
deleted_at="DeletedAt"
```

Behavior:

- plain `version`, `created_at`, `updated_at`, `deleted_at` are shorthand for boolean `true`
- boolean `true` means “enable this managed role using the default Go field name”
- boolean `false` means “do not set this managed role”; in practice, omitting the key is clearer
- string means “use this exact **Go struct field name**”
- the default Go field names are:
  - `Version`
  - `CreatedAt`
  - `UpdatedAt`
  - `DeletedAt`

These names are **Go struct field names**, not SQL column names. The actual SQL column name still comes from the field's `db` tag or the generator's column rules.

#### `ux` and `idx`

Each item is an object:

```txt
ux=[{name="ux_user_email", fields=["Email"]}]
idx=[{fields=["OrgID","Status"]}]
```

Supported keys inside each index object:

| key | type | purpose |
| --- | --- | --- |
| `name` | string | physical index name; optional |
| `fields` | array of strings | Go field names in index order; required |

Rules:

- omitted index names are auto-generated
- field order is preserved
- duplicated fields in one index are invalid
- duplicated field combinations across indexes are invalid

#### `search`

Example:

```txt
search=["Name","Email"]
```

It declares which fields participate in generated keyword-search helpers.

Use Go field names here, not SQL column names.

### `@RESULT`

Use `@RESULT` for query result shapes that are not physical tables.

Example:

```go
// @RESULT(name="UserOrder")
type UserOrder struct {
	UserID   int64  `tsq:"User.ID"`
	UserName string `tsq:"User.Name"`
	OrgName  string `tsq:"Org.Name"`
}
```

Use `@RESULT` for:

- joined rows
- aggregate rows
- API-facing DTO-like result shapes

`@RESULT` fields themselves normally use `tsq:"Struct.Field"` tags to point at generated source columns:

```go
type UserOrder struct {
	UserID   int64  `tsq:"User.ID"`
	UserName string `tsq:"User.Name"`
}
```

#### Practical `@RESULT` keys

In normal usage, keep `@RESULT` simple:

| key | type | purpose |
| --- | --- | --- |
| `name` | string | generated result name |
| `search` | array of strings | optional search fields for generated result helpers |

`name` and `search` also use **Go-side names**, not SQL column names.

#### Important `@RESULT` behavior

Even though the parser shares some table-DSL machinery, table-only keys such as `pk`, `version`, `created_at`, `updated_at`, `deleted_at`, `ux`, and `idx` do **not** provide normal table semantics for `@RESULT`.

Practical rule for agents:

- for `@RESULT`, use only `name` and `search`
- treat table-only keys on `@RESULT` as unsupported / no-op in normal usage
- do not rely on them for validation, mutation behavior, indexes, or managed fields

Important validation rules for result fields:

- each `tsq:"Struct.Field"` reference must point to an existing generated source struct and field
- result field types must be scan-compatible with the referenced source field
- result references are normalized internally, so colliding projections should be avoided

## 4. Generated outputs

From table structs, TSQ commonly generates:

- `TableXxx`
- `Xxx__Cols`
- typed columns like `Xxx_ID`, `Xxx_Name`
- CRUD helpers
- list/page/search helpers

From result structs, TSQ commonly generates:

- `*_result_tsq.go`
- helpers that return the result owner

In projects that keep schema artifacts, TSQ may also generate:

- `sqlite.sql`
- `mysql.sql`
- `postgres.sql`
- `ddl.json`
- incremental DDL files

Do not hand-edit generated outputs in normal usage.

## 4.1 Managed-field semantics

These fields are important enough to remember separately because they change runtime behavior, not just metadata.

### `version`

`version` enables **automatic optimistic locking**.

Example:

```go
// @TABLE(
//   version
// )
type Enrollment struct {
	ID      int64 `db:"id"`
	Version int64 `db:"version"`
}
```

Semantics:

- DSL forms:
  - `version`
  - `version=true`
  - `version="Version"`
  - `version="CustomField"`
- boolean `true` enables optimistic locking using the default Go field name `Version`
- string names the **Go struct field**, not the SQL column name
- boolean `false` disables it and is equivalent to not configuring `version`
- the referenced field must exist on the Go struct
- the field must be a **non-pointer integer type**
- supported practical choices are integer fields such as `int`, `int32`, `int64`, `uint`, `uint32`, `uint64`; do not use string, time, pointer, slice, or nullable wrapper types
- DDL generation uses default `1` for a non-null integer version column
- `Update(...)` matches rows by primary key **and** current version
- successful updates increment the database version by `+1`
- successful updates also increment the in-memory struct field
- `Delete(...)` also matches by primary key and version
- if fewer rows match than expected, TSQ returns `ErrOptimisticLockConflict`

Use `version` when you want lost-update protection.

Do **not** declare it if you want plain last-write-wins behavior.

### `created_at`

`created_at` marks the creation timestamp field.

Semantics:

- generated insert helpers set it to the current time before insert
- DDL generation uses `CURRENT_TIMESTAMP` for compatible non-null time columns
- the field should use a timestamp-compatible type supported by TSQ

Typical usage:

```go
// @TABLE(
//   created_at
// )
```

Supported field types:

- `time.Time`
- `*time.Time`
- `sql.NullTime`
- `null.Time`

Do not use string or integer fields for `created_at`.

### `updated_at`

`updated_at` marks the modification timestamp field.

Semantics:

- DSL forms:
  - `updated_at`
  - `updated_at=true`
  - `updated_at="UpdatedAt"`
  - `updated_at="MTime"`
- string names the **Go struct field**, not the SQL column name
- generated insert helpers set it to the current time
- generated update helpers refresh it to the current time before update
- generated soft-delete helpers also refresh it
- use it when the project wants an auto-maintained modification time

Supported field types:

- `time.Time`
- `*time.Time`
- `sql.NullTime`
- `null.Time`

### `deleted_at`

`deleted_at` marks the soft-delete field.

Semantics:

- DSL forms:
  - `deleted_at`
  - `deleted_at=true`
  - `deleted_at="DeletedAt"`
- string names the **Go struct field**, not the SQL column name
- generated `SoftDelete(...)` helpers set it to the provided delete timestamp or a current-time/tombstone value
- generated list/get/page helpers automatically add the active-row filter for soft-delete-aware tables
- with unique indexes, portable behavior prefers an integer tombstone style rather than nullable-time semantics

Supported field types:

- `int64`
- `uint64`
- `*time.Time`
- `sql.NullTime`
- `null.Time`

Additional rule:

- if the table also declares unique indexes, prefer `int64` or `uint64` tombstone semantics for `deleted_at`; nullable-time soft-delete fields are not portable there

Use `deleted_at` when the project wants soft-delete behavior rather than only hard deletes.

## 5. Query DSL overview

The main query flow is:

```go
query, err := tsq.
	Select(database.User__Cols...).
	From(database.TableUser).
	Where(database.User_Name.Contains("alice")).
	OrderBy(database.User_ID.Desc()).
	Build()
```

Then execute it:

```go
users, err := tsq.List[database.User](ctx, runtime, query)
```

Typical stages include:

- `Select(...)`
- `From(...)`
- `Join(...)`
- `Where(...)`
- `Search(...)`
- `GroupBy(...)`
- `Having(...)`
- `OrderBy(...)`
- `Limit(...)`
- `Offset(...)`
- `Build()`

Builder state can branch safely, but the main reusable object is the built query.

## 6. Common condition and expression patterns

### Predicates from generated columns

Common examples:

```go
database.User_ID.EQ(1)
database.User_Name.Contains("alice")
database.User_Email.Like("%@example.com")
database.User_DeletedAt.IsNull()
```

### Combine conditions

`Where(...)` is a setter. Put all logic in one call:

```go
Where(
	database.User_OrgID.EQ(1),
	tsq.Or(
		database.User_Name.Contains("alice"),
		database.User_Email.Contains("alice"),
	),
)
```

### Custom expressions and predicates

Use the current escape hatches:

- `Expr(...)`
- `Exprf(...)`
- `Pred(...)`

Use them for deliberate custom SQL expressions, not as a replacement for normal typed columns.

### Ordinary values are bound parameters

TSQ normally binds values as parameters instead of inlining SQL literals. If the task needs a database function, column reference, or subquery, use an expression object explicitly instead of pretending a plain string is SQL.

## 7. Pagination and keyword search

Use `PageReq` for list endpoints that need:

- page number
- page size
- order field
- order direction
- keyword search

Useful rules:

- prefer `Validate()` for external API input
- use `Normalize()` only when compatibility-style fallback behavior is desired
- use `Offset()` instead of hand-calculating offset
- use `HasNext()` / `HasPrev()` for UI navigation logic

`EscapeKeywordSearch(...)` only escapes LIKE wildcards. It is not SQL injection protection.

## 8. Execution helpers

Common execution entrypoints include:

- `tsq.List`
- `tsq.Get`
- `tsq.GetOrErr`
- `tsq.Page`
- `tsq.Count`
- generated list/get/page helpers

They all take an explicit `context.Context` and an executor.

## 9. Runtime and transactions

### Runtime

`Runtime` is the TSQ-managed executor and runtime container.

- it implements `SQLExecutor` directly
- use `tsq.Init(db, dialect)` for the default runtime
- use `rt := tsq.NewRuntime(); rt.Init(...)` when isolation is needed

### Transactions

Use:

```go
runtime.WithTx(ctx, opts, func(ctx context.Context, txExec tsq.SQLExecutor) error {
	...
})
```

Use transaction helpers when:

- several TSQ writes must be atomic
- row-locking queries must share the same transaction
- chunked helpers must be atomic as one unit

Useful rules:

- transaction boundaries stay explicit
- `ChunkedInsert`, `ChunkedUpdate`, and `ChunkedDelete` do not silently create outer transactions
- automatic optimistic-lock retries can be configured with `TxOptions`

## 10. Aliases, rebinding, and result mapping

### `WithTable()`

Use `WithTable()` or the generated alias/rebinding support when a column must be rebound onto:

- an aliased table
- a self-join
- a CTE

### `Into(...)`

Use package-level `tsq.Into(...)` for result projection mapping. Do not depend on older `col.Into(...)` style guidance.

### `@RESULT`

Prefer `@RESULT` when the query result shape is stable and meaningful in the project. Use `Into(...)` when the result mapping is local and does not need a generated result model.

## 11. Advanced query features

TSQ supports more than simple list queries. Common advanced shapes include:

- aggregate queries with `GroupBy(...)` and `Having(...)`
- `CASE` expressions
- subqueries such as `InSub`, `ExistsSub`, `EQSub`
- non-recursive CTEs
- set operations such as `UNION` and `EXCEPT`
- row-lock clauses such as `ForUpdate()` and `ForShare()`

Important subquery rule:

- scalar comparisons and `InSub`-style usage require subqueries that select exactly one column

## 12. Dialect capability boundaries

TSQ separates structure validation from dialect execution.

### `Build()` guarantees

`Build()` checks:

- clause order
- ownership
- projection shape
- basic query structure

### Execution-time guarantees

Execution checks whether the actual dialect supports the feature.

Important examples:

- CTE execution is dialect-dependent
- `FULL JOIN` can be rendered but execution is dialect-dependent
- row locks are not universally supported

Do not claim that a query is portable just because it builds.

## 13. Optimistic locking

If a table declares a `version` column:

- `Update(...)` uses optimistic-lock conditions automatically
- successful updates increment the in-memory version
- `Delete(...)` also checks version
- conflicts return `ErrOptimisticLockConflict`

If the desired behavior is “no optimistic locking,” do not declare a managed `version` column.

## 14. Important semantic edges

### `Where(...)` and `Search(...)`

They are overwrite-style setters.

### `InVar()`

If the runtime slice is empty or nil, TSQ renders an explicit no-match shape instead of silently dropping the filter.

### Generated helpers

If a generated helper reports an initialization error, usually:

- annotations changed
- generated files are stale
- the query shape is no longer valid for the current source model

## 15. Migration advice

When moving a project from handwritten SQL to TSQ:

1. migrate one table or one query path at a time
2. reuse the existing DB bootstrap path
3. preserve existing package boundaries where possible
4. use generated columns instead of handwritten string column names
5. introduce `@RESULT` only where the result shape is stable and reused

## 16. What this skill should not assume

- do not assume the target environment also has the TSQ repository checked out
- do not reference repository `docs/` as required installed skill content
- do not use removed or legacy repo vocabulary when current TSQ APIs already expose the preferred path
