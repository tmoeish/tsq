# TSQ Quickstart

This file is the **shortest practical path** for adding TSQ to a Go project.

## Outcome

After this flow, the target project should have:

1. one annotated table struct
2. one generated `*.tsq.go` file
3. one initialized runtime
4. one successful typed query

## 1. Add TSQ to the module

```bash
go get github.com/tmoeish/tsq/v5@latest
go get modernc.org/sqlite@latest
go install github.com/tmoeish/tsq/v5/cmd/tsq@latest
```

TSQ does not ship a database driver; the project only needs the driver it actually uses. This quickstart picks `modernc.org/sqlite` because it works without CGO.

## 2. Create or choose a package for database models

Typical package names:

- `database`
- `internal/database`
- `internal/persistence`

Use the package that already owns table-shaped structs if one exists.

## 3. Add a table model

```go
package database

//tsq:table
//tsq:search Name,Email
type User struct {
	ID    int64  `db:"id" json:"id"`
	Name  string `db:"name" json:"name"`
	Email string `db:"email" json:"email"`
}
```

Minimum requirements:

- a normal Go struct
- a `//tsq:table` directive above it
- `db` tags on persisted fields

If a field uses a custom Go type that TSQ cannot map directly to a SQL column type, keep the runtime codec on the Go type and add an explicit DDL override in the `db` tag:

```go
type SkillItems []*SkillItem

type Track struct {
	SkillItems SkillItems `db:"skill_items,type:JSON" json:"skill_items"`
}
```

## 4. Generate TSQ files

```bash
tsq gen ./database
```

Expected output:

```txt
database/
  runtime.tsq.go
  user.go
  user.tsq.go
```

Generated files are outputs. Change the source struct or annotation, then regenerate.

## 5. Initialize TSQ runtime

The shortest SQLite example:

```go
runtime, err := tsq.Open(
	ctx,
	"sqlite",
	"file:app.db?cache=shared",
	database.TSQTables(),
	tsq.WithSchemaPolicy(tsq.SchemaPolicyCreateMissing),
)
if err != nil {
	return err
}
defer runtime.Close()
```

`Open` opens the pool itself and resolves the dialect from `driverName`; the context bounds the
ping and any bootstrap DDL. When the project already opens its own pool, for instance to wrap it with
instrumentation, use `tsq.NewRuntime(ctx, db, dialect, tables, options...)` instead and keep
that pool: TSQ will not close a pool it did not open. If the project manages schema by migrations,
pass no policy and keep the default manual mode.

## 6. Run a first query

```go
query, err := tsq.
	Select(database.User__Cols...).
	From(database.TableUser).
	Where(database.User_Name.ContainsVal("alice")).
	Build()
if err != nil {
	return err
}

users, err := query.List(ctx, runtime)
if err != nil {
	return err
}
```

This is the main TSQ shape:

- use value helpers such as `EQVal(...)` / `ContainsVal(...)` for values fixed in the code
- use `EQ(col.Param())` and pass `col.Bind(v)` when the value comes at execution time
- use `EQ(otherCol)` when the right-hand side is another column or a typed subquery

```go
byName := tsq.Select(database.User__Cols...).
	From(database.TableUser).
	Where(database.User_Name.EQ(database.User_Name.Param())).
	MustBuild()

amy, err := byName.Get(ctx, runtime, database.User_Name.Bind("amy"))
```

1. choose columns
2. choose source table
3. add predicates
4. `Build()`
5. execute via methods on the built query: `query.List(ctx, exec)`, `query.Get(ctx, exec)`, `query.Find(ctx, exec)`, `query.Page(ctx, exec, pageReq)`, `query.Count(ctx, exec)`, or generated helpers

## 7. Add a transaction when needed

If multiple TSQ operations must share one transaction:

```go
if err := runtime.WithTx(ctx, nil, func(ctx context.Context, txExec tsq.Executor) error {
	if err := user.Insert(ctx, txExec); err != nil {
		return err
	}
	return profile.Update(ctx, txExec)
}); err != nil {
	return err
}
```

## 8. First checks when something is wrong

### `tsq gen` cannot find the package

- make sure the command runs from the module root
- make sure the package contains at least one `.go` file
- make sure `go.mod` exists

### the package panics while it is imported

Generated queries are built at package initialization. Usually the source struct changed but the
generated files were not refreshed. Regenerate first.

### query builds but execution fails on dialect support

That usually means the query is structurally valid but not executable on the current dialect. Capability checks for things like CTEs, row locks, or `FULL JOIN` happen at execution time.

## Next file

Read `REFERENCE.md` for the full TSQ DSL and feature surface.
