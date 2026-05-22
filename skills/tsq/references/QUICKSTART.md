# TSQ Quickstart

This file is the **shortest practical path** for adding TSQ to a Go project.

## Outcome

After this flow, the target project should have:

1. one annotated table struct
2. one generated `*_tsq.go` file
3. one initialized runtime
4. one successful typed query

## 1. Add TSQ to the module

```bash
go get github.com/tmoeish/tsq/v4@latest
go get github.com/mattn/go-sqlite3@latest
go install github.com/tmoeish/tsq/v4/cmd/tsq@latest
```

If the project already uses MySQL or PostgreSQL, keep its existing driver instead of adding SQLite just for TSQ.

## 2. Create or choose a package for database models

Typical package names:

- `database`
- `internal/database`
- `internal/persistence`

Use the package that already owns table-shaped structs if one exists.

## 3. Add a table model

```go
package database

// @TABLE(
//   search=["Name","Email"]
// )
type User struct {
	ID    int64  `db:"id" json:"id"`
	Name  string `db:"name" json:"name"`
	Email string `db:"email" json:"email"`
}
```

Minimum requirements:

- a normal Go struct
- an `@TABLE` annotation
- `db` tags on persisted fields

## 4. Generate TSQ files

```bash
tsq fmt ./database
tsq gen ./database
```

Expected output:

```txt
database/
  runtime_tsq.go
  user.go
  user_tsq.go
```

Generated files are outputs. Change the source struct or annotation, then regenerate.

## 5. Initialize TSQ runtime

The shortest SQLite example:

```go
db, err := sql.Open("sqlite3", ":memory:")
if err != nil {
	return err
}

runtime, err := tsq.NewRuntime(db, dialect.SQLiteDialect{}, database.TSQTables())
if err != nil {
	return err
}
```

If the target project already has a DB bootstrap path, integrate TSQ there instead of creating a second runtime path.

## 6. Run a first query

```go
query, err := tsq.
	Select(database.User__Cols...).
	From(database.TableUser).
	Where(database.User_Name.Contains("alice")).
	Build()
if err != nil {
	return err
}

users, err := tsq.List[database.User](ctx, runtime, query)
if err != nil {
	return err
}
```

This is the main TSQ shape:

1. choose columns
2. choose source table
3. add predicates
4. `Build()`
5. execute with `List`, `Get`, `Page`, `Count`, or generated helpers

## 7. Add a transaction when needed

If multiple TSQ operations must share one transaction:

```go
if err := runtime.WithTx(ctx, nil, func(ctx context.Context, txExec tsq.SQLExecutor) error {
	if err := tsq.Insert(ctx, txExec, user); err != nil {
		return err
	}
	return tsq.Update(ctx, txExec, profile)
}); err != nil {
	return err
}
```

## 8. First checks when something is wrong

### `tsq gen` cannot find the package

- make sure the command runs from the module root
- make sure the package contains at least one `.go` file
- make sure `go.mod` exists

### generated helper returns an initialization error

Usually the source struct changed but generated files were not refreshed. Regenerate first.

### query builds but execution fails on dialect support

That usually means the query is structurally valid but not executable on the current dialect. Capability checks for things like CTEs, row locks, or `FULL JOIN` happen at execution time.

## Next file

Read `REFERENCE.md` for the full TSQ DSL and feature surface.
