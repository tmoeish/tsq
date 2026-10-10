# Runtime

Creating and configuring a `*tsq.Runtime`: `tsq.Open` / `tsq.NewRuntime`, driver and DSN
requirements, schema policies, logging, SQL logging, tracers, the page-size cap, executors over
pools TSQ did not open, and closing. Engine capabilities and per-engine behavior are in
`dialects.md`.

## Opening a runtime

```go
runtime, err := tsq.Open(ctx, "pgx", dsn, database.TSQTables(),
	tsq.WithSchemaPolicy(tsq.SchemaPolicyValidate),
	tsq.WithLogger(logger),
	tsq.WithTracers(otelTracer),
)
if err != nil {
	return err
}
defer runtime.Close()

// Over a pool the application opened and configured itself:
runtime, err = tsq.NewRuntime(ctx, db, dialect.Postgres, database.TSQTables(), opts...)
```

- it implements `Executor` directly
- use `tsq.Open(ctx, "sqlite", dsn, database.TSQTables())` for one generated package; `TSQTables()` returns the package's `[]tsq.Table`. A MySQL DSN must set `parseTime` (`true` or `1`; `Open` refuses one that does not, and `NewRuntime` asks the pool it is handed how its driver reads a time and refuses it the same way, for `parseTime` and for `loc`), or times are read as bytes It must also leave `loc` at the driver's default, UTC: with `loc=Local` the driver stores TSQ's UTC times in that zone after all and reads the times the database fills (`default:CURRENT_TIMESTAMP`, written in UTC) as if they were in it, hours off and without an error, so `Open` refuses a `loc` other than `UTC`. Convert for display with `time.Time.In`. Text goes over the wire as UTF-8, which is what a Go string is, and a runtime refuses a session where it does not: a MySQL DSN with `charset` or `collation` of another character set (leave them out; `utf8mb4` is the driver's default), and a PostgreSQL session over a database of another encoding, which pgx leaves at the database's (add `client_encoding=UTF8` to the DSN; a `SQL_ASCII` database converts nothing and is not checked). In such a session the bytes of `é` are stored as the two characters `Ã©`, counted as two and cut apart by `Substring`, and read back whole by the same session, so nothing fails until another client reads the table. A session the driver itself refuses is refused as well, with the driver's reason: pgx in `simple_protocol` mode runs no query at all over a `client_encoding` other than `UTF8`, and that is better said by `Open` than by the first query
- MySQL's `sql_mode` is the deployment's to choose, and TSQ works under each: identifiers are quoted with backquotes, which `ANSI_QUOTES` leaves alone, and no statement TSQ writes depends on backslash escapes (a `default:` you write is SQL as written; see `default:`). What the mode decides is what happens to a value that does not fit its column. Outside strict mode (no `STRICT_TRANS_TABLES` or `STRICT_ALL_TABLES`) MySQL cuts a string to the column's length and clamps a number to the type's range, with a warning the driver does not pass on, so a write that reports success stored another value: a runtime over such a pool says so once in its log at startup. The schema policies do not depend on it: the connection they run on is strict while they run, so a change of type that would cut a value is refused under any mode, and the session gets its own mode back afterwards
- combine multiple generated packages by concatenating their `TSQTables()` slices before calling `Open` or `NewRuntime`
- `Open` opens the pool itself and resolves the dialect from `driverName`; the context bounds the ping and any bootstrap DDL
- `tsq.NewRuntime(ctx, db, dialect.Postgres, tables, options...)` builds a runtime over a pool the caller already opened, which is how an instrumented or specially configured `*sql.DB` keeps working while still getting SQL logging, tracers and the page-size cap
- call `runtime.Close()` when the process is done with the database. It closes **only** a pool `Open` opened; a pool passed to `NewRuntime` belongs to its caller and stays open
- configure both constructors with options: `tsq.WithSchemaPolicy(p)` sets the table and index policy together, `tsq.WithTablePolicy(p)` / `tsq.WithIndexPolicy(p)` set them apart for a schema whose tables come from migrations while its indexes do not, `tsq.WithLogger(l)`, `tsq.WithSQLLogging()`, `tsq.WithTracers(...)` and `tsq.WithMaxPageSize(n)`

The options (`tsq.RuntimeOption`; a nil one is an error):

| option | effect |
| --- | --- |
| `tsq.WithSchemaPolicy(p)` | the table and the index policy together (below) |
| `tsq.WithTablePolicy(p)`, `tsq.WithIndexPolicy(p)` | the two apart, for tables owned by migrations and indexes owned by TSQ |
| `tsq.WithLogger(l)` | where bootstrap DDL, warnings and SQL logging go; default `slog.Default()` |
| `tsq.WithSQLLogging()` | log every statement and its arguments at debug level |
| `tsq.WithTracers(t...)` | wrap every operation (below) |
| `tsq.WithMaxPageSize(n)` | the page-size cap of `Page` / `PageKeyset`, default `tsq.DefaultMaxPageSize` |

What a runtime offers besides being an executor: `runtime.Dialect()` (its `dialect.Name`),
`runtime.DB()` (the `*sql.DB`, for what TSQ does not do), `runtime.WithTx` / `WithTxResult`
(`transactions.md`) and `runtime.Close()`. Raw SQL can go through the runtime too:
`runtime.QueryContext` / `QueryRowContext` / `ExecContext` pass SQL and positional arguments, in the
driver's placeholder syntax, straight to the pool as `database/sql` does. They are neither traced
nor logged and get none of TSQ's checks; prefer the builder.

## Schema policies

`tsq.SchemaPolicy` decides what a runtime does with the declared tables at startup:
`tsq.SchemaPolicyManual` (the default), `tsq.SchemaPolicyValidate`,
`tsq.SchemaPolicyCreateMissing` or `tsq.SchemaPolicyReconcile`.

- the policies, from doing nothing to doing the most: `SchemaPolicyManual` (default: log the mode and change nothing), `SchemaPolicyValidate` (fail to start on a mismatch: `*tsq.MissingTableError`, `*tsq.MissingIndexError`, or `*tsq.SchemaMismatchError` listing the differing columns and, for each, what differs: the type as the database reports it against the declared spelling, NULL against NOT NULL, the default, the range constraint; and, where the database could not be asked whether two spellings are one — see "schema comparison" below, a user without the right to a temporary table on MySQL — that it was not, with the engine's reason), `SchemaPolicyCreateMissing` (create missing tables, columns and indexes; a column that differs from its declaration, or one the table no longer declares, still fails startup), `SchemaPolicyReconcile` (also alter columns back to what is declared, drop columns no longer declared — a renamed field is one column dropped and one added of the same shape, which Reconcile cannot tell from a replacement: it copies nothing and warns, naming the migration's `RENAME COLUMN` that keeps the values — and drop the indexes of a declared table that carry a name `tsq gen` derives, `ux_<table>_...`, `idx_<table>_...` or `ft_<table>_...`, once the table no longer declares them: a `//tsq:unique` that was removed or widened would otherwise go on refusing rows the declaration allows. Such an index over a column the same start alters goes before the column does, since MySQL will not alter an indexed column into a `BLOB` or `TEXT`. An index under any other name is never dropped, by any policy; a full-text index kept under one name follows its columns where the engine reports them, on MySQL). Production keeps `Manual` and owns its schema through migrations; development and test want `Reconcile`, where changing a struct and restarting is enough. Before any of their DDL, `CreateMissing` and `Reconcile` look at the rows a **unique index** they are about to create (or, under `Reconcile`, rebuild under its name with another definition) would cover: where rows already share its values, the start fails with a `*tsq.DuplicateRowsError` naming them and nothing is changed — created after the columns, the index was refused by the engine and the table was left altered without it. A new column of the index holds one value in every row (its zero value or default) and tells no rows apart; one that can hold NULL and has no default is NULL everywhere, which never conflicts, so that index is left to the engine. What a column carries that TSQ does not declare — a comment, a collation of its own — is neither compared nor changed: an `ALTER` the runtime runs, or a SQLite rebuild, restates the column with them (MySQL's `MODIFY COLUMN`, PostgreSQL's `ALTER COLUMN TYPE` and a table written anew would otherwise reset them). A generated migration knows nothing of the database and restates the column from the declaration alone: repeat such attributes in it by hand
- default policy is manual: TSQ logs a reminder but does not automatically reconcile missing tables or indexes
- a policy that adds a column does it on a table that already holds rows: a new NOT NULL column without a default gives those rows its type's zero value (`0`, `''`, `false`, the zero time), as a generated migration does, by adding the column with that default and dropping the default again. SQLite cannot drop a default, nor add a column whose default is not a constant (`CURRENT_TIMESTAMP`), so there the table is rebuilt with the column, under `CreateMissing` too. `Reconcile` fills the NULLs of a column that becomes NOT NULL the same way, with its default or zero value. A column of an explicit `type:` has no zero value TSQ knows, and a field that cannot hold NULL takes no `default:`: add such a column in a migration you write (add it with a default, then drop the default), or declare the field so that it can hold NULL. A column of an explicit `type:` that becomes NOT NULL over rows holding NULL is the same case: `tsq gen` writes the statement with a note above it saying the NULLs must be filled first, and the runtime's `Reconcile` is refused by the engine
- `tsq gen` refuses a table, column or index name longer than any built-in dialect allows, and suggests the directive that fixes it (usually `name=` on the index). A runtime checks again at construction, and there is no way to turn that off. Such a name does not reach the server intact, so the objects TSQ creates stop matching the names its queries reference. Name the index explicitly (`//tsq:unique Email name=ux_short`) when a derived index name is what runs over the limit
- schema comparison follows each engine's own spelling. Where a declared type or default does not compare equal as text to what the database reports, the runtime asks the database: on MySQL and PostgreSQL it creates the column as declared in a temporary table of its own session, reads back how the engine spells it, and takes the two for the same when the spellings agree (`DECIMAL(10)` is `decimal(10,0)`, `INT[]` is `integer[]`, `NVARCHAR(10)` is `varchar(10)`, a default `(1+1)` is `(1 + 1)`). On MySQL, where a temporary table and a kept one are described from different sources, the live column goes into a temporary table too, as `SHOW CREATE TABLE` writes it, and the two temporary columns are compared. A PostgreSQL `SERIAL` or `BIGSERIAL` off the key is recognized by the sequence of its own it draws from. That happens only for a column that differs as text, under any policy that inspects the schema (`Validate` included), and needs the right to create a temporary table; without it the text comparison stands, with a warning in the log. The text comparison itself knows these: a raw `type:` matches the name the engine reports for it (`TIMESTAMP(3)` and `timestamp(3) without time zone`, `INT4` and `integer`, MySQL's `BOOL` and `tinyint(1)`, `INT(11)` and `int`, a bare `DECIMAL` and `decimal(10,0)`), a literal default that MySQL reports without its quotes is the declared one, MySQL reading a `true` default back as `1` or a decimal `0` as `0.00`, or the default of a `TEXT` column (an expression there) as `_utf8mb4\'x\'`, is not a difference; on SQLite, which enforces a type affinity rather than the declared type, `VARCHAR(20)` and `VARCHAR(40)` or `INT` and `BIGINT` are the same column (a primary key excepted), so `Reconcile` does not rebuild the table for them; SQLite table and index names match in any case, and on SQLite a hand-written `id INTEGER PRIMARY KEY` (without `AUTOINCREMENT`) matches a declared auto-increment key, since the database assigns it either way. A `Reconcile` rebuild on SQLite keeps the table's `AUTOINCREMENT` counter, so keys of deleted rows are not handed out again. A rebuild that changes a column's type converts what converts (a fraction to the nearest integer, a number to a boolean) and checks the rows before it commits: a value that is not of the new type (`'Hello'` in a column that becomes an integer) fails startup with the column, the count and an example, and leaves the table as it was, where SQLite itself would have kept the value under a type no read takes it as. A current-time default is UTC on every dialect, and a live `CURRENT_TIMESTAMP` default on MySQL or PostgreSQL (the session's local time, as tables created before TSQ wrote UTC have) is a difference that `Validate` reports and `Reconcile` corrects
- several instances may start at once under `CreateMissing` or `Reconcile` (replicas, a rolling deploy): the policies run under a lock, one instance at a time for a database, and the ones that waited find the schema done. PostgreSQL holds a session advisory lock and MySQL a named lock (`GET_LOCK`), on one connection that also runs every statement of the policies, so a pool of a single connection is enough; on SQLite the runtimes of one process take turns, and separate processes are not coordinated. `Validate` changes nothing and takes no lock. A pooler that hands each statement another session (PgBouncer in transaction mode) defeats a session lock: run the policies over a direct connection
- one table's schema change is all or nothing on PostgreSQL and SQLite: the statements a policy runs for a table (its columns under `CreateMissing` / `Reconcile`, then its indexes) run in one transaction, so a statement the engine refuses (a retype the rows do not fit) leaves the table exactly as it was, and the previous release still starts. **MySQL cannot do this**: it commits every DDL statement on its own, so a change it refuses partway leaves the statements before it applied, and the error names them so that they can be undone or completed by hand. Run a risky retype on MySQL as a migration you have tried on a copy of the data
- **TSQ never drops a table.** No policy does, so several services can share one database and bring up their own tables independently. Removing a table that is no longer declared is a migration, not a boot-time decision: a runtime knows only its own declarations and cannot tell "this table is obsolete" from "this table belongs to someone else". Inside a table it declares, `SchemaPolicyReconcile` goes further: it drops a column the table no longer declares, with its data, and an index named the way `tsq gen` names them that is no longer declared. It is the prototype setting, where the database follows the code; production keeps `Manual`
- schema policies log the mode they are in at info level; `SchemaPolicyManual` (the default) is a normal production choice, not a warning

## Logging

- `tsq.WithLogger(l)` receives bootstrap DDL and execution-time warnings (for example a skipped batch-insert ID assignment); without it that is `slog.Default()`. A nil logger, tracer, runtime option, batch option or transaction option is an error, never a way to ask for the default
- `tsq.WithSQLLogging()` logs every rendered statement and its bound arguments through the logger at debug level: the message names the operation as `TraceInfo.Op` does (a hard delete is `hard_delete`), with the attributes `sql` and `args`. It is off by default and logs arguments verbatim, so leave it off wherever query parameters carry secrets or personal data. Only executors that belong to a runtime log; a `WrapExecutor` result has no runtime to read the setting from

`tsq.Logger` is the subset of `*slog.Logger` TSQ calls — `Enabled(ctx, level)` and
`LogAttrs(ctx, level, msg, attrs...)` — so a `*slog.Logger` is one, and any adapter with those two
methods is too.

## Tracers

```go
var otelTracer tsq.Tracer = func(ctx context.Context, info tsq.TraceInfo, next func(context.Context) error) error {
	ctx, span := tracer.Start(ctx, "tsq."+string(info.Op)+" "+info.Table)
	defer span.End()
	err := next(ctx)
	if err != nil {
		span.RecordError(err)
	}
	return err
}
```

- `tsq.WithTracers(t...)` wraps every traced operation. A tracer receives the context, a `tsq.TraceInfo` and the continuation, and must call the continuation once, with a context, and return its error. The runtime holds it to that: a tracer that returns nil without calling the continuation, calls it a second time, or passes it a nil context makes the operation fail with an error that says so, where it would otherwise report a success that never ran or run the statement twice. A tracer may still refuse an operation by returning an error of its own. `TraceInfo.Op` names the work (table below) and `TraceInfo.Table` the table it writes or the query's `FROM` table (empty for `tx`), which is what a span name needs. The rendered SQL is not passed: tracing brackets the whole operation, binding and dialect rendering included, so statements come from `WithSQLLogging()` instead

| `TraceOp` | value | operations |
| --- | --- | --- |
| `tsq.TraceOpInsert` | `insert` | `Insert`, `BatchInsert` |
| `tsq.TraceOpUpsert` | `upsert` | `Upsert`, `BatchUpsert` |
| `tsq.TraceOpUpdate` | `update` | `Update`, `BatchUpdate`, `tsq.UpdateTable` statements |
| `tsq.TraceOpDelete` | `delete` | soft deletes: `Delete`, `BatchDelete`, `BatchDeleteByPK`, `tsq.DeleteFrom` |
| `tsq.TraceOpHardDelete` | `hard_delete` | `HardDelete`, `BatchHardDelete`, `BatchHardDeleteByPK`, `tsq.HardDeleteFrom` |
| `tsq.TraceOpRestore` | `restore` | `Restore`, `BatchRestore` |
| `tsq.TraceOpGet` | `get` | `Get`, `Find` and the table's key lookups |
| `tsq.TraceOpExists` | `exists` | `Exists` |
| `tsq.TraceOpList` | `list` | `List`, `ListIn`, `Fetch`, `FetchBy` |
| `tsq.TraceOpIter` | `iter` | `Iter` |
| `tsq.TraceOpPage` | `page` | `Page`, `PageKeyset` |
| `tsq.TraceOpCount` | `count` | `Count` |
| `tsq.TraceOpTx` | `tx` | `WithTx`, `WithTxResult` |

## Page-size cap

- `tsq.WithMaxPageSize(n)` sets the page-size cap for paged queries on that runtime, in either direction. `tsq.DefaultMaxPageSize` (1000) is the default, not a ceiling

## Executors over pools TSQ did not open

The executor `db` is a `*tsq.Runtime`, the executor `WithTx` passes to its callback, or
`tsq.WrapExecutor(handle, dialect.Postgres)` (it returns an error for a nil handle or an unknown dialect) around a `*sql.DB` / `*sql.Tx` / `*sql.Conn` (any `tsq.DBTX`) opened elsewhere. A bare
`*sql.DB` does not compile: TSQ has to know the dialect to render a statement. A wrapped `*sql.Tx`
refuses its statements after an error the engine rolled the transaction back on (a deadlock on
MySQL), as the `WithTx` executor does; its `Commit` stays yours, and commits nothing then.

`tsq.DBTX` is the three `database/sql` methods (`QueryContext`, `QueryRowContext`, `ExecContext`)
that `*sql.DB`, `*sql.Tx` and `*sql.Conn` share. `tsq.WrapExecutor(handle, dialect)` returns
`(tsq.Executor, error)`. A wrapped executor has no runtime: no SQL logging, no tracers, no page-size
cap other than the default. `tsq.DialectOf(executor)` returns the dialect of any executor (inside a
`WithTx` callback, for instance).
