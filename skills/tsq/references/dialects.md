# Dialects

The three engines TSQ supports, what each can run, how to check a capability before relying on it,
and the engine-specific behavior that shows through. DSN and driver requirements for opening a
runtime are in `runtime.md`.

## The three engines

TSQ speaks MySQL, PostgreSQL and SQLite, and nothing else. The `dialect` package holds their names
(`dialect.MySQL`, `dialect.Postgres`, `dialect.SQLite`), what each supports, and the column types
generated code declares; it has no interface to implement. What the three spell differently (date
parts, `ROUND`, NULL ordering, full-text search) is chosen inside the library by name, and the
library supports the engines its tests run against.

Driver names `tsq.Open` understands: `sqlite` (modernc.org/sqlite), `sqlite3`
(github.com/mattn/go-sqlite3), `mysql`, and `postgres` / `postgresql` / `pgx` / `pq`. Both SQLite
drivers work, error classification included. The MySQL dialect is MySQL 8.0.19 or later: a runtime
refuses to start against MariaDB, which answers the MySQL protocol but takes neither the
`INSERT ... AS alias` every upsert uses nor a `JSON` column type, and against an older MySQL, where
the first upsert would otherwise fail with a syntax error. `tsq.NewRuntime` takes the dialect name directly, for a
pool opened elsewhere or a driver registered under another name.

SQLite lets one connection write at a time, and with a bare DSN a second one does not wait: a
goroutine that reads or writes while another writes gets `database is locked` at once. A program
that uses a SQLite runtime from more than one goroutine sets a busy timeout, the write-ahead log and
immediate transactions in the DSN:
`file:app.db?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_txlock=immediate` for
modernc.org/sqlite, `file:app.db?_busy_timeout=5000&_journal_mode=WAL&_txlock=immediate` for
mattn/go-sqlite3. `_txlock=immediate` makes every `WithTx` take the write lock as it begins: a
transaction that reads first and writes later is otherwise a reader that has to become the writer,
and SQLite refuses that at once (`database is locked`, without waiting the busy timeout) whenever
another transaction wrote in between. What still times out is a busy database, which
`tsq.IsRetryableTxError` reports and `WithRetry` retries.

## Capability matrix

| capability (`dialect.Capability…`) | builder construct | MySQL 8.0 | PostgreSQL | SQLite 3.39+ |
| --- | --- | --- | --- | --- |
| `CapabilityCTE` | `tsq.CTE` | yes | yes | yes |
| `CapabilityFullJoin` | `FullJoin` | **no** | yes | yes |
| `CapabilityIntersect`, `CapabilityExcept` | `Intersect`, `Except` | yes (8.0.31+) | yes | yes |
| `CapabilityIntersectAll`, `CapabilityExceptAll` | `IntersectAll`, `ExceptAll` | yes (8.0.31+) | yes | **no** |
| `CapabilityForUpdate`, `CapabilityForShare` | `ForUpdate`, `ForShare` | yes | yes | **no** |
| `CapabilityNoWait`, `CapabilitySkipLocked` | `NoWait`, `SkipLocked` | yes | yes | **no** |
| `CapabilityFullTextSearch` | `tsq.Matches` | yes | yes | substring match instead |

`Union` / `UnionAll` and everything not listed run everywhere. A construct marked **no** builds, and
fails when it runs on that engine with `*dialect.UnsupportedCapabilityError`. `Matches` is never
refused: on SQLite it falls back to a substring match (`paging-search.md`).


## Structure at `Build()`, capability at execution

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

- `FULL JOIN` builds everywhere but MySQL rejects it at execution (SQLite 3.39+ and PostgreSQL run it)
- row locks (`ForUpdate` / `ForShare`) are rejected on SQLite
- CTEs, `INTERSECT`, and `EXCEPT` run on all three built-in dialects; TSQ does not probe server versions, so MySQL 5.7 (end of life) gets a database error instead of `UnsupportedCapabilityError`

Do not claim that a query is portable just because it builds.

### Checking support ahead of execution

`dialect.Supports(name, capability)` answers for one capability, and `dialect.Check`
returns the same `*dialect.UnsupportedCapabilityError` execution would. Use them to gate a
feature before building a query that will fail at execution:

```go
if dialect.Supports(runtime.Dialect(), dialect.CapabilityFullJoin) {
	// build the FULL JOIN variant
}
```

Inside a `WithTx` callback, or with any other `Executor`, `tsq.DialectOf(db)` gives the dialect.

The error carries `Capability` and `Dialect` fields; match it with
`errors.AsType[*dialect.UnsupportedCapabilityError](err)`.

Every dialect takes an explicit position on every capability, so an unrecognized
capability name is reported as unsupported rather than quietly allowed.

## SQLite specifics

Two SQLite limits to know:

- SQLite's `UPPER` / `LOWER` change ASCII letters only
- SQLite keeps a time as text, and TSQ writes every time there as `2006-01-02 15:04:05.000000+00:00`
  (UTC, six fraction digits), whichever driver: the form mattn/go-sqlite3 writes and SQLite's own date
  functions read, so a file written through one driver reads back through the other and through
  hand-written SQL, and two times compare and sort as text the way they do as times. Left to
  itself, `modernc.org/sqlite` writes Go's `String()` form (`... +0000 UTC`), which mattn reads back as
  the zero time without an error. TSQ reads that form too, so rows a file already holds still read;
  an old row and a new value for the same instant are not equal as text, so load and save such rows
  once (`BatchUpdate`) where a query compares times for equality. The driver's other time parameters are not supported (`_time_integer_format` stores
  a time as an integer no time field reads back, `_inttotime` / `_texttotime` turn the result of
  `tsq.Date` into a time). `_txlock`, and pragmas such as `foreign_keys`, `journal_mode` and
  `busy_timeout`, are yours to set
