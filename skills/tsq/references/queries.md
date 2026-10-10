# Queries: the builder and read methods

How to build a `SELECT` with TSQ and every way to run it: the stages, joins, grouping, ordering,
limits and row locks, the read methods, lookups by key, `ListIn` and `AttachMany`. Conditions,
values, parameters and functions are in `expressions.md`; subqueries, CTEs, set operations and
aliases in `advanced-queries.md`; paging and search in `paging-search.md`.

## The builder

The main query flow is:

```go
query, err := tsq.
	Select(database.TableUser.Columns()...).
	From(database.TableUser).
	Where(tsq.Contains(database.TableUser.Name, tsq.Val("alice"))).
	OrderBy(database.TableUser.ID.Desc()).
	Build()
```

Then execute it:

```go
users, err := query.List(ctx, runtime)
```

Typical stages include:

- `Select(cols...)`, `SelectDistinct(cols...)`, or `SelectValue(expr)` / `SelectNullValue(expr)` when
  one value is the whole row (`expressions.md`)
- `From(table)`: a table, an alias (`TableXxx.As("a")`) or a CTE
- `InnerJoin(t, on, more...)` / `LeftJoin(...)` / `RightJoin(...)` / `FullJoin(...)`, which need an `ON`
  condition, and `CrossJoin(t)`, which takes none
- `Where(cond, more...)`
- `Search(tsq.Searchable(col), more...)`: each search column is wrapped in `tsq.Searchable`, which
  takes string columns only
- `GroupBy(col, more...)`
- `Having(cond, more...)`
- `OrderBy(term, more...)`, then `Limit(...)`, then `Offset(...)`: in SQL's order, each at most once,
  and `Offset` only after `Limit`
- `ForUpdate()` / `ForShare()`, optionally followed by one of `NoWait()` / `SkipLocked()`
- `Build()`

Every clause that needs an argument takes its first one as a separate parameter, so leaving it out
(`Where()`, `GroupBy()`, a join with no `ON`) does not compile. A list assembled at run time goes to
`Where` as `tsq.And(conds...)`; a slice of columns or terms as `GroupBy(cols[0], cols[1:]...)`.

Builder state can branch safely, but the main reusable object is the built query. A built `*Query`
composes like a stage: it is a set-operation operand (`Union(q)`), a CTE body (`tsq.CTE("x", q)`)
and a subquery.

Writes by condition use the same staged style with `tsq.UpdateTable(table)` / `tsq.DeleteFrom(table)` (`writes.md`).

The stage interfaces are named after where a chain is (`JoinStage`, `WhereStage`, `GroupedStage`,
...), and are composed from capability interfaces a helper can accept instead:
`tsq.Sortable[O]` (`OrderBy` / `Limit` on rows, leading to `OrderedStage`, `LimitedStage` and
`OffsetStage`, which can still lock), `tsq.ResultSortable[O]` (the same after `GroupBy`, `Having` or
a set operation, leading to `OrderedResultStage` and `LimitedResultStage`, which cannot),
`tsq.Lockable[O]` (`ForUpdate` / `ForShare`),
`tsq.Combinable[O]` (`Union` / `Intersect` / `Except`) and `tsq.Groupable[O]` (`GroupBy`). They are
sealed: only this package's builders implement them.

## Soft-delete scope

A table that declares `deleted_at` is scoped to live rows wherever it appears, so a query cannot
forget the filter:

- as the `FROM` table or in an `INNER` / `CROSS` join, `deleted_at` is checked in `WHERE`
- in a `LEFT JOIN`, the check joins the `ON` condition, so a deleted row does not match and the
  left row is kept with `NULL`s
- when the query has a `RIGHT` or `FULL` join, every scoped table is read through a derived table
  of its live rows, so a deleted row is never a preserved row
- `UpdateTable` and `DeleteFrom` skip deleted rows (a second soft delete does not restamp);
  `HardDeleteFrom` reaches every row
- `table.WithDeleted()` is the same table without the scope, in any of those positions. It
  changes which rows a statement reaches, never what the statement does: a `Delete`,
  `BatchDeleteByPK` or `DeleteFrom` through it is still a soft delete, and stamps a deleted row
  again
- a CTE and a subquery are scoped by the tables inside them

## Ordering, limits and locks

`OrderBy` takes terms built from typed columns with `Asc()` / `Desc()`:

```go
query, err := tsq.
	Select(database.TableUser.Columns()...).
	From(database.TableUser).
	OrderBy(database.TableUser.Name.Asc(), database.TableUser.ID.Desc()).
	Limit(20).
	Offset(40).
	Build()
```

Rules:

- `OrderBy` / `Limit` / `Offset` are reachable from every complete stage (after `Where`, `Search`, `GroupBy`, `Having`, or a set operation). Only `ForUpdate()` / `ForShare()` may follow them, matching SQL clause order, and not after `GroupBy`, `Having`, a set operation, `SelectDistinct` or an aggregate: those rows are not rows of a table, and ordering them first does not change that
- `Offset` exists only after `Limit`: a bare `OFFSET` is a syntax error on MySQL and SQLite
- a row lock does not cover a `LEFT`, `RIGHT` or `FULL` join (PostgreSQL cannot lock the side that can
  be NULL), and a `SelectDistinct` query orders only by what it selects; `Build()` refuses both
- an aggregate belongs in the select list, `Having` or `OrderBy`: `Build()` refuses one in `Where`, a
  join condition or `GroupBy`, and one aggregate inside another
- an expression with bound values that is both selected and grouped or ordered by (a `CASE` bucket)
  is written as its position in `GROUP BY` / `ORDER BY`: PostgreSQL numbers each placeholder anew and
  would not see the two as the same expression
- the ordered column must belong to a table the query already selects from or joins
- `Count()` counts the rows `List` returns: the count query drops `ORDER BY`, and a query with `Limit` / `Offset` is counted over the limited rows. It takes the same arguments as `List`
- **do not combine builder-level paging with `query.Page(...)`**. `Page` appends its own `LIMIT`/`OFFSET`, and its own `ORDER BY` when `Paging.OrderBy` is set, so a builder-level clause would be emitted a second time rather than replaced. `Page` returns an error instead of guessing. A builder `OrderBy` combined with an empty `Paging.OrderBy` is fine: the builder's ordering stands and `Page` only adds the window
- on a set operation (`Union`, ...) an `OrderBy` term refers to the output column by name, which is the only form every dialect accepts there. The term must be an output column: a selected projection (`upper := tsq.MapInto(tsq.Upper(col), ...)`, then `OrderBy(upper.Asc())`) or a column selected under that name (the output of that name has to be that column, not an expression derived from it, which carries its name); `Build()` refuses an expression that is not selected
- a select item that is not a plain column is written `AS` its name (the name of the column it is derived from), so a CTE and a set operation's `ORDER BY` find it by that name on every dialect
- a set operation whose operand is itself combined (`a.Union(b.Union(c))`) groups the operand as a derived table, which every dialect accepts
- every operand's rows are read through the first operand's columns, by position, so each operand must select into the same fields in the same order; `Build()` refuses an operand that selects them in another order
- a chain is evaluated left to right, as it reads: `a.Union(b).Intersect(c)` is `(a ∪ b) ∩ c` on every dialect. SQL itself binds `INTERSECT` tighter than `UNION` / `EXCEPT` on MySQL and PostgreSQL but not on SQLite, so TSQ groups the part before such an `INTERSECT` as a derived table. For `a ∪ (b ∩ c)`, pass the combined operand: `a.Union(b.Intersect(c))`
- a select list that names one column twice (`users.id` and `orders.id`) keeps the first name and writes the later one under another: a column of another table as `table_column` (`orders_id`), anything else as `name_2`, `name_3`. The query can still be counted or grouped as a derived table; rows are read by position, so nothing changes for the caller
- a set operation's operands cannot have their own `OrderBy`, `Limit`, `Offset` or lock: each operand is written as a bare `SELECT`, so `Build()` refuses them rather than dropping the clause. Order and limit the combined result instead
- where the ordered value can be NULL (a `NullColumn`, an outer-joined column, ...), NULLs sort as
  the **smallest value on every dialect**: first when ascending, last when descending. MySQL and
  SQLite do that already; PostgreSQL is told with `NULLS FIRST` / `NULLS LAST`. `col.Asc().NullsLast()`
  and `col.Desc().NullsFirst()` choose otherwise (MySQL gets an `col IS NULL` key; on a MySQL set
  operation that is refused). A value that is never NULL is ordered as written, so indexes stay usable


## Read methods

Reads are methods on the built `*Query[O]`, and on every complete stage, which builds first (all
but `ListIn`, a generic method, which an interface cannot declare: call it on the `*Query`) (build once and reuse the `*Query` on hot paths); `args` are the `tsq.Arg` values made by `Bind`:

- `query.List(ctx, db, args...)` → `[]*O, error`; no rows is an empty list, never nil
- `query.ListIn(ctx, db, listParam, values, args...)` → `[]*O, error`: `List` for a list parameter that may hold more values than one statement can bind (65535 on MySQL and PostgreSQL, 32766 on SQLite). Values are deduplicated and split; a list that fits in one statement runs as one and keeps the query's `ORDER BY`, and the parts of a split list share one snapshot and are concatenated, each in that order, with no order across them; in a query that reads one table, a row that two parts both matched (two values the database takes as one key, such as `"Ada"` and `"ada"` under a case-insensitive collation) is kept once, by primary key; a query that joins keeps every row its parts returned, since a join repeats rows of its own. The query must use the parameter once, as `col.In(param)` passed directly to `Where`, and have no `GROUP BY`, aggregate, `DISTINCT`, set operation or `LIMIT`; anything else is refused, because splitting would change which rows come back. `TableXxx.Fetch` and `FetchBy` use it. A statement that binds more values than the dialect takes is refused before it is sent, with an error that says so (`col.In(tsq.Vals(...))` over 70000 values, for one): pass a long list with `ListIn`
- `query.Iter(ctx, db, args...)` → `iter.Seq2[*O, error]`: `for row, err := range query.Iter(ctx, db) { ... }` scans one row at a time, so exports and batch jobs do not hold the whole result in memory. `break` stops the query; a failure is yielded once with a nil row. The rows hold a connection until the loop ends, so inside a transaction finish the loop before running another statement on it; on a pool of one connection the same goes for a transaction opened inside the loop (`WithTx`, `Page`), which waits for that connection until the context ends — `List` the rows first, or give the pool a second connection
- `query.Get(ctx, db, args...)` → `*O, error` (an error wrapping `sql.ErrNoRows` when not found)
- `query.Find(ctx, db, args...)` → `*O, error` (`nil, nil` when not found)
- `query.Exists(ctx, db, args...)` → `bool, error`
- `query.Count(ctx, db, args...)` → `int64, error`
- `query.Page(ctx, db, paging, args...)` → `*Page[O], error`
- `query.PageKeyset(ctx, db, keyset, args...)` → `*KeysetPage[O], error` (`paging-search.md`)
- `query.SQL(dialect, args...)` → the SQL and arguments the query would run with, for logging and tests

```go
q := tsq.Select(database.TableUser.Columns()...).From(database.TableUser).
	Where(database.TableUser.OrgID.EQ(database.TableUser.OrgID.Param())).
	OrderBy(database.TableUser.ID.Asc()).
	MustBuild()

users, err := q.List(ctx, runtime, database.TableUser.OrgID.Bind(orgID))
first, err := q.Get(ctx, runtime, database.TableUser.OrgID.Bind(orgID))    // sql.ErrNoRows when none
maybe, err := q.Find(ctx, runtime, database.TableUser.OrgID.Bind(orgID))   // nil, nil when none
n, err := q.Count(ctx, runtime, database.TableUser.OrgID.Bind(orgID))
for u, err := range q.Iter(ctx, runtime, database.TableUser.OrgID.Bind(orgID)) {
	if err != nil {
		return err
	}
	_ = u
}
text, args, err := q.SQL(dialect.Postgres, database.TableUser.OrgID.Bind(orgID)) // what would run

// Any number of ids: ListIn splits the list to fit the engine's bind limit.
ids := database.TableUser.ID.ListParam()
byIDs := tsq.Select(database.TableUser.Columns()...).From(database.TableUser).
	Where(database.TableUser.ID.In(ids)).MustBuild()
users, err = byIDs.ListIn(ctx, runtime, ids, idSlice)
```

`Get`, `Find` and `Exists` read at most one row: they add `LIMIT 1` unless the builder
set its own limit. `Exists` does not count, and does not read the row either: it selects `1`
instead of the columns (a grouped, `DISTINCT` or limited query keeps its shape, since that decides
its rows), so a selected value that could be `NULL` in a field that cannot hold it is no reason for
it to fail.

A query is rendered for a dialect the first time it runs on one, and the rendering is cached.
Build package-level queries once and reuse them.

## Reading by key

The primary-key lookups are on the table itself, typed by the key, and need no query:

- `TableXxx.Get(ctx, db, id)` reads one row and fails with an error wrapping `sql.ErrNoRows` when there is none; `Find` returns `nil, nil` instead
- `TableXxx.Fetch(ctx, db, ids...)` reads rows in the order given, for any number of keys (they are split to fit the bind parameter limit). A missing key fails the call with an error wrapping `sql.ErrNoRows`, so `errors.Is(err, sql.ErrNoRows)` tells "not there" from a database failure
- `TableXxx.GetBy(ctx, db, col, value, conds...)`, `FindBy` and `TableXxx.FetchBy(ctx, db, col, values, conds...)` do the same for another unique column; the generated `GetByX` / `FindByX` / `FetchByX` call them. The column, together with the columns the `conds` fix with `EQ`, must cover the primary key or a unique index (the live-row scope counts as fixing an integer `deleted_at`); anything else is refused rather than answered with an arbitrary row, and is a query to write with `Select`. Without `conds` the query is built once and reused. Matching follows the database: on a case-insensitive column `"ADA"` finds the row holding `"Ada"`
- `TableXxx.Query()` is the query over every row, with keyword search over the declared search columns: `TableXxx.Query().Page(ctx, db, paging, tsq.Keyword(q))`. Because it carries `Search`, it is not a subquery, CTE body or set-operation operand on a table with search columns; build those with `tsq.Select`


## Loading children without N+1

TSQ has no relation DSL: a join or a `//tsq:result` says what a query returns. What it does have is a
way to give a list of parents their children in **one** extra query instead of one per parent:

```go
children := tsq.Select(database.TableEnrollment.Columns()...).
	From(database.TableEnrollment).
	Where(database.TableEnrollment.LearnerID.In(database.TableEnrollment.LearnerID.ListParam())).
	MustBuild()

err := tsq.AttachMany(ctx, db, learners, database.TableLearner.ID, children, database.TableEnrollment.LearnerID,
	func(l *database.Learner, es []*database.Enrollment) { l.Enrollments = es })
```

- the child query is yours: its `Where`, `OrderBy` and soft-delete scope decide which children
  count, and further `args` bind its other parameters. Its only list parameter is the child key
- the parents' keys are collected, deduplicated and read through `ListIn`, so any number of parents
  works; within one statement the children keep the child query's order, and a key list long enough
  to be split has no overall order
- `tsq.AttachOne` is the same for a single child, such as the row a foreign key points at: the first
  match in the child query's order wins, and a parent without one is left alone
- no parents means no query at all
- the child query must select the child key, and a nullable key is compared by value: a child
  whose key is NULL belongs to no parent. Keys match by Go equality, not by the database's
  collation, so `"Ada"` and `"ada"` are different parents even where a `_ci` collation matched both

## Stage types

Each builder call returns an interface named after where the chain is. They matter when a helper
takes or returns a partial query; normally you only chain calls.

| stage | reached by | offers |
| --- | --- | --- |
| `SelectStage[O]` | `tsq.Select` / `SelectDistinct` / `SelectValue` / `SelectNullValue` | `From` |
| `JoinStage[O]` | `From`, a join | joins, `Correlate`, `Where`, `Search`, `GroupBy`, ordering, locks, set operations, reads |
| `WhereStage[O]` | `Where` | `Search`, `GroupBy`, ordering, locks, set operations, reads |
| `SearchStage[O]` | `Search` | `Where`, `GroupBy`, ordering, locks, reads |
| `FilteredStage[O]` | `Where` + `Search` | `GroupBy`, ordering, locks, reads |
| `GroupedStage[O]` / `SearchGroupedStage[O]` | `GroupBy` | `Having`, result ordering, set operations (not after `Search`), reads |
| `HavingStage[O]` / `SearchHavingStage[O]` | `Having` | result ordering, set operations (not after `Search`), reads |
| `OrderedStage[O]` → `LimitedStage[O]` → `OffsetStage[O]` | `OrderBy`, `Limit`, `Offset` on table rows | locks, reads |
| `OrderedResultStage[O]` → `LimitedResultStage[O]` | the same after grouping or a set operation | reads (no locks) |
| `LockedStage[O]` | `ForUpdate` / `ForShare` | `NoWait`, `SkipLocked`, reads |
| `CompoundStage[O]` | `Union`, `Intersect`, `Except` and their `All` forms | more set operations, result ordering, reads |
| `QueryStage[O]` | every complete stage | `Build`, `MustBuild`, `SQL` and the read methods |

The capability interfaces they are composed of — `tsq.Sortable[O]`, `tsq.ResultSortable[O]`,
`tsq.Lockable[O]`, `tsq.Combinable[O]`, `tsq.Groupable[O]` — are what a helper can accept to add
one clause. All are sealed. `Build()` returns `(*tsq.Query[O], error)`; `MustBuild()` panics on an
error and is meant for package-level queries, which are built once at init.
