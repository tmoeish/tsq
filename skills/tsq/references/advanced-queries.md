# Advanced queries

Aliases and rebinding, grouping, subqueries, `EXISTS`, correlated subqueries, CTEs, set operations,
`DISTINCT` and row locks. The builder and read methods are in `queries.md`; conditions and functions
in `expressions.md`; which engine runs what in `dialects.md`.

## Aliases and `Rebind`

`TableXxx.As("alias")` names a second reference to a table, with every column bound to it:

```go
manager := database.TableUser.As("manager")
query := tsq.Select(database.TableUser.ID).
	From(database.TableUser).
	InnerJoin(manager, database.TableUser.ManagerID.EQ(manager.ID))
```

A statement by condition (`UpdateTable`, `DeleteFrom`) writes the table itself and refuses an
alias.

`col.Rebind(source)` returns the column bound to another source that has a column of the same name:
an alias, a CTE, a derived table. It returns a `tsq.Column`; for a `tsq.NullColumn`, use
`tsq.RebindNull(col, source)`, which keeps it nullable (so it can still `SetNull` and read into a
nullable field). A derived expression cannot be rebound: rebind the column first, then apply
functions to it.

## Grouping and aggregates

- aggregate queries with `GroupBy(...)` and `Having(...)`. `Build()` refuses a selected, `HAVING` or `ORDER BY` column that is neither grouped nor inside an aggregate (SQLite would return an arbitrary row's value, PostgreSQL refuses it); grouping by a table's primary key allows the table's other columns, and a column read only inside a grouped expression is grouped (`GroupBy(tsq.Upper(note))` allows `Having(tsq.Upper(note).NE(...))`). MySQL recognizes a grouped expression only where the select list or `ORDER BY` repeats it whole, and PostgreSQL compares expressions with their parameters, so one with a bound value (`tsq.Add(qty, tsq.Val(10))`) is another expression each time it is written; TSQ writes such an occurrence as `MAX(expression)` on those engines, which within a group is the expression's one value, so the query runs on all three. An occurrence inside an aggregate is left as written (`tsq.Count(tsq.Upper(note))` is valid as it is), and so is one inside a function TSQ does not write itself (an aggregate from `Expr`) or inside a subquery
- `tsq.SelectDistinct(cols...)` is `SELECT DISTINCT` (its `Count()` counts distinct rows), and
  `tsq.CountDistinct(col)` is `COUNT(DISTINCT col)`
- `Having(cond, more...)` follows `GroupBy` only; after grouping, `OrderBy` / `Limit` take result
  order terms and the query can no longer lock rows

```go
type OrgSize struct {
	OrgID int64
	Users int64
}

sizes, err := tsq.Select(
	tsq.MapInto(database.TableUser.OrgID, func(r *OrgSize) *int64 { return &r.OrgID }),
	tsq.MapInto(tsq.Count(database.TableUser.ID), func(r *OrgSize) *int64 { return &r.Users }),
).
	From(database.TableUser).
	GroupBy(database.TableUser.OrgID).
	Having(tsq.Count(database.TableUser.ID).GT(tsq.Val(int64(10)))).
	List(ctx, runtime)
```

## Subqueries

A stage from `tsq.SelectValue` is a subquery of its values, typed by them, and goes wherever a value
of that type does: the right of a comparison, `Between`, `In`, `Set`, `Case`. It needs no `Build`:
the enclosing query builds it and reports its errors.

```go
dataTrackIDs := tsq.SelectValue(database.TableTrack.ID).
	From(database.TableTrack).
	Where(database.TableTrack.Name.EQ(tsq.Val("Data & AI")))

courses, err := tsq.Select(database.TableCourse.Columns()...).
	From(database.TableCourse).
	Where(database.TableCourse.TrackID.In(dataTrackIDs)).
	List(ctx, db)
```

A built `*Query` from `SelectValue` works the same way. A subquery used as a value selects one
column; a query selecting a table's rows has the table's row type and does not compile there. Any
stage or query goes to `tsq.Exists` / `tsq.NotExists`, whatever it selects.

- subqueries such as `In(subquery)`, `tsq.Exists(subquery)`, and typed RHS comparisons like `EQ(subquery)` or `tsq.Like(col, subquery)`. An `In` subquery may set `Limit` (it is written as a derived table, which MySQL requires). A subquery cannot use `Search`: the keyword is an argument of the statement that runs, so `Build()` refuses it instead of dropping the predicate

`tsq.Exists(sq)` / `tsq.NotExists(sq)` take any stage or query, whatever it selects.

## Correlated subqueries

A subquery may reference a column of an enclosing query's table, but it has to declare that table first with `Correlate(...)`. Without the declaration the build fails with `table X is referenced but is not in this query's FROM/JOIN; join it, or, if it belongs to an enclosing query, declare it with Correlate(X)`, because every table a query mentions must otherwise be in that query's own `FROM`/`JOIN` graph.

```go
sub := tsq.Select(database.TableOrder.ID).
	From(database.TableOrder).
	Correlate(database.TableUser).
	Where(database.TableOrder.UserID.EQ(database.TableUser.ID))
// ... then: tsq.NotExists(sub)
// SELECT ... FROM users WHERE NOT EXISTS (
//   SELECT orders.id FROM orders WHERE orders.user_id = users.id)
```

Rules that go with it:

- `Correlate(...)` sits before `Where(...)`, on the same stage as the join methods
- a table that this query also puts in its own `FROM`/`JOIN` clause cannot be correlated. That combination is a build error: the local table would shadow the outer one and the predicate would quietly stop being correlated, evaluating the same for every outer row. Do not resolve the join-graph error by joining the outer table in
- a query built with `Correlate(...)` only makes sense inside an enclosing query. Executing it on its own (`List`, `Get`, `Count`, `Exists`, `Page`, ...) is refused, because its SQL references a table its own `FROM` clause does not introduce
- the outer table must actually be in scope at the point where the subquery is used, which SQL, not TSQ, decides

The older workaround, rewriting `NOT EXISTS` as `NotIn(subquery)`, still works and is often the better plan on MySQL. It is equivalent only when the subquery column cannot be `NULL`: in SQL three-valued logic `NOT IN` over a result set containing `NULL` returns no rows at all, while the correlated `NOT EXISTS` returns the non-matching rows. Filter the `NULL`s out in the subquery when the column is nullable. TSQ refuses `NotIn` over a subquery whose value can be NULL at build time; filter the NULLs out with `Coalesce` or use `NotExists`.

## CTEs

```go
big := tsq.CTE("big_orders", tsq.Select(database.TableOrder.Columns()...).
	From(database.TableOrder).
	Where(database.TableOrder.Total.GT(tsq.Val(int64(10000)))))

rows, err := tsq.Select(database.TableUser.Columns()...).
	From(database.TableUser).
	InnerJoin(big, database.TableUser.ID.EQ(database.TableOrder.UserID.Rebind(big))).
	List(ctx, runtime)
```

- non-recursive CTEs: `cte := tsq.CTE("big_orders", stage)`, then join `cte` and reference its columns with `col.Rebind(cte)` (all built-in dialects; MySQL baseline is 8.0). A CTE's columns are found by name, so its select list cannot name one column twice: `SUM(amount)` and `MAX(amount)` are both `amount`, and `Build()` refuses them. Whether `col.Rebind(cte)` can be NULL follows the CTE body, not the column: a nullable column the CTE coalesces reads into a plain field
- `tsq.CTE(name, query)` returns a `tsq.Table`; the body is any stage or built query. A body cannot
  use `Correlate` (a CTE does not see the enclosing query)
- `dialect.CapabilityCTE`: all three engines

## Set operations

```go
active := tsq.Select(database.TableUser.Columns()...).From(database.TableUser).
	Where(database.TableUser.Status.EQ(tsq.Val("active")))
admins := tsq.Select(database.TableUser.Columns()...).From(database.TableUser).
	Where(database.TableUser.Role.EQ(tsq.Val("admin")))

rows, err := active.Union(admins).OrderBy(database.TableUser.ID.Asc()).List(ctx, runtime)
```

- `Union`, `UnionAll`, `Intersect`, `IntersectAll`, `Except`, `ExceptAll` take another stage or
  built query of the same row type and return a `tsq.CompoundStage[O]`, which can combine further,
  order, limit and run
- set operations such as `UNION`, `INTERSECT`, and `EXCEPT` (all built-in dialects; MySQL needs 8.0.31+). `IntersectAll` / `ExceptAll` run on MySQL and PostgreSQL; SQLite has no `ALL` form and returns `UnsupportedCapabilityError` (`dialect.CapabilityIntersectAll` / `CapabilityExceptAll`)
- ordering a combined result, operand shape and evaluation order are in `queries.md` § Ordering,
  limits and locks: operands select the same fields in the same order, carry no `OrderBy` / `Limit` /
  lock of their own, and a chain reads left to right

## Row locks

`ForUpdate()` / `ForShare()` lock the rows a query reads, optionally followed by `NoWait()` (fail at
once on a locked row; `tsq.IsTxConflictError` reports it) or `SkipLocked()` (leave locked rows out).

```go
err := runtime.WithTx(ctx, func(ctx context.Context, tx tsq.Executor) error {
	job, err := tsq.Select(database.TableJob.Columns()...).From(database.TableJob).
		Where(database.TableJob.State.EQ(tsq.Val("queued"))).
		OrderBy(database.TableJob.ID.Asc()).Limit(1).
		ForUpdate().SkipLocked().
		Find(ctx, tx)
	// ...
	return err
})
```

- a lock only means something inside a transaction (`transactions.md`); outside one it is released
  when the statement ends
- a lock comes last, after `OrderBy` / `Limit` / `Offset`, and only on table rows: not after
  `GroupBy`, `Having`, a set operation, `SelectDistinct` or an aggregate, and not over a `LEFT`,
  `RIGHT` or `FULL` join (PostgreSQL cannot lock the side that can be NULL); `Build()` refuses those
- SQLite has no row locks: `dialect.CapabilityForUpdate`, `CapabilityForShare`,
  `CapabilityNoWait` and `CapabilitySkipLocked` are unsupported there and the query fails when it
  runs with `*dialect.UnsupportedCapabilityError`. Use `dialect.Supports` to choose another plan
