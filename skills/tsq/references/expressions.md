# Expressions: conditions, values, parameters and functions

Everything that goes inside `Where`, `Having`, `Set`, `OrderBy` and the select list: predicates,
combining conditions, fixed values and parameters, nullable columns, column functions, `CASE`,
custom SQL, projecting expressions into result fields, and the compile errors that point at a
mistake. The builder itself is in `queries.md`.

## Predicates

Every column and expression has these methods; the right-hand side is an operand of the same type
(`tsq.Val(v)`, a parameter, another column or expression, a typed subquery):

| method | SQL |
| --- | --- |
| `EQ(x)`, `NE(x)` | `=`, `<>` |
| `GT(x)`, `GTE(x)`, `LT(x)`, `LTE(x)` | `>`, `>=`, `<`, `<=` |
| `Between(lo, hi)`, `NotBetween(lo, hi)` | `BETWEEN` / `NOT BETWEEN` (inclusive) |
| `In(list)`, `NotIn(list)` | `IN` / `NOT IN` over `tsq.Vals(...)`, a list parameter or a subquery |
| `IsNull()`, `IsNotNull()` | `IS NULL` / `IS NOT NULL` |
| `Pred(format, args...)` | custom SQL, see "Custom expressions and predicates" |
| `Asc()`, `Desc()` | order terms (`queries.md`), with `.NullsFirst()` / `.NullsLast()` |

Text columns (`tsq.Text`: any `~string`, nullable ones included) also take the package functions
`tsq.StartsWith`, `tsq.EndsWith`, `tsq.Contains` and `tsq.NotStartsWith`, `tsq.NotEndsWith`,
`tsq.NotContains`, whose pattern (`tsq.Pattern[S]`: a `tsq.Val` or a parameter) is matched
literally, and `tsq.Like` / `tsq.NotLike`, whose pattern is used as written. `tsq.Matches(index,
term)` is full-text search (`paging-search.md`), `tsq.Exists(sq)` / `tsq.NotExists(sq)` test a
subquery (`advanced-queries.md`).

All of these return a `tsq.Condition`.

Common examples:

```go
database.TableUser.ID.EQ(tsq.Val(int64(1)))
tsq.Contains(database.TableUser.Name, tsq.Val("alice"))
tsq.Like(database.TableUser.Email, tsq.Val("%@example.com")) // pattern as written; text columns only
database.TableUser.ManagerID.IsNull()
```

## Combine conditions

The builder is stage-based: `Where(...)` appears at most once per chain (the type system enforces this at compile time). Put all filter logic in a single `Where(...)` call:

```go
Where(
	database.TableUser.OrgID.EQ(tsq.Val(int64(1))),
	tsq.Or(
		tsq.Contains(database.TableUser.Name, tsq.Val("alice")),
		tsq.Contains(database.TableUser.Email, tsq.Val("alice")),
	),
)
```


- `tsq.And(conds...)` and `tsq.Or(conds...)` group conditions; `tsq.Not(cond)` negates one
- `tsq.And()` with no arguments is always true — it is how an `UPDATE` / `DELETE` over every row is
  spelled (`Where(tsq.And())`) — and `tsq.Or()` is always false
- a list built at run time goes in as `Where(tsq.And(conds...))`
- the negation of `In` over an empty list (`tsq.Not(col.In(empty))`) matches everything

## Parameters

A value that is only known when the query runs is a **parameter**. Every generated column has one:

```go
var QueryUsersByOrg = tsq.
	Select(database.TableUser.Columns()...).
	From(database.TableUser).
	Where(database.TableUser.OrgID.EQ(database.TableUser.OrgID.Param())).
	MustBuild()

users, err := QueryUsersByOrg.List(ctx, runtime, database.TableUser.OrgID.Bind(orgID))
```

- `col.Param()` is the column's parameter and `col.Bind(v)` supplies it; `col.ListParam()` and
  `col.BindList(vs...)` are the list form for `In` / `NotIn`
- a query that compares one column to two values declares its own:
  `low, high := tsq.NewParam[int64]("low"), tsq.NewParam[int64]("high")`, then
  `Where(col.Between(low, high))` and `List(ctx, db, low.Bind(1), high.Bind(9))`
- a `Param[T]` is an RHS, so it goes wherever a column of the same type could: `EQ`, `GT`, `tsq.Like`,
  `Between`, `Set`, `Case().When`. A `ListParam[T]` goes to `In` / `NotIn`
- values are matched **by parameter, not by position**, and `Bind` only accepts a `T`: the order of
  the arguments does not matter and a value of the wrong type does not compile
- a missing value, a value for a parameter the statement does not use, and two values for one
  parameter are errors at execution
- `tsq.StartsWith(col, p)` / `tsq.EndsWith` / `tsq.Contains` (and their `Not` forms) take a
  `Param` or a `tsq.Val` of the column's string type (`tsq.Pattern[S]`) and match it literally:
  `%` and `_` are escaped
- a parameter bound to `nil` is an error; use `IsNull()` / `IsNotNull()`

- `tsq.NewListParam[T]("name")` is the list form of `tsq.NewParam`; bind it with `p.Bind(vs...)`.
  Parameters are compared by identity, so declare each once (a package-level `var`) and use that
  value both in the query and when binding

## Expressions and columns

A **column** (`tsq.Column[O, T]`, or `tsq.NullColumn[O, T]`) belongs to a row type: it knows which
field of `O` it scans into, which is what `Select` needs. Everything built from one — a function, a
`CASE`, `Expr`/`Exprf` — is an **expression** (`tsq.Expression[T]`), which has the same predicates,
`Asc`/`Desc` and `Pred`, but no row of its own:

```go
tsq.Upper(database.TableUser.Name)                 // tsq.Expression[string]
tsq.Count(database.TableOrder.ID)                  // tsq.Expression[int64]
tsq.Case(cond, tsq.Val("x"))./* ... */.End()  // tsq.Expression[string]
```

An expression cannot be passed to `Select`, because what it holds has nothing to do with the field
its source column scans into: `tsq.Date(TableUser.CreatedAt)` holds text while `CreatedAt` is a
`time.Time`, and selecting it used to compile and then fail scanning. Say where the value goes:

```go
// Into a field of a result type.
tsq.Select(tsq.MapInto(tsq.Upper(database.TableUser.Name), func(r *Row) *string { return &r.Name }))

// Or on its own, when the value is the whole row. n is *int64.
n, err := tsq.SelectValue(tsq.Count(database.TableOrder.ID)).From(database.TableOrder).MustBuild().Get(ctx, db)

// SUM over no rows is NULL, so SelectValue refuses it; SelectNullValue reads a *sql.Null[int64].
total, err := tsq.SelectNullValue(tsq.Sum(database.TableOrder.Amount)).From(database.TableOrder).MustBuild().Get(ctx, db)
```

`SelectValue` / `SelectNullValue` build ordinary queries: `Where`, `OrderBy`, `Page`, `List` and
the rest work as usual, with the value as the row type, and a `SelectValue` stage is also a subquery
of its values (see `advanced-queries.md` § Subqueries). Only columns have `Rebind`, `Param` and `Bind`;
rebind a column before applying functions to it.

## Nullable columns

A field that can hold NULL — a pointer, `sql.NullString` and the other `sql.NullX`,
`sql.Null[T]`, `null.String` and the other nullbio types — is generated as a
`tsq.NullColumn[Xxx, T]`, where `T` is the value it holds when it is not NULL:

```go
// Inside newUserTable, which declares the table as t; generated code does this.
Nickname: tsq.NewNullColumn[string](t, "nickname", "nickname",
	func(r *User) *sql.NullString { return &r.Nickname }),
```

- `col.Rebind(alias)` returns a `Column`; `tsq.RebindNull(col, alias)` keeps it a `NullColumn`

- it compares with its value type like any column: `TableUser.Nickname.EQ(tsq.Val("ada"))`,
  `tsq.Upper(TableUser.Nickname)`, `tsq.Contains(TableUser.Nickname, tsq.Val("a"))`. NULL rows never match a
  comparison; `IsNull()` / `IsNotNull()` find them
- `tsq.UpdateTable(t).SetNull(col)` writes NULL and only takes a `NullColumn`. `Set` on a NOT NULL
  column refuses a value that can be NULL (a nullable column, a scalar subquery); wrap it in
  `tsq.Coalesce`
- a hand-written `tsq.NewColumn` over a nullable field type is a definition error that names the
  `NewNullColumn[T]` to use instead

**Reading a value that can be NULL needs a field that can hold it.** A query knows when a
selected value can be NULL:

- a `NullColumn`
- any column of a table on the optional side of an outer join: the joined table of a `LEFT JOIN`,
  everything before a `RIGHT JOIN`, both sides of a `FULL JOIN`
- `SUM`, `AVG`, `MAX`, `MIN` without `GROUP BY` (they are NULL over no rows); `COUNT` never is
- `NullIf`, a `CASE` without `Else`, a scalar subquery, and any function of a value that can be NULL.
  `tsq.Coalesce(x, y)` is NULL only if both can be

Reading such a value into a field that cannot hold NULL (`MapInto`, or a NOT NULL table column)
fails with an error naming the column and the reason, **before** the query runs, rather than with a
scan error on the first NULL row. The fixes are, in order of preference: an inner join where the
row always exists, `tsq.Coalesce(x, tsq.Val(...))`, or a nullable field with
`tsq.MapIntoNull(source, func(r *R) *sql.NullString { ... })`. Building such a query is
fine, since a subquery or CTE never reads its rows. A generated result does the same: give the
field a nullable form of the column's value type (`sql.Null[string]`, `*string`, `sql.NullString`)
for a column of a `LEFT JOIN`ed table, and `tsq gen` projects it with `MapIntoNull`.

`tsq.SelectValue` refuses a value that can be NULL the same way; `tsq.SelectNullValue` reads it as
a `sql.Null[T]`.

## `CASE`, `COALESCE`, `NULLIF`

```go
bucket := tsq.Case(database.TableOrder.Total.GTE(tsq.Val(int64(10000))), tsq.Val("large")).
	When(database.TableOrder.Total.GTE(tsq.Val(int64(1000))), tsq.Val("medium")).
	Else(tsq.Val("small")).
	End() // tsq.Expression[string]
```

- `tsq.Case(cond, result)` takes the first branch, which fixes the result type `T`; it returns a
  `tsq.CaseStage[T]` with `When(cond, result)`, `Else(result)` and `End()`. After `Else` only `End()`
  remains (`tsq.CaseElseStage[T]`). A branch of another type does not compile
- a `CASE` without `Else` can be NULL, so it reads only into a nullable field
- on PostgreSQL a `CASE` whose results are all bound values is cast to the result type, since
  PostgreSQL would read them as text
- `tsq.Coalesce(expr, fallback)` is NULL only where both are; with `tsq.Val(x)` or a NOT NULL column
  as the fallback it reads into a non-nullable field
- `tsq.NullIf(expr, value)` is NULL where they are equal

## Operand types

The types behind the operands, for helpers that take one:

| type | is |
| --- | --- |
| `tsq.Operand[T]` | anything that stands for one `T`: a column, expression, `tsq.Value[T]`, `tsq.Param[T]`, a value subquery |
| `tsq.ListOperand[T]` | anything that stands for a list of `T`: `tsq.ValueList[T]`, `tsq.ListParam[T]`, a subquery |
| `tsq.Pattern[S]` | a literal-match pattern: `tsq.Value[S]` or `tsq.Param[S]` |
| `tsq.MatchTerm` | a full-text term: a string `tsq.Value` or `tsq.Param` |
| `tsq.Condition` | the result of every predicate, `And`, `Or`, `Not` |
| `tsq.Arg` | an execution argument: `col.Bind(v)`, `p.Bind(v)`, `tsq.Keyword(term)` |
| `tsq.Number` | the constraint of numeric functions: every integer and float kind |
| `tsq.Text` | the constraint of string functions: `~string` |

`tsq.Value[T]` is what `tsq.Val` returns and `tsq.ValueList[T]` what `tsq.Vals` returns.

## Custom expressions and predicates

Use the escape hatches deliberately, not as a replacement for typed columns:

- `col.Pred(format, args...)` builds a condition. The first `%s` is the column; each further `%s`
  takes the next argument, which may be a column, a `Param`, a typed subquery or a plain value
  (bound). `%%` is a literal percent sign
- `col.Expr(format)` / `col.Exprf(format, args...)` build a derived column the same way
- the format text is emitted verbatim for every dialect, in parentheses, so an `OR` in a `Pred` stays
  one condition next to the others and the soft-delete filter; keeping the text portable is yours

## Values fixed in the code

`tsq.Val(v)` is a Go value that stands wherever a column of the same type could: `EQ`, `GT`,
`tsq.Like`, `Between`, `Set`, `Case` / `When` / `Else`, `tsq.Coalesce`, `tsq.NullIf`. `tsq.Vals(vs...)`
is the list form for `In` / `NotIn`. Both are always bound, never inlined into the SQL text, and so
are plain values passed to `Pred` / `Exprf`.

```go
database.TableUser.Name.EQ(tsq.Val("alice"))
database.TableUser.ID.In(tsq.Vals(ids...))            // ids is []int64
database.TableUser.Age.Between(tsq.Val(int64(18)), tsq.Val(int64(65)))
```

The value's type is inferred from the value alone, so an **untyped constant takes its default
type**: `tsq.Val(90)` is a `Value[int]`. Against an `int64` column write `tsq.Val(int64(90))`
(or `tsq.Val[int64](90)`); the mismatch does not compile:

```
tsq.Value[int] does not implement tsq.Operand[int64] (wrong type for method valueOfType)
        have valueOfType(int)
        want valueOfType(int64)
```

String constants, typed constants (`tsq.Val(StatusActive)`) and typed variables need no
conversion.

The compile errors name their fix. The method named in "missing method ..." or "wrong type for
method ..." is chosen to say what to write:

| the error says | write instead |
| --- | --- |
| `missing method needsTsqVal` | wrap the literal: `col.EQ(tsq.Val(x))`, `tsq.StartsWith(col, tsq.Val("x"))` |
| `missing method needsTsqVals` | wrap the slice: `col.In(tsq.Vals(ids...))` |
| `does not implement tsq.SearchColumn (missing method searchable)` | wrap the column: `Search(tsq.Searchable(col))` |
| `wrong type for method valueOfType` / `valuesOfType`, `have ... want ...` | give the value the column's type, `tsq.Val(int64(90))`, or compare with a column of that type |
| `missing method needsRuntimeOrWrapExecutor` | pass the `*tsq.Runtime`, the `WithTx` executor, or `tsq.WrapExecutor(db, dialect.X)` instead of a `*sql.DB` / `*sql.Tx` |
| `WhereStage ... has no field or method Where` | pass every condition to the one `Where(a, b, ...)`; `tsq.Or(...)` for OR |

- a `NULL` comparison is refused (use `IsNull()` / `IsNotNull()`); `NULL` is written with
  `SetNull`
- `tsq.Val` takes a Go value; passing a column or condition to it is an error, pass the
  expression itself


## Projecting into result fields: `MapInto`

`tsq.MapInto(source, fieldPointer)` projects any expression into a field of a result type, and
`tsq.MapIntoNull(source, fieldPointer)` into a field that can hold NULL (a nullable form of the
source's value type). Generated result code is made of these, chosen by the field type.

A projection's JSON name, which `PageRequest.OrderBy` and keyset cursors sort by, is the source
column's. Rename it with `.Named("user_name")` when the result's field is called something else and
clients sort by it.

## Column functions

Functions are package-level and constrained by the column's Go type, so applying one to a column it
does not fit does not compile:

| function | accepts | returns |
| --- | --- | --- |
| `tsq.Count`, `tsq.CountDistinct` | any column | `int64` |
| `tsq.Max`, `tsq.Min` | any column | the column's type |
| `tsq.Sum`, `tsq.Ceil`, `tsq.Floor`, `tsq.Abs`, `tsq.Round(col, digits)` | `tsq.Number`: integer and float kinds (a `NullColumn[O, int64]` is numeric too) | the column's type |
| `tsq.Avg` | `tsq.Number` | `float64` |
| `tsq.Add(a, b)`, `tsq.Sub`, `tsq.Mul`, `tsq.Div`: `b` is a column, `Param`, `tsq.Val` or subquery of the same type | `tsq.Number` | the column's type |
| `tsq.Upper`, `tsq.Lower`, `tsq.Trim`, `tsq.Substring(col, start, length)` | `tsq.Text`: string kinds, nullable ones included | the column's type |
| `tsq.Length` | `tsq.Text` | `int64` |
| `tsq.Date` | `time.Time` columns, nullable or not | `string` (`'YYYY-MM-DD'`) |
| `tsq.Year`, `tsq.Month`, `tsq.Day` | `time.Time` columns, nullable or not | `int64` |
| `tsq.StartsWith(col, pattern)`, `EndsWith`, `Contains` and `Not` forms, where `pattern` is `tsq.Val(s)` or a `Param` | `tsq.Text` | a condition |
| `tsq.Like(col, pattern)`, `tsq.NotLike`: the pattern as written, wildcards included | `tsq.Text` | a condition |

```go
type NameCount struct {
	Name  string
	Count int64
}

upper := tsq.Upper(database.TableUser.Name)
tsq.Select(
	tsq.MapInto(upper, func(r *NameCount) *string { return &r.Name }),
	tsq.MapInto(tsq.Count(database.TableUser.ID), func(r *NameCount) *int64 { return &r.Count }),
).
	From(database.TableUser).
	GroupBy(upper)
```

Each runs on all three dialects and returns the same value; TSQ spells it per dialect where they
differ:

- `Div` of integers truncates toward zero everywhere: `DIV` on MySQL, whose `/` returns a decimal, and
  `DIV()` on PostgreSQL, where an operand can be `NUMERIC` (a `SUM`, a `uint64` column).
  Division by zero is NULL on MySQL and SQLite and an error on PostgreSQL, so a quotient whose
  divisor is not a non-zero `tsq.Val` can be NULL: read it with `MapIntoNull` / `SelectNullValue`,
  or `Coalesce` it
- `Length` counts characters (MySQL's `LENGTH` counts bytes, so it is `CHAR_LENGTH` there)
- `Substring` uses a 1-based start
- `Round` rounds a tie away from zero on every dialect (`2.5` is `3`, `-2.5` is `-3`): PostgreSQL and MySQL round a
  floating-point column through an exact decimal, since PostgreSQL has no `ROUND(double precision, n)` and MySQL
  rounds a `DOUBLE` to the nearest even digit (`2.5` is `2`). A value that is a tie only as it is written
  is where the engines part: `1.005` is stored as `1.00499999999999989`, which PostgreSQL and MySQL round to
  `1.01` (they round what is written) and SQLite to `1.0` (it rounds what is stored). Keep amounts that
  must round one way in a `DECIMAL` column or as an integer of the smallest unit
- `Round`, `Ceil` and `Floor` of an integer are the integer itself, exact at any size and still an integer
  to `Div`: no engine function is called
- `Avg` of an integer column is a `float64` with all its digits; MySQL's own `AVG` of integers keeps four
  decimals (`1.6667`), so the column is averaged as a `DOUBLE` there
- `Max` / `Min` take any column type, and what can be ordered is the engine's to say. Over a boolean they
  answer "is any row true" and "is every row true" on all three engines (`BOOL_OR` / `BOOL_AND` on
  PostgreSQL, which has no `MAX(boolean)`). A time read through `Max`, `Min` or `Coalesce` is a `time.Time`
  on SQLite too
- `Coalesce(col, fallback)` is NULL only where both are: with `tsq.Val(x)` or a NOT NULL column as the
  fallback it reads into a field that cannot hold NULL
- `Ceil` / `Floor` work on every SQLite build: they are written without SQLite's math functions, which
  `mattn/go-sqlite3` leaves out unless built with `-tags sqlite_math_functions`. The answer is a
  floating-point value there as on the other engines, so `Div(Ceil(a), Floor(b))` keeps its fraction

A table's search columns must be string-kind: `tsq.Searchable(col)` is how a `TableSpec` lists
them, and `//tsq:search` / `//tsq:fulltext` accept `string` fields and named types whose underlying type is `string`.

