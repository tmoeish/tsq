# Paging and search

Offset paging (`Page`), HTTP page requests (`PageRequest`), keyset paging (`PageKeyset`), keyword
search (`Search` + `tsq.Keyword`) and full-text search (`//tsq:fulltext` + `tsq.Matches`). The
builder they run on is in `queries.md`.

## Offset paging: `Page`

`query.Page(ctx, db, paging, args...)` takes a typed `tsq.Paging`:

```go
page, err := database.TableUser.Query().Page(ctx, runtime, tsq.Paging{
	Page:    2,
	Size:    50,
	OrderBy: []tsq.OrderBy{database.TableUser.Name.Asc()},
}, tsq.Keyword("alice"))
```

- `Page` below 1 means 1 and is capped at `tsq.MaxPageNumber`; `Size` 0 means 20 and is capped
  by the runtime's `WithMaxPageSize`
- `OrderBy` is built from columns, so a sort field that does not exist does not compile
- the result is a `*tsq.Page[O]` with `Page`, `Size`, `Total`, `TotalPages` and `Data`
  (never nil), plus `HasNext()`
- `Total` and `Data` come from one snapshot: `Page` runs its count and its rows in a read-only
  transaction (`REPEATABLE READ` on MySQL and PostgreSQL), so a concurrent write cannot make them
  disagree. Called with a transaction executor, it uses that transaction and its isolation

The types, for handlers that return them:

```go
type Paging struct { Page, Size int; OrderBy []tsq.OrderBy }
type Page[T any] struct {
	Page       int   `json:"page"`
	Size       int   `json:"size"`
	Total      int64 `json:"total"`
	TotalPages int64 `json:"total_pages"`
	Data       []*T  `json:"data"`
}
type PageRequest struct { // json and query tags: size, page, order_by, order, keyword, after
	Size, Page                   int
	OrderBy, Order, Keyword, After string
}
type PageRequestError struct { Field, Reason string } // answer it with 400
```

`tsq.MaxPageNumber` is 1000000 and `tsq.DefaultMaxPageSize` 1000.

An HTTP endpoint receives strings. `tsq.PageRequest` is that shape (`page`, `size`, `order_by`,
`order`, `keyword`), and `Paging(sortable...)` turns it into a `Paging` against the columns the
endpoint allows to sort by. It also carries the request's `keyword`, which `Page` searches with, so
the handler does not pass it again:

```go
paging, err := req.Paging(database.TableUser.Name, database.TableUser.CreatedAt)
if err != nil {
	return err // 400: a *tsq.PageRequestError (negative page or size, unknown or ambiguous sort field, bad direction, NUL in the keyword, bad cursor)
}

resp, err := database.TableUser.Query().Page(ctx, runtime, paging)
```

- `order_by` is a comma-separated list of column or JSON names; `order` is `asc` / `desc`, one
  per field or one for all
- the sortable list is explicit: selecting a column does not make it sortable, since sorting
  on an unindexed column is a cost the endpoint decides to pay
- the carried keyword applies only to a query built with `Search`; an endpoint that does not
  search ignores it, as it ignores any parameter it does not know. Passing a different
  `tsq.Keyword` as well is an error
- `Paging` (and `Keyset`) is where the request is validated: a negative page or size, a page
  above `tsq.MaxPageNumber`, an order other than `asc` / `desc`, or a keyword holding a NUL byte
  (PostgreSQL refuses it in any text parameter; the others match nothing) is an error. Zero means the first
  page and the default size (20). A size above the runtime's `WithMaxPageSize` (default
  `tsq.DefaultMaxPageSize`) is not an error: `Page` serves the capped size and says so in `Size`
- parsing the request out of a query string is the caller's job; the struct's `query` and `json`
  tags cover the usual binders

## Keyset paging

Offset paging reads and discards every row before the page, and a row inserted meanwhile shifts
the pages. `query.PageKeyset` pages by position instead:

```go
k := tsq.Keyset{
	Size:    50,
	OrderBy: []tsq.OrderBy{database.TablePost.CreatedAt.Desc(), database.TablePost.ID.Desc()},
	After:   req.After, // the previous page's Next; empty for the first page
}

page, err := database.TablePost.Query().PageKeyset(ctx, runtime, k, tsq.Keyword(term))
// page is a *tsq.KeysetPage[Post]: Size, Data (never nil), Next ("" on the last page), HasNext()
```

`req.Keyset(sortable...)` builds the `Keyset` from a `PageRequest` (its `order_by`, `size` and `after`)
and, like `Paging`, carries its keyword, so `PageKeyset(ctx, runtime, k)` searches with it.

- `OrderBy` is required, every column in it must be selected by the query, and it must **include
  the primary key of every table in the query**, the FROM table and each joined one, so that every
  position is unique: in a one-to-many join the parent's key repeats, and seeking past it would skip
  the rest of its children. A source without a primary key (a CTE) cannot be keyset-paged. An index
  on the order columns is what makes it fast
- the query must not set its own `OrderBy`, `Limit` or `Offset`, and must not group, aggregate,
  use `DISTINCT` or set operations
- `Next` is an opaque string carrying the last row's order values; it is refused for a different
  `OrderBy`. Order columns must never be NULL (a `NullColumn` or an outer-joined column is refused), and must be columns the query selects: an expression over one (`tsq.Upper(t.Title)`) is not a selected column and is refused as such
- there is no `Total`: not counting is the point. Use `Count` if the endpoint needs one
- mixed directions work; the condition is spelled `a < ? OR (a = ? AND b > ?)`
- over HTTP, `PageRequest` carries `after`, and `req.Keyset(sortable...)` resolves it like
  `Paging`; append the primary keys to the resolved `OrderBy` yourself


## Full-text search

`//tsq:fulltext Title,Summary` declares a full-text index; `tsq.Matches` searches it:

```go
tsq.Select(database.TableCourse.Columns()...).
	From(database.TableCourse).
	Where(tsq.Matches(database.TableCourse.FullTextTitleAndSummary(), tsq.Val(term)))   // or a Param
```

- the generated `FullTextX()` method (named after the index's fields) returns the index; a
  hand-written table calls `FullText("index_name")`, which fails the query when there is no such index
- the term is a `tsq.Val` or a `Param` of a string, bound like any value
- the index is created by the schema policies, and compared **by name only**: PostgreSQL indexes an
  expression, MySQL reports another index type, and SQLite has none, so comparing columns would ask
  to rebuild it on every boot
- **what "match" means is the dialect's own**: MySQL runs `MATCH ... AGAINST` in natural language
  mode, PostgreSQL compares `to_tsvector('simple', ...)` against `plainto_tsquery` (every word must
  appear), and SQLite, which has no index TSQ manages, matches the term as a **substring** of any
  indexed column. Ranking and operator syntax are not portable.
  `dialect.Supports(runtime.Dialect(), dialect.CapabilityFullTextSearch)` says which kind a
  deployment gets, so a test on SQLite can still exercise the query path
- the term is words on every dialect, so a search box's text can be passed as it is: operator
  characters (`+ - " ( ) ~ < > @ *`) are plain text in MySQL's natural language mode and in
  `plainto_tsquery`, never a syntax error. On MySQL the asterisks are removed first
  (`AGAINST (REPLACE(?, '*', '') ...)`): they mean nothing there, yet InnoDB's parser refuses a
  term where one stands alone or follows a phrase
- the fields must be plain `string` columns

## Keyword search

A query built with `Search(cols...)` is searched by passing `tsq.Keyword(term)` with the other
arguments, to any read: `List`, `Iter`, `Count`, `Page`, `ListIn`, `Get`. A row matches when any of
the search columns contains the term. An empty term searches nothing, so a search box can pass its
value as it is; a non-empty term on a query without `Search` is an error.

```go
rows, err := database.TableUser.Query().List(ctx, runtime, tsq.Keyword("alice"))
for row, err := range database.TableUser.Query().Iter(ctx, runtime, tsq.Keyword(term)) { ... }
```

The term is escaped for LIKE wildcards, so `%`, `_` and the escape character itself are matched literally on every supported dialect; the keyword still matches as a substring. The generated predicate carries an explicit `ESCAPE '~'` clause, because SQLite has no default LIKE escape character. A backslash in a keyword is an ordinary character.

The pattern functions (`tsq.StartsWith`, `tsq.EndsWith`, `tsq.Contains`, and their `Not` forms, with a `Val` or a `Param`) escape wildcards the same way. `Like` takes a pattern as written, wildcards included, so escaping one is the database's business: a backslash escapes on MySQL and PostgreSQL and does nothing on SQLite. Wildcard escaping is about matching the right rows, not SQL injection protection — that comes from parameter binding.

Case sensitivity is the database's, and it differs: SQLite ignores ASCII case, MySQL follows the column's collation (the default `_ci` collations ignore case), PostgreSQL respects case. Keyword search and the pattern functions all behave this way. For one answer on every dialect, match `tsq.Lower(col)` against a lowercased term.
