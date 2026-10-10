# Generated code

What `tsq gen` declares in Go, how to use it, and how to write the same thing by hand when a table
is not generated. The directives that drive it are in `annotations.md`; the files themselves and
the `.sql` / `tsq.json` outputs are in `cli.md`.

## Table files (`<struct>.tsq.go`)

From each `//tsq:table` struct `tsq gen` generates:

- `XxxTable`, a struct that embeds `*tsq.TableOf[Xxx, K]` (K is the primary key's type), or
  `*tsq.SoftDeleteTableOf[Xxx, K]` when the struct declares `deleted_at`, and has one
  field per column, in the order the struct declares them (an embedded struct's fields where it is
  embedded): `TableXxx.ID`, `TableXxx.Name`. A NOT NULL field is a `tsq.Column[Xxx, T]` and a
  field that can hold NULL a `tsq.NullColumn[Xxx, T]` (see `expressions.md` § Nullable columns)
- `TableXxx`, the table value. `TableXxx.Columns()` lists every column, for `tsq.Select`
- `TableXxx.As(alias)`, which returns an `XxxTable` with every column bound to the alias, and
  `TableXxx.WithDeleted()` on a soft-delete table, which returns an `XxxTableWithDeleted`: the same
  columns, `As` and full-text indexes, and no `GetByX` / `FindByX` / `FetchByX`. A soft-delete
  table's unique indexes include `deleted_at`, so a value is unique only among live rows, and the
  lookups by it are not generated there. The table's own `GetBy` / `FindBy` / `FetchBy` still compile
  on it and refuse such a lookup when they run. Read deleted rows by primary key or with `Select`
- per unique index, `TableXxx.GetByEmail(ctx, db, email)` (one row, `sql.ErrNoRows` when there is
  none), `TableXxx.FindByEmail(ctx, db, email)` (`nil, nil` when there is none) and
  `TableXxx.FetchByEmail(ctx, db, emails...)`; a composite index `A,B` gives
  `GetByAAndB(ctx, db, a, b)`, `FindByAAndB` and `FetchByAAndB(ctx, db, a, bs...)`. A plain `//tsq:index` is a
  schema object only; a query on it has an ordering, a limit and a page size the generator cannot
  guess, so write it with the builder
- per full-text index, `TableXxx.FullTextTitleAndSummary()`, named after its fields, to pass to
  `tsq.Matches`
- row methods: `Insert`, `Update`, `HardDelete`, and `Delete()` / `Restore()` / `IsDeleted()` on
  soft-delete tables. A table without `deleted_at` has no `Delete`: removing a row always says
  `Hard`
- the errors returned by `Update`, `Delete` and `HardDelete` name the row by its primary key; they
  never serialize the row, so column values do not leak into logs

Reads by primary key and unique column (`Get`, `Find`, `Fetch`, `GetBy`, `FindBy`, `FetchBy`) and
`TableXxx.Query()` are methods of the embedded table; they are described in `queries.md` § Reading by
key.

On a table that declares `deleted_at`, deleted rows are out of scope for **every** query and
statement that names the table, generated or hand-written (see `queries.md` § Soft-delete scope).
Reading them is an audit-time need, and `WithDeleted()` says so:

```go
// Deleted rows included.
var EveryEnrollment = tsq.
	Select(database.TableEnrollment.Columns()...).
	From(database.TableEnrollment.WithDeleted()).
	MustBuild()
```

## Result files (`<struct>.result.tsq.go`)

From each `//tsq:result` struct:

- `XxxResult`, a struct with one `tsq.ResultColumn` field per result field, each mapped onto its
  source column (`ResultXxx.LearnerName`)
- `ResultXxx`, the projection value; select it with `tsq.Select(ResultXxx.Columns()...)`


```go
type ProductListingResult struct {
	ProductID   tsq.ResultColumn[ProductListing, int64]
	ProductName tsq.ResultColumn[ProductListing, string]
}

var ResultProductListing = ProductListingResult{
	ProductID: tsq.MapInto(TableProduct.ID, func(r *ProductListing) *int64 { return &r.ProductID }).Named("product_id"),
	// ...
}

rows, err := tsq.Select(shop.ResultProductListing.Columns()...).
	From(shop.TableProduct).
	InnerJoin(shop.TableCategory, shop.TableProduct.CategoryID.EQ(shop.TableCategory.ID)).
	List(ctx, runtime) // []*shop.ProductListing
```

A result column is a projection, not a table column: it has `Asc()` / `Desc()` (to order a set
operation or a grouped query by it) and `Named(json)`, but no predicates. Filter with the table's
columns.

## `runtime.tsq.go`

```go
func TSQTables() []tsq.Table // every table of the package, for tsq.Open / tsq.NewRuntime
```

Pass it to the runtime so the schema policies know the tables (`runtime.md`); concatenate the slices
of several generated packages: `slices.Concat(users.TSQTables(), billing.TSQTables())`.

## What a generated table looks like

```go
type CourseTable struct {
	*tsq.TableOf[Course, int64]

	ID    tsq.Column[Course, int64]
	Title tsq.Column[Course, string]
}

var TableCourse = newCourseTable()

func newCourseTable() CourseTable {
	t := tsq.NewTable[Course, int64]("course")
	c := CourseTable{
		TableOf: t,
		ID:      tsq.NewColumn(t, "id", "id", func(r *Course) *int64 { return &r.ID }),
		Title:   tsq.NewColumn(t, "title", "title", func(r *Course) *string { return &r.Title }),
	}

	t.Define(tsq.TableSpec[Course, int64]{
		Columns:       []tsq.BoundColumn[Course]{c.ID, c.Title},
		PrimaryKey:    c.ID,
		AutoIncrement: true,
		Search:        []tsq.SearchColumn{tsq.Searchable(c.Title)},
		ColumnSpecs:   []dialect.ColumnSpec{ /* ... */ },
		Indexes:       []tsq.IndexSpec{ /* ... */ },
	})

	return c
}
```

A row type describes one table: `Define` refuses a second table (another name) over the same `R`,
because a row read from one would be checked against the other's columns when it is saved. Give an
archive or shard table its own type, such as `type OrderArchive Order`.

`TableCourse` is the table's name, columns, key, managed columns, search columns, physical
schema and indexes in one value, and it is what queries select from (`From(TableCourse)`) and
statements write (`tsq.UpdateTable(TableCourse)`). The row struct itself carries no TSQ methods
besides the generated `Insert` / `Update` / `HardDelete` (and, on a soft-delete table, `Delete` /
`Restore` / `IsDeleted`), which delegate to the table.

In a hand-written `TableSpec`, `ColumnSpecs` (when given) must list every column, and mark the
primary key and auto-increment exactly as `PrimaryKey` and `AutoIncrement` do; `Define` reports a
disagreement, since the schema is what `SchemaPolicyCreateMissing` builds. A column held in a type
database/sql cannot carry (an `any` or other interface, a struct, map or array that has no `Value`
and `Scan` methods; `[N]byte` and `time.Time` carry themselves) is refused by `Define`, as `tsq gen`
refuses the field. A table that was never
made this way (a zero `tsq.TableOf` value, or a nil pointer to one) is an error from every method
that runs or builds something, from `Open`, and from `Err()`; its accessors return nothing.

One function creates the table, its columns and its definition, so anything that names
`TableCourse` is initialized after the table is complete; there is no declaration order to get
right. A table written by hand follows the same shape. A soft-delete table starts with
`tsq.NewSoftDeleteTable[R, K](name)`, binds its columns to the embedded `t.TableOf`, and passes
the tombstone column to `Define`:

```go
t := tsq.NewSoftDeleteTable[Enrollment, int64]("enrollment")
c := EnrollmentTable{
	SoftDeleteTableOf: t,
	ID:                tsq.NewColumn(t.TableOf, "id", "id", func(r *Enrollment) *int64 { return &r.ID }),
	DeletedAt:         tsq.NewColumn(t.TableOf, "deleted_at", "deleted_at", func(r *Enrollment) *int64 { return &r.DeletedAt }),
}

t.Define(tsq.TableSpec[Enrollment, int64]{ /* ... */ }, c.DeletedAt)
```

Each constructor refuses the other's shape: `TableOf.Define` on a soft-delete table, or
`SoftDeleteTableOf.Define` without a `deleted_at` column, is a definition error.

A column field cannot share a name with a method of the table (`Update`, `Query`, `Columns`,
`As`, ...): `tsq gen` refuses it and names the field. Rename the Go field; the `db` tag keeps the
column name.

`TableOf` also exposes `TableName()`, `Columns()`, `ColumnSpecs()`, `Indexes()`,
`As(alias)` and `Err()`, which reports a definition error such as a primary key that is not one of
the columns. `SoftDeleteTableOf` adds `WithDeleted()`.

## Column and table types

The interfaces a signature can name, from the most general:

| type | is | where it shows up |
| --- | --- | --- |
| `tsq.SQLColumn` | anything selectable; `Name()` is its column name | `GroupBy`, `PageRequest.Paging(sortable...)` |
| `tsq.ValueColumn[T]` | a selectable value holding a `T` | `SelectValue`, `MapInto` sources |
| `tsq.BoundColumn[O]` | a selectable that scans into row `O` | `Select(cols...)`, `TableSpec.Columns`, `Update(..., cols...)`, `OnConflict` |
| `tsq.TypedColumn[O, T]` | scans into `O` and holds a `T` | `AttachMany` / `AttachOne` parent keys |
| `tsq.Expression[T]` | a value with predicates, `Asc` / `Desc`, `Pred` / `Expr` | function results, `CASE` (`expressions.md`) |
| `tsq.Column[O, T]` | a table column: an expression bound to row `O`, with `Param`, `ListParam`, `Bind`, `BindList`, `Rebind` | `TableXxx.Name` |
| `tsq.NullColumn[O, T]` | a nullable table column, compared by `T` | nullable fields; `SetNull` |
| `tsq.ResultColumn[O, T]` | a projection into a result field | `ResultXxx.Field`, `tsq.MapInto` |
| `tsq.SearchColumn` | a string column wrapped by `tsq.Searchable(col)` | `Search(...)`, `TableSpec.Search` |
| `tsq.Table` | a query source: a table, an alias or a CTE; `TableName()` | `From`, joins, `Correlate` |
| `tsq.RowTable[R]` | a writable table of rows `R` | `tsq.UpdateTable`, `tsq.HardDeleteFrom` |
| `tsq.SoftDeleteTable[R]` | a writable table of rows `R` that declares `deleted_at` | `tsq.DeleteFrom` |

They are sealed: only TSQ's own constructors implement them, so a helper can accept them but not
fake them.

`tsq.FullTextIndex` is the value `TableXxx.FullTextX()` / `t.FullText(name)` returns, for
`tsq.Matches` (`paging-search.md`).

## Writing a table by hand

Generated code uses only exported API, so a table can be declared without `tsq gen` (a table of
another tool's package, a test fixture). The shape is the one above:

- `tsq.NewTable[R, K](name)` / `tsq.NewSoftDeleteTable[R, K](name)` create the descriptor
- `tsq.NewColumn(t, column, jsonName, func(r *R) *T { return &r.F })` declares a NOT NULL column;
  `tsq.NewNullColumn[T](t, column, jsonName, func(r *R) *F { return &r.F })` a nullable one, where
  `F` is the nullable field type (`*T`, `sql.Null[T]`, `sql.NullString`, `null.String`, ...) and `T`
  the value it holds. `NewColumn` over a nullable field type is a definition error naming the
  `NewNullColumn[T]` to use
- `t.Define(tsq.TableSpec[R, K]{...})` (or `Define(spec, deletedAtColumn)` on a soft-delete table)
  completes it. The `TableSpec` fields:

| field | meaning |
| --- | --- |
| `Columns` | every column, in table order |
| `PrimaryKey` | the key column, a `Column[R, K]` |
| `AutoIncrement` | the database generates the key (an integer key) |
| `Version`, `CreatedAt`, `UpdatedAt` | the managed columns, or nil (`writes.md` § Managed columns) |
| `Search` | the keyword-search columns, each `tsq.Searchable(col)` |
| `ColumnSpecs` | the physical schema, `[]dialect.ColumnSpec`, for the schema policies; optional |
| `Indexes` | `[]tsq.IndexSpec{{Name, Columns, Unique, FullText}}`, for the schema policies |

`dialect.ColumnSpec` describes one column independently of the engine: `Name`, `Type`
(`dialect.ColumnType`), `PrimaryKey`, `AutoIncrement`, `Default` (SQL text), `Fill` and
`Generated` (the expression of a generated column). `ColumnType` is `Kind` (a `dialect.ColumnKind`:
`dialect.ColumnKindBool`, `ColumnKindBytes`, `ColumnKindFloat`, `ColumnKindInt`,
`ColumnKindString`, `ColumnKindTime`), `Bits` (integer or float width), `Unsigned`, `Nullable`,
`Size` (string or bytes length) and `RawType` (a `type:` override, used verbatim). `Fill` says who
writes the column on insert: `dialect.FillCaller` (the row's value), `dialect.FillDefault` (the
database default when the field is nil) or `dialect.FillGenerated` (the database, always).

`TableOf.ColumnSpecs()` and `Indexes()` return what a table declares; a hand-written variant of a
generated table can start from them (copy, change one column, `Define` a new table).
