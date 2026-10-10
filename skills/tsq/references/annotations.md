# Annotations: `//tsq:` directives and struct tags

Everything `tsq gen` reads from your source: the directive lines above a struct, and the `db`,
`tsq` and `json` tags on its fields. What gets generated from them is in `generated-code.md`; how
managed columns behave when rows are written is in `writes.md`.

TSQ reads `//tsq:` directive lines above a struct. They follow the `//go:` convention: no space
after the slashes, one concern per line, and gofmt leaves them alone.

```go
//tsq:table name=course pk=ID
//tsq:managed created_at
//tsq:unique Title
//tsq:index TrackID
//tsq:search Title,Summary
type Course struct {
	ID      int64  `db:"id"`
	TrackID int64  `db:"track_id"`
	Title   string `db:"title,size:160"`
	Summary string `db:"summary,size:4096"`
}
```

All field references are **Go struct field names**, not SQL column names. The SQL column still comes
from the field's `db` tag.

Directives go on struct types, and a table or result is one concrete struct: a directive on another
kind of type, on a generic struct, or on a field inside the struct body (only the comments above the
type are read), is an error. A struct that embeds another embeds it by value
(`Base`, not `*Base`: a nil embedded pointer would make every generated accessor panic), and what it
embeds must be a struct TSQ can read. Other structs in the package are left alone, whatever their
fields. A field cannot be named like a method TSQ generates on the row (`Insert`, `Update`,
`HardDelete`, and on a soft-delete table `Delete`, `Restore`, `IsDeleted`), nor like a generated table
method (`GetByX`, `FindByX`, `FetchByX`, `FullTextX`); rename the Go field and keep
the column with the `db` tag. The row type cannot declare those methods itself either, a method you
write on the generated `XTable` cannot be named like one generated there (`As`, `WithDeleted`,
`GetByX`, `FindByX`, `FullTextX`), and nothing in
the package can already be named like what `tsq gen` declares (`TableX`, `XTable`, `ResultX`,
`XResult`, `TSQTables`); `tsq gen` says where the clash is. In a `type ( ... )` group, write the
directive on the type it is for: one above the group belongs to no type and is an error. A package
with no directive, or a table or result with no column field, is an error rather than an empty
output.

## Which fields are columns

- a table field is a column when it has a `db` tag; a result field when it has a `tsq` tag. An
  untagged field is not part of the table (it can hold anything), and `db:"-"` leaves a field out
  explicitly
- the column name is the `db` tag's name, before the first comma (`db:"email,size:160"` → `email`)
- the **JSON name** of a column is the field's `json` tag name, or the Go field name when there is
  none. It is what `PageRequest.OrderBy` accepts beside the column name, and what keyset cursors and
  result projections use (`paging-search.md`)
- an embedded struct's tagged fields are columns of the table, in the place it is embedded
- the struct's fields, in declaration order, are the table's column order


## The directives

| directive | purpose |
| --- | --- |
| `//tsq:table [name=X] [pk=Field] [assigned]` | declares a physical table. Required once per table struct |
| `//tsq:result` | declares a projection that is not a table. Required once per result struct; it takes no options |
| `//tsq:managed role[=Field] ...` | enables managed columns: `version`, `created_at`, `updated_at`, `deleted_at` |
| `//tsq:fulltext Field[,Field] [name=X]` | a full-text index over string fields, searched with `tsq.Matches` |
| `//tsq:unique Fields[,Fields] [name=X]` | a unique index |
| `//tsq:index Fields[,Fields] [name=X]` | a non-unique index |
| `//tsq:search Fields[,Fields]` | the columns generated keyword search covers |

Repeat `//tsq:unique` and `//tsq:index` for each index. `//tsq:managed` may be repeated or take
several roles on one line.

## `//tsq:table`

- `name=` is the physical table name; it defaults to the struct name in snake_case
- `pk=` is the primary-key **Go field**; it defaults to `ID`
- one field only: composite primary keys are not supported in v5, and `pk=A,B` is an error. Give
  the table a single-column key (usually an auto-increment `ID`) and declare the natural key with
  `//tsq:unique A,B`, which also generates `TableXxx.GetByAAndB`, `FindByAAndB` and `FetchByAAndB`
- the primary key is auto-increment unless the line says `assigned`, which means the caller supplies
  the value and a zero primary key is not filled in by the database. The database generates integer
  keys only, so a string key must say `assigned`

```go
//tsq:table                          // table "user", pk ID, auto-increment
//tsq:table name=enrollment pk=UID   // table "enrollment", pk UID, auto-increment
//tsq:table pk=Code assigned         // the application assigns Code
```

## `db` tags: column name, type, size, default, generated

Examples:

```go
Name  string `db:"name"`
Email string `db:"email,size:160"`
Meta  SkillItems `db:"skill_items,type:JSON"`
```

Rules:

- `db:"col"` keeps the default DDL mapping for that Go field type
- `string`, `sql.NullString`, `null.String`, and their type alias / custom string forms default to `VARCHAR(255)` when `size` is omitted. A value written to the column (by `Insert`, `Update`, `Upsert`, the `Batch*` methods, or `Set(col, tsq.Val(v))` / `Set(col, param)`) is held to that length in characters on every engine: MySQL (in strict mode) and PostgreSQL refuse a longer one themselves, and SQLite, which enforces no length, is told by TSQ before the statement runs, so what passes on a SQLite development database passes in production. A `type:` column and a size past `VARCHAR` (a `TEXT` family type) are not held. A comparison is not either: a longer value matches nothing, and that is an answer. A `json.RawMessage` that is not JSON is refused the same way on every engine (MySQL and PostgreSQL refuse it, SQLite would store the text)
- `int`, `uint`, and enum-like custom types built on them default to regular integer width; `int64` / `uint64` map to big-integer types. PostgreSQL has no unsigned types, so an unsigned field takes the next wider one there (`uint16` an `INTEGER`, `uint32` a `BIGINT`, `uint64` a `NUMERIC(20)`). An auto-increment key there is the `SERIAL` of that width, and a `uint64` key a `BIGSERIAL`, the widest a sequence counts. SQLite's integers are signed 64-bit, so a `uint64` above `math.MaxInt64` cannot be written there (`database/sql` refuses the value). Every engine keeps a column to its field's range: MySQL by its `UNSIGNED` types and widths, PostgreSQL and SQLite by a `CHECK` constraint TSQ writes with the column, named `ck_<column>`, where the engine's type is wider than the field (an unsigned field on PostgreSQL; on SQLite, whose `INTEGER` is 64 bits whatever the field, every integer field but `int64`). A statement that computes a value outside it (`Set(t.Stock, tsq.Sub(t.Stock, qty))` below zero, a product past the field's width) is refused, as MySQL refuses it, instead of storing a value the field could not read back. The constraint is part of the declared column: a table from before it was written is a mismatch under `Validate`, `Reconcile` adds it (and is refused where a row is already outside the range), and a migration written by `tsq gen` carries it in `CREATE TABLE`. A raw `type:` column, a key and a generated column carry none. When the column changes type, the constraint is dropped before the change and added back where the new type needs one, since PostgreSQL reads it against the new type. Putting the bound in the statement's `Where` (`t.Stock.GTE(qty)`) is still what tells a sold-out row from an updated one
- `db:"col,size:N"` sets an explicit string width, or for `[]byte` the size MySQL picks `BLOB`,
  `MEDIUMBLOB` or `LONGBLOB` by; on any other field (a number, a bool, a time) it is an error, since
  it would be read as a width the column does not have. The options are `size:N`, `type:SQL`, `default:SQL` and
  `generated[:SQL]`; anything else, or one without a usable value, is an error
- a `[]byte` field (or a named byte-slice type such as `json.RawMessage`) is a NOT NULL binary
  column, and an unset (nil) one is written as empty bytes; `sql.Null[[]byte]` is the nullable form. Empty bytes are
  not JSON, so an unset or empty `json.RawMessage` is written as the JSON `null`, the document `encoding/json`
  writes for a nil one, and reads back as `null`
- a `[N]byte` field (or a named array type such as `type UUID [16]byte`), the shape a key or a hash is
  kept in, needs a `type:` (`BINARY(16)`, `BYTEA`, ...), and is written and read as bytes on every
  engine: `*UUID` and `sql.Null[[16]byte]` are its nullable forms, and a stored value of another
  length is a read error rather than a truncated key. A type with `Value` / `Scan` methods of its
  own (`uuid.UUID`) keeps them
- what the engines part on, which TSQ leaves to them: a float that is not a number or infinite is refused by MySQL, stored by PostgreSQL, and on SQLite `Inf` is stored while `NaN` is NULL (refused in a NOT NULL column); a string holding a NUL byte is refused by PostgreSQL and stored by the other two; MySQL stores a JSON document in a form of its own (keys sorted, a space after each colon and comma), the others as written
- `db:"col,type:SQL_TYPE"` sets an explicit raw SQL type override for DDL generation and runtime schema metadata.
  It names the column, not how the value travels: a field database/sql cannot carry (an `any` or
  other interface, a struct without `Value` and `Scan` methods) is refused whatever it says
- `db:"col,default:SQL"` gives the column a DDL `DEFAULT SQL` **and** leaves it to the database when
  the field holds NULL: an `INSERT` of such a row omits the column, and a single-row `Insert` reads
  the value back into the field (with `RETURNING` in the `INSERT` itself on PostgreSQL and SQLite). The field must be able to hold NULL (`*T`, `sql.Null[T]`, ...), and
  `tsq gen` refuses `default:` on one that cannot: a zero value is a value, so `false` or `0` is
  written, and only nil leaves the column to the default. `default:CURRENT_TIMESTAMP` on a time column
  stores the current **UTC** time on every dialect, like every time TSQ writes; a boolean default may be
  written as `1` / `0` or `TRUE` / `FALSE`, and is spelled as each engine takes it. A string default is
  SQL as written: a quote is doubled (`default:'it''s'`), and a backslash is an escape on MySQL (unless
  the session runs `NO_BACKSLASH_ESCAPES`) but a plain character on PostgreSQL and SQLite, so the same
  declaration stores different values there and no check notices; `tsq gen` warns about it. `default:''` is a
  default (the empty string), distinct from none: dropping it from the declaration is a change that
  the generated migration and `Reconcile` carry out with `DROP DEFAULT`
- `db:"col,generated:SQL"` declares a column the database computes:
  `GENERATED ALWAYS AS (SQL) STORED` in the DDL, never written by `Insert`, `Update` or `Upsert`, and
  read back after a single-row `Insert`. `db:"col,generated"` without an expression says the same
  about a column whose schema comes from migrations: the DDL TSQ writes leaves it out with a comment,
  and a runtime policy that would create the table refuses to, since it cannot write the column
- a database-filled column cannot be the primary key or a managed column (`version`, `created_at`,
  `updated_at`, `deleted_at`): those are TSQ's to write, and `tsq gen` refuses it
- a batch insert does not read database-filled values back; that would be one query per row. Reload
  the rows when the values matter. Rows that leave different `default:` columns to the database go in
  different statements; only neighbouring rows share one, so generated keys follow the slice's order
- a generated column that exists is never altered: every dialect reports its expression differently,
  so the schema policies do not compare it, and a changed expression is a migration you write. One
  that is declared and missing from the table is a missing column like any other: `Validate` reports
  it, and `CreateMissing` and `Reconcile` add it, computed for the rows present (SQLite rebuilds the
  table for it, since it adds a generated column only as `VIRTUAL`). A `generated` column without an
  expression cannot be added by TSQ, and a table that lacks it fails startup under those policies
- a type that implements `driver.Valuer` or `sql.Scanner` (a JSON slice, a UUID, a nullable wrapper) **must** declare `type:`: what it stores is up to its `Value` method, which neither its Go type nor its underlying type tells, and `tsq gen` refuses to guess. A named basic type such as `type Level int` needs neither method, because database/sql stores it as its underlying type, and its column type is derived (a named bool included: MySQL and SQLite report a boolean as an integer, and TSQ reads it into the named type). The nullable wrappers of `database/sql` are derived too, each as the nullable form of its value: `sql.NullString`, `sql.NullInt64`, `sql.NullInt32`, `sql.NullInt16`, `sql.NullByte`, `sql.NullFloat64`, `sql.NullBool`, `sql.NullTime` and `sql.Null[T]`
- a codec type that is a nullable form (a pointer, or a struct with a `Valid bool` and a `Scan` method) is a `NullColumn` in Go and a column that accepts NULL in the DDL
- `type:` is emitted verbatim to generated dialect DDL, so only reuse the same value across dialects when that is actually correct
- dialects may still choose a more suitable large-text type for oversized strings; for example, MySQL upgrades very large strings to `MEDIUMTEXT` / `LONGTEXT`, and PostgreSQL writes `TEXT` above 10485760 characters, the longest `VARCHAR` it declares
- TSQ never creates a MySQL `TEXT` or `TINYTEXT` column for a string, so a live one matches only a column declared `type:TEXT` / `type:TINYTEXT`; declared as a string of some size it is reported as a mismatch, which `Reconcile` alters

## `//tsq:managed`

Each word enables one managed role, optionally naming the Go field that carries it:

```go
//tsq:managed version created_at updated_at deleted_at
//tsq:managed updated_at=MTime
```

Default Go field names are `Version`, `CreatedAt`, `UpdatedAt` and `DeletedAt`. Omitting a role
leaves it unset; there is no "off" spelling because omission is the off.

What each role does when rows are written is in `writes.md` § Managed columns.

Supported field types:

- `version`: an integer field
- `created_at`, `updated_at`: `time.Time`, `*time.Time`, `sql.NullTime`, `null.Time` (or a type alias of one, `type Stamp = time.Time`, which is the type it names)
- `deleted_at`: `int64`, `uint64`, `*time.Time`, `sql.NullTime`, `null.Time`

## `//tsq:unique` and `//tsq:index`

```go
//tsq:unique Email
//tsq:unique Email name=ux_user_email
//tsq:index OrgID,Status
```

- the field list is comma-separated and its order is the index order; a space after a comma is fine
- MySQL limits an index key to 3072 bytes and counts a string column at 4 bytes a character, and it
  cannot index a `TEXT` column at all. PostgreSQL and SQLite accept such an index, so `tsq gen`
  generates it and warns: it prints the reason, and `mysql.sql` carries it as a comment above the
  statement that will fail there (`VARCHAR(2000)` alone exceeds the limit). MySQL also limits a row to 65535
  bytes and counts every `VARCHAR` at its longest: a string of `size:16383` fills the row by itself, and
  several long strings pass the limit together (error 1118 on `CREATE TABLE`). `tsq gen` warns the same
  way; a `size:` above 16383 makes the column a `TEXT`, which is kept off the row. For a schema that runs
  on MySQL, lower the `size:` or leave the column out of the index. MySQL also limits a row to 65535 bytes across its `VARCHAR`
  columns: several strings of a few thousand characters, or one of `size:16383` beside any other column, fail
  `CREATE TABLE` there (error 1118), and `type:TEXT` or a `size:` above 16383 (a `MEDIUMTEXT`) keeps the value out of the row.
  InnoDB limits a row's page as well, to 8126 bytes with the default 16 KiB page: a string column of over 40 bytes counts 40
  there (the rest goes off the page), so a table of some two hundred short strings fails `CREATE TABLE` the same way, and
  `tsq gen` warns about that too
- `name=` is optional; an omitted name is derived from the table and the indexed **columns**:
  `//tsq:unique SKU` over the column `sku` of `products` is `ux_products_sku`
- a field repeated inside one index is invalid, and so are two indexes over the same field list
- on a table declaring `deleted_at`, a unique index needs an integer tombstone (`int64` / `uint64`):
  `tsq gen` refuses a nullable time `deleted_at` beside one, since NULL never collides and the index
  would not keep live values unique
- on such a table a unique or plain index leads with `deleted_at`, so a deleted row does not hold a
  unique value; a full-text index covers exactly the fields listed

## `//tsq:search`

```go
//tsq:search Name,Email
```

It declares the columns keyword search matches: `TableXxx.Query()` searches them when given
`tsq.Keyword(term)`. It belongs to a table: a
result generates no query to put the search in, so `tsq gen` refuses it there. A query that selects
into a result calls `Search(...)` itself.

## `//tsq:result`

A result is a struct a query reads into without being a table: a join row, an API shape, a report
line. Each field maps one table column with a `tsq` tag naming the **table struct and its Go field**:

```go
//tsq:result
type ProductListing struct {
	ProductID    int64           `json:"product_id"    tsq:"Product.ID"`
	ProductName  string          `json:"product_name"  tsq:"Product.Name"`
	CategoryName string          `json:"category_name" tsq:"Category.Name"`
	OrderID      sql.Null[int64] `json:"order_id"      tsq:"Order.ID"` // from a LEFT JOIN: nullable
}
```

- `//tsq:result` takes no options; `//tsq:managed`, indexes, `//tsq:search` and `//tsq:fulltext`
  belong to tables and are refused on a result
- the named table must be a `//tsq:table` struct **of the same package** (`tsq:"shop.Product.ID"`
  is refused), and the field one of its columns; a result cannot name a field of another result,
  which has no columns of its own. A result field with a `db` tag instead of a `tsq` tag is an error;
  an untagged field is not read
- the result field's type is the column's value type, or a nullable form of it (`sql.Null[T]`,
  `*T`, `sql.NullX`) — required for a column of a table on the optional side of an outer join,
  since a query refuses to read a value that can be NULL into a field that cannot hold it
- the result's JSON names come from its own `json` tags
- what a result does not cover — an expression, an aggregate — is projected by hand with
  `tsq.MapInto` (`expressions.md`); prefer a generated result when the shape is stable and reused
