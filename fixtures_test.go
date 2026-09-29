package tsq

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
)

// The fixtures are declared the way generated code declares tables.

type user struct {
	ID        int64
	Name      string
	Email     string
	Version   int64
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt int64
}

var usersHandle = NewSoftDeleteTable[user, int64]("users")

var (
	User_ID        = NewColumn(usersHandle.TableOf, "id", "id", func(r *user) *int64 { return &r.ID })
	User_Name      = NewColumn(usersHandle.TableOf, "name", "name", func(r *user) *string { return &r.Name })
	User_Email     = NewColumn(usersHandle.TableOf, "email", "email", func(r *user) *string { return &r.Email })
	User_Version   = NewColumn(usersHandle.TableOf, "version", "version", func(r *user) *int64 { return &r.Version })
	User_CreatedAt = NewColumn(usersHandle.TableOf, "created_at", "created_at", func(r *user) *time.Time { return &r.CreatedAt })
	User_UpdatedAt = NewColumn(usersHandle.TableOf, "updated_at", "updated_at", func(r *user) *time.Time { return &r.UpdatedAt })
	User_DeletedAt = NewColumn(usersHandle.TableOf, "deleted_at", "deleted_at", func(r *user) *int64 { return &r.DeletedAt })
)

var Users = usersHandle.Define(TableSpec[user, int64]{
	Columns:       []BoundColumn[user]{User_ID, User_Name, User_Email, User_Version, User_CreatedAt, User_UpdatedAt, User_DeletedAt},
	PrimaryKey:    User_ID,
	AutoIncrement: true,
	Version:       User_Version,
	CreatedAt:     User_CreatedAt,
	UpdatedAt:     User_UpdatedAt,
	Search:        []SearchColumn{Searchable(User_Name), Searchable(User_Email)},
	ColumnSpecs: []tsqdialect.ColumnSpec{
		{Name: "id", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64}, PrimaryKey: true, AutoIncrement: true},
		{Name: "name", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindString, Size: 64}},
		{Name: "email", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindString, Size: 128}},
		{Name: "version", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64}},
		{Name: "created_at", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindTime}},
		{Name: "updated_at", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindTime}},
		{Name: "deleted_at", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64}},
	},
	Indexes: []IndexSpec{{Name: "ux_users_email", Columns: []string{"email", "deleted_at"}, Unique: true}},
}, User_DeletedAt)

var User__Cols = Users.Columns()

type order struct {
	ID     int64
	UserID int64
	Amount int64
	Note   string
}

var ordersHandle = NewTable[order, int64]("orders")

var (
	Order_ID     = NewColumn(ordersHandle, "id", "id", func(r *order) *int64 { return &r.ID })
	Order_UserID = NewColumn(ordersHandle, "user_id", "user_id", func(r *order) *int64 { return &r.UserID })
	Order_Amount = NewColumn(ordersHandle, "amount", "amount", func(r *order) *int64 { return &r.Amount })
	Order_Note   = NewColumn(ordersHandle, "note", "note", func(r *order) *string { return &r.Note })
)

var Orders = ordersHandle.Define(TableSpec[order, int64]{
	Columns:       []BoundColumn[order]{Order_ID, Order_UserID, Order_Amount, Order_Note},
	PrimaryKey:    Order_ID,
	AutoIncrement: true,
	ColumnSpecs: []tsqdialect.ColumnSpec{
		{Name: "id", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64}, PrimaryKey: true, AutoIncrement: true},
		{Name: "user_id", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64}},
		{Name: "amount", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64}},
		{Name: "note", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindString, Size: 255}},
	},
})

// namedTable is a two-column table with a caller-chosen name, for tests that need
// several distinct tables. A row type describes one table, so tests that need two
// of them at once use namedTableOf with another type.
type namedRow struct {
	ID   int64
	Name string
}

// Row types of the other tables tests declare with namedTableOf and wideTableOf.
type (
	serviceARow  namedRow
	serviceBRow  namedRow
	longNamedRow namedRow
	notesRow     wideRow
	orgsRow      wideRow
	longWideRow  wideRow
	thingsRow    wideRow
	cratesRow    wideRow
	gadgetsRow   wideRow
)

func namedTable(name string) *TableOf[namedRow, int64] {
	return namedTableOf(name, func(r *namedRow) (*int64, *string) { return &r.ID, &r.Name })
}

func namedTableOf[R any](name string, fields func(*R) (*int64, *string)) *TableOf[R, int64] {
	h := NewTable[R, int64](name)
	id := NewColumn(h, "id", "id", func(r *R) *int64 { p, _ := fields(r); return p })
	label := NewColumn(h, "name", "name", func(r *R) *string { _, p := fields(r); return p })

	return h.Define(TableSpec[R, int64]{
		Columns:       []BoundColumn[R]{id, label},
		PrimaryKey:    id,
		AutoIncrement: true,
		ColumnSpecs: []tsqdialect.ColumnSpec{
			{Name: "id", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64}, PrimaryKey: true, AutoIncrement: true},
			{Name: "name", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindString, Size: 64}},
		},
	})
}

// newSQLite opens a runtime over a fresh SQLite file with the fixture tables created.
func newSQLite(t *testing.T, options ...RuntimeOption) *Runtime {
	t.Helper()

	options = append([]RuntimeOption{WithSchemaPolicy(SchemaPolicyReconcile)}, options...)

	rt, err := Open(context.Background(), "sqlite", filepath.Join(t.TempDir(), "test.db"), []Table{Users, Orders}, options...)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	t.Cleanup(func() { _ = rt.Close() })

	return rt
}

// sqlOf renders a built query for d with args, failing the test on error.
func sqlOf[O any](t *testing.T, q *Query[O], d tsqdialect.Name, args ...Arg) (string, []any) {
	t.Helper()

	sql, values, err := q.SQL(d, args...)
	if err != nil {
		t.Fatalf("SQL() error = %v", err)
	}

	return sql, values
}

var (
	onSQLite   = tsqdialect.SQLite
	onMySQL    = tsqdialect.MySQL
	onPostgres = tsqdialect.Postgres
)

// wideRow scans any column into an untyped slot, for tables described only by
// their schema. A row type describes one table, so a test that needs a second
// table at once declares it over otherWideRow with wideTableOf.
type wideRow struct {
	Fields [16]any
}

// wideTable declares a table whose columns are names plus fields of schema.
func wideTable(name string, names []string, schema []tsqdialect.ColumnSpec, indexes []IndexSpec) *TableOf[wideRow, any] {
	return wideTableOf(name, names, schema, indexes, func(r *wideRow) *[16]any { return &r.Fields })
}

func wideTableOf[R any](name string, names []string, schema []tsqdialect.ColumnSpec, indexes []IndexSpec, slots func(*R) *[16]any) *TableOf[R, any] {
	h := NewTable[R, any](name)

	seen := map[string]bool{}
	all := []string{}

	for _, n := range append(append([]string{}, names...), specNames(schema)...) {
		if !seen[n] {
			seen[n] = true
			all = append(all, n)
		}
	}

	if !seen["id"] {
		all = append([]string{"id"}, all...)
	}

	cols := make([]BoundColumn[R], 0, len(all))

	var pk Column[R, any]

	for i, n := range all {
		c := NewColumn(h, n, n, func(r *R) *any { return &slots(r)[i] })
		cols = append(cols, c)

		if n == "id" {
			pk = c
		}
	}

	auto := false

	for _, s := range schema {
		if s.Name == "id" && s.AutoIncrement {
			auto = true
		}
	}

	return h.Define(TableSpec[R, any]{Columns: cols, PrimaryKey: pk, AutoIncrement: auto, ColumnSpecs: schema, Indexes: indexes})
}

func specNames(schema []tsqdialect.ColumnSpec) []string {
	names := make([]string, 0, len(schema))
	for _, s := range schema {
		names = append(names, s.Name)
	}

	return names
}

// newStrictMockTable declares a schema-less table with the given columns.
func newStrictMockTable(name string, fields ...string) (*TableOf[wideRow, any], []string) {
	return wideTable(name, fields, nil, nil), fields
}

func mustStrictMockTable(t *testing.T, name string, fields ...string) *TableOf[wideRow, any] {
	t.Helper()

	table, _ := newStrictMockTable(name, fields...)

	return table
}

// registered redeclares table with a schema and indexes.
func registered(table *TableOf[wideRow, any], schema []tsqdialect.ColumnSpec, indexes ...IndexSpec) Table {
	names := make([]string, 0, len(table.def.columns))
	for _, c := range table.def.columns {
		names = append(names, c.name)
	}

	return wideTable(table.TableName(), names, schema, indexes)
}

func newSQLiteIndexTestEngine(t *testing.T) (*Runtime, string) {
	t.Helper()

	dsn := filepath.Join(t.TempDir(), "test.db")

	rt, err := Open(context.Background(), "sqlite", dsn, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	t.Cleanup(func() { _ = rt.Close() })

	return rt, dsn
}

// mustWrap is WrapExecutor for a handle and engine the test knows are valid.
func mustWrap(t testing.TB, db DBTX, engine tsqdialect.Name) Executor {
	t.Helper()

	exec, err := WrapExecutor(db, engine)
	if err != nil {
		t.Fatal(err)
	}

	return exec
}
