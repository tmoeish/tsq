package tsq

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
)

// tag is a table keyed by a case-insensitive string, the shape where Go equality
// and the database's disagree.
type tag struct {
	Name  string
	Label string
}

type tagTable struct {
	*TableOf[tag, string]

	Name  Column[tag, string]
	Label Column[tag, string]
}

var tags = func() tagTable {
	t := NewTable[tag, string]("tags")
	c := tagTable{
		TableOf: t,
		Name:    NewColumn(t, "name", "name", func(r *tag) *string { return &r.Name }),
		Label:   NewColumn(t, "label", "label", func(r *tag) *string { return &r.Label }),
	}

	t.Define(TableSpec[tag, string]{
		Columns:    []BoundColumn[tag]{c.Name, c.Label},
		PrimaryKey: c.Name,
		ColumnSpecs: []tsqdialect.ColumnSpec{
			{Name: "name", Type: tsqdialect.ColumnType{RawType: "TEXT COLLATE NOCASE"}, PrimaryKey: true},
			{Name: "label", Type: tsqdialect.ColumnType{Kind: tsqdialect.ColumnKindString, Size: 64}},
		},
	})

	return c
}()

func newLookupRuntime(t *testing.T) *Runtime {
	t.Helper()

	rt, err := Open(context.Background(), "sqlite", filepath.Join(t.TempDir(), "lookup.db"),
		[]Table{Users, tags}, WithSchemaPolicy(SchemaPolicyCreateMissing))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = rt.Close() })

	return rt
}

func TestTableGetFindFetchByPrimaryKey(t *testing.T) {
	ctx := context.Background()
	rt := newLookupRuntime(t)

	rows := []*user{{Name: "a", Email: "a@x"}, {Name: "b", Email: "b@x"}, {Name: "c", Email: "c@x"}}
	if err := Users.BatchInsert(ctx, rt, rows); err != nil {
		t.Fatal(err)
	}

	got, err := Users.Get(ctx, rt, rows[1].ID)
	if err != nil || got.Name != "b" {
		t.Fatalf("Get = %+v, %v", got, err)
	}

	if _, err := Users.Get(ctx, rt, 999); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("Get(missing) = %v, want sql.ErrNoRows", err)
	}

	if found, err := Users.Find(ctx, rt, 999); found != nil || err != nil {
		t.Fatalf("Find(missing) = %v, %v; want nil, nil", found, err)
	}

	list, err := Users.Fetch(ctx, rt, rows[2].ID, rows[0].ID, rows[2].ID)
	if err != nil || len(list) != 3 || list[0].Name != "c" || list[1].Name != "a" || list[2].Name != "c" {
		t.Fatalf("Fetch keeps input order and repeats = %v, %v", list, err)
	}

	if _, err := Users.Fetch(ctx, rt, rows[0].ID, 998, 999); !errors.Is(err, sql.ErrNoRows) || !strings.Contains(err.Error(), "[998 999]") {
		t.Fatalf("Fetch(missing) = %v, want both missing keys and sql.ErrNoRows", err)
	}

	// A deleted row is out of scope unless the table is WithDeleted.
	if err := Users.Delete(ctx, rt, rows[0]); err != nil {
		t.Fatal(err)
	}

	if _, err := Users.Get(ctx, rt, rows[0].ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("Get(deleted) = %v, want sql.ErrNoRows", err)
	}

	if got, err := Users.WithDeleted().Get(ctx, rt, rows[0].ID); err != nil || got.DeletedAt == 0 {
		t.Fatalf("WithDeleted().Get = %+v, %v", got, err)
	}

	if n, err := Users.Query().Count(ctx, rt); err != nil || n != 2 {
		t.Fatalf("Query().Count = %d, %v; want the two live rows", n, err)
	}

	if list, err := Users.Query().List(ctx, rt, Keyword("b@")); err != nil || len(list) != 1 {
		t.Fatalf("Query() searches the declared search columns: %v, %v", list, err)
	}

	if err := Users.BatchHardDeleteByPK(ctx, rt, []int64{rows[1].ID, rows[2].ID}); err != nil {
		t.Fatal(err)
	}

	if n, err := Users.Query().Count(ctx, rt); err != nil || n != 0 {
		t.Fatalf("after BatchHardDeleteByPK Count = %d, %v", n, err)
	}
}

// TestFetchMatchesUnderTheDatabaseCollation is the reason fetchInOrder asks the
// database about a string it could not match in Go: "ADA" finds the row "ada" on a
// NOCASE column, and must not be reported missing.
func TestFetchMatchesUnderTheDatabaseCollation(t *testing.T) {
	ctx := context.Background()
	rt := newLookupRuntime(t)

	if err := tags.BatchInsert(ctx, rt, []*tag{{Name: "ada", Label: "A"}, {Name: "bob", Label: "B"}}); err != nil {
		t.Fatal(err)
	}

	list, err := tags.Fetch(ctx, rt, "BOB", "Ada")
	if err != nil || len(list) != 2 || list[0].Name != "bob" || list[1].Name != "ada" {
		t.Fatalf("Fetch under NOCASE = %v, %v", list, err)
	}

	list, err = tags.FetchBy(ctx, rt, tags.Name, []string{"ADA"}, tags.Label.EQ(Val("A")))
	if err != nil || len(list) != 1 || list[0].Label != "A" {
		t.Fatalf("FetchBy with a fixed condition = %v, %v", list, err)
	}

	if _, err := tags.FetchBy(ctx, rt, tags.Name, []string{"ADA"}, tags.Label.EQ(Val("B"))); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("FetchBy honours its conditions: %v", err)
	}

	if _, err := tags.Fetch(ctx, rt, "ada", "carol"); !errors.Is(err, sql.ErrNoRows) || !strings.Contains(err.Error(), "carol") {
		t.Fatalf("Fetch(missing) = %v", err)
	}
}

func TestAliasedTableBindsItsColumns(t *testing.T) {
	other := Users.As("u2")
	if other.TableName() != "u2" || Users.TableName() != "users" {
		t.Fatalf("alias names = %s / %s", other.TableName(), Users.TableName())
	}

	q := Select(User_ID).From(Users).
		InnerJoin(other, User_ID.EQ(User_ID.Rebind(other))).
		Where(User_Name.Rebind(other).IsNotNull()).
		MustBuild()

	sql, _ := sqlOf(t, q, onSQLite)
	if !strings.Contains(sql, `JOIN "users" AS "u2"`) || !strings.Contains(sql, `"u2"."name" IS NOT NULL`) {
		t.Fatalf("aliased join rendered %s", sql)
	}

	if self := Users.As("users"); self.aliased() || len(other.Columns()) != len(Users.Columns()) {
		t.Fatal("aliasing a table to its own name must leave it unaliased")
	}

	if _, err := UpdateTable(other).Set(User_Name, Val("x")).Where(And()).Build(); err == nil {
		t.Fatal("expected a statement by condition on an alias to be refused")
	}
}

// TestStatementsByConditionAcceptTableStructs covers the inference that lets
// tsq.UpdateTable(TableCourse) take a generated struct: RowTable for a plain
// table, and the SoftDeleteTable constraint DeleteFrom infers R through.
func TestStatementsByConditionAcceptTableStructs(t *testing.T) {
	for name, stage := range map[string]interface {
		Build() (*Mutation[tag], error)
	}{
		"update": UpdateTable(tags).Set(tags.Label, Val("x")).Where(tags.Name.EQ(Val("a"))),
		"hard":   HardDeleteFrom(tags).Where(tags.Name.EQ(Val("a"))),
	} {
		if _, err := stage.Build(); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}

	for name, stage := range map[string]interface {
		Build() (*Mutation[ticket], error)
	}{
		"soft":         DeleteFrom(tickets).Where(tickets.Body.EQ(Val("a"))),
		"soft deleted": DeleteFrom(tickets.WithDeleted()).Where(tickets.Body.EQ(Val("a"))),
		"hard":         HardDeleteFrom(tickets).Where(tickets.Body.EQ(Val("a"))),
	} {
		if _, err := stage.Build(); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// TestGetByCachesItsQuery covers what generated GetByX relies on: one query per
// column and soft-delete scope, reused across calls, and a column taken from an
// aliased table still reading the table itself.
func TestGetByCachesItsQuery(t *testing.T) {
	ctx := context.Background()
	rt := newLookupRuntime(t)

	row := &user{Name: "ada", Email: "ada@x"}
	if err := Users.Insert(ctx, rt, row); err != nil {
		t.Fatal(err)
	}

	for range 3 {
		got, err := Users.GetBy(ctx, rt, User_Email, "ada@x")
		if err != nil || got.ID != row.ID {
			t.Fatalf("GetBy = %+v, %v", got, err)
		}
	}

	cached := 0
	Users.keys.by.Range(func(any, any) bool { cached++; return true })

	if cached != 1 {
		t.Fatalf("GetBy cached %d queries, want 1", cached)
	}

	alias := Users.As("u")
	if got, err := alias.GetBy(ctx, rt, User_Email.Rebind(alias), "ada@x"); err != nil || got.ID != row.ID {
		t.Fatalf("GetBy through an alias = %+v, %v", got, err)
	}

	if _, err := Users.GetBy(ctx, rt, User_Email, "nobody@x"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("GetBy(missing) = %v, want sql.ErrNoRows", err)
	}

	// FindBy shares the cached query and reports a missing row as nil, nil.
	if got, err := Users.FindBy(ctx, rt, User_Email, "ada@x"); err != nil || got == nil || got.ID != row.ID {
		t.Fatalf("FindBy = %+v, %v", got, err)
	}

	if got, err := Users.FindBy(ctx, rt, User_Email, "nobody@x"); err != nil || got != nil {
		t.Fatalf("FindBy(missing) = %+v, %v; want nil, nil", got, err)
	}

	if err := Users.Delete(ctx, rt, row); err != nil {
		t.Fatal(err)
	}

	if _, err := Users.GetBy(ctx, rt, User_Email, "ada@x"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("GetBy(deleted) = %v, want sql.ErrNoRows", err)
	}

	// Deleted rows may repeat an email, so without the live scope the email alone
	// is no longer unique; fixing the tombstone makes it so again.
	if _, err := Users.WithDeleted().GetBy(ctx, rt, User_Email, "ada@x"); err == nil || !strings.Contains(err.Error(), "not unique") {
		t.Fatalf("WithDeleted().GetBy by email alone = %v; want it refused", err)
	}

	if got, err := Users.WithDeleted().GetBy(ctx, rt, User_Email, "ada@x", User_DeletedAt.EQ(Val(row.DeletedAt))); err != nil || got.ID != row.ID {
		t.Fatalf("WithDeleted().GetBy = %+v, %v", got, err)
	}
}

// TestLookupsNeedAUniqueKey covers GetBy, FindBy and FetchBy by a column that is
// not unique: GetBy returned one arbitrary row of several and FetchBy reported
// the others as missing, without an error.
func TestLookupsNeedAUniqueKey(t *testing.T) {
	ctx := context.Background()
	rt := newLookupRuntime(t)

	if err := Users.BatchInsert(ctx, rt, []*user{{Name: "ann", Email: "a1@x"}, {Name: "ann", Email: "a2@x"}}); err != nil {
		t.Fatal(err)
	}

	if _, err := Users.GetBy(ctx, rt, User_Name, "ann"); err == nil || !strings.Contains(err.Error(), "a lookup by name is not unique") {
		t.Fatalf("GetBy(name) = %v; want it refused", err)
	}

	if _, err := Users.FindBy(ctx, rt, User_Name, "ann"); err == nil {
		t.Fatal("FindBy(name): want it refused")
	}

	if _, err := Users.FetchBy(ctx, rt, User_Name, []string{"ann"}); err == nil {
		t.Fatal("FetchBy(name): want it refused")
	}

	// The primary key is unique whatever the column: fixing it by EQ makes the
	// lookup unique, while any other comparison does not.
	if _, err := Users.GetBy(ctx, rt, User_Name, "ann", User_ID.EQ(Val(int64(1)))); err != nil {
		t.Fatalf("GetBy(name, id = 1) = %v", err)
	}

	if _, err := Users.GetBy(ctx, rt, User_Name, "ann", User_ID.GT(Val(int64(0)))); err == nil {
		t.Fatal("GetBy(name, id > 0): want it refused")
	}
}

type event struct {
	ID int64
	At time.Time
}

var (
	eventsHandle = NewTable[event, int64]("events")
	Event_ID     = NewColumn(eventsHandle, "id", "id", func(r *event) *int64 { return &r.ID })
	Event_At     = NewColumn(eventsHandle, "at", "at", func(r *event) *time.Time { return &r.At })
	Events       = eventsHandle.Define(TableSpec[event, int64]{
		Columns:       []BoundColumn[event]{Event_ID, Event_At},
		PrimaryKey:    Event_ID,
		AutoIncrement: true,
		ColumnSpecs: []tsqdialect.ColumnSpec{
			{Name: "id", Type: tsqdialect.ColumnType{Kind: tsqdialect.ColumnKindInt, Bits: 64}, PrimaryKey: true, AutoIncrement: true},
			{Name: "at", Type: tsqdialect.ColumnType{Kind: tsqdialect.ColumnKindTime}},
		},
		Indexes: []IndexSpec{{Name: "ux_events_at", Columns: []string{"at"}, Unique: true}},
	})
)

// TestFetchByATimeFindsTheStoredRow covers FetchBy by a time: the rows it read were
// matched to the values asked for with Go's ==, which compares the zone, so a
// value in another zone than the one read back was reported missing.
func TestFetchByATimeFindsTheStoredRow(t *testing.T) {
	ctx := context.Background()

	rt, err := Open(ctx, "sqlite", filepath.Join(t.TempDir(), "events.db"), []Table{Events}, WithSchemaPolicy(SchemaPolicyCreateMissing))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = rt.Close() })

	at := time.Date(2024, 1, 2, 3, 4, 5, 0, time.FixedZone("UTC+8", 8*3600))
	if err := Events.Insert(ctx, rt, &event{At: at}); err != nil {
		t.Fatal(err)
	}

	rows, err := Events.FetchBy(ctx, rt, Event_At, []time.Time{at})
	if err != nil || len(rows) != 1 || !rows[0].At.Equal(at) {
		t.Fatalf("FetchBy(at) = %v, %v", rows, err)
	}
}
