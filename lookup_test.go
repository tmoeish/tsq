package tsq

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"

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
			{Name: "label", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindString, Size: 64}},
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
		Join(other, User_ID.EQ(User_ID.WithTable(other))).
		Where(User_Name.WithTable(other).IsNotNull()).
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

// TestStatementsByConditionAcceptTableStructs covers the RowTable inference that
// lets tsq.UpdateTable(TableCourse) take a generated struct.
func TestStatementsByConditionAcceptTableStructs(t *testing.T) {
	for name, stage := range map[string]interface {
		Build() (*Mutation[tag], error)
	}{
		"update": UpdateTable(tags).Set(tags.Label, Val("x")).Where(tags.Name.EQ(Val("a"))),
		"delete": DeleteFrom(tags).Where(tags.Name.EQ(Val("a"))),
		"hard":   HardDeleteFrom(tags).Where(tags.Name.EQ(Val("a"))),
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
	if got, err := alias.GetBy(ctx, rt, User_Email.WithTable(alias), "ada@x"); err != nil || got.ID != row.ID {
		t.Fatalf("GetBy through an alias = %+v, %v", got, err)
	}

	if _, err := Users.GetBy(ctx, rt, User_Email, "nobody@x"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("GetBy(missing) = %v, want sql.ErrNoRows", err)
	}

	if err := Users.Delete(ctx, rt, row); err != nil {
		t.Fatal(err)
	}

	if _, err := Users.GetBy(ctx, rt, User_Email, "ada@x"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("GetBy(deleted) = %v, want sql.ErrNoRows", err)
	}

	if got, err := Users.WithDeleted().GetBy(ctx, rt, User_Email, "ada@x"); err != nil || got.ID != row.ID {
		t.Fatalf("WithDeleted().GetBy = %+v, %v", got, err)
	}
}
