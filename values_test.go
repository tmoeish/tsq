package tsq

import (
	"reflect"
	"strings"
	"testing"
)

type note struct {
	ID   int64
	Body *string
}

var notesHandle = NewTable[note]("notes")

var (
	Note_ID   = NewColumn(notesHandle, "id", "id", func(r *note) *int64 { return &r.ID })
	Note_Body = NewColumn(notesHandle, "body", "body", func(r *note) **string { return &r.Body })
)

var Notes = notesHandle.Define(TableSpec[note]{Columns: []BoundColumn[note]{Note_ID, Note_Body}, PrimaryKey: Note_ID})

func TestValIsBoundAndNeverNullInComparisons(t *testing.T) {
	q := Select(User_ID).From(Users.WithDeleted()).Where(User_Name.EQ(Val("amy")), User_ID.Between(Val(int64(1)), Val(int64(9)))).MustBuild()

	sql, args := sqlOf(t, q, onSQLite)
	if !strings.HasSuffix(sql, `("users"."name" = ? AND "users"."id" BETWEEN ? AND ?)`) {
		t.Fatalf("SQL = %s", sql)
	}

	if !reflect.DeepEqual(args, []any{"amy", int64(1), int64(9)}) {
		t.Fatalf("args = %#v", args)
	}

	// = NULL never matches; the comparison says so instead of returning no rows.
	if _, err := Select(Note_ID).From(Notes).Where(Note_Body.EQ(Val[*string](nil))).Build(); err == nil || !strings.Contains(err.Error(), "IsNull") {
		t.Fatalf("expected a NULL comparison to be refused, got %v", err)
	}

	// Val holds Go values; an expression goes in as itself.
	if _, err := Select(User_ID).From(Users).Where(User_ID.Pred("%s = %s", Val(User_Version))).Build(); err == nil {
		t.Fatal("expected Val of a column to be refused")
	}
}

func TestSetAssignsNullFromVal(t *testing.T) {
	m := UpdateTable(Notes).Set(Note_Body, Val[*string](nil)).Where(Note_ID.EQ(Val(int64(1)))).MustBuild()

	sql, args, err := m.SQL(onSQLite)
	if err != nil {
		t.Fatal(err)
	}

	if sql != `UPDATE "notes" SET "body" = ? WHERE "notes"."id" = ?` {
		t.Fatalf("SQL = %s", sql)
	}

	// database/sql binds a nil pointer as NULL.
	if len(args) != 2 || !isNilValue(args[0]) {
		t.Fatalf("args = %#v", args)
	}
}

func TestValsExpandAndKeepEmptyListsExplicit(t *testing.T) {
	render := func(cond Condition) string {
		t.Helper()

		sql, _ := sqlOf(t, Select(User_ID).From(Users.WithDeleted()).Where(cond).MustBuild(), onPostgres)

		return sql
	}

	if sql := render(User_ID.In(Vals[int64](4, 5))); !strings.HasSuffix(sql, `"users"."id" IN ($1, $2)`) {
		t.Fatalf("IN rendered %s", sql)
	}

	if sql := render(User_ID.In(Vals[int64]())); !strings.HasSuffix(sql, `IN (NULL)`) {
		t.Fatalf("empty IN rendered %s", sql)
	}

	if sql := render(User_ID.NotIn(Vals[int64]())); !strings.HasSuffix(sql, `NOT IN (SELECT 1 WHERE 1 = 0)`) {
		t.Fatalf("empty NOT IN rendered %s", sql)
	}
}
