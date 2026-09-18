package tsq

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
)

type note struct {
	ID     int64
	Body   *string
	Title  sql.NullString
	Rating sql.Null[int64]
}

var notesHandle = NewTable[note]("notes")

var (
	Note_ID     = NewColumn(notesHandle, "id", "id", func(r *note) *int64 { return &r.ID })
	Note_Body   = NewNullColumn[string](notesHandle, "body", "body", func(r *note) **string { return &r.Body })
	Note_Title  = NewNullColumn[string](notesHandle, "title", "title", func(r *note) *sql.NullString { return &r.Title })
	Note_Rating = NewNullColumn[int64](notesHandle, "rating", "rating", func(r *note) *sql.Null[int64] { return &r.Rating })
)

var Notes = notesHandle.Define(TableSpec[note]{
	Columns:       []BoundColumn[note]{Note_ID, Note_Body, Note_Title, Note_Rating},
	PrimaryKey:    Note_ID,
	AutoIncrement: true,
	Indexes:       []TableIndex{{Name: "ft_notes_title_body", FullText: true, Fields: []string{"title", "body"}}},
	Schema: []tsqdialect.ColumnSpec{
		{Name: "id", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64}, PrimaryKey: true, AutoIncrement: true},
		{Name: "body", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindString, Size: 64, Nullable: true}},
		{Name: "title", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindString, Size: 64, Nullable: true}},
		{Name: "rating", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64, Nullable: true}},
	},
})

type labelRow struct {
	Name  string
	Label sql.NullString
	Count int64
}

func TestColumnsDeclareWhetherTheyHoldNull(t *testing.T) {
	type row struct {
		Ptr  *int64
		Null sql.NullInt64
		Time scannerTime
		Text string
	}

	h := NewTable[row]("declared")

	cases := map[string]error{
		"pointer as NOT NULL":      NewColumn(h, "a", "a", func(r *row) **int64 { return &r.Ptr }).core().err(),
		"NullInt64 as NOT NULL":    NewColumn(h, "b", "b", func(r *row) *sql.NullInt64 { return &r.Null }).core().err(),
		"wrong value type":         NewNullColumn[string](h, "c", "c", func(r *row) *sql.NullInt64 { return &r.Null }).core().err(),
		"not a nullable form":      NewNullColumn[string](h, "d", "d", func(r *row) *string { return &r.Text }).core().err(),
		"mapped into wrong":        MapIntoNull(User_Name, func(r *labelRow) *sql.Null[int64] { return nil }, "x").core().err(),
		"mapped into plain string": MapIntoNull(User_Name, func(r *labelRow) *string { return nil }, "x").core().err(),
	}

	for name, err := range cases {
		if err == nil {
			t.Errorf("%s: expected a declaration error", name)
		}
	}

	accepted := map[string]error{
		"pointer":           NewNullColumn[int64](h, "e", "e", func(r *row) **int64 { return &r.Ptr }).core().err(),
		"NullInt64":         NewNullColumn[int64](h, "f", "f", func(r *row) *sql.NullInt64 { return &r.Null }).core().err(),
		"struct with Valid": NewNullColumn[time.Time](h, "g", "g", func(r *row) *scannerTime { return &r.Time }).core().err(),
	}

	for name, err := range accepted {
		if err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestNullColumnsCompareAndWriteTheirValueType(t *testing.T) {
	ctx := context.Background()

	rt, err := Open(ctx, "sqlite", filepath.Join(t.TempDir(), "notes.db"), []Table{Notes}, WithSchemaPolicy(SchemaPolicyReconcile))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = rt.Close() })

	body := "hello"
	rows := []*note{
		{Body: &body, Title: sql.NullString{String: "t", Valid: true}, Rating: sql.Null[int64]{V: 5, Valid: true}},
		{},
	}

	if err := Notes.BatchInsert(ctx, rt, rows); err != nil {
		t.Fatal(err)
	}

	// Comparisons take the value type; NULL rows never match.
	q := Select(Notes.Columns()...).From(Notes).Where(Note_Body.EQ(Val("hello")), Note_Rating.GT(Val(int64(1)))).MustBuild()

	got, err := q.List(ctx, rt)
	if err != nil || len(got) != 1 || *got[0].Body != "hello" || got[0].Title.String != "t" {
		t.Fatalf("List = %v, %v", got, err)
	}

	if n, err := Select(Note_ID).From(Notes).Where(Note_Body.IsNull()).MustBuild().Count(ctx, rt); err != nil || n != 1 {
		t.Fatalf("IsNull count = %d, %v", n, err)
	}

	// Text functions accept a nullable text column, and pattern functions too.
	if n, err := Select(Note_ID).From(Notes).Where(Upper(Note_Title).EQ(Val("T")), Contains(Note_Body, Val("ell"))).MustBuild().Count(ctx, rt); err != nil || n != 1 {
		t.Fatalf("functions on nullable columns = %d, %v", n, err)
	}

	// SetNull is only on nullable columns; Set refuses a value that can be NULL for a
	// NOT NULL column.
	clear := UpdateTable(Notes).SetNull(Note_Body).Set(Note_Rating, Note_Rating).Where(Note_ID.EQ(Val(rows[0].ID)))
	if n, err := clear.Exec(ctx, rt); err != nil || n != 1 {
		t.Fatalf("SetNull = %d, %v", n, err)
	}

	if _, err := UpdateTable(Notes).Set(Note_ID, Note_Rating).Where(And()).Build(); err == nil || !strings.Contains(err.Error(), "NOT NULL") {
		t.Fatalf("NOT NULL target = %v", err)
	}

	stored, err := Select(Notes.Columns()...).From(Notes).Where(Note_ID.EQ(Val(rows[0].ID))).MustBuild().Get(ctx, rt)
	if err != nil || stored.Body != nil || stored.Rating.V != 5 {
		t.Fatalf("after SetNull = %+v, %v", stored, err)
	}

	// SelectNullValue reads NULL; SelectValue refuses a value that can be NULL.
	if _, err := SelectValue(Max(Note_Rating)).From(Notes).MustBuild().Get(ctx, rt); err == nil {
		t.Fatal("expected SelectValue to refuse a nullable value")
	}

	if v, err := SelectNullValue(Max(Note_Rating)).From(Notes).MustBuild().Get(ctx, rt); err != nil || !v.Valid || v.V != 5 {
		t.Fatalf("SelectNullValue = %v, %v", v, err)
	}

	if _, err := Select(Notes.Columns()...).From(Notes).MustBuild().PageKeyset(ctx, rt, Keyset{OrderBy: []OrderBy{Note_Rating.Asc(), Note_ID.Asc()}}); err == nil {
		t.Fatal("expected a nullable keyset column to be refused")
	}

	empty := SelectNullValue(Max(Note_Rating)).From(Notes).Where(Note_ID.LT(Val(int64(0)))).MustBuild()
	if v, err := empty.Get(ctx, rt); err != nil || v.Valid {
		t.Fatalf("SelectNullValue over no rows = %v, %v", v, err)
	}
}

func TestReadingAValueThatCanBeNullNeedsANullableField(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)
	seedUsers(t, rt, "a", "b")

	name := MapInto(User_Name, func(r *labelRow) *string { return &r.Name }, "name")
	count := MapInto(Count(Order_ID), func(r *labelRow) *int64 { return &r.Count }, "count")
	label := func(src ValueColumn[string]) ResultColumn[labelRow, string] {
		return MapInto(src, func(r *labelRow) *string { return &r.Name }, "label")
	}
	nullLabel := func(src ValueColumn[string]) ResultColumn[labelRow, string] {
		return MapIntoNull(src, func(r *labelRow) *sql.NullString { return &r.Label }, "label")
	}

	orderNote := Order_Note
	joined := func(cols ...BoundColumn[labelRow]) *Query[labelRow] {
		return Select(cols...).From(Users).LeftJoin(Orders, Order_UserID.EQ(User_ID)).GroupBy(User_Name, Order_Note).MustBuild()
	}

	refused := map[string]*Query[labelRow]{
		"outer-joined column":     joined(name, label(orderNote)),
		"aggregate without group": Select(label(Max(User_Name))).From(Users).MustBuild(),
		"case without else":       Select(label(Case[string]().When(User_ID.GT(Val(int64(1))), User_Name).End())).From(Users).MustBuild(),
		"nullif":                  Select(label(NullIf(User_Name, Val("a")))).From(Users).MustBuild(),
		"set operation operand":   Select(name).From(Users).Union(Select(label(Case[string]().When(User_ID.GT(Val(int64(1))), User_Name).End())).From(Users)).MustBuild(),
		"right joined preserved":  Select(name).From(Users).RightJoin(Orders, Order_UserID.EQ(User_ID)).MustBuild(),
	}

	for what, q := range refused {
		if _, err := q.List(ctx, rt); err == nil || !strings.Contains(err.Error(), "NULL") {
			t.Errorf("%s: List = %v; want the nullable value refused", what, err)
		}

		// The same query is fine where it is not read: as a subquery or CTE.
		if _, _, err := q.SQL(onSQLite); err != nil {
			t.Errorf("%s: SQL = %v", what, err)
		}
	}

	allowed := map[string]*Query[labelRow]{
		"mapped nullable":     joined(name, nullLabel(orderNote), count),
		"coalesced":           joined(name, label(Coalesce(orderNote, Val("none"))), count),
		"count is never null": Select(name, count).From(Users).LeftJoin(Orders, Order_UserID.EQ(User_ID)).GroupBy(User_Name).MustBuild(),
		"grouped aggregate":   Select(label(Max(User_Name))).From(Users).GroupBy(User_Email).MustBuild(),
		"case with else":      Select(label(Case[string]().When(User_ID.GT(Val(int64(1))), User_Name).Else(Val("x")).End())).From(Users).MustBuild(),
		"condition on outer":  Select(label(Case[string]().When(Order_Note.IsNull(), Val("none")).Else(Val("some")).End())).From(Users).LeftJoin(Orders, Order_UserID.EQ(User_ID)).MustBuild(),
	}

	for what, q := range allowed {
		if _, err := q.List(ctx, rt); err != nil {
			t.Errorf("%s: List = %v", what, err)
		}
	}

	// A CTE column that can be NULL stays nullable through the CTE.
	inner := Select(nullLabel(orderNote)).From(Users).LeftJoin(Orders, Order_UserID.EQ(User_ID))
	cte := CTE("labels", inner)

	outer := Select(label(orderNote.WithTable(cte))).From(cte).MustBuild()
	if _, err := outer.List(ctx, rt); err == nil || !strings.Contains(err.Error(), "can be NULL") {
		t.Errorf("nullable CTE column = %v", err)
	}

	if _, err := Select(nullLabel(orderNote.WithTable(cte))).From(cte).MustBuild().List(ctx, rt); err != nil {
		t.Errorf("nullable CTE column into a nullable field = %v", err)
	}
}
