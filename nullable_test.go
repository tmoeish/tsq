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

var notesHandle = NewTable[note, int64]("notes")

var (
	Note_ID     = NewColumn(notesHandle, "id", "id", func(r *note) *int64 { return &r.ID })
	Note_Body   = NewNullColumn[string](notesHandle, "body", "body", func(r *note) **string { return &r.Body })
	Note_Title  = NewNullColumn[string](notesHandle, "title", "title", func(r *note) *sql.NullString { return &r.Title })
	Note_Rating = NewNullColumn[int64](notesHandle, "rating", "rating", func(r *note) *sql.Null[int64] { return &r.Rating })
)

var Notes = notesHandle.Define(TableSpec[note, int64]{
	Columns:       []BoundColumn[note]{Note_ID, Note_Body, Note_Title, Note_Rating},
	PrimaryKey:    Note_ID,
	AutoIncrement: true,
	Indexes:       []IndexSpec{{Name: "ft_notes_title_body", FullText: true, Columns: []string{"title", "body"}}},
	ColumnSpecs: []tsqdialect.ColumnSpec{
		{Name: "id", Type: tsqdialect.ColumnType{Kind: tsqdialect.ColumnKindInt, Bits: 64}, PrimaryKey: true, AutoIncrement: true},
		{Name: "body", Type: tsqdialect.ColumnType{Kind: tsqdialect.ColumnKindString, Size: 64, Nullable: true}},
		{Name: "title", Type: tsqdialect.ColumnType{Kind: tsqdialect.ColumnKindString, Size: 64, Nullable: true}},
		{Name: "rating", Type: tsqdialect.ColumnType{Kind: tsqdialect.ColumnKindInt, Bits: 64, Nullable: true}},
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

	h := NewTable[row, int64]("declared")

	cases := map[string]error{
		"pointer as NOT NULL":      NewColumn(h, "a", "a", func(r *row) **int64 { return &r.Ptr }).core().err(),
		"NullInt64 as NOT NULL":    NewColumn(h, "b", "b", func(r *row) *sql.NullInt64 { return &r.Null }).core().err(),
		"wrong value type":         NewNullColumn[string](h, "c", "c", func(r *row) *sql.NullInt64 { return &r.Null }).core().err(),
		"not a nullable form":      NewNullColumn[string](h, "d", "d", func(r *row) *string { return &r.Text }).core().err(),
		"mapped into wrong":        MapIntoNull(User_Name, func(r *labelRow) *sql.Null[int64] { return nil }).Named("x").core().err(),
		"mapped into plain string": MapIntoNull(User_Name, func(r *labelRow) *string { return nil }).Named("x").core().err(),
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

	name := MapInto(User_Name, func(r *labelRow) *string { return &r.Name }).Named("name")
	count := MapInto(Count(Order_ID), func(r *labelRow) *int64 { return &r.Count }).Named("count")
	label := func(src ValueColumn[string]) ResultColumn[labelRow, string] {
		return MapInto(src, func(r *labelRow) *string { return &r.Name }).Named("label")
	}
	nullLabel := func(src ValueColumn[string]) ResultColumn[labelRow, string] {
		return MapIntoNull(src, func(r *labelRow) *sql.NullString { return &r.Label }).Named("label")
	}

	orderNote := Order_Note
	joined := func(cols ...BoundColumn[labelRow]) *Query[labelRow] {
		return Select(cols...).From(Users).LeftJoin(Orders, Order_UserID.EQ(User_ID)).GroupBy(User_Name, Order_Note).MustBuild()
	}

	refused := map[string]*Query[labelRow]{
		"outer-joined column":     joined(name, label(orderNote)),
		"aggregate without group": Select(label(Max(User_Name))).From(Users).MustBuild(),
		"case without else":       Select(label(Case[string](User_ID.GT(Val(int64(1))), User_Name).End())).From(Users).MustBuild(),
		"nullif":                  Select(label(NullIf(User_Name, Val("a")))).From(Users).MustBuild(),
		"set operation operand":   Select(name).From(Users).Union(Select(label(Case[string](User_ID.GT(Val(int64(1))), User_Name).End())).From(Users)).MustBuild(),
		"nested set operand":      Select(name).From(Users).Union(Select(name).From(Users).Union(Select(label(Case[string](User_ID.GT(Val(int64(1))), User_Name).End())).From(Users))).MustBuild(),
		"right joined preserved":  Select(name).From(Users).RightJoin(Orders, Order_UserID.EQ(User_ID)).MustBuild(),
	}

	for what, q := range refused {
		if _, err := q.List(ctx, rt); err == nil || !strings.Contains(err.Error(), "can be NULL here") {
			t.Errorf("%s: List = %v; want the nullable value refused", what, err)
		}

		// Whether a row exists does not depend on reading it.
		if _, err := q.Exists(ctx, rt); err != nil {
			t.Errorf("%s: Exists = %v; want an answer", what, err)
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
		"case with else":      Select(label(Case[string](User_ID.GT(Val(int64(1))), User_Name).Else(Val("x")).End())).From(Users).MustBuild(),
		"condition on outer":  Select(label(Case[string](Order_Note.IsNull(), Val("none")).Else(Val("some")).End())).From(Users).LeftJoin(Orders, Order_UserID.EQ(User_ID)).MustBuild(),
	}

	for what, q := range allowed {
		if _, err := q.List(ctx, rt); err != nil {
			t.Errorf("%s: List = %v", what, err)
		}
	}

	// A CTE column that can be NULL stays nullable through the CTE.
	inner := Select(nullLabel(orderNote)).From(Users).LeftJoin(Orders, Order_UserID.EQ(User_ID))
	cte := CTE("labels", inner)

	outer := Select(label(orderNote.Rebind(cte))).From(cte).MustBuild()
	if _, err := outer.List(ctx, rt); err == nil || !strings.Contains(err.Error(), "can be NULL") {
		t.Errorf("nullable CTE column = %v", err)
	}

	if _, err := Select(nullLabel(orderNote.Rebind(cte))).From(cte).MustBuild().List(ctx, rt); err != nil {
		t.Errorf("nullable CTE column into a nullable field = %v", err)
	}

	// A nullable column the CTE coalesces is not NULL through it: the CTE decides,
	// not the column's declaration.
	coalesced := CTE("bodies", Select(MapInto(Coalesce(Note_Body, Val("none")), func(r *labelRow) *string { return &r.Name })).From(Notes))
	through := Select(MapInto(Note_Body.Rebind(coalesced), func(r *labelRow) *string { return &r.Name })).From(coalesced).MustBuild()

	if through.scanErr != nil {
		t.Errorf("coalesced CTE column = %v; want it readable into a string", through.scanErr)
	}
}

// TestCTEColumnNamesAreDistinct covers a CTE that selects two columns of one
// name, SUM(amount) and MAX(amount): its columns are found by name, so either
// reference was ambiguous, and the database said so only when it ran.
func TestCTEColumnNamesAreDistinct(t *testing.T) {
	sum := MapInto(Sum(Order_Amount), func(r *labelRow) *int64 { return &r.Count })
	most := MapInto(Max(Order_Amount), func(r *labelRow) *int64 { return &r.Count })
	cte := CTE("totals", Select(sum, most).From(Orders))

	_, err := Select(MapInto(Order_Amount.Rebind(cte), func(r *labelRow) *int64 { return &r.Count })).From(cte).Build()
	if err == nil || !strings.Contains(err.Error(), "two columns named amount") {
		t.Fatalf("Build = %v; want the repeated name refused", err)
	}
}

// TestNotInOverANullableSubqueryIsRefused covers NotIn over a subquery whose
// column can be NULL: one NULL makes NOT IN never true, and the query silently
// matched nothing.
func TestNotInOverANullableSubqueryIsRefused(t *testing.T) {
	ratings := SelectValue(Note_Rating).From(Notes)

	if _, err := Select(Note_ID).From(Notes).Where(Note_ID.NotIn(ratings)).Build(); err == nil || !strings.Contains(err.Error(), "use NotExists") {
		t.Fatalf("NotIn over a nullable subquery: Build = %v", err)
	}

	if _, err := Select(Note_ID).From(Notes).Where(Note_ID.In(ratings)).Build(); err != nil {
		t.Fatalf("In over the same subquery = %v; NULL does not matter to IN", err)
	}

	coalesced := SelectValue(Coalesce(Note_Rating, Val(int64(0)))).From(Notes)
	if _, err := Select(Note_ID).From(Notes).Where(Note_ID.NotIn(coalesced)).Build(); err != nil {
		t.Fatalf("NotIn over a coalesced subquery = %v", err)
	}
}

// TestRebindNullKeepsTheNullableType covers Rebind on a NullColumn, which
// returns a Column: the rebound column needed a type assertion to be read into a
// nullable field or compared as nullable again.
func TestRebindNullKeepsTheNullableType(t *testing.T) {
	alias := Notes.As("n")

	rating := RebindNull(Note_Rating, alias)
	if _, err := Select(Note_ID.Rebind(alias), rating).From(alias).Where(rating.IsNull()).Build(); err != nil {
		t.Fatalf("query over the rebound column: %v", err)
	}

	if _, err := Select(Note_ID).From(Notes).Where(RebindNull[note, int64](nil, alias).IsNull()).Build(); err == nil {
		t.Fatal("RebindNull(nil): want an error")
	}
}

// TestCoalesceIsNullOnlyWhereBothAre covers Coalesce over a nullable column with a
// NOT NULL column as the fallback: it was still called nullable, and the error
// reading it into a plain field said to use Coalesce.
func TestCoalesceIsNullOnlyWhereBothAre(t *testing.T) {
	type result struct {
		Rating int64
		Text   string
	}

	rating := func(r *result) *int64 { return &r.Rating }

	settled := Select(MapInto(Coalesce[int64](Note_Rating, Note_ID), rating)).From(Notes).MustBuild()
	if settled.scanErr != nil {
		t.Fatalf("a nullable column over a NOT NULL one: %v", settled.scanErr)
	}

	both := Select(MapInto(Coalesce[string](Note_Body, Note_Title), func(r *result) *string { return &r.Text })).From(Notes).MustBuild()
	if both.scanErr == nil {
		t.Fatal("two nullable columns read into a field that cannot hold NULL")
	}

	// The fallback's table is filled with NULLs by the outer join.
	outer := Select(MapInto(Coalesce[int64](Note_Rating, User_ID), rating)).From(Notes).LeftJoin(Users, User_ID.EQ(Note_ID)).MustBuild()
	if outer.scanErr == nil {
		t.Fatal("a fallback from the optional side of an outer join read into a field that cannot hold NULL")
	}
}
