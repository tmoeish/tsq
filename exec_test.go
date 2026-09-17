package tsq

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"
)

func seedUsers(t *testing.T, rt *Runtime, names ...string) []*user {
	t.Helper()

	rows := make([]*user, 0, len(names))
	for _, name := range names {
		rows = append(rows, &user{Name: name, Email: name + "@example.com"})
	}

	if err := Users.BatchInsert(context.Background(), rt, rows); err != nil {
		t.Fatalf("BatchInsert() error = %v", err)
	}

	return rows
}

func TestInsertAssignsKeysAndManagedTimestamps(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)

	earlier := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	kept := &user{Name: "kept", Email: "kept@example.com", CreatedAt: earlier}

	if err := Users.Insert(ctx, rt, kept); err != nil {
		t.Fatalf("Insert() error = %v", err)
	}

	if kept.ID == 0 {
		t.Fatal("expected the generated key to be written back")
	}

	if !kept.CreatedAt.Equal(earlier) {
		t.Fatalf("a created_at the caller set must survive, got %v", kept.CreatedAt)
	}

	if kept.UpdatedAt.IsZero() {
		t.Fatal("expected an unset updated_at to be filled")
	}

	rows := seedUsers(t, rt, "a", "b", "c")
	for i, row := range rows {
		if row.ID != kept.ID+int64(i)+1 {
			t.Fatalf("row %d got key %d", i, row.ID)
		}
	}

	loaded, err := QueryByID.Get(ctx, rt, User_ID.Bind(rows[1].ID))
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if loaded.Name != "b" {
		t.Fatalf("loaded %+v", loaded)
	}
}

var QueryByID = Select(User__Cols...).From(Users).Where(User_ID.EQ(User_ID.Param())).MustBuild()

func TestUpdateChecksAndBumpsTheVersion(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)
	row := seedUsers(t, rt, "amy")[0]

	stale := *row

	row.Name = "amy2"
	if err := Users.Update(ctx, rt, row); err != nil {
		t.Fatalf("Update() error = %v", err)
	}

	if row.Version != 1 {
		t.Fatalf("version = %d, want 1", row.Version)
	}

	stale.Name = "lost"

	err := Users.Update(ctx, rt, &stale)
	if !IsOptimisticLockError(err) {
		t.Fatalf("stale update error = %v", err)
	}

	if _, ok := errors.AsType[*OptimisticLockError](err); !ok {
		t.Fatalf("expected *OptimisticLockError, got %T", err)
	}

	if !strings.Contains(err.Error(), "users id=") || strings.Contains(err.Error(), "lost") {
		t.Fatalf("a single-row error names the key and never the row: %v", err)
	}

	rows := seedUsers(t, rt, "b", "c")
	for _, r := range rows {
		r.Name += "!"
	}

	if err := Users.BatchUpdate(ctx, rt, rows, WithBatchSize(1)); err != nil {
		t.Fatalf("BatchUpdate() error = %v", err)
	}

	names, err := Select(User_Name).From(Users).OrderBy(User_ID.Asc()).List(ctx, rt)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}

	if len(names) != 3 || names[0].Name != "amy2" || names[1].Name != "b!" || names[2].Name != "c!" {
		t.Fatalf("names = %+v", names)
	}
}

func TestDeleteIsSoftWhenTheTableHasDeletedAt(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)
	rows := seedUsers(t, rt, "a", "b", "c", "d")

	if err := Users.Delete(ctx, rt, rows[0]); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	if rows[0].DeletedAt == 0 || rows[0].Version != 1 {
		t.Fatalf("soft delete must stamp the row and bump its version: %+v", rows[0])
	}

	if err := Users.BatchDeleteByPK(ctx, rt, User_ID.BindList(rows[1].ID)); err != nil {
		t.Fatalf("BatchDeleteByPK() error = %v", err)
	}

	if err := Users.HardDelete(ctx, rt, rows[2]); err != nil {
		t.Fatalf("HardDelete() error = %v", err)
	}

	if err := Users.BatchHardDeleteByPK(ctx, rt, User_ID.BindList(rows[3].ID)); err != nil {
		t.Fatalf("BatchHardDeleteByPK() error = %v", err)
	}

	type state struct {
		ID        int64
		DeletedAt int64
	}

	all, err := Select(User__Cols...).From(Users.WithDeleted()).OrderBy(User_ID.Asc()).List(ctx, rt)
	if err != nil {
		t.Fatal(err)
	}

	got := make([]state, 0, len(all))
	for _, u := range all {
		got = append(got, state{ID: u.ID, DeletedAt: u.DeletedAt})
	}

	if len(got) != 2 || got[0].DeletedAt == 0 || got[1].DeletedAt == 0 {
		t.Fatalf("expected two tombstoned rows and two removed ones, got %+v", got)
	}

	if n, err := Select(User_ID).From(Users).Count(ctx, rt); err != nil || n != 0 {
		t.Fatalf("queries must leave deleted rows out: %d, %v", n, err)
	}

	if err := Users.BatchDeleteByPK(ctx, rt, User_Version.BindList(1)); err == nil {
		t.Fatal("expected keys bound on a non-key column to be refused")
	}
}

func TestRowWritesRejectBadInput(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)

	if err := Users.Insert(ctx, rt, nil); err == nil {
		t.Fatal("expected a nil row to be refused")
	}

	if err := Users.Update(ctx, rt, &user{Name: "x"}); err == nil {
		t.Fatal("expected a zero key to be refused")
	}

	if err := Users.BatchUpdate(ctx, rt, nil, WithSkipDuplicates()); err == nil {
		t.Fatal("expected WithSkipDuplicates outside BatchInsert to be refused")
	}

	if err := Users.BatchInsert(ctx, rt, nil, WithBatchSize(0)); err == nil {
		t.Fatal("expected a zero batch size to be refused")
	}

	if err := undefinedTable.Insert(ctx, rt, &user{}); err == nil {
		t.Fatal("expected an undefined table to be refused")
	}
}

func TestSkipDuplicatesInsideATransaction(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)
	seedUsers(t, rt, "dup")

	err := rt.WithTx(ctx, nil, func(ctx context.Context, tx Executor) error {
		rows := []*user{{Name: "dup", Email: "dup@example.com"}, {Name: "new", Email: "new@example.com"}}
		if err := Users.BatchInsert(ctx, tx, rows, WithSkipDuplicates()); err != nil {
			return err
		}

		// The transaction must still work after the skipped row.
		_, err := Select(User_ID).From(Users).Count(ctx, tx)

		return err
	})
	if err != nil {
		t.Fatalf("WithTx() error = %v", err)
	}

	n, err := Select(User_ID).From(Users).Count(ctx, rt)
	if err != nil || n != 2 {
		t.Fatalf("Count() = %d, %v; want 2", n, err)
	}
}

func TestReadsAgainstSQLite(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)
	rows := seedUsers(t, rt, "ann", "bob", "cid")

	if err := Orders.BatchInsert(ctx, rt, []*order{
		{UserID: rows[0].ID, Amount: 50, Note: "50% off"},
		{UserID: rows[0].ID, Amount: 150},
		{UserID: rows[1].ID, Amount: 300},
	}); err != nil {
		t.Fatal(err)
	}

	q := Select(User__Cols...).From(Users).Where(User_ID.In(User_ID.ListParam())).MustBuild()

	list, err := q.List(ctx, rt, User_ID.BindList(rows[0].ID, rows[2].ID))
	if err != nil || len(list) != 2 {
		t.Fatalf("List() = %d rows, %v", len(list), err)
	}

	if empty, err := q.List(ctx, rt, User_ID.BindList()); err != nil || len(empty) != 0 {
		t.Fatalf("an empty IN list must match nothing: %d rows, %v", len(empty), err)
	}

	if _, err := QueryByID.Get(ctx, rt, User_ID.Bind(999)); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("Get() of a missing row error = %v", err)
	}

	if row, err := QueryByID.Find(ctx, rt, User_ID.Bind(999)); row != nil || err != nil {
		t.Fatalf("Find() of a missing row = %v, %v", row, err)
	}

	if ok, err := QueryByID.Exists(ctx, rt, User_ID.Bind(rows[1].ID)); !ok || err != nil {
		t.Fatalf("Exists() = %v, %v", ok, err)
	}

	total, err := Select(Order_Amount).From(Orders).MustBuild().Scalar(ctx, rt, Order_Amount)
	if err != nil || total != 50 {
		t.Fatalf("Scalar() = %d, %v", total, err)
	}

	sum := Select(Sum(Order_Amount)).From(Orders).MustBuild()
	if v, err := sum.ScalarNull(ctx, rt, Sum(Order_Amount)); err != nil || !v.Valid || v.V != 500 {
		t.Fatalf("ScalarNull(SUM) = %v, %v", v, err)
	}

	big, err := BuildSubquery(
		Select(Order_ID).From(Orders).Correlate(Users).Where(Order_UserID.EQ(User_ID), Order_Amount.GT(Val(int64(100)))),
		Order_ID,
	)
	if err != nil {
		t.Fatal(err)
	}

	withBig, err := Select(User_ID).From(Users).Where(Exists(big)).OrderBy(User_ID.Asc()).List(ctx, rt)
	if err != nil || len(withBig) != 2 {
		t.Fatalf("correlated EXISTS = %d rows, %v", len(withBig), err)
	}

	perUser, err := Select(Order_UserID).From(Orders).GroupBy(Order_UserID).Count(ctx, rt)
	if err != nil || perUser != 2 {
		t.Fatalf("grouped Count() = %d, %v", perUser, err)
	}
}

func TestPageSearchesSortsAndCounts(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)
	seedUsers(t, rt, "a_1", "ab1", "b_2", "zz")

	q := Select(User__Cols...).From(Users).Search(Users.SearchColumns()...).MustBuild()

	// "_" is a LIKE wildcard; the keyword must match it literally.
	page, err := q.Page(ctx, rt, Paging{Size: 10, OrderBy: []OrderBy{User_Name.Desc()}}, Keyword("_"))
	if err != nil {
		t.Fatalf("Page() error = %v", err)
	}

	if page.Total != 2 || len(page.Data) != 2 || page.Data[0].Name != "b_2" {
		t.Fatalf("page = total %d %+v", page.Total, page.Data)
	}

	request := &PageRequest{Size: 3, Page: 2, OrderBy: "id"}

	paging, err := request.Paging(User_ID, User_Name)
	if err != nil {
		t.Fatal(err)
	}

	page, err = q.Page(ctx, rt, paging)
	if err != nil || page.Total != 4 || len(page.Data) != 1 || page.TotalPages != 2 || !page.HasPrev() || page.HasNext() {
		t.Fatalf("second page = %+v, %v", page, err)
	}

	// Only the columns the endpoint names are sortable, whatever the query selects.
	if _, err := (&PageRequest{OrderBy: "email"}).Paging(User_ID, User_Name); !isErr[*UnknownSortFieldError](err) {
		t.Fatalf("unknown sort field error = %v", err)
	}

	if _, err := (&PageRequest{OrderBy: "id,name", Order: "asc"}).Paging(User_ID, User_Name); !isErr[*OrderCountMismatchError](err) {
		t.Fatalf("order count mismatch error = %v", err)
	}

	if _, err := (&PageRequest{OrderBy: "id"}).Paging(User_ID, Order_ID); !isErr[*AmbiguousSortFieldError](err) {
		t.Fatalf("ambiguous sort field error = %v", err)
	}

	limited := Select(User_ID).From(Users).Limit(1).MustBuild()
	if _, err := limited.Page(ctx, rt, Paging{}); err == nil {
		t.Fatal("expected Page to refuse a query with its own Limit")
	}

	ordered := Select(User_ID).From(Users).OrderBy(User_ID.Asc()).MustBuild()
	if _, err := ordered.Page(ctx, rt, Paging{OrderBy: []OrderBy{User_Name.Asc()}}); err == nil {
		t.Fatal("expected Page to refuse a second ordering")
	}

	empty, err := Select(User_ID).From(Users).Where(User_ID.EQ(Val(int64(-1)))).MustBuild().Page(ctx, rt, Paging{})
	if err != nil || empty.Data == nil || !empty.IsEmpty() || empty.Size != 20 || empty.Page != 1 {
		t.Fatalf("empty page = %+v, %v", empty, err)
	}
}

func TestUpsertMatchesLiveRowsOfASoftDeletedUniqueIndex(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)

	gone := &user{Name: "old", Email: "same@example.com"}
	if err := Users.Upsert(ctx, rt, gone, User_Email); err != nil {
		t.Fatal(err)
	}

	if err := Users.Delete(ctx, rt, gone); err != nil {
		t.Fatal(err)
	}

	// The unique index is (email, deleted_at); the deleted row does not match.
	live := &user{Name: "new", Email: "same@example.com"}
	if err := Users.Upsert(ctx, rt, live, User_Email); err != nil {
		t.Fatal(err)
	}

	if live.ID == gone.ID {
		t.Fatal("expected a new row next to the deleted one")
	}

	if err := Users.Upsert(ctx, rt, &user{Name: "renamed", Email: "same@example.com"}, User_Email); err != nil {
		t.Fatal(err)
	}

	stored, err := QueryByID.Get(ctx, rt, User_ID.Bind(live.ID))
	if err != nil || stored.Name != "renamed" || stored.Version != live.Version+1 {
		t.Fatalf("stored = %+v, %v", stored, err)
	}

	if err := Users.Upsert(ctx, rt, &user{}, User_Name); err == nil {
		t.Fatal("expected a key that is not unique to be refused")
	}

	if err := Users.Upsert(ctx, rt, &user{}, User_Email.As("u")); err == nil {
		t.Fatal("expected an aliased key column to be refused")
	}
}

func TestKeywordIsAnArgumentForEveryRead(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)
	seedUsers(t, rt, "alice", "alfred", "bob")

	q := Select(User__Cols...).From(Users).Search(Searchable(User_Name)).MustBuild()

	if n, err := q.Count(ctx, rt, Keyword("al")); err != nil || n != 2 {
		t.Fatalf("Count = %d, %v", n, err)
	}

	names := 0

	for row, err := range q.Iter(ctx, rt, Keyword("bo")) {
		if err != nil || row.Name != "bob" {
			t.Fatalf("Iter = %v, %v", row, err)
		}

		names++
	}

	if names != 1 {
		t.Fatalf("Iter yielded %d rows", names)
	}

	// An empty term searches nothing, whatever the query.
	if list, err := q.List(ctx, rt, Keyword("")); err != nil || len(list) != 3 {
		t.Fatalf("empty keyword = %d rows, %v", len(list), err)
	}

	plain := Select(User__Cols...).From(Users).MustBuild()
	if _, err := plain.List(ctx, rt, Keyword("")); err != nil {
		t.Fatalf("empty keyword without Search = %v", err)
	}

	if _, err := plain.List(ctx, rt, Keyword("al")); err == nil {
		t.Fatal("expected a keyword on a query without Search to be refused")
	}

	if _, err := q.List(ctx, rt, Keyword("al"), Keyword("bo")); err == nil {
		t.Fatal("expected two keywords to be refused")
	}

	sql, args, err := q.SQL(onSQLite, Keyword("50%"))
	if err != nil || !strings.Contains(sql, `"users"."name" LIKE ? ESCAPE`) || len(args) != 1 || args[0] != "%50~%%" {
		t.Fatalf("SQL = %s %v, %v", sql, args, err)
	}
}

func TestListInSplitsListsBeyondTheBindLimit(t *testing.T) {
	if raceEnabled {
		t.Skip("binds 40000 values on SQLite, which is slow under the race detector")
	}

	ctx := context.Background()
	rt := newSQLite(t)
	rows := seedUsers(t, rt, "a", "b", "c")

	// More values than SQLite binds in one statement, with duplicates and misses.
	ids := make([]int64, 0, 40002)
	for i := range int64(40000) {
		ids = append(ids, 1000+i)
	}

	ids = append(ids, rows[2].ID, rows[0].ID, rows[0].ID)

	byID := Select(User__Cols...).From(Users).Where(User_ID.In(User_ID.ListParam())).MustBuild()

	got, err := byID.ListIn(ctx, rt, User_ID.ListParam(), ids)
	if err != nil {
		t.Fatal(err)
	}

	if len(got) != 2 {
		t.Fatalf("got %d rows, want 2", len(got))
	}

	// Other parameters are bound in every statement.
	name := NewParam[string]("name")
	filtered := Select(User__Cols...).From(Users).Where(User_ID.In(User_ID.ListParam()), User_Name.EQ(name)).MustBuild()

	got, err = filtered.ListIn(ctx, rt, User_ID.ListParam(), ids, name.Bind("c"))
	if err != nil || len(got) != 1 || got[0].Name != "c" {
		t.Fatalf("filtered = %v, %v", got, err)
	}

	if got, err := byID.ListIn(ctx, rt, User_ID.ListParam(), nil); err != nil || len(got) != 0 {
		t.Fatalf("no values = %v, %v", got, err)
	}

	list := User_ID.ListParam()
	refused := map[string]*Query[user]{
		"ordered":    Select(User__Cols...).From(Users).Where(User_ID.In(list)).OrderBy(User_ID.Asc()).MustBuild(),
		"not in":     Select(User__Cols...).From(Users).Where(User_ID.NotIn(list)).MustBuild(),
		"under or":   Select(User__Cols...).From(Users).Where(Or(User_ID.In(list), User_Name.EQ(Val("a")))).MustBuild(),
		"used twice": Select(User__Cols...).From(Users).Where(User_ID.In(list), User_Version.In(Vals[int64]()), User_ID.NotIn(list)).MustBuild(),
		"distinct":   SelectDistinct(User__Cols...).From(Users).Where(User_ID.In(list)).MustBuild(),
	}

	for name, q := range refused {
		if _, err := q.ListIn(ctx, rt, list, ids); err == nil {
			t.Errorf("%s: expected ListIn to refuse the query", name)
		}
	}
}

func TestIterStreamsRowsAndStops(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)
	seedUsers(t, rt, "a", "b", "c")

	q := Select(User__Cols...).From(Users).OrderBy(User_Name.Asc()).MustBuild()

	var names []string

	for row, err := range q.Iter(ctx, rt) {
		if err != nil {
			t.Fatal(err)
		}

		names = append(names, row.Name)
	}

	if !slices.Equal(names, []string{"a", "b", "c"}) {
		t.Fatalf("names = %v", names)
	}

	// Breaking out closes the rows; the connection is usable afterwards.
	for row, err := range q.Iter(ctx, rt) {
		if err != nil || row.Name != "a" {
			t.Fatalf("first row = %v, %v", row, err)
		}

		break
	}

	if n, err := q.Count(ctx, rt); err != nil || n != 3 {
		t.Fatalf("Count after break = %d, %v", n, err)
	}

	// A failure is yielded once with a nil row.
	calls := 0

	for row, err := range q.Iter(ctx, rt, User_ID.Bind(1)) {
		calls++

		if row != nil || err == nil {
			t.Fatalf("got %v, %v; want the unused argument reported", row, err)
		}
	}

	if calls != 1 {
		t.Fatalf("yielded %d times, want 1", calls)
	}
}

func TestPageInsideATransactionUsesIt(t *testing.T) {
	ctx := context.Background()

	var ops []TraceOp

	rt := newSQLite(t, WithTracers(func(ctx context.Context, op TraceOp, next func(context.Context) error) error {
		ops = append(ops, op)
		return next(ctx)
	}))

	q := Select(User__Cols...).From(Users).MustBuild()

	if _, err := q.Page(ctx, rt, Paging{}); err != nil {
		t.Fatal(err)
	}

	if !slices.Equal(ops, []TraceOp{TraceOpPage, TraceOpTx}) {
		t.Fatalf("Page ops = %v; want the read in its own transaction", ops)
	}

	ops = nil

	err := rt.WithTx(ctx, nil, func(ctx context.Context, tx Executor) error {
		if err := Users.Insert(ctx, tx, &user{Name: "pending", Email: "p@example.com"}); err != nil {
			return err
		}

		page, err := q.Page(ctx, tx, Paging{})
		if err == nil && page.Total != 1 {
			err = fmt.Errorf("Total = %d; want the uncommitted row", page.Total)
		}

		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	if !slices.Equal(ops, []TraceOp{TraceOpTx, TraceOpInsert, TraceOpPage}) {
		t.Fatalf("ops = %v; want no nested transaction", ops)
	}
}

func TestPageOrdersCompoundQueriesByOutputName(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)
	seedUsers(t, rt, "b", "a", "c")

	q := Select(User_Name).From(Users).Where(User_Name.LT(Val("c"))).
		Union(Select(User_Name).From(Users).Where(User_Name.EQ(Val("c")))).
		MustBuild()

	page, err := q.Page(ctx, rt, Paging{Size: 2, OrderBy: []OrderBy{User_Name.Desc()}})
	if err != nil {
		t.Fatalf("Page() error = %v", err)
	}

	if page.Total != 3 || len(page.Data) != 2 || page.Data[0].Name != "c" || page.Data[1].Name != "b" {
		t.Fatalf("page = total %d %+v", page.Total, page.Data)
	}

	// SELECT DISTINCT is a grouped query: Count counts distinct rows.
	for _, name := range []string{"a", "b"} {
		dup := &user{Name: name, Email: name + "2@example.com"}
		if err := Users.Insert(ctx, rt, dup); err != nil {
			t.Fatal(err)
		}
	}

	distinct := SelectDistinct(User_Name).From(Users).MustBuild()
	if n, err := distinct.Count(ctx, rt); err != nil || n != 3 {
		t.Fatalf("SelectDistinct count = %d, %v; want 3", n, err)
	}

	if n, err := Select(CountDistinct(User_Name)).From(Users).MustBuild().Count(ctx, rt); err != nil || n != 1 {
		t.Fatalf("aggregate count = %d, %v; want one row", n, err)
	}
}

// TestSoftDeleteScope checks that deleted rows stay out of every place a query
// can put a table, and come back with WithDeleted.
func TestSoftDeleteScope(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)
	rows := seedUsers(t, rt, "live", "gone")

	if err := Orders.BatchInsert(ctx, rt, []*order{{UserID: rows[0].ID, Amount: 1}, {UserID: rows[1].ID, Amount: 2}}); err != nil {
		t.Fatal(err)
	}

	if err := Users.Delete(ctx, rt, rows[1]); err != nil {
		t.Fatal(err)
	}

	count := func(stage QueryStage[order]) int64 {
		t.Helper()

		n, err := stage.Count(ctx, rt)
		if err != nil {
			t.Fatal(err)
		}

		return n
	}

	// INNER JOIN: the order of the deleted user disappears.
	if n := count(Select(Order_ID).From(Orders).Join(Users, User_ID.EQ(Order_UserID))); n != 1 {
		t.Errorf("inner join = %d, want 1", n)
	}

	// LEFT JOIN: both orders stay, but the deleted user does not match.
	left := Select(Order_ID).From(Orders).LeftJoin(Users, User_ID.EQ(Order_UserID)).Where(User_ID.IsNull())
	if n := count(left); n != 1 {
		t.Errorf("left join rows without a live user = %d, want 1", n)
	}

	// RIGHT JOIN: the deleted user is not a preserved row.
	right := Select(Order_ID).From(Orders).RightJoin(Users, User_ID.EQ(Order_UserID))
	if n := count(right); n != 1 {
		t.Errorf("right join = %d, want 1", n)
	}

	withDeleted := Select(Order_ID).From(Orders).Join(Users.WithDeleted(), User_ID.EQ(Order_UserID))
	if n := count(withDeleted); n != 2 {
		t.Errorf("inner join WithDeleted = %d, want 2", n)
	}

	// UpdateTable leaves deleted rows alone unless told otherwise.
	rename := UpdateTable(Users).Set(User_Name, Val("renamed")).Where(And())
	if n, err := rename.Exec(ctx, rt); err != nil || n != 1 {
		t.Fatalf("UpdateTable = %d, %v; want 1", n, err)
	}

	all := UpdateTable(Users.WithDeleted()).Set(User_Name, Val("renamed")).Where(And())
	if n, err := all.Exec(ctx, rt); err != nil || n != 2 {
		t.Fatalf("UpdateTable WithDeleted = %d, %v; want 2", n, err)
	}

	// A second soft delete does not restamp the row.
	before, err := Select(User__Cols...).From(Users.WithDeleted()).Where(User_ID.EQ(Val(rows[1].ID))).Get(ctx, rt)
	if err != nil {
		t.Fatal(err)
	}

	if n, err := DeleteFrom(Users).Where(And()).Exec(ctx, rt); err != nil || n != 1 {
		t.Fatalf("DeleteFrom = %d, %v; want only the live row", n, err)
	}

	after, err := Select(User__Cols...).From(Users.WithDeleted()).Where(User_ID.EQ(Val(rows[1].ID))).Get(ctx, rt)
	if err != nil || after.DeletedAt != before.DeletedAt {
		t.Fatalf("tombstone changed from %d to %d, %v", before.DeletedAt, after.DeletedAt, err)
	}
}

func isErr[E error](err error) bool {
	_, ok := errors.AsType[E](err)

	return ok
}

func TestConditionalWrites(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)
	rows := seedUsers(t, rt, "a", "b", "c")

	rename := UpdateTable(Users).
		Set(User_Name, User_Email).
		Where(User_ID.EQ(User_ID.Param())).
		MustBuild()

	n, err := rename.Exec(ctx, rt, User_ID.Bind(rows[0].ID))
	if err != nil || n != 1 {
		t.Fatalf("Exec() = %d, %v", n, err)
	}

	// UpdateTable does not check versions but bumps them, so the stale row now fails.
	if err := Users.Update(ctx, rt, rows[0]); !IsOptimisticLockError(err) {
		t.Fatalf("expected the loaded row to be stale, got %v", err)
	}

	// The soft-delete stamp is taken at execution, not when the statement is built.
	remove := DeleteFrom(Users).Where(User_ID.EQ(User_ID.Param())).MustBuild()

	before := time.Now().UnixNano()

	if _, err := remove.Exec(ctx, rt, User_ID.Bind(rows[1].ID)); err != nil {
		t.Fatal(err)
	}

	stored, err := Select(User__Cols...).From(Users.WithDeleted()).Where(User_ID.EQ(User_ID.Param())).MustBuild().
		Get(ctx, rt, User_ID.Bind(rows[1].ID))
	if err != nil {
		t.Fatal(err)
	}

	if stored.DeletedAt < before {
		t.Fatalf("tombstone %d is older than the execution (%d)", stored.DeletedAt, before)
	}

	if n, err := HardDeleteFrom(Users).Where(And()).Exec(ctx, rt); err != nil || n != 3 {
		t.Fatalf("HardDeleteFrom(all) = %d, %v", n, err)
	}

	bad := map[string]MutationStage[user]{
		"foreign table":  UpdateTable(Users).Set(User_Name, Val("x")).Where(Order_Amount.GT(Val(int64(1)))),
		"version target": UpdateTable(Users).Set(User_Version, Val(int64(1))).Where(And()),
		"no where":       UpdateTable(Users).Set(User_Name, Val("x")).Where(),
		"no assignment":  UpdateTable(Users).Where(And()),
		"aliased target": UpdateTable(Users).Set(User_Name.As("u"), Val("x")).Where(And()),
		"assigned twice": UpdateTable(Users).Set(User_Name, Val("x")).Set(User_Name, Val("y")).Where(And()),
	}

	for name, stage := range bad {
		if _, err := stage.Build(); err == nil {
			t.Errorf("%s: expected Build to fail", name)
		}
	}
}

func TestTracersAndExecutorScopes(t *testing.T) {
	ctx := context.Background()

	var ops []TraceOp

	rt := newSQLite(t, WithTracers(func(ctx context.Context, op TraceOp, next func(context.Context) error) error {
		ops = append(ops, op)
		return next(ctx)
	}))

	seedUsers(t, rt, "a")

	if _, err := QueryByID.Find(ctx, rt, User_ID.Bind(1)); err != nil {
		t.Fatal(err)
	}

	if len(ops) != 2 || ops[0] != TraceOpInsert || ops[1] != TraceOpGet {
		t.Fatalf("traced %v", ops)
	}

	// A wrapped pool runs statements without the runtime's tracers.
	wrapped := WrapExecutor(rt.DB(), rt.Dialect())
	if _, err := QueryByID.Find(ctx, wrapped, User_ID.Bind(1)); err != nil {
		t.Fatal(err)
	}

	if len(ops) != 2 {
		t.Fatalf("a wrapped executor must not trace, got %v", ops)
	}

	if WrapExecutor(nil, rt.Dialect()) != nil || WrapExecutor(rt.DB(), nil) != nil {
		t.Fatal("WrapExecutor must refuse missing arguments")
	}

	if _, err := QueryByID.Find(ctx, nil, User_ID.Bind(1)); err == nil {
		t.Fatal("expected a nil executor to be refused")
	}
}
