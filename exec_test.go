package tsq

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"
	"weak"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
	sqld "github.com/tmoeish/tsq/v5/internal/sqldialect"
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

	// A new row starts at version 1, the DDL default; the update makes it 2.
	if row.Version != 2 {
		t.Fatalf("version = %d, want 2", row.Version)
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

// TestBatchUpdateWithAStaleRowSaysWhichAndKeepsTheRest covers a batch in which one
// row is stale. A batch is not a transaction, so the statement writes the others;
// they used to be put back to their old updated_at and version, the error could
// not say which row was stale, and retrying the same rows could never succeed.
func TestBatchUpdateWithAStaleRowSaysWhichAndKeepsTheRest(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)
	rows := seedUsers(t, rt, "a", "b", "c")

	// Another writer updates b first.
	other := *rows[1]
	if err := Users.Update(ctx, rt, &other); err != nil {
		t.Fatal(err)
	}

	before := rows[1].UpdatedAt

	for _, r := range rows {
		r.Name += "!"
	}

	err := Users.BatchUpdate(ctx, rt, rows)

	conflict, ok := errors.AsType[*OptimisticLockError](err)
	if !ok || len(conflict.Keys) != 1 || conflict.Keys[0] != rows[1].ID {
		t.Fatalf("BatchUpdate = %v; want an OptimisticLockError naming %d", err, rows[1].ID)
	}

	if rows[0].Version != 2 || rows[2].Version != 2 || rows[1].Version != 1 || !rows[1].UpdatedAt.Equal(before) {
		t.Fatalf("versions = %d %d %d; want the written rows advanced and the stale one untouched", rows[0].Version, rows[1].Version, rows[2].Version)
	}

	// Across statements too: the rows after a stale one are still written.
	for _, r := range rows {
		r.Name += "?"
	}

	err = Users.BatchUpdate(ctx, rt, rows, WithBatchSize(1))
	if conflict, ok := errors.AsType[*OptimisticLockError](err); !ok || len(conflict.Keys) != 1 || conflict.Expected != 3 || conflict.Actual != 2 {
		t.Fatalf("BatchUpdate in statements of one = %v; want one stale key of three", err)
	}

	if rows[0].Version != 3 || rows[2].Version != 3 {
		t.Fatalf("versions = %d, %d; want the rows around the stale one written", rows[0].Version, rows[2].Version)
	}

	// The written rows are current: writing them again succeeds.
	if err := Users.BatchUpdate(ctx, rt, []*user{rows[0], rows[2]}); err != nil {
		t.Fatalf("retrying the written rows = %v", err)
	}

	// Only the stale row needs reloading.
	fresh, err := Users.Get(ctx, rt, rows[1].ID)
	if err != nil {
		t.Fatal(err)
	}

	fresh.Name = "b!"
	if err := Users.BatchUpdate(ctx, rt, []*user{fresh}); err != nil {
		t.Fatalf("retrying the reloaded row = %v", err)
	}
}

// TestUpdateWithoutAVersionReportsAMissingRow covers a table without a version
// column, where an update that matched nothing used to report success.
func TestUpdateWithoutAVersionReportsAMissingRow(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)

	missing := &order{ID: 404, UserID: 1, Amount: 1}

	err := Orders.Update(ctx, rt, missing)
	if state, ok := errors.AsType[*RowStateError](err); !ok || len(state.Keys) != 1 || state.Keys[0] != int64(404) {
		t.Fatalf("Update of a missing row = %v; want a RowStateError naming it", err)
	}

	kept := &order{UserID: 1, Amount: 1}
	if err := Orders.Insert(ctx, rt, kept); err != nil {
		t.Fatal(err)
	}

	// Writing the values a row already holds is not a missing row, although MySQL
	// reports no row affected.
	if err := Orders.BatchUpdate(ctx, rt, []*order{kept}); err != nil {
		t.Fatalf("Update with unchanged values = %v", err)
	}
}

// TestFailedWritesLeaveRowsAsTheyWere covers Insert and Upsert failing on a
// unique index: the rows used to keep the timestamps (and Upsert the cleared
// tombstone) of a write the database never stored.
func TestFailedWritesLeaveRowsAsTheyWere(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)
	seedUsers(t, rt, "taken")

	dup := &user{Name: "dup", Email: "taken@example.com"}
	if err := Users.Insert(ctx, rt, dup); err == nil {
		t.Fatal("expected the duplicate email to be refused")
	}

	if !dup.CreatedAt.IsZero() || !dup.UpdatedAt.IsZero() || dup.ID != 0 {
		t.Fatalf("failed Insert left %+v", dup)
	}

	// A batch keeps what its earlier statements stored.
	batch := []*user{{Name: "ok", Email: "ok@example.com"}, {Name: "dup", Email: "taken@example.com"}}
	if err := Users.BatchInsert(ctx, rt, batch, WithBatchSize(1)); err == nil {
		t.Fatal("expected the duplicate email to be refused")
	}

	if batch[0].ID == 0 || batch[0].CreatedAt.IsZero() || !batch[1].CreatedAt.IsZero() {
		t.Fatalf("batch rows = %+v, %+v; want the first stored and the second untouched", batch[0], batch[1])
	}

	clash := &user{ID: batch[0].ID, Name: "clash", Email: "taken@example.com", DeletedAt: 7}
	if err := Users.Upsert(ctx, rt, clash); err == nil {
		t.Fatal("expected the duplicate email to be refused")
	}

	if clash.DeletedAt != 7 || !clash.UpdatedAt.IsZero() || !clash.CreatedAt.IsZero() {
		t.Fatalf("failed Upsert left %+v", clash)
	}
}

func TestDeleteIsSoftWhenTheTableHasDeletedAt(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)
	rows := seedUsers(t, rt, "a", "b", "c", "d")

	if err := Users.Delete(ctx, rt, rows[0]); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	if rows[0].DeletedAt == 0 || rows[0].Version != 2 {
		t.Fatalf("soft delete must stamp the row and bump its version: %+v", rows[0])
	}

	if err := Users.BatchDeleteByPK(ctx, rt, []int64{rows[1].ID}); err != nil {
		t.Fatalf("BatchDeleteByPK() error = %v", err)
	}

	if err := Users.HardDelete(ctx, rt, rows[2]); err != nil {
		t.Fatalf("HardDelete() error = %v", err)
	}

	if err := Users.BatchHardDeleteByPK(ctx, rt, []int64{rows[3].ID}); err != nil {
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

	if err := Users.BatchDeleteByPK(ctx, rt, nil); err != nil {
		t.Fatalf("no keys must delete nothing: %v", err)
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

	// Two rows with one key: the statement would write only the first.
	row := seedUsers(t, rt, "one")[0]
	twin := *row
	twin.Name = "two"

	if err := Users.BatchUpdate(ctx, rt, []*user{row, &twin}); err == nil || !strings.Contains(err.Error(), "same primary key") {
		t.Fatalf("BatchUpdate of one key twice = %v; want it refused", err)
	}
}

func TestSkipDuplicatesInsideATransaction(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)
	seedUsers(t, rt, "dup")

	err := rt.WithTx(ctx, func(ctx context.Context, tx Executor) error {
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

	total, err := SelectValue(Order_Amount).From(Orders).MustBuild().Get(ctx, rt)
	if err != nil || *total != 50 {
		t.Fatalf("SelectValue() = %v, %v", total, err)
	}

	sum := SelectNullValue(Sum(Order_Amount)).From(Orders).MustBuild()
	if v, err := sum.Get(ctx, rt); err != nil || !v.Valid || v.V != 500 {
		t.Fatalf("SelectNullValue(SUM) = %v, %v", v, err)
	}

	big := Select(Order_ID).From(Orders).Correlate(Users).Where(Order_UserID.EQ(User_ID), Order_Amount.GT(Val(int64(100))))

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

	q := Select(User__Cols...).From(Users).Search(Users.searchColumns()[0], Users.searchColumns()[1:]...).MustBuild()

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
	if err != nil || page.Total != 4 || len(page.Data) != 1 || page.TotalPages != 2 || page.Page != 2 || page.HasNext() {
		t.Fatalf("second page = %+v, %v", page, err)
	}

	// Only the columns the endpoint names are sortable, whatever the query selects.
	if _, err := (&PageRequest{OrderBy: "email"}).Paging(User_ID, User_Name); !isErr[*PageRequestError](err) {
		t.Fatalf("unknown sort field error = %v", err)
	}

	// One direction applies to every field, as PageRequest.Order documents.
	one, err := (&PageRequest{OrderBy: "id,name", Order: "desc"}).Paging(User_ID, User_Name)
	if err != nil || len(one.OrderBy) != 2 || one.OrderBy[0].direction != orderDesc || one.OrderBy[1].direction != orderDesc {
		t.Fatalf("one direction for every field = %+v, %v", one, err)
	}

	if _, err := (&PageRequest{OrderBy: "id,name", Order: "asc,desc,asc"}).Paging(User_ID, User_Name); !isErr[*PageRequestError](err) {
		t.Fatalf("order count mismatch error = %v", err)
	}

	if _, err := (&PageRequest{OrderBy: "id"}).Paging(User_ID, Order_ID); !isErr[*PageRequestError](err) {
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
	if err != nil || empty.Data == nil || len(empty.Data) != 0 || empty.Size != 20 || empty.Page != 1 {
		t.Fatalf("empty page = %+v, %v", empty, err)
	}
}

// TestBatchUpsertComparesTheKeysItWrites covers the check that refuses two rows
// with one key in a batch. It ran before deleted_at was cleared, so a row passed
// with a tombstone and a live row of the same email looked different, and SQLite
// silently kept one of them.
func TestBatchUpsertComparesTheKeysItWrites(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)

	rows := []*user{
		{Name: "one", Email: "dup@example.com", DeletedAt: 12345},
		{Name: "two", Email: "dup@example.com"},
	}

	err := Users.BatchUpsert(ctx, rt, rows, OnConflict(User_Email))
	if err == nil || !strings.Contains(err.Error(), "two rows have the key") {
		t.Fatalf("BatchUpsert = %v; want the duplicate key refused", err)
	}

	// Keys compare by value, not by the address a pointer holds.
	a, b := "x", "x"
	if keyText(&a) != keyText(&b) || keyText(&a) != keyText("x") {
		t.Fatalf("keyText(&a) = %s, keyText(&b) = %s; want equal values to be equal keys", keyText(&a), keyText(&b))
	}
}

func TestUpsertMatchesLiveRowsOfASoftDeletedUniqueIndex(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)

	gone := &user{Name: "old", Email: "same@example.com"}
	if err := Users.Upsert(ctx, rt, gone, OnConflict(User_Email)); err != nil {
		t.Fatal(err)
	}

	if err := Users.Delete(ctx, rt, gone); err != nil {
		t.Fatal(err)
	}

	// The unique index is (email, deleted_at); the deleted row does not match.
	live := &user{Name: "new", Email: "same@example.com"}
	if err := Users.Upsert(ctx, rt, live, OnConflict(User_Email)); err != nil {
		t.Fatal(err)
	}

	if live.ID == gone.ID {
		t.Fatal("expected a new row next to the deleted one")
	}

	if err := Users.Upsert(ctx, rt, &user{Name: "renamed", Email: "same@example.com"}, OnConflict(User_Email)); err != nil {
		t.Fatal(err)
	}

	stored, err := QueryByID.Get(ctx, rt, User_ID.Bind(live.ID))
	if err != nil || stored.Name != "renamed" || stored.Version != live.Version+1 {
		t.Fatalf("stored = %+v, %v", stored, err)
	}

	if err := Users.Upsert(ctx, rt, &user{}, OnConflict(User_Name)); err == nil {
		t.Fatal("expected a key that is not unique to be refused")
	}

	if err := Users.Upsert(ctx, rt, &user{}, OnConflict(User_Email.Rebind(Users.As("u")))); err == nil {
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

	// One statement is not worth a transaction; several share one snapshot.
	var ops []TraceOp

	traced := newSQLite(t, WithTracers(func(ctx context.Context, info TraceInfo, next func(context.Context) error) error {
		ops = append(ops, info.Op)
		return next(ctx)
	}))

	seedUsers(t, traced, "a")

	small := Select(User__Cols...).From(Users).Where(User_ID.In(User_ID.ListParam())).MustBuild()
	if _, err := small.ListIn(ctx, traced, User_ID.ListParam(), []int64{1, 2}); err != nil {
		t.Fatal(err)
	}

	if len(ops) != 2 || ops[1] != TraceOpList {
		t.Fatalf("ops = %v; want the insert and one list without a transaction", ops)
	}

	list := User_ID.ListParam()
	refused := map[string]*Query[user]{
		"limited":    Select(User__Cols...).From(Users).Where(User_ID.In(list)).OrderBy(User_ID.Asc()).Limit(10).MustBuild(),
		"not in":     Select(User__Cols...).From(Users).Where(User_ID.NotIn(list)).MustBuild(),
		"under or":   Select(User__Cols...).From(Users).Where(Or(User_ID.In(list), User_Name.EQ(Val("a")))).MustBuild(),
		"under not":  Select(User__Cols...).From(Users).Where(Not(User_ID.In(list))).MustBuild(),
		"used twice": Select(User__Cols...).From(Users).Where(User_ID.In(list), User_Version.In(Vals[int64]()), User_ID.NotIn(list)).MustBuild(),
		"distinct":   SelectDistinct(User__Cols...).From(Users).Where(User_ID.In(list)).MustBuild(),
	}

	for name, q := range refused {
		if _, err := q.ListIn(ctx, rt, list, ids); err == nil {
			t.Errorf("%s: expected ListIn to refuse the query", name)
		}
	}
}

func TestSelectValueReadsOneExpression(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)
	seedUsers(t, rt, "a", "b", "c")

	// The rows are the values themselves, and the query is a query like any other.
	names, err := SelectValue(Upper(User_Name)).From(Users).Where(User_Name.NE(Val("b"))).
		OrderBy(User_Name.Desc()).MustBuild().List(ctx, rt)
	if err != nil || len(names) != 2 || *names[0] != "C" || *names[1] != "A" {
		t.Fatalf("List = %v, %v", names, err)
	}

	page, err := SelectValue(User_Name).From(Users).MustBuild().
		Page(ctx, rt, Paging{Size: 2, OrderBy: []OrderBy{User_Name.Asc()}})
	if err != nil || page.Total != 3 || len(page.Data) != 2 || *page.Data[0] != "a" {
		t.Fatalf("Page = %+v, %v", page, err)
	}

	// It is a subquery like any other, too.
	last, err := Select(User__Cols...).From(Users).Where(User_ID.EQ(SelectValue(Max(User_ID)).From(Users))).MustBuild().Get(ctx, rt)
	if err != nil || last.Name != "c" {
		t.Fatalf("subquery = %+v, %v", last, err)
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

	rt := newSQLite(t, WithTracers(func(ctx context.Context, info TraceInfo, next func(context.Context) error) error {
		ops = append(ops, info.Op)
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

	err := rt.WithTx(ctx, func(ctx context.Context, tx Executor) error {
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

	if n, err := SelectValue(CountDistinct(User_Name)).From(Users).MustBuild().Get(ctx, rt); err != nil || *n != 3 {
		t.Fatalf("COUNT(DISTINCT) = %v, %v; want 3", n, err)
	}
}

// TestSetOperationChainsRunLeftToRight runs A UNION B INTERSECT C, which reads as
// (A ∪ B) ∩ C. MySQL and PostgreSQL would compute A ∪ (B ∩ C) from the flat SQL.
func TestSetOperationChainsRunLeftToRight(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)
	rows := seedUsers(t, rt, "a", "b", "c")

	named := func(names ...string) WhereStage[int64] {
		return SelectValue(User_ID).From(Users).Where(User_Name.In(Vals(names...)))
	}

	ids, err := named("a", "b").Union(named("c")).Intersect(named("a", "c")).OrderBy(User_ID.Asc()).MustBuild().List(ctx, rt)
	if err != nil {
		t.Fatal(err)
	}

	if len(ids) != 2 || *ids[0] != rows[0].ID || *ids[1] != rows[2].ID {
		t.Fatalf("ids = %v; want a and c", ids)
	}

	// The count wraps the query as a derived table.
	n, err := named("a", "b").Union(named("c")).Intersect(named("a", "c")).MustBuild().Count(ctx, rt)
	if err != nil || n != 2 {
		t.Fatalf("count = %d, %v; want 2", n, err)
	}
}

// TestPageChecksTheOrderOfACompoundQuery covers Paging.OrderBy on a set
// operation, which skipped the check the builder's OrderBy gets: Upper(col) was
// ordered by col, and another table's column was bound by its name.
func TestPageChecksTheOrderOfACompoundQuery(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)
	seedUsers(t, rt, "a", "b")

	q := Select(User_Name).From(Users).Union(Select(User_Name).From(Users)).MustBuild()

	for name, tc := range map[string]struct {
		ob   OrderBy
		want string
	}{
		"expression":  {Upper(User_Name).Asc(), "ordered by its output columns"},
		"not output":  {User_Email.Asc(), "ordered by its output columns"},
		"other table": {Order_Note.Asc(), "is not in this query's FROM/JOIN"},
	} {
		if _, err := q.Page(ctx, rt, Paging{OrderBy: []OrderBy{tc.ob}}); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: Page = %v; want the term refused", name, err)
		}
	}

	if _, err := q.Page(ctx, rt, Paging{OrderBy: []OrderBy{User_Name.Desc()}}); err != nil {
		t.Fatalf("ordering by an output column: %v", err)
	}
}

// TestCountOfAGroupedQueryWithARepeatedName runs the count of a DISTINCT query
// that selects one name twice, a derived table MySQL would refuse (error 1060).
func TestCountOfAGroupedQueryWithARepeatedName(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)
	seedUsers(t, rt, "a", "b")

	upper := MapInto(Upper(User_Name), func(u *user) *string { return &u.Email })

	page, err := SelectDistinct(User_Name, upper).From(Users).MustBuild().Page(ctx, rt, Paging{Size: 1})
	if err != nil || page.Total != 2 || len(page.Data) != 1 {
		t.Fatalf("page = %+v, %v", page, err)
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
	if n := count(Select(Order_ID).From(Orders).InnerJoin(Users, User_ID.EQ(Order_UserID))); n != 1 {
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

	withDeleted := Select(Order_ID).From(Orders).InnerJoin(Users.WithDeleted(), User_ID.EQ(Order_UserID))
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
		"aliased target": UpdateTable(Users).Set(User_Name.Rebind(Users.As("u")), Val("x")).Where(And()),
		"assigned twice": UpdateTable(Users).Set(User_Name, Val("x")).Set(User_Name, Val("y")).Where(And()),
		// A nil table or column is a build error, not a panic while building.
		"nil table":  UpdateTable[user](nil).Set(User_Name, Val("x")).Where(And()),
		"nil column": UpdateTable(Users).Set(Column[user, string](nil), Val("x")).Where(And()),
		"nil soft":   DeleteFrom((*SoftDeleteTableOf[user, int64])(nil)).Where(And()),
		"nil hard":   HardDeleteFrom[user](nil).Where(And()),
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

	rt := newSQLite(t, WithTracers(func(ctx context.Context, info TraceInfo, next func(context.Context) error) error {
		ops = append(ops, info.Op)
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
	wrapped := mustWrap(t, rt.DB(), rt.Dialect())
	if _, err := QueryByID.Find(ctx, wrapped, User_ID.Bind(1)); err != nil {
		t.Fatal(err)
	}

	if len(ops) != 2 {
		t.Fatalf("a wrapped executor must not trace, got %v", ops)
	}

	// Exists is its own operation: a tracer must tell it from reading the row.
	if _, err := QueryByID.Exists(ctx, rt, User_ID.Bind(1)); err != nil {
		t.Fatal(err)
	}

	if len(ops) != 3 || ops[2] != TraceOpExists {
		t.Fatalf("Exists traced as %v", ops)
	}

	if _, err := WrapExecutor(nil, rt.Dialect()); err == nil {
		t.Fatal("WrapExecutor must refuse a nil handle")
	}

	if _, err := WrapExecutor(rt.DB(), ""); err == nil {
		t.Fatal("WrapExecutor must refuse an unknown engine")
	}

	if _, err := QueryByID.Find(ctx, nil, User_ID.Bind(1)); err == nil {
		t.Fatal("expected a nil executor to be refused")
	}
}

// TestUpdateWritesOnlyTheNamedColumns is the partial-Select hazard: a row read
// with some columns and saved with Update(cols...) keeps the columns it did not
// read, and still gets its version and updated_at maintained.
func TestUpdateWritesOnlyTheNamedColumns(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)

	row := &user{Name: "ada", Email: "ada@x"}
	if err := Users.Insert(ctx, rt, row); err != nil {
		t.Fatal(err)
	}

	partial, err := Select(User_ID, User_Name, User_Version).From(Users).Where(User_ID.EQ(Val(row.ID))).Get(ctx, rt)
	if err != nil {
		t.Fatal(err)
	}

	partial.Name = "Ada"
	if err := Users.Update(ctx, rt, partial, User_Name); err != nil {
		t.Fatal(err)
	}

	stored, err := Users.Get(ctx, rt, row.ID)
	if err != nil {
		t.Fatal(err)
	}

	if stored.Name != "Ada" || stored.Email != "ada@x" || stored.Version != row.Version+1 || stored.UpdatedAt.IsZero() {
		t.Fatalf("partial update stored %+v", stored)
	}

	for name, col := range map[string]BoundColumn[user]{"primary key": User_ID, "version": User_Version, "created_at": User_CreatedAt} {
		if err := Users.Update(ctx, rt, stored, col); err == nil {
			t.Errorf("Update(%s) was accepted", name)
		}
	}
}

// TestPartialRowsRefuseAFullUpdate is the partial-Select hazard closed: a row read
// with some columns cannot be saved whole, only with the columns it holds.
func TestPartialRowsRefuseAFullUpdate(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)

	row := &user{Name: "ada", Email: "ada@x"}
	if err := Users.Insert(ctx, rt, row); err != nil {
		t.Fatal(err)
	}

	partial, err := Select(User_ID, User_Name, User_Version).From(Users).Where(User_ID.EQ(Val(row.ID))).Get(ctx, rt)
	if err != nil {
		t.Fatal(err)
	}

	partial.Name = "Ada"

	err = Users.Update(ctx, rt, partial)
	if err == nil || !strings.Contains(err.Error(), "read with only id, name, version") {
		t.Fatalf("full Update of a partial row = %v, want a refusal naming the columns read", err)
	}

	if err := Users.BatchUpdate(ctx, rt, []*user{partial}); err == nil {
		t.Fatal("expected BatchUpdate of a partial row to be refused")
	}

	if err := Users.Upsert(ctx, rt, partial); err == nil {
		t.Fatal("expected Upsert of a partial row to be refused")
	}

	// Inserting it would write zero values into the columns it lacks.
	if err := Users.Insert(ctx, rt, partial); err == nil {
		t.Fatal("expected Insert of a partial row to be refused")
	}

	if err := Users.Update(ctx, rt, partial, User_Name); err != nil {
		t.Fatalf("Update naming the columns = %v", err)
	}

	// Rows read whole, and projections into other types, are not partial.
	full, err := Users.Get(ctx, rt, row.ID)
	if err != nil {
		t.Fatal(err)
	}

	full.Email = "ada@y"
	if err := Users.Update(ctx, rt, full); err != nil || full.Name != "Ada" {
		t.Fatalf("full Update of a full row = %v (name %q)", err, full.Name)
	}
}

type slugged struct {
	ID    int64
	Title string
	Slug  string
}

// sluggedTable has a generated column, which no write path ever writes.
var (
	sluggedHandle = NewTable[slugged, int64]("slugged")
	Slugged_ID    = NewColumn(sluggedHandle, "id", "id", func(r *slugged) *int64 { return &r.ID })
	Slugged_Title = NewColumn(sluggedHandle, "title", "title", func(r *slugged) *string { return &r.Title })
	Slugged_Slug  = NewColumn(sluggedHandle, "slug", "slug", func(r *slugged) *string { return &r.Slug })
	sluggedTable  = sluggedHandle.Define(TableSpec[slugged, int64]{
		Columns:       []BoundColumn[slugged]{Slugged_ID, Slugged_Title, Slugged_Slug},
		PrimaryKey:    Slugged_ID,
		AutoIncrement: true,
		ColumnSpecs: []tsqdialect.ColumnSpec{
			{Name: "id", Type: tsqdialect.ColumnType{Kind: tsqdialect.ColumnKindInt, Bits: 64}, PrimaryKey: true, AutoIncrement: true},
			{Name: "title", Type: tsqdialect.ColumnType{Kind: tsqdialect.ColumnKindString, Size: 64}},
			{Name: "slug", Type: tsqdialect.ColumnType{Kind: tsqdialect.ColumnKindString, Size: 64}, Fill: tsqdialect.FillGenerated, Generated: "LOWER(title)"},
		},
	})
)

// TestRowsWithoutTheirGeneratedColumnsAreWhole covers a row read with every
// column but a generated one. No write path writes a generated column, so such a
// row is whole; it used to be refused as partial.
func TestRowsWithoutTheirGeneratedColumnsAreWhole(t *testing.T) {
	ctx := context.Background()

	rt, err := Open(ctx, "sqlite", filepath.Join(t.TempDir(), "slug.db"), []Table{sluggedTable}, WithSchemaPolicy(SchemaPolicyCreateMissing))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = rt.Close() })

	row := &slugged{Title: "Go"}
	if err := sluggedTable.Insert(ctx, rt, row); err != nil {
		t.Fatal(err)
	}

	read, err := Select(Slugged_ID, Slugged_Title).From(sluggedTable).Where(Slugged_ID.EQ(Val(row.ID))).Get(ctx, rt)
	if err != nil {
		t.Fatal(err)
	}

	read.Title = "Rust"
	if err := sluggedTable.Update(ctx, rt, read); err != nil {
		t.Fatalf("Update of a row read without its generated column = %v", err)
	}

	if got, err := sluggedTable.Get(ctx, rt, row.ID); err != nil || got.Slug != "rust" {
		t.Fatalf("stored = %+v, %v", got, err)
	}
}

// TestPartialRowsAreForgotten keeps the registry from growing without bound: a
// partial row that is no longer referenced leaves it after a collection.
func TestPartialRowsAreForgotten(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)
	seedUsers(t, rt, "a", "b", "c")

	rows, err := Select(User_ID, User_Name).From(Users).List(ctx, rt)
	if err != nil || len(rows) != 3 {
		t.Fatalf("List = %d rows, %v", len(rows), err)
	}

	keys := make([]weak.Pointer[user], 0, len(rows))
	for _, row := range rows {
		keys = append(keys, weak.Make(row))
	}

	remembered := func() int {
		n := 0

		for _, key := range keys {
			if _, ok := partialRows.Load(key); ok {
				n++
			}
		}

		return n
	}

	if remembered() != 3 {
		t.Fatalf("registry holds %d of the rows, want 3", remembered())
	}

	rows = nil
	_ = rows

	for range 50 {
		runtime.GC()

		if remembered() == 0 {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("registry still holds %d of the rows after collection", remembered())
}

// TestSkipDuplicatesKeepsTheRowsItStored covers BatchInsert with
// WithSkipDuplicates, which never recorded the rows it stored: when a later row
// failed, the rows already in the table lost their keys and stamps in memory, and
// a row skipped as a duplicate kept stamps the database never saw.
func TestSkipDuplicatesKeepsTheRowsItStored(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)
	seedUsers(t, rt, "taken")

	if _, err := rt.ExecContext(ctx, `CREATE TRIGGER no_boom BEFORE INSERT ON users WHEN NEW.name = 'boom' BEGIN SELECT RAISE(ABORT, 'boom refused'); END`); err != nil {
		t.Fatal(err)
	}

	stored := &user{Name: "ok", Email: "ok@example.com"}
	skipped := &user{Name: "dup", Email: "taken@example.com"}
	failed := &user{Name: "boom", Email: "boom@example.com"}

	err := Users.BatchInsert(ctx, rt, []*user{stored, skipped, failed}, WithSkipDuplicates())
	if err == nil || !strings.Contains(err.Error(), "boom refused") {
		t.Fatalf("BatchInsert = %v; want the trigger's refusal", err)
	}

	if stored.ID == 0 || stored.CreatedAt.IsZero() {
		t.Fatalf("stored row = %+v; want its key and stamps kept", stored)
	}

	if skipped.ID != 0 || !skipped.CreatedAt.IsZero() || !failed.CreatedAt.IsZero() {
		t.Fatalf("skipped = %+v, failed = %+v; want both as they were", skipped, failed)
	}

	// Without a failure, a skipped row still keeps no stamps.
	again := &user{Name: "dup", Email: "taken@example.com"}
	if err := Users.BatchInsert(ctx, rt, []*user{again}, WithSkipDuplicates()); err != nil || !again.CreatedAt.IsZero() {
		t.Fatalf("skipped duplicate = %+v, %v; want no stamps", again, err)
	}
}

type blobRow struct {
	ID   int64
	Data []byte
	Raw  json.RawMessage
}

var (
	blobHandle = NewTable[blobRow, int64]("blobs")
	Blob_ID    = NewColumn(blobHandle, "id", "id", func(r *blobRow) *int64 { return &r.ID })
	Blob_Data  = NewColumn(blobHandle, "data", "data", func(r *blobRow) *[]byte { return &r.Data })
	Blob_Raw   = NewColumn(blobHandle, "raw", "raw", func(r *blobRow) *json.RawMessage { return &r.Raw })
	blobTable  = blobHandle.Define(TableSpec[blobRow, int64]{
		Columns:       []BoundColumn[blobRow]{Blob_ID, Blob_Data, Blob_Raw},
		PrimaryKey:    Blob_ID,
		AutoIncrement: true,
		ColumnSpecs: []tsqdialect.ColumnSpec{
			{Name: "id", Type: tsqdialect.ColumnType{Kind: tsqdialect.ColumnKindInt, Bits: 64}, PrimaryKey: true, AutoIncrement: true},
			{Name: "data", Type: tsqdialect.ColumnType{Kind: tsqdialect.ColumnKindBytes}},
			{Name: "raw", Type: tsqdialect.ColumnType{Kind: tsqdialect.ColumnKindBytes}},
		},
	})
)

// TestUnsetBytesAreWrittenEmpty covers a []byte field, and one of a named byte
// slice type (json.RawMessage), a NOT NULL column whose zero value is nil: the
// drivers bound it as NULL, and every Insert that left it unset failed.
func TestUnsetBytesAreWrittenEmpty(t *testing.T) {
	ctx := context.Background()

	rt, err := Open(ctx, "sqlite", filepath.Join(t.TempDir(), "blob.db"), []Table{blobTable}, WithSchemaPolicy(SchemaPolicyCreateMissing))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = rt.Close() })

	row := &blobRow{}
	if err := blobTable.Insert(ctx, rt, row); err != nil {
		t.Fatalf("Insert with unset bytes = %v", err)
	}

	if got, err := blobTable.Get(ctx, rt, row.ID); err != nil || len(got.Data) != 0 {
		t.Fatalf("stored = %+v, %v", got, err)
	}
}

// TestRowsReadThroughACTEArePartial covers a row of a table read through a CTE,
// which selects the table's columns rebound to the CTE: it was not recognised as
// partial, and Update wrote zero values over the columns it did not read.
func TestRowsReadThroughACTEArePartial(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)
	user := seedUsers(t, rt, "a")[0]

	if err := Orders.Insert(ctx, rt, &order{UserID: user.ID, Amount: 42, Note: "keep"}); err != nil {
		t.Fatal(err)
	}

	cte := CTE("recent", Select(Order_ID, Order_UserID).From(Orders))

	read, err := Select(Order_ID.Rebind(cte), Order_UserID.Rebind(cte)).From(cte).MustBuild().Get(ctx, rt)
	if err != nil {
		t.Fatal(err)
	}

	if err := Orders.Update(ctx, rt, read); err == nil || !strings.Contains(err.Error(), "read with only id, user_id") {
		t.Fatalf("Update of a row read through a CTE = %v; want it refused", err)
	}

	stored, err := Orders.Get(ctx, rt, read.ID)
	if err != nil || stored.Amount != 42 || stored.Note != "keep" {
		t.Fatalf("stored = %+v, %v; want the unread columns kept", stored, err)
	}
}

// TestCountAgreesWithList covers Count on a limited query, which counted every
// matching row, and on a query ordered by a parameter, which Count refused as an
// argument its statement does not use.
func TestCountAgreesWithList(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)
	seedUsers(t, rt, "a", "b", "c", "d", "e")

	limited := Select(User_ID).From(Users).OrderBy(User_ID.Asc()).Limit(2).MustBuild()
	if n, err := limited.Count(ctx, rt); err != nil || n != 2 {
		t.Fatalf("Count of a query limited to 2 = %d, %v", n, err)
	}

	pinned := NewParam[string]("pinned")
	first := MapInto(Case[int64](User_Name.EQ(pinned), Val(int64(0))).Else(Val(int64(1))).End(), func(r *user) *int64 { return &r.Version })
	ordered := Select(User_ID, first).From(Users).OrderBy(first.Asc()).MustBuild()

	if n, err := ordered.Count(ctx, rt, pinned.Bind("c")); err != nil || n != 5 {
		t.Fatalf("Count with the arguments List takes = %d, %v", n, err)
	}
}

// TestQueriesRefuseWhatTheyCannotRun covers shapes that built and then failed:
// a lock on DISTINCT or aggregated rows (PostgreSQL refuses it), Correlate in a
// CTE (which sees no outer query) or in a top-level set-operation operand, and
// Get on a nil query, which panicked.
func TestQueriesRefuseWhatTheyCannotRun(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)

	for name, stage := range map[string]QueryStage[user]{
		"distinct":  SelectDistinct(User_Name).From(Users).ForUpdate(),
		"aggregate": Select(MapInto(Count(User_ID), func(r *user) *int64 { return &r.Version })).From(Users).ForShare(),
	} {
		if _, err := stage.Build(); err == nil || !strings.Contains(err.Error(), "cannot be locked") {
			t.Errorf("lock on %s rows: Build = %v", name, err)
		}
	}

	cte := CTE("outer_ref", Select(Order_ID).From(Orders).Correlate(Users).Where(Order_UserID.EQ(User_ID)))
	if _, err := Select(User_ID).From(Users).InnerJoin(cte, Order_ID.Rebind(cte).EQ(User_ID)).Build(); err == nil || !strings.Contains(err.Error(), "cannot use Correlate") {
		t.Errorf("Correlate in a CTE: Build = %v", err)
	}

	operand := Select(User_ID).From(Users).Union(Select(User_ID).From(Users).Correlate(Orders).Where(User_ID.EQ(Order_UserID))).MustBuild()
	if _, err := operand.List(ctx, rt); err == nil || !strings.Contains(err.Error(), "Correlate") {
		t.Errorf("Correlate in a top-level operand: List = %v", err)
	}

	var missing *Query[user]
	if _, err := missing.Get(ctx, rt); err == nil {
		t.Error("Get on a nil query: want an error")
	}
}

// TestStatementsByConditionMeanTheSameOnEveryDialect covers two UpdateTable and
// DeleteFrom shapes MySQL runs differently or not at all: an assignment reading a
// column assigned before it (MySQL reads the new value, the others the old one),
// and a subquery reading the table the statement writes (MySQL error 1093).
func TestStatementsByConditionMeanTheSameOnEveryDialect(t *testing.T) {
	swap := UpdateTable(Orders).Set(Order_Amount, Order_UserID).Set(Order_UserID, Order_Amount).Where(And())
	if _, err := swap.Build(); err == nil || !strings.Contains(err.Error(), "assigns before it") {
		t.Fatalf("a swap: Build = %v; want it refused", err)
	}

	small := SelectValue(Order_ID).From(Orders).Where(Order_Amount.LT(Val(int64(10))))

	cleanup, err := HardDeleteFrom(Orders).Where(Order_ID.In(small)).Build()
	if err != nil {
		t.Fatal(err)
	}

	if _, _, err := cleanup.SQL(onMySQL); err == nil || !strings.Contains(err.Error(), "error 1093") {
		t.Fatalf("on MySQL: SQL = %v; want it refused", err)
	}

	if _, _, err := cleanup.SQL(onSQLite); err != nil {
		t.Fatalf("on SQLite: %v", err)
	}
}

// TestAConflictOnlyInTimesIsNotMistakenForOurWrite covers the read-back after a
// version conflict, which left times out: another writer that set the same values
// and only a different updated_at looked like our own write, so the row took the
// new version in memory and the next Update overwrote the other writer's change.
func TestAConflictOnlyInTimesIsNotMistakenForOurWrite(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)
	row := seedUsers(t, rt, "a")[0]

	if _, err := UpdateTable(Users).Set(User_Name, Val("renamed")).Where(User_ID.EQ(Val(row.ID))).Exec(ctx, rt); err != nil {
		t.Fatal(err)
	}

	row.Name = "renamed"

	err := Users.Update(ctx, rt, row)

	conflict, ok := errors.AsType[*OptimisticLockError](err)
	if !ok || len(conflict.Keys) != 1 {
		t.Fatalf("Update = %v; want a conflict naming the row", err)
	}

	if row.Version != 1 {
		t.Fatalf("version in memory = %d; want the loaded 1, the write did not happen", row.Version)
	}

	if err := Users.Update(ctx, rt, row); !IsOptimisticLockError(err) {
		t.Fatalf("second Update = %v; want the conflict again", err)
	}
}

type account struct {
	Code  string
	Email string
	Name  string
}

var (
	accountsHandle = NewTable[account, string]("accounts")
	Account_Code   = NewColumn(accountsHandle, "code", "code", func(r *account) *string { return &r.Code })
	Account_Email  = NewColumn(accountsHandle, "email", "email", func(r *account) *string { return &r.Email })
	Account_Name   = NewColumn(accountsHandle, "name", "name", func(r *account) *string { return &r.Name })
	Accounts       = accountsHandle.Define(TableSpec[account, string]{
		Columns:    []BoundColumn[account]{Account_Code, Account_Email, Account_Name},
		PrimaryKey: Account_Code,
		ColumnSpecs: []tsqdialect.ColumnSpec{
			{Name: "code", Type: tsqdialect.ColumnType{Kind: tsqdialect.ColumnKindString, Size: 16}, PrimaryKey: true},
			{Name: "email", Type: tsqdialect.ColumnType{Kind: tsqdialect.ColumnKindString, Size: 64}},
			{Name: "name", Type: tsqdialect.ColumnType{Kind: tsqdialect.ColumnKindString, Size: 64}},
		},
		Indexes: []IndexSpec{{Name: "ux_accounts_email", Columns: []string{"email"}, Unique: true}},
	})
)

// TestUpsertByAUniqueKeyAdoptsTheStoredKey covers an upsert by a unique column of
// a table whose key the caller assigns: a row that conflicted updated the stored
// row, but kept the key it proposed, and Upsert then failed reading it back.
func TestUpsertByAUniqueKeyAdoptsTheStoredKey(t *testing.T) {
	ctx := context.Background()

	rt, err := Open(ctx, "sqlite", filepath.Join(t.TempDir(), "accounts.db"), []Table{Accounts}, WithSchemaPolicy(SchemaPolicyCreateMissing))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = rt.Close() })

	if err := Accounts.BatchInsert(ctx, rt, []*account{{Code: "a1", Email: "a@x"}, {Code: "b1", Email: "b@x"}}); err != nil {
		t.Fatal(err)
	}

	one := &account{Code: "a2", Email: "a@x", Name: "second"}
	if err := Accounts.Upsert(ctx, rt, one, OnConflict(Account_Email)); err != nil || one.Code != "a1" {
		t.Fatalf("Upsert = %v, row %+v; want the stored key a1", err, one)
	}

	batch := []*account{{Code: "b2", Email: "b@x", Name: "again"}, {Code: "c1", Email: "c@x", Name: "new"}}
	if err := Accounts.BatchUpsert(ctx, rt, batch, OnConflict(Account_Email)); err != nil || batch[0].Code != "b1" || batch[1].Code != "c1" {
		t.Fatalf("BatchUpsert = %v, rows %+v %+v; want keys b1 and c1", err, batch[0], batch[1])
	}
}

// TestKeysetTakesAProjectionOfTheKey covers PageKeyset over a result type: the
// order had to name the table's own key column, so a projection of it (MapInto)
// was refused as not the primary key, and PageRequest.Keyset could not serve a
// result at all.
func TestKeysetTakesAProjectionOfTheKey(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)
	seedUsers(t, rt, "a", "b", "c")

	id := MapInto(User_ID, func(r *namedRow) *int64 { return &r.ID })
	q := Select(id, MapInto(User_Name, func(r *namedRow) *string { return &r.Name })).From(Users).MustBuild()

	page, err := q.PageKeyset(ctx, rt, Keyset{Size: 2, OrderBy: []OrderBy{id.Asc()}})
	if err != nil || len(page.Data) != 2 || page.Next == "" {
		t.Fatalf("PageKeyset by a projection of the key = %+v, %v", page, err)
	}
}

type badge struct {
	Code  string
	Color *string
}

var (
	badgesHandle = NewTable[badge, string]("badges")
	Badge_Code   = NewColumn(badgesHandle, "code", "code", func(r *badge) *string { return &r.Code })
	Badge_Color  = NewNullColumn[string](badgesHandle, "color", "color", func(r *badge) **string { return &r.Color })
	Badges       = badgesHandle.Define(TableSpec[badge, string]{
		Columns:    []BoundColumn[badge]{Badge_Code, Badge_Color},
		PrimaryKey: Badge_Code,
		ColumnSpecs: []tsqdialect.ColumnSpec{
			{Name: "code", Type: tsqdialect.ColumnType{Kind: tsqdialect.ColumnKindString, Size: 8}, PrimaryKey: true},
			{Name: "color", Type: tsqdialect.ColumnType{Kind: tsqdialect.ColumnKindString, Size: 8, Nullable: true}, Default: "'red'", Fill: tsqdialect.FillDefault},
		},
	})
)

// TestASkippedRowReadsNothingBack covers a single-row BatchInsert with
// WithSkipDuplicates whose row collided: the read-back of the database-filled
// columns ran anyway and copied the stored row's values into the skipped one.
func TestASkippedRowReadsNothingBack(t *testing.T) {
	ctx := context.Background()

	rt, err := Open(ctx, "sqlite", filepath.Join(t.TempDir(), "badges.db"), []Table{Badges}, WithSchemaPolicy(SchemaPolicyCreateMissing))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = rt.Close() })

	blue := "blue"
	if err := Badges.Insert(ctx, rt, &badge{Code: "A", Color: &blue}); err != nil {
		t.Fatal(err)
	}

	skipped := &badge{Code: "A"}
	if err := Badges.BatchInsert(ctx, rt, []*badge{skipped}, WithSkipDuplicates()); err != nil || skipped.Color != nil {
		t.Fatalf("skipped row = %+v, %v; want nothing read into it", skipped, err)
	}

	fresh := &badge{Code: "B"}
	if err := Badges.Insert(ctx, rt, fresh); err != nil || fresh.Color == nil || *fresh.Color != "red" {
		t.Fatalf("inserted row = %+v, %v; want the default read back", fresh, err)
	}
}

// TestEmptyResultsAreEmptyLists covers List over no rows, which returned nil where
// Fetch and Page.Data return an empty slice: the same empty result marshaled to
// null or to [] depending on the call.
func TestEmptyResultsAreEmptyLists(t *testing.T) {
	rows, err := Select(User_ID).From(Users).MustBuild().List(context.Background(), newSQLite(t))
	if err != nil || rows == nil || len(rows) != 0 {
		t.Fatalf("List over no rows = %#v, %v; want an empty, non-nil list", rows, err)
	}
}

// TestStagesRunEveryRead covers the reads a stage runs without Build first: it
// had Page but not Iter or PageKeyset.
func TestStagesRunEveryRead(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)
	seedUsers(t, rt, "a", "b")

	stage := Select(User__Cols...).From(Users)

	n := 0
	for _, err := range stage.Iter(ctx, rt) {
		if err != nil {
			t.Fatal(err)
		}

		n++
	}

	page, err := stage.PageKeyset(ctx, rt, Keyset{Size: 1, OrderBy: []OrderBy{User_ID.Asc()}})
	if n != 2 || err != nil || len(page.Data) != 1 {
		t.Fatalf("Iter read %d rows; PageKeyset = %+v, %v", n, page, err)
	}

	for _, err := range Select(User_ID).From(Orders).Iter(ctx, rt) {
		if err == nil {
			t.Fatal("Iter of a stage that does not build: want its error")
		}
	}
}

type swatch struct {
	ID    int64
	Color *string
}

var (
	swatchesHandle = NewTable[swatch, int64]("swatches")
	Swatch_ID      = NewColumn(swatchesHandle, "id", "id", func(r *swatch) *int64 { return &r.ID })
	Swatch_Color   = NewNullColumn[string](swatchesHandle, "color", "color", func(r *swatch) **string { return &r.Color })
	Swatches       = swatchesHandle.Define(TableSpec[swatch, int64]{
		Columns:       []BoundColumn[swatch]{Swatch_ID, Swatch_Color},
		PrimaryKey:    Swatch_ID,
		AutoIncrement: true,
		ColumnSpecs: []tsqdialect.ColumnSpec{
			{Name: "id", Type: tsqdialect.ColumnType{Kind: tsqdialect.ColumnKindInt, Bits: 64}, PrimaryKey: true, AutoIncrement: true},
			{Name: "color", Type: tsqdialect.ColumnType{Kind: tsqdialect.ColumnKindString, Size: 8, Nullable: true}, Default: "'red'", Fill: tsqdialect.FillDefault},
		},
	})
)

// TestBatchInsertKeepsTheSliceOrder covers a batch whose rows leave different
// default columns to the database: every row of one shape was inserted first, so
// the generated keys did not follow the order of the slice.
func TestBatchInsertKeepsTheSliceOrder(t *testing.T) {
	ctx := context.Background()

	rt, err := Open(ctx, "sqlite", filepath.Join(t.TempDir(), "swatches.db"), []Table{Swatches}, WithSchemaPolicy(SchemaPolicyCreateMissing))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = rt.Close() })

	blue := "blue"
	rows := []*swatch{{Color: &blue}, {}, {Color: &blue}, {}}

	if err := Swatches.BatchInsert(ctx, rt, rows); err != nil {
		t.Fatal(err)
	}

	for i, row := range rows {
		if row.ID != int64(i+1) {
			t.Fatalf("row %d got key %d; want the keys in the order of the slice", i, row.ID)
		}
	}
}

// TestInsertStartsAtVersionOne covers a row TSQ inserted with version 0 while the
// DDL default is 1, so rows TSQ wrote and rows written by hand started apart.
func TestInsertStartsAtVersionOne(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)

	fresh := seedUsers(t, rt, "a")[0]
	if fresh.Version != 1 {
		t.Fatalf("new row version = %d; want 1", fresh.Version)
	}

	imported := &user{Name: "b", Email: "b@example.com", Version: 7}
	if err := Users.Insert(ctx, rt, imported); err != nil {
		t.Fatal(err)
	}

	stored, err := Users.Get(ctx, rt, imported.ID)
	if err != nil || stored.Version != 7 {
		t.Fatalf("imported row = %+v, %v; want the version the caller set", stored, err)
	}

	upserted := &user{Name: "c", Email: "c@example.com"}
	if err := Users.Upsert(ctx, rt, upserted, OnConflict(User_Email)); err != nil || upserted.Version != 1 {
		t.Fatalf("upserted row = %+v, %v; want version 1", upserted, err)
	}
}

// TestSingleRowWritesReadBackInTheStatement covers a single-row Insert or Upsert
// that read the columns the database filled with a second query, on engines whose
// RETURNING reads them in the write itself, and a hard delete logged as "delete",
// the name of a soft delete.
func TestSingleRowWritesReadBackInTheStatement(t *testing.T) {
	ctx := context.Background()
	logger := &recordingLogger{}

	rt, err := Open(ctx, "sqlite", filepath.Join(t.TempDir(), "swatches.db"), []Table{Swatches},
		WithSchemaPolicy(SchemaPolicyCreateMissing), WithLogger(logger), WithSQLLogging())
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = rt.Close() })

	row := &swatch{}
	if err := Swatches.Insert(ctx, rt, row); err != nil || row.ID == 0 || row.Color == nil || *row.Color != "red" {
		t.Fatalf("Insert = %+v, %v; want the key and the default read back", row, err)
	}

	again := &swatch{ID: row.ID}
	if err := Swatches.Upsert(ctx, rt, again); err != nil || again.Color == nil || *again.Color != "red" {
		t.Fatalf("Upsert = %+v, %v; want the default read back", again, err)
	}

	if err := Swatches.HardDelete(ctx, rt, again); err != nil {
		t.Fatal(err)
	}

	if logger.count("insert") != 1 || logger.count("upsert") != 1 || logger.count("reload") != 0 || logger.count("upsert keys") != 0 {
		t.Fatalf("statements = %v; want one per write and no read-back query", logger.messages)
	}

	if logger.count("hard_delete") != 1 || logger.count("delete") != 0 {
		t.Fatalf("statements = %v; want the hard delete logged as hard_delete", logger.messages)
	}
}

// TestUpsertUpdatesOnlyTheNamedColumns covers an upsert that could only write the
// whole row over the row it matched, so a field the caller left nil wrote NULL over
// a stored value.
func TestUpsertUpdatesOnlyTheNamedColumns(t *testing.T) {
	ctx := context.Background()

	rt, err := Open(ctx, "sqlite", filepath.Join(t.TempDir(), "notes.db"), []Table{Notes, Users}, WithSchemaPolicy(SchemaPolicyReconcile))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = rt.Close() })

	body := "kept"
	stored := &note{Body: &body, Title: sql.NullString{String: "old", Valid: true}}
	if err := Notes.Insert(ctx, rt, stored); err != nil {
		t.Fatal(err)
	}

	changed := &note{ID: stored.ID, Title: sql.NullString{String: "new", Valid: true}}
	if err := Notes.Upsert(ctx, rt, changed, OnConflict(Note_ID).Update(Note_Title)); err != nil {
		t.Fatal(err)
	}

	got, err := Notes.Get(ctx, rt, stored.ID)
	if err != nil || got.Title.String != "new" || got.Body == nil || *got.Body != "kept" {
		t.Fatalf("stored = %+v, %v; want the title written and the body kept", got, err)
	}

	// A row that matches nothing is inserted whole.
	fresh := &note{ID: stored.ID + 1, Body: &body}
	if err := Notes.BatchUpsert(ctx, rt, []*note{fresh}, OnConflict(Note_ID).Update(Note_Title)); err != nil {
		t.Fatal(err)
	}

	if got, err := Notes.Get(ctx, rt, fresh.ID); err != nil || got.Body == nil {
		t.Fatalf("inserted = %+v, %v; want the whole row", got, err)
	}

	// The update still refreshes what TSQ maintains.
	seeded := seedUsers(t, rt, "a")[0]
	again := &user{Name: "b", Email: seeded.Email}
	if err := Users.Upsert(ctx, rt, again, OnConflict(User_Email).Update(User_Name)); err != nil || again.Version != 2 || again.ID != seeded.ID {
		t.Fatalf("upsert = %+v, %v; want the stored row at version 2", again, err)
	}

	for name, conflict := range map[string]Conflict[user]{
		"the key":         OnConflict(User_Email).Update(User_Email),
		"the primary key": OnConflict(User_Email).Update(User_ID),
		"a managed one":   OnConflict(User_Email).Update(User_Version),
		"another alias":   OnConflict(User_Email).Update(User_Name.Rebind(Users.As("u"))),
	} {
		if err := Users.Upsert(ctx, rt, &user{Name: "c", Email: "c@example.com"}, conflict); err == nil {
			t.Errorf("Update naming %s: want it refused", name)
		}
	}

	if err := Users.Upsert(ctx, rt, again, OnConflict(User_Email), OnConflict(User_Email)); err == nil {
		t.Error("two Conflicts: want them refused")
	}
}

// TestDeleteByPKNamesTheKeysItDidNotDelete covers BatchDeleteByPK and
// BatchHardDeleteByPK, which reported success for keys that matched nothing.
func TestDeleteByPKNamesTheKeysItDidNotDelete(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)
	users := seedUsers(t, rt, "a", "b", "c")

	err := Users.BatchHardDeleteByPK(ctx, rt, []int64{users[0].ID, 404, users[0].ID, users[1].ID})

	state, ok := errors.AsType[*RowStateError](err)
	if !ok || state.Need != RowExisting || len(state.Keys) != 1 || state.Keys[0] != int64(404) || state.Expected != 3 || state.Actual != 2 {
		t.Fatalf("BatchHardDeleteByPK = %v; want a RowStateError naming 404 only", err)
	}

	left, err := Users.WithDeleted().Fetch(ctx, rt, users[2].ID)
	if err != nil || len(left) != 1 {
		t.Fatalf("the untouched row = %v, %v", left, err)
	}

	if _, err := Users.WithDeleted().Get(ctx, rt, users[0].ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("a deleted key is still there: %v", err)
	}

	// Every key there: no error.
	if err := Users.BatchHardDeleteByPK(ctx, rt, []int64{users[2].ID}); err != nil {
		t.Fatal(err)
	}
}

// TestExistsTakesTheArgumentsOfTheQuery covers Exists, rendered as SELECT 1, which
// refused a parameter the select list or ORDER BY used as unused.
func TestExistsTakesTheArgumentsOfTheQuery(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)
	seedUsers(t, rt, "a")

	type row struct{ N int64 }

	p := NewParam[int64]("threshold")
	q := Select(MapInto(Add(User_ID, p), func(r *row) *int64 { return &r.N })).From(Users).OrderBy(Add(User_ID, p).Asc()).MustBuild()

	if ok, err := q.Exists(ctx, rt, p.Bind(5)); err != nil || !ok {
		t.Fatalf("Exists = %v, %v; want true", ok, err)
	}
}

// TestHardDeleteOfAMissingRowSaysSo covers HardDelete and BatchHardDelete on a
// table without a version column, which reported success for a row that was not
// there while BatchHardDeleteByPK named it.
func TestHardDeleteOfAMissingRowSaysSo(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)
	users := seedUsers(t, rt, "a")

	kept := &order{UserID: users[0].ID, Amount: 1, Note: "kept"}
	gone := &order{UserID: users[0].ID, Amount: 2, Note: "gone"}

	if err := Orders.BatchInsert(ctx, rt, []*order{kept, gone}); err != nil {
		t.Fatal(err)
	}

	if err := Orders.HardDelete(ctx, rt, gone); err != nil {
		t.Fatal(err)
	}

	err := Orders.HardDelete(ctx, rt, gone)
	if state, ok := errors.AsType[*RowStateError](err); !ok || state.Op != TraceOpHardDelete || state.Need != RowExisting {
		t.Fatalf("second HardDelete = %v; want a RowStateError", err)
	}

	err = Orders.BatchHardDelete(ctx, rt, []*order{kept, gone}, WithBatchSize(1))
	if state, ok := errors.AsType[*RowStateError](err); !ok || len(state.Keys) != 1 || state.Keys[0] != gone.ID {
		t.Fatalf("BatchHardDelete = %v; want the missing row named", err)
	}

	if n, err := Select(Orders.Columns()...).From(Orders).Count(ctx, rt); err != nil || n != 0 {
		t.Fatalf("rows left = %d, %v; want the present row deleted", n, err)
	}
}

// TestUpsertUpdateWritesANamedDefaultColumnAsNull covers Conflict.Update naming a
// default: column the row leaves NULL: the stored value was kept, while a row
// Update of that column writes NULL.
func TestUpsertUpdateWritesANamedDefaultColumnAsNull(t *testing.T) {
	ctx := context.Background()

	rt, err := Open(ctx, "sqlite", filepath.Join(t.TempDir(), "swatches.db"), []Table{Swatches}, WithSchemaPolicy(SchemaPolicyCreateMissing))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = rt.Close() })

	blue := "blue"
	row := &swatch{Color: &blue}
	if err := Swatches.Insert(ctx, rt, row); err != nil {
		t.Fatal(err)
	}

	if err := Swatches.Upsert(ctx, rt, &swatch{ID: row.ID}, OnConflict(Swatch_ID).Update(Swatch_Color)); err != nil {
		t.Fatal(err)
	}

	got, err := Swatches.Get(ctx, rt, row.ID)
	if err != nil || got.Color != nil {
		t.Fatalf("color = %v, %v; want NULL, the named column was nil", got.Color, err)
	}

	// Unnamed, the stored value stays.
	if err := Swatches.Update(ctx, rt, &swatch{ID: row.ID, Color: &blue}); err != nil {
		t.Fatal(err)
	}

	if err := Swatches.Upsert(ctx, rt, &swatch{ID: row.ID}); err != nil {
		t.Fatal(err)
	}

	if got, err := Swatches.Get(ctx, rt, row.ID); err != nil || got.Color == nil || *got.Color != "blue" {
		t.Fatalf("color = %v, %v; want the stored blue kept", got, err)
	}
}

// TestBatchUpsertChecksEveryRowBeforeWriting covers a BatchUpsert whose later rows
// could not be written: earlier rows were stored before it failed, rows were
// written out of the slice's order, and a row with only a generated key to write,
// which Insert takes, was refused.
func TestBatchUpsertChecksEveryRowBeforeWriting(t *testing.T) {
	ctx := context.Background()

	rt, err := Open(ctx, "sqlite", filepath.Join(t.TempDir(), "swatches.db"), []Table{Swatches}, WithSchemaPolicy(SchemaPolicyCreateMissing))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = rt.Close() })

	blue := "blue"
	rows := []*swatch{{Color: &blue}, {}, {Color: &blue}, {}}

	if err := Swatches.BatchUpsert(ctx, rt, rows, Conflict[swatch]{}); err != nil {
		t.Fatalf("BatchUpsert = %v; want the rows with only a generated key inserted", err)
	}

	all, err := Select(Swatches.Columns()...).From(Swatches).OrderBy(Swatch_ID.Asc()).MustBuild().List(ctx, rt)
	if err != nil || len(all) != 4 {
		t.Fatalf("rows = %d, %v; want 4", len(all), err)
	}

	for i, want := range []string{"blue", "red", "blue", "red"} {
		if all[i].Color == nil || *all[i].Color != want {
			t.Fatalf("row %d = %v; want %s, in the order of the slice", i, all[i].Color, want)
		}
	}
}

// TestBatchUpsertLeavesTheRowsAsPassed covers a BatchUpsert that stamped
// updated_at and created_at on the rows without the version the database moved
// to, leaving rows that looked current and were not.
func TestBatchUpsertLeavesTheRowsAsPassed(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)
	loaded := seedUsers(t, rt, "a")[0]

	before := *loaded
	if err := Users.BatchUpsert(ctx, rt, []*user{loaded}, Conflict[user]{}); err != nil {
		t.Fatal(err)
	}

	if loaded.Version != before.Version || !loaded.UpdatedAt.Equal(before.UpdatedAt) || !loaded.CreatedAt.Equal(before.CreatedAt) {
		t.Fatalf("row = %+v; want the managed columns as passed (%+v)", *loaded, before)
	}

	stored, err := Users.Get(ctx, rt, loaded.ID)
	if err != nil || stored.Version != before.Version+1 {
		t.Fatalf("stored = %+v, %v; want the upsert written", stored, err)
	}
}

// TestDeleteByPKMatchesKeysAsTheDatabaseDoes covers a string key the database
// matched without case: the key it reported back did not equal the one passed,
// and the deleted row was named as not deleted.
func TestDeleteByPKMatchesKeysAsTheDatabaseDoes(t *testing.T) {
	hit := map[string]bool{"abc": true}

	if got := missingKeys(sqld.MySQLDialect{}, []string{"ABC ", "zz"}, hit); len(got) != 1 || got[0] != "zz" {
		t.Fatalf("missing = %v; want only zz", got)
	}

	if got := missingKeys(sqld.MySQLDialect{}, []string{"ABC"}, hit); got != nil {
		t.Fatalf("missing = %v; want none when every key matched", got)
	}

	if got := missingKeys(sqld.MySQLDialect{}, []int64{1, 2}, map[int64]bool{1: true}); len(got) != 1 || got[0] != int64(2) {
		t.Fatalf("missing = %v; want 2", got)
	}

	// PostgreSQL and SQLite compare keys exactly: "ABC" beside a deleted "abc" was
	// not found there, and was passed over as if it had been.
	for _, exact := range []sqld.Dialect{sqld.PostgresDialect{}, sqld.SQLiteDialect{}} {
		if got := missingKeys(exact, []string{"ABC", "abc"}, hit); len(got) != 1 || got[0] != "ABC" {
			t.Fatalf("%s: missing = %v; want ABC", exact.Name(), got)
		}
	}
}

// TestASplitListInReturnsEachRowOnce covers two keys the database takes as one
// landing in two parts of a split ListIn: both parts matched the row, and the
// result held it twice.
func TestASplitListInReturnsEachRowOnce(t *testing.T) {
	a, b := &user{ID: 1, Name: "a"}, &user{ID: 2, Name: "b"}

	got := dedupeByPrimaryKey([]*user{a, b, {ID: 1, Name: "a again"}})
	if len(got) != 2 || got[0] != a || got[1] != b {
		t.Fatalf("rows = %v; want each key once, the first kept", got)
	}

	type projection struct{ Name string }

	plain := []*projection{{"x"}, {"x"}}
	if got := dedupeByPrimaryKey(plain); len(got) != 2 {
		t.Fatalf("rows = %v; want a result that is not a table's rows left alone", got)
	}
}
