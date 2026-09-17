package tsq

import (
	"context"
	"database/sql"
	"errors"
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

	if err := BatchDeleteByPK(ctx, rt, User_ID, []int64{rows[1].ID}); err != nil {
		t.Fatalf("BatchDeleteByPK() error = %v", err)
	}

	if err := Users.HardDelete(ctx, rt, rows[2]); err != nil {
		t.Fatalf("HardDelete() error = %v", err)
	}

	if err := BatchHardDeleteByPK(ctx, rt, User_ID, []int64{rows[3].ID}); err != nil {
		t.Fatalf("BatchHardDeleteByPK() error = %v", err)
	}

	type state struct {
		ID        int64
		DeletedAt int64
	}

	all, err := Select(User__Cols...).From(Users).OrderBy(User_ID.Asc()).List(ctx, rt)
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

	if err := BatchDeleteByPK(ctx, rt, User_ID.As("u"), []int64{1}); err == nil {
		t.Fatal("expected an aliased key column to be refused")
	}

	if err := BatchDeleteByPK(ctx, rt, User_Version, []int64{1}); err == nil {
		t.Fatal("expected a non-key column to be refused")
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

	sum := Select(Order_Amount.Sum()).From(Orders).MustBuild()
	if v, err := sum.Scalar(ctx, rt, Order_Amount.Sum()); err != nil || v != 500 {
		t.Fatalf("Scalar(SUM) = %d, %v", v, err)
	}

	big, err := BuildSubquery(
		Select(Order_ID).From(Orders).Correlate(Users).Where(Order_UserID.EQ(User_ID), Order_Amount.GTVal(100)),
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
	page, err := q.Page(ctx, rt, &PageRequest{Size: 10, Keyword: "_", OrderBy: "name", Order: "desc"})
	if err != nil {
		t.Fatalf("Page() error = %v", err)
	}

	if page.Total != 2 || len(page.Data) != 2 || page.Data[0].Name != "b_2" {
		t.Fatalf("page = total %d %+v", page.Total, page.Data)
	}

	page, err = q.Page(ctx, rt, &PageRequest{Size: 3, Page: 2, OrderBy: "id"})
	if err != nil || page.Total != 4 || len(page.Data) != 1 || page.TotalPages != 2 {
		t.Fatalf("second page = %+v, %v", page, err)
	}

	if _, err := q.Page(ctx, rt, &PageRequest{OrderBy: "nope"}); !isErr[*UnknownSortFieldError](err) {
		t.Fatalf("unknown sort field error = %v", err)
	}

	if _, err := q.Page(ctx, rt, &PageRequest{OrderBy: "id,name", Order: "asc"}); !isErr[*OrderCountMismatchError](err) {
		t.Fatalf("order count mismatch error = %v", err)
	}

	limited := Select(User_ID).From(Users).Limit(1).MustBuild()
	if _, err := limited.Page(ctx, rt, nil); err == nil {
		t.Fatal("expected Page to refuse a query with its own Limit")
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

	stored, err := QueryByID.Get(ctx, rt, User_ID.Bind(rows[1].ID))
	if err != nil || stored.DeletedAt < before {
		t.Fatalf("tombstone %d is older than the execution (%d): %v", stored.DeletedAt, before, err)
	}

	if n, err := HardDeleteFrom(Users).Where(And()).Exec(ctx, rt); err != nil || n != 3 {
		t.Fatalf("HardDeleteFrom(all) = %d, %v", n, err)
	}

	bad := map[string]MutationStage[user]{
		"foreign table":  UpdateTable(Users).SetVal(User_Name, "x").Where(Order_Amount.GTVal(1)),
		"version target": UpdateTable(Users).SetVal(User_Version, 1).Where(And()),
		"no where":       UpdateTable(Users).SetVal(User_Name, "x").Where(),
		"no assignment":  UpdateTable(Users).Where(And()),
		"aliased target": UpdateTable(Users).SetVal(User_Name.As("u"), "x").Where(And()),
		"assigned twice": UpdateTable(Users).SetVal(User_Name, "x").SetVal(User_Name, "y").Where(And()),
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
