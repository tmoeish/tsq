package tsq

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
)

// memo is a soft-deleted table without a version column: nothing but the WHERE
// clause keeps a stale Update from touching a row deleted in the meantime.
type memo struct {
	ID        int64
	Body      string
	CreatedAt time.Time
	DeletedAt int64
}

var memosHandle = NewSoftDeleteTable[memo, int64]("memos")

var (
	Memo_ID        = NewColumn(memosHandle.TableOf, "id", "id", func(r *memo) *int64 { return &r.ID })
	Memo_Body      = NewColumn(memosHandle.TableOf, "body", "body", func(r *memo) *string { return &r.Body })
	Memo_CreatedAt = NewColumn(memosHandle.TableOf, "created_at", "created_at", func(r *memo) *time.Time { return &r.CreatedAt })
	Memo_DeletedAt = NewColumn(memosHandle.TableOf, "deleted_at", "deleted_at", func(r *memo) *int64 { return &r.DeletedAt })
)

var Memos = memosHandle.Define(TableSpec[memo, int64]{
	Columns:       []BoundColumn[memo]{Memo_ID, Memo_Body, Memo_CreatedAt, Memo_DeletedAt},
	PrimaryKey:    Memo_ID,
	AutoIncrement: true,
	CreatedAt:     Memo_CreatedAt,
	ColumnSpecs: []tsqdialect.ColumnSpec{
		{Name: "id", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64}, PrimaryKey: true, AutoIncrement: true},
		{Name: "body", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindString, Size: 64}},
		{Name: "created_at", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindTime}},
		{Name: "deleted_at", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64}},
	},
}, Memo_DeletedAt)

// ticket is a soft-deleted table in the shape tsq gen writes: a struct that embeds
// *SoftDeleteTableOf, with its columns as fields.
type ticket struct {
	ID        int64
	Body      string
	DeletedAt int64
}

type ticketTable struct {
	*SoftDeleteTableOf[ticket, int64]

	ID        Column[ticket, int64]
	Body      Column[ticket, string]
	DeletedAt Column[ticket, int64]
}

var tickets = func() ticketTable {
	t := NewSoftDeleteTable[ticket, int64]("tickets")
	c := ticketTable{
		SoftDeleteTableOf: t,
		ID:                NewColumn(t.TableOf, "id", "id", func(r *ticket) *int64 { return &r.ID }),
		Body:              NewColumn(t.TableOf, "body", "body", func(r *ticket) *string { return &r.Body }),
		DeletedAt:         NewColumn(t.TableOf, "deleted_at", "deleted_at", func(r *ticket) *int64 { return &r.DeletedAt }),
	}

	t.Define(TableSpec[ticket, int64]{
		Columns:       []BoundColumn[ticket]{c.ID, c.Body, c.DeletedAt},
		PrimaryKey:    c.ID,
		AutoIncrement: true,
		ColumnSpecs: []tsqdialect.ColumnSpec{
			{Name: "id", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64}, PrimaryKey: true, AutoIncrement: true},
			{Name: "body", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindString, Size: 64}},
			{Name: "deleted_at", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64}},
		},
	}, c.DeletedAt)

	return c
}()

var memoByID = Select(Memos.Columns()...).From(Memos.WithDeleted()).Where(Memo_ID.EQ(Memo_ID.Param())).MustBuild()

func TestUpdateNeverWritesCreatedAtOrDeletedAt(t *testing.T) {
	ctx := context.Background()

	rt, err := Open(ctx, "sqlite", filepath.Join(t.TempDir(), "memo.db"), []Table{Memos}, WithSchemaPolicy(SchemaPolicyReconcile))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = rt.Close() })

	loaded := &memo{Body: "draft"}
	if err := Memos.Insert(ctx, rt, loaded); err != nil {
		t.Fatal(err)
	}

	// A row built by hand has a zero created_at; Update must not write it.
	byHand := &memo{ID: loaded.ID, Body: "by hand"}
	if err := Memos.Update(ctx, rt, byHand); err != nil {
		t.Fatal(err)
	}

	stored, err := memoByID.Get(ctx, rt, Memo_ID.Bind(loaded.ID))
	if err != nil || stored.Body != "by hand" || stored.CreatedAt.IsZero() {
		t.Fatalf("stored = %+v, %v; want the body updated and created_at kept", stored, err)
	}

	// Another writer deletes the row; a stale copy must not bring it back.
	if err := Memos.Delete(ctx, rt, stored); err != nil {
		t.Fatal(err)
	}

	loaded.Body = "stale"
	if err := Memos.Update(ctx, rt, loaded); err != nil {
		t.Fatal(err)
	}

	stored, err = memoByID.Get(ctx, rt, Memo_ID.Bind(loaded.ID))
	if err != nil || stored.DeletedAt == 0 || stored.Body != "by hand" {
		t.Fatalf("stored = %+v, %v; want the row still deleted and untouched", stored, err)
	}

	// Deleting writes only the tombstone, not pending edits.
	if err := Memos.Restore(ctx, rt, stored); err != nil {
		t.Fatal(err)
	}

	if stored.DeletedAt != 0 {
		t.Fatalf("restored row still has tombstone %d", stored.DeletedAt)
	}

	stored.Body = "unsaved"
	if err := Memos.Delete(ctx, rt, stored); err != nil {
		t.Fatal(err)
	}

	again, err := memoByID.Get(ctx, rt, Memo_ID.Bind(loaded.ID))
	if err != nil || again.Body != "by hand" || again.DeletedAt != stored.DeletedAt {
		t.Fatalf("after delete = %+v, %v; want only the tombstone written", again, err)
	}

	// Without a version column the state is still checked: a second delete and a
	// restore of a live row report it instead of doing nothing.
	if err := Memos.Delete(ctx, rt, again); !isRowState(err) {
		t.Fatalf("second Delete without version = %v", err)
	}

	if err := Memos.Restore(ctx, rt, again); err != nil {
		t.Fatal(err)
	}

	if err := Memos.Restore(ctx, rt, again); !isRowState(err) {
		t.Fatalf("second Restore = %v", err)
	}

	if err := Memos.Delete(ctx, rt, again); err != nil {
		t.Fatal(err)
	}

	// WithDeleted updates deleted rows, still without touching the tombstone.
	again.Body = "audited"
	if err := Memos.WithDeleted().Update(ctx, rt, again); err != nil {
		t.Fatal(err)
	}

	final, err := memoByID.Get(ctx, rt, Memo_ID.Bind(loaded.ID))
	if err != nil || final.Body != "audited" || final.DeletedAt == 0 {
		t.Fatalf("final = %+v, %v", final, err)
	}
}

func TestRestoreAndDeleteMatchOnlyTheRightState(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)
	rows := seedUsers(t, rt, "a")
	row := rows[0]

	// Restoring a live row matches nothing.
	if err := Users.Restore(ctx, rt, row); !isRowState(err) || IsOptimisticLockError(err) {
		t.Fatalf("Restore of a live row = %v; want a RowStateError", err)
	}

	if err := Users.Delete(ctx, rt, row); err != nil {
		t.Fatal(err)
	}

	// Deleting it twice matches nothing and keeps the first tombstone.
	stamp := row.DeletedAt
	if err := Users.Delete(ctx, rt, row); !isRowState(err) || row.DeletedAt != stamp {
		t.Fatalf("second Delete = %v, tombstone %d -> %d", err, stamp, row.DeletedAt)
	}

	version := row.Version
	if err := Users.Restore(ctx, rt, row); err != nil {
		t.Fatal(err)
	}

	if row.DeletedAt != 0 || row.Version != version+1 {
		t.Fatalf("restored row = %+v", row)
	}

	if _, err := QueryByID.Get(ctx, rt, User_ID.Bind(row.ID)); err != nil {
		t.Fatalf("restored row is not visible: %v", err)
	}
}

// isRowState reports a *RowStateError, which callers match with errors.AsType.
func isRowState(err error) bool {
	_, ok := errors.AsType[*RowStateError](err)

	return ok
}

// TestWithDeletedOnlyDropsTheLiveRowFilter covers the rule that WithDeleted
// changes which rows a statement reaches and never what it does to them: every
// delete through it is still soft, and a row already deleted is stamped again. The
// version column shows the stamp, since a soft delete increments it.
func TestWithDeletedOnlyDropsTheLiveRowFilter(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)

	rows := seedUsers(t, rt, "a", "b", "c")
	if err := Users.BatchDelete(ctx, rt, rows); err != nil {
		t.Fatal(err)
	}

	stamped := func(label string, id, version int64) {
		t.Helper()

		got, err := Users.WithDeleted().Get(ctx, rt, id)
		if err != nil {
			t.Fatalf("%s: the row is gone, so the delete was hard: %v", label, err)
		}

		if got.DeletedAt == 0 || got.Version != version {
			t.Fatalf("%s: row = %+v; want a tombstone and version %d", label, got, version)
		}
	}

	// Without WithDeleted, a deleted row is left alone and keeps its tombstone.
	if err := Users.BatchDeleteByPK(ctx, rt, []int64{rows[1].ID}); err != nil {
		t.Fatal(err)
	}

	stamped("BatchDeleteByPK", rows[1].ID, rows[1].Version)

	if err := Users.WithDeleted().Delete(ctx, rt, rows[0]); err != nil {
		t.Fatal(err)
	}

	stamped("WithDeleted().Delete", rows[0].ID, rows[0].Version)

	if err := Users.WithDeleted().BatchDeleteByPK(ctx, rt, []int64{rows[1].ID}); err != nil {
		t.Fatal(err)
	}

	stamped("WithDeleted().BatchDeleteByPK", rows[1].ID, rows[1].Version+1)

	deleteC, err := DeleteFrom(Users.WithDeleted()).Where(User_ID.EQ(Val(rows[2].ID))).Build()
	if err != nil {
		t.Fatal(err)
	}

	if n, err := deleteC.Exec(ctx, rt); err != nil || n != 1 {
		t.Fatalf("DeleteFrom(WithDeleted()) = %d, %v; want 1 row", n, err)
	}

	stamped("DeleteFrom(WithDeleted())", rows[2].ID, rows[2].Version+1)
}

// TestSoftDeleteTablesAreDefinedByTheirOwnDefine covers the two constructors
// refusing each other's shape, so a hand-written definition cannot claim a
// tombstone it does not have, or drop one it does.
func TestSoftDeleteTablesAreDefinedByTheirOwnDefine(t *testing.T) {
	plain := NewSoftDeleteTable[memo, int64]("plain_define")
	id := NewColumn(plain.TableOf, "id", "id", func(r *memo) *int64 { return &r.ID })
	plain.TableOf.Define(TableSpec[memo, int64]{Columns: []BoundColumn[memo]{id}, PrimaryKey: id})

	if err := plain.Err(); err == nil || !strings.Contains(err.Error(), "SoftDeleteTableOf.Define") {
		t.Fatalf("TableOf.Define on a soft-delete table = %v; want it refused", err)
	}

	missing := NewSoftDeleteTable[memo, int64]("missing_tombstone")
	id = NewColumn(missing.TableOf, "id", "id", func(r *memo) *int64 { return &r.ID })
	missing.Define(TableSpec[memo, int64]{Columns: []BoundColumn[memo]{id}, PrimaryKey: id}, nil)

	if err := missing.Err(); err == nil || !strings.Contains(err.Error(), "needs its deleted_at column") {
		t.Fatalf("Define without deleted_at = %v; want it refused", err)
	}
}

// TestStaleSoftDeletesAreVersionConflicts covers the two ways a delete or restore
// can match nothing. The statement checks the version and the state together, and
// every miss used to be a RowStateError, which WithRetry does not retry: a copy of
// a live row changed since it was loaded is a version conflict, which it should.
func TestStaleSoftDeletesAreVersionConflicts(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)
	row := seedUsers(t, rt, "a")[0]

	stale := *row
	row.Name = "moved on"

	if err := Users.Update(ctx, rt, row); err != nil {
		t.Fatal(err)
	}

	if err := Users.Delete(ctx, rt, &stale); !IsOptimisticLockError(err) || isRowState(err) {
		t.Fatalf("Delete of a stale copy = %v; want an OptimisticLockError", err)
	}

	if err := Users.Delete(ctx, rt, row); err != nil {
		t.Fatal(err)
	}

	gone := *row
	if err := Users.WithDeleted().Update(ctx, rt, row); err != nil {
		t.Fatal(err)
	}

	if err := Users.Restore(ctx, rt, &gone); !IsOptimisticLockError(err) || isRowState(err) {
		t.Fatalf("Restore of a stale copy = %v; want an OptimisticLockError", err)
	}

	// The same version in the wrong state is still a state error: deleting twice.
	if err := Users.Restore(ctx, rt, row); err != nil {
		t.Fatal(err)
	}

	if err := Users.Restore(ctx, rt, row); !isRowState(err) || IsOptimisticLockError(err) {
		t.Fatalf("Restore of a live row = %v; want a RowStateError", err)
	}
}
