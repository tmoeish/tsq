package tsq

import (
	"context"
	"path/filepath"
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

var memosHandle = NewTable[memo, int64]("memos")

var (
	Memo_ID        = NewColumn(memosHandle, "id", "id", func(r *memo) *int64 { return &r.ID })
	Memo_Body      = NewColumn(memosHandle, "body", "body", func(r *memo) *string { return &r.Body })
	Memo_CreatedAt = NewColumn(memosHandle, "created_at", "created_at", func(r *memo) *time.Time { return &r.CreatedAt })
	Memo_DeletedAt = NewColumn(memosHandle, "deleted_at", "deleted_at", func(r *memo) *int64 { return &r.DeletedAt })
)

var Memos = memosHandle.Define(TableSpec[memo, int64]{
	Columns:       []BoundColumn[memo]{Memo_ID, Memo_Body, Memo_CreatedAt, Memo_DeletedAt},
	PrimaryKey:    Memo_ID,
	AutoIncrement: true,
	CreatedAt:     Memo_CreatedAt,
	DeletedAt:     Memo_DeletedAt,
	Schema: []tsqdialect.ColumnSpec{
		{Name: "id", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64}, PrimaryKey: true, AutoIncrement: true},
		{Name: "body", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindString, Size: 64}},
		{Name: "created_at", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindTime}},
		{Name: "deleted_at", Type: tsqdialect.ColumnType{Kind: tsqdialect.KindInt, Bits: 64}},
	},
})

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
	if err := Memos.Delete(ctx, rt, again); !IsRowStateError(err) {
		t.Fatalf("second Delete without version = %v", err)
	}

	if err := Memos.Restore(ctx, rt, again); err != nil {
		t.Fatal(err)
	}

	if err := Memos.Restore(ctx, rt, again); !IsRowStateError(err) {
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
	if err := Users.Restore(ctx, rt, row); !IsRowStateError(err) || IsOptimisticLockError(err) {
		t.Fatalf("Restore of a live row = %v; want a RowStateError", err)
	}

	if err := Users.Delete(ctx, rt, row); err != nil {
		t.Fatal(err)
	}

	// Deleting it twice matches nothing and keeps the first tombstone.
	stamp := row.DeletedAt
	if err := Users.Delete(ctx, rt, row); !IsRowStateError(err) || row.DeletedAt != stamp {
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

	if err := Orders.Restore(ctx, rt, &order{ID: 1}); err == nil {
		t.Fatal("expected Restore on a table without deleted_at to be refused")
	}
}
