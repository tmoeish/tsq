package tsq

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
)

func TestAttachManyAndOneReadChildrenOnce(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)
	users := seedUsers(t, rt, "a", "b", "c")

	orders := []*order{
		{UserID: users[0].ID, Amount: 10, Note: "a1"},
		{UserID: users[0].ID, Amount: 20, Note: "a2"},
		{UserID: users[1].ID, Amount: 30, Note: "b1"},
	}
	if err := Orders.BatchInsert(ctx, rt, orders); err != nil {
		t.Fatal(err)
	}

	// One extra query for every parent, not one per parent.
	var ops []TraceOp

	traced := newSQLite(t, WithTracers(func(ctx context.Context, info TraceInfo, next func(context.Context) error) error {
		ops = append(ops, info.Op)
		return next(ctx)
	}))

	byUser := Select(Orders.Columns()...).From(Orders).Where(Order_UserID.In(Order_UserID.ListParam())).MustBuild()

	attached := map[int64][]string{}
	if err := AttachMany(ctx, rt, users, User_ID, byUser, Order_UserID, func(u *user, os []*order) {
		for _, o := range os {
			attached[u.ID] = append(attached[u.ID], o.Note)
		}
	}); err != nil {
		t.Fatal(err)
	}

	if len(attached) != 2 || strings.Join(attached[users[0].ID], ",") != "a1,a2" || attached[users[1].ID][0] != "b1" {
		t.Fatalf("attached = %v", attached)
	}

	// The parent of each order, by its foreign key.
	owners := map[string]string{}
	byID := Select(User__Cols...).From(Users).Where(User_ID.In(User_ID.ListParam())).MustBuild()

	if err := AttachOne(ctx, rt, orders, Order_UserID, byID, User_ID, func(o *order, u *user) {
		owners[o.Note] = u.Name
	}); err != nil {
		t.Fatal(err)
	}

	if owners["a1"] != "a" || owners["a2"] != "a" || owners["b1"] != "b" {
		t.Fatalf("owners = %v", owners)
	}

	// Parents without children are left as they are, and no parents means no query.
	seedUsers(t, traced, "x")
	ops = nil

	if err := AttachMany(ctx, traced, []*user{}, User_ID, byUser, Order_UserID, func(*user, []*order) {}); err != nil {
		t.Fatal(err)
	}

	if len(ops) != 0 {
		t.Fatalf("no parents ran %v", ops)
	}

	single, err := Select(User__Cols...).From(Users).MustBuild().List(ctx, traced)
	if err != nil {
		t.Fatal(err)
	}

	ops = nil
	none := 0

	if err := AttachMany(ctx, traced, single, User_ID, byUser, Order_UserID, func(_ *user, os []*order) {
		if os == nil {
			none++
		}
	}); err != nil {
		t.Fatal(err)
	}

	if none != 1 || len(ops) != 1 {
		t.Fatalf("childless parent: none=%d ops=%v", none, ops)
	}

	// An expression is not a column of the row.
	if err := AttachMany(ctx, rt, users, User_ID, byUser, Order_UserID, nil); err == nil {
		t.Fatal("expected a nil assign to be refused")
	}
}

// TestAttachChecksItsChildKey covers a child query that does not select the key
// children are grouped by, which left every child under the zero key and gave no
// parent any child without an error, and a nullable key, which failed only after
// the child query had run.
func TestAttachChecksItsChildKey(t *testing.T) {
	ctx := context.Background()

	rt, err := Open(ctx, "sqlite", filepath.Join(t.TempDir(), "attach.db"), []Table{Users, Notes}, WithSchemaPolicy(SchemaPolicyReconcile))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = rt.Close() })

	users := seedUsers(t, rt, "a", "b")

	unselected := Select(Order_ID).From(Orders).Where(Order_UserID.In(Order_UserID.ListParam())).MustBuild()
	if err := AttachMany(ctx, rt, users, User_ID, unselected, Order_UserID, func(*user, []*order) {}); err == nil || !strings.Contains(err.Error(), "does not select user_id") {
		t.Fatalf("AttachMany with an unselected key = %v; want it refused", err)
	}

	rated := func(r int64) *note { return &note{Rating: sql.Null[int64]{V: r, Valid: true}} }
	notes := []*note{rated(users[0].ID), rated(users[0].ID), {}}

	if err := Notes.BatchInsert(ctx, rt, notes); err != nil {
		t.Fatal(err)
	}

	byRating := Select(Notes.Columns()...).From(Notes).Where(Note_Rating.In(Note_Rating.ListParam())).MustBuild()
	counts := map[int64]int{}

	if err := AttachMany(ctx, rt, users, User_ID, byRating, Note_Rating, func(u *user, ns []*note) { counts[u.ID] = len(ns) }); err != nil {
		t.Fatalf("AttachMany by a nullable key = %v", err)
	}

	if counts[users[0].ID] != 2 || counts[users[1].ID] != 0 {
		t.Fatalf("counts = %v; want a's two notes, and the NULL one attached to no one", counts)
	}
}
