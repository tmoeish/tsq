package tsq

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

func TestTimesAreBoundInUTC(t *testing.T) {
	east := time.FixedZone("UTC+8", 8*60*60)
	at := time.Date(2026, 1, 2, 8, 0, 0, 0, east)

	for name, v := range map[string]any{
		"time":      at,
		"pointer":   &at,
		"null time": sql.NullTime{Time: at, Valid: true},
		"scanner":   scannerTime{Time: at, Valid: true},
	} {
		got, ok := bindValue(v).(time.Time)
		if !ok || got.Location() != time.UTC || !got.Equal(at) {
			t.Errorf("%s: bound %#v", name, bindValue(v))
		}
	}

	var none *time.Time
	if bindValue(none) != any(none) || bindValue(sql.NullTime{}) == nil {
		t.Error("NULL times must pass through unchanged")
	}

	// On SQLite a time is stored as text, so rows written in two zones only compare
	// correctly because both went in as UTC.
	ctx := context.Background()
	rt := newSQLite(t)

	early := &user{Name: "early", Email: "early@example.com", CreatedAt: at}
	late := &user{Name: "late", Email: "late@example.com", CreatedAt: at.In(time.UTC).Add(time.Minute)}

	if err := Users.BatchInsert(ctx, rt, []*user{late, early}); err != nil {
		t.Fatal(err)
	}

	rows, err := Select(User__Cols...).From(Users).Where(User_CreatedAt.LT(Val(at.Add(30*time.Second)))).
		OrderBy(User_CreatedAt.Asc()).MustBuild().List(ctx, rt)
	if err != nil || len(rows) != 1 || rows[0].Name != "early" {
		t.Fatalf("rows before the cutoff = %v, %v", rows, err)
	}
}

func TestUpdateTableRefreshesUpdatedAt(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)
	rows := seedUsers(t, rt, "a")

	old := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	pinned := UpdateTable(Users).Set(User_UpdatedAt, Val(old)).Where(User_ID.EQ(Val(rows[0].ID)))

	if _, err := pinned.Exec(ctx, rt); err != nil {
		t.Fatal(err)
	}

	stored, err := QueryByID.Get(ctx, rt, User_ID.Bind(rows[0].ID))
	if err != nil || !stored.UpdatedAt.Equal(old) {
		t.Fatalf("an explicit updated_at = %v, %v; want it kept", stored.UpdatedAt, err)
	}

	before := time.Now().Add(-time.Second)
	if _, err := UpdateTable(Users).Set(User_Name, Val("b")).Where(User_ID.EQ(Val(rows[0].ID))).Exec(ctx, rt); err != nil {
		t.Fatal(err)
	}

	stored, err = QueryByID.Get(ctx, rt, User_ID.Bind(rows[0].ID))
	if err != nil || stored.UpdatedAt.Before(before) || stored.UpdatedAt.Location() != time.UTC {
		t.Fatalf("updated_at = %v, %v; want it refreshed in UTC", stored.UpdatedAt, err)
	}

	sql, args, err := UpdateTable(Orders).Set(Order_Note, Val("x")).Where(And()).MustBuild().SQL(onSQLite)
	if err != nil || sql != `UPDATE "orders" SET "note" = ? WHERE 1 = 1` || len(args) != 1 {
		t.Fatalf("a table without updated_at = %s %v, %v", sql, args, err)
	}
}
