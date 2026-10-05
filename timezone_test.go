package tsq

import (
	"context"
	"database/sql"
	"testing"
	"time"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
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

// TestBoundTimesKeepMicroseconds covers the precision a time is bound with. A
// time.Now() carries nanoseconds the columns do not keep: SQLite stored them (as
// text), MySQL rounded to the microsecond and PostgreSQL cut to it, so one
// predicate over one instant matched three different sets of rows.
func TestBoundTimesKeepMicroseconds(t *testing.T) {
	at := time.Date(2024, 2, 29, 23, 59, 59, 999999999, time.FixedZone("east", 8*3600))
	want := time.Date(2024, 2, 29, 15, 59, 59, 999999000, time.UTC)

	for name, bound := range map[string]any{
		"time":    bindValue(at),
		"pointer": bindValue(&at),
		"valuer":  bindValue(sql.NullTime{Time: at, Valid: true}),
	} {
		if got, ok := bound.(time.Time); !ok || !got.Equal(want) || got.Location() != time.UTC || got.Nanosecond() != 999999000 {
			t.Errorf("%s is bound as %v (%T), want %v", name, bound, bound, want)
		}
	}

	if bound := bindValue((*time.Time)(nil)); bound != (*time.Time)(nil) {
		t.Errorf("a nil time is bound as %v", bound)
	}

	ctx := context.Background()
	rt := newSQLite(t)

	if _, err := UpdateTable(Users).Set(User_UpdatedAt, Val(at)).Where(And()).Exec(ctx, rt); err != nil {
		t.Fatal(err)
	}

	sqlText, args, err := Select(User_ID).From(Users).Where(User_UpdatedAt.EQ(Val(at))).SQL(tsqdialect.SQLite)
	if err != nil || len(args) == 0 {
		t.Fatalf("SQL() = %q, %v, %v", sqlText, args, err)
	}

	if got, ok := args[0].(time.Time); !ok || got.Nanosecond() != 999999000 {
		t.Errorf("a predicate binds %v, want the microsecond", args[0])
	}
}
