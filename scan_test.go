package tsq

import (
	"context"
	"database/sql"
	"reflect"
	"testing"
	"time"
)

// TestTimeTextIsReadInEverySpelling covers the text a time reaches TSQ as on
// SQLite, where it is stored as written: by either driver, by SQLite's own
// CURRENT_TIMESTAMP, or by a date function.
func TestTimeTextIsReadInEverySpelling(t *testing.T) {
	want := time.Date(2024, 3, 4, 5, 6, 7, 123456000, time.UTC)

	for _, text := range []string{
		"2024-03-04 05:06:07.123456 +0000 UTC",             // modernc.org/sqlite
		"2024-03-04 05:06:07.123456 +0000 UTC m=+0.000123", // with a monotonic reading
		"2024-03-04 05:06:07.123456+00:00",                 // mattn/go-sqlite3
		"2024-03-04 13:06:07.123456+08:00",
		"2024-03-04T05:06:07.123456Z",
		"2024-03-04 05:06:07.123456",
		"2024-03-04T05:06:07.123456",
	} {
		got, err := parseTimeText(text)
		if err != nil || !got.Equal(want) {
			t.Errorf("parseTimeText(%q) = %v, %v; want %v", text, got, err, want)
		}
	}

	for text, want := range map[string]time.Time{
		"2024-03-04 05:06:07": time.Date(2024, 3, 4, 5, 6, 7, 0, time.UTC), // CURRENT_TIMESTAMP
		"2024-03-04 05:06":    time.Date(2024, 3, 4, 5, 6, 0, 0, time.UTC),
		"2024-03-04":          time.Date(2024, 3, 4, 0, 0, 0, 0, time.UTC),
	} {
		got, err := parseTimeText(text)
		if err != nil || !got.Equal(want) {
			t.Errorf("parseTimeText(%q) = %v, %v; want %v", text, got, err, want)
		}
	}

	if _, err := parseTimeText("yesterday"); err == nil {
		t.Error("parseTimeText took text that is not a time")
	}
}

type scanFlag bool

// TestScanAdaptersReadWhatDatabaseSQLDoesNot covers the two field types whose
// values database/sql does not read from every driver: a time that arrives as
// text, and a named bool that arrives as an integer.
func TestScanAdaptersReadWhatDatabaseSQLDoesNot(t *testing.T) {
	for _, plain := range []reflect.Type{reflect.TypeFor[bool](), reflect.TypeFor[*bool](), reflect.TypeFor[string](), reflect.TypeFor[int64](), reflect.TypeFor[sql.Null[bool]]()} {
		if scanAdapterFor(plain) != nil {
			t.Errorf("%v needs no adapter and got one", plain)
		}
	}

	scan := func(field, src any) error {
		adapt := scanAdapterFor(reflect.TypeOf(field).Elem())
		if adapt == nil {
			t.Fatalf("%T has no adapter", field)
		}

		return adapt(field).(sql.Scanner).Scan(src)
	}

	when := time.Date(2024, 3, 4, 5, 6, 7, 0, time.UTC)

	var (
		plain    time.Time
		pointer  *time.Time
		nullTime sql.NullTime
		nullOf   sql.Null[time.Time]
	)

	for _, src := range []any{"2024-03-04 05:06:07 +0000 UTC", []byte("2024-03-04 05:06:07"), when} {
		if err := scan(&plain, src); err != nil || !plain.Equal(when) {
			t.Errorf("time.Time from %T = %v, %v", src, plain, err)
		}

		if err := scan(&pointer, src); err != nil || pointer == nil || !pointer.Equal(when) {
			t.Errorf("*time.Time from %T = %v, %v", src, pointer, err)
		}

		if err := scan(&nullTime, src); err != nil || !nullTime.Valid || !nullTime.Time.Equal(when) {
			t.Errorf("sql.NullTime from %T = %v, %v", src, nullTime, err)
		}

		if err := scan(&nullOf, src); err != nil || !nullOf.Valid || !nullOf.V.Equal(when) {
			t.Errorf("sql.Null[time.Time] from %T = %v, %v", src, nullOf, err)
		}
	}

	if err := scan(&plain, nil); err == nil {
		t.Error("NULL was read into a time.Time")
	}

	if err := scan(&pointer, nil); err != nil || pointer != nil {
		t.Errorf("NULL into *time.Time = %v, %v", pointer, err)
	}

	if err := scan(&nullTime, nil); err != nil || nullTime.Valid {
		t.Errorf("NULL into sql.NullTime = %v, %v", nullTime, err)
	}

	if err := scan(&nullOf, nil); err != nil || nullOf.Valid {
		t.Errorf("NULL into sql.Null[time.Time] = %v, %v", nullOf, err)
	}

	if err := scan(&plain, 42); err == nil {
		t.Error("an integer was read into a time.Time")
	}

	var (
		flag     scanFlag
		flagPtr  *scanFlag
		flagNull sql.Null[scanFlag]
	)

	for _, src := range []any{int64(1), true, []byte("1")} {
		flag, flagPtr, flagNull = false, nil, sql.Null[scanFlag]{}

		if err := scan(&flag, src); err != nil || !bool(flag) {
			t.Errorf("named bool from %T = %v, %v", src, flag, err)
		}

		if err := scan(&flagPtr, src); err != nil || flagPtr == nil || !bool(*flagPtr) {
			t.Errorf("pointer to a named bool from %T = %v, %v", src, flagPtr, err)
		}

		if err := scan(&flagNull, src); err != nil || !flagNull.Valid || !bool(flagNull.V) {
			t.Errorf("sql.Null of a named bool from %T = %v, %v", src, flagNull, err)
		}
	}

	if err := scan(&flag, int64(0)); err != nil || bool(flag) {
		t.Errorf("named bool from 0 = %v, %v", flag, err)
	}

	if err := scan(&flag, nil); err == nil {
		t.Error("NULL was read into a named bool")
	}

	if err := scan(&flagPtr, nil); err != nil || flagPtr != nil {
		t.Errorf("NULL into a pointer to a named bool = %v, %v", flagPtr, err)
	}

	if err := scan(&flagNull, nil); err != nil || flagNull.Valid {
		t.Errorf("NULL into sql.Null of a named bool = %v, %v", flagNull, err)
	}

	if err := scan(&flag, int64(7)); err == nil {
		t.Error("7 was read into a named bool")
	}
}

// TestTimeExpressionsAreReadOnSQLite covers MAX, MIN and COALESCE over a time
// column: SQLite's drivers return a time.Time only for a column declared as one,
// and the text an expression came back as failed to scan.
func TestTimeExpressionsAreReadOnSQLite(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)

	for _, name := range []string{"amy", "bob"} {
		if err := Users.Insert(ctx, rt, &user{Name: name, Email: name + "@example.com"}); err != nil {
			t.Fatal(err)
		}
	}

	stored, err := Select(Users.Columns()...).From(Users).OrderBy(User_ID.Asc()).List(ctx, rt)
	if err != nil {
		t.Fatal(err)
	}

	latest, err := SelectNullValue(Max(User_CreatedAt)).From(Users).Get(ctx, rt)
	if err != nil || !latest.Valid || !latest.V.Equal(stored[1].CreatedAt) {
		t.Fatalf("Max(created_at) = %+v, %v; want %v", latest, err, stored[1].CreatedAt)
	}

	first, err := SelectNullValue(Min(User_UpdatedAt)).From(Users).Get(ctx, rt)
	if err != nil || !first.Valid || !first.V.Equal(stored[0].UpdatedAt) {
		t.Fatalf("Min(updated_at) = %+v, %v", first, err)
	}

	none, err := SelectNullValue(Max(User_CreatedAt)).From(Users).Where(User_ID.EQ(Val(int64(99)))).Get(ctx, rt)
	if err != nil || none.Valid {
		t.Fatalf("Max over no rows = %+v, %v; want NULL", none, err)
	}
}

// TestTimesAreReadInUTC covers a time a driver hands back in a zone of its own:
// pgx reads a TIMESTAMPTZ in the session's local zone, so a column of that type
// was the one place a time came back in another zone than UTC.
func TestTimesAreReadInUTC(t *testing.T) {
	zoned := time.Date(2024, 6, 1, 12, 0, 0, 0, time.FixedZone("east", 8*3600))

	for name, form := range map[string]any{
		"time.Time":           new(time.Time),
		"*time.Time":          new(*time.Time),
		"sql.NullTime":        new(sql.NullTime),
		"sql.Null[time.Time]": new(sql.Null[time.Time]),
	} {
		dest := scanAdapterFor(reflect.TypeOf(form).Elem())(form).(sql.Scanner)
		if err := dest.Scan(zoned); err != nil {
			t.Fatalf("%s: %v", name, err)
		}

		var got time.Time

		switch v := form.(type) {
		case *time.Time:
			got = *v
		case **time.Time:
			got = **v
		case *sql.NullTime:
			got = v.Time
		case *sql.Null[time.Time]:
			got = v.V
		}

		if !got.Equal(zoned) || got.Location() != time.UTC {
			t.Errorf("%s read %v, want the same instant in UTC", name, got)
		}
	}
}
