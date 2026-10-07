package sqldialect

import (
	"strings"
	"testing"
)

// TestRangeCheckFollowsTheFieldWhereTheTypeDoesNot covers the constraint that
// keeps an integer column to its Go field's range: PostgreSQL has no unsigned
// types, SQLite no widths, and MySQL's own types need none.
func TestRangeCheckFollowsTheFieldWhereTheTypeDoesNot(t *testing.T) {
	integer := func(bits int, unsigned bool) ColumnSpec {
		return ColumnSpec{Name: "n", Type: ColumnType{Kind: KindInt, Bits: bits, Unsigned: unsigned}}
	}

	for _, c := range []struct {
		dialect Name
		column  ColumnSpec
		want    string
	}{
		{Postgres, integer(32, true), `"n" >= 0 AND "n" <= 4294967295`},
		{Postgres, integer(8, true), `"n" >= 0 AND "n" <= 255`},
		{Postgres, integer(64, true), `"n" >= 0 AND "n" <= 18446744073709551615`},
		{Postgres, integer(0, true), `"n" >= 0 AND "n" <= 18446744073709551615`},
		{Postgres, integer(16, false), ""},
		{Postgres, integer(64, false), ""},
		{SQLite, integer(16, false), `"n" >= -32768 AND "n" <= 32767`},
		{SQLite, integer(32, false), `"n" >= -2147483648 AND "n" <= 2147483647`},
		{SQLite, integer(8, true), `"n" >= 0 AND "n" <= 255`},
		{SQLite, integer(64, true), `"n" >= 0`},
		{SQLite, integer(64, false), ""},
		{SQLite, integer(0, false), ""},
		{MySQL, integer(32, true), ""},
		{MySQL, integer(16, false), ""},
		{Postgres, ColumnSpec{Name: "n", Type: ColumnType{RawType: "INTEGER UNSIGNED"}}, ""},
		{Postgres, ColumnSpec{Name: "id", Type: ColumnType{Kind: KindInt, Bits: 64, Unsigned: true}, PrimaryKey: true, AutoIncrement: true}, ""},
		{SQLite, ColumnSpec{Name: "n", Type: ColumnType{Kind: KindInt, Bits: 16}, Generated: "1 + 1"}, ""},
	} {
		d, _ := For(c.dialect)

		got, ok := RangeCheck(d, c.column)
		if got != c.want || ok != (c.want != "") {
			t.Errorf("%s %+v: check %q, %v; want %q", c.dialect, c.column.Type, got, ok, c.want)
		}
	}

	// The column definition carries it, named after the column.
	definition, err := ColumnDefinitionSQL(PostgresDialect{}, integer(32, true))
	if err != nil || definition != `"n" BIGINT NOT NULL CONSTRAINT "ck_n" CHECK ("n" >= 0 AND "n" <= 4294967295)` {
		t.Errorf("definition = %s, %v", definition, err)
	}

	mysql, _ := ColumnDefinitionSQL(MySQLDialect{}, integer(32, true))
	if strings.Contains(mysql, "CHECK") {
		t.Errorf("MySQL definition carries a check: %s", mysql)
	}
}

func TestRangeChecksCompareByTheirBounds(t *testing.T) {
	for _, c := range []struct {
		column, reported, wanted string
		same                     bool
	}{
		{"qty", `CHECK (((qty >= 0) AND (qty <= 4294967295)))`, `"qty" >= 0 AND "qty" <= 4294967295`, true},
		{"c1", `CHECK (((c1 >= '-32768'::integer) AND (c1 <= 32767)))`, `"c1" >= -32768 AND "c1" <= 32767`, true},
		{"qty", `CHECK ((qty >= 0))`, `"qty" >= 0 AND "qty" <= 255`, false},
		{"qty", `"qty" >= 0 AND "qty" <= 65535`, `"qty" >= 0 AND "qty" <= 4294967295`, false},
		{"qty", "", "", true},
		{"qty", "", `"qty" >= 0`, false},
		{"qty", `"qty" >= 0`, "", false},
	} {
		if got := SameRangeCheck(c.column, c.reported, c.wanted); got != c.same {
			t.Errorf("SameRangeCheck(%q, %q) = %v, want %v", c.reported, c.wanted, got, c.same)
		}
	}
}

// TestPostgresAlterColumnFollowsTheRangeCheck covers the constraint statements of
// a column change: added where the table has none, replaced where the field's
// width changed, dropped where the field no longer needs one.
func TestPostgresAlterColumnFollowsTheRangeCheck(t *testing.T) {
	d := PostgresDialect{}
	unsigned32 := ColumnSpec{Name: "qty", Type: ColumnType{Kind: KindInt, Bits: 32, Unsigned: true}}
	unsigned16 := ColumnSpec{Name: "qty", Type: ColumnType{Kind: KindInt, Bits: 16, Unsigned: true}}
	signed64 := ColumnSpec{Name: "qty", Type: ColumnType{Kind: KindInt, Bits: 64}}

	added := d.AlterColumnSQL("t", Column{ColumnSpec: unsigned32, NativeType: "bigint"}, unsigned32)
	if len(added) != 1 || added[0] != `ALTER TABLE "t" ADD CONSTRAINT "ck_qty" CHECK ("qty" >= 0 AND "qty" <= 4294967295);` {
		t.Errorf("a table without the constraint = %v", added)
	}

	// The constraint goes before the type changes: its expression is read against
	// the new type, and over a VARCHAR "qty >= 0" is "operator does not exist".
	narrowed := d.AlterColumnSQL("t", Column{ColumnSpec: unsigned32, NativeType: "bigint", Check: `CHECK (((qty >= 0) AND (qty <= 4294967295)))`}, unsigned16)
	want := []string{
		`ALTER TABLE "t" DROP CONSTRAINT "ck_qty";`,
		`ALTER TABLE "t" ALTER COLUMN "qty" TYPE INTEGER;`,
		`ALTER TABLE "t" ADD CONSTRAINT "ck_qty" CHECK ("qty" >= 0 AND "qty" <= 65535);`,
	}
	if strings.Join(narrowed, "\n") != strings.Join(want, "\n") {
		t.Errorf("a narrower field:\n%s\nwant:\n%s", strings.Join(narrowed, "\n"), strings.Join(want, "\n"))
	}

	text := ColumnSpec{Name: "qty", Type: ColumnType{Kind: KindString, Size: 40}}
	toText := d.AlterColumnSQL("t", Column{ColumnSpec: unsigned32, NativeType: "bigint", Check: `CHECK (((qty >= 0) AND (qty <= 4294967295)))`}, text)
	if len(toText) != 2 || !strings.HasSuffix(toText[0], `DROP CONSTRAINT "ck_qty";`) || !strings.Contains(toText[1], `TYPE VARCHAR(40) USING "qty"::TEXT`) {
		t.Errorf("a text field = %v; want the constraint dropped before the type, and not added back", toText)
	}

	dropped := d.AlterColumnSQL("t", Column{ColumnSpec: unsigned32, NativeType: "bigint", Check: `CHECK (((qty >= 0) AND (qty <= 4294967295)))`}, signed64)
	if len(dropped) != 1 || dropped[0] != `ALTER TABLE "t" DROP CONSTRAINT "ck_qty";` {
		t.Errorf("a field that needs none = %v", dropped)
	}

	same := d.AlterColumnSQL("t", Column{ColumnSpec: unsigned32, NativeType: "bigint", Check: `CHECK (((qty >= 0) AND (qty <= 4294967295)))`}, unsigned32)
	if len(same) != 0 {
		t.Errorf("an unchanged column = %v", same)
	}
}

// TestSQLiteReadsItsOwnRangeChecksAndKeepsOthers covers the CREATE TABLE text
// sqlite_master keeps: the constraint TSQ named is read back per column, and only
// a CHECK of another's blocks a rebuild.
func TestSQLiteReadsItsOwnRangeChecksAndKeepsOthers(t *testing.T) {
	create := `CREATE TABLE "t" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "qty" INTEGER NOT NULL CONSTRAINT "ck_qty" CHECK ("qty" >= 0 AND "qty" <= 4294967295),
    "small" INTEGER CONSTRAINT ck_small CHECK ("small" >= -32768 AND "small" <= 32767),
    "note" VARCHAR(10) CHECK (length("note") > 0),
    CONSTRAINT "mine" CHECK ("qty" <> 7)
)`

	for column, want := range map[string]string{
		"qty":   `"qty" >= 0 AND "qty" <= 4294967295`,
		"small": `"small" >= -32768 AND "small" <= 32767`,
		"note":  "",
		"id":    "",
	} {
		if got := sqliteRangeCheck(create, column); got != want {
			t.Errorf("check of %s = %q, want %q", column, got, want)
		}
	}

	if stripped := sqliteWithoutRangeChecks(create); !sqliteCheck.MatchString(stripped) || strings.Contains(stripped, "ck_") {
		t.Errorf("without TSQ's checks = %s", stripped)
	}

	own := `CREATE TABLE "t" ("qty" INTEGER CONSTRAINT "ck_qty" CHECK ("qty" >= 0 AND "qty" <= 255), "s" TEXT DEFAULT 'a (b')`
	if stripped := sqliteWithoutRangeChecks(own); sqliteCheck.MatchString(stripped) {
		t.Errorf("TSQ's own check would block a rebuild: %s", stripped)
	}
}
