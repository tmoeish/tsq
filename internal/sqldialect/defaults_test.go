package sqldialect

import (
	"strings"
	"testing"
)

// TestColumnDefaultsCompareByMeaning covers the default comparison of schema
// reconcile. It cut a value at its first "::", so PostgreSQL's 'a::b'::text never
// matched a declared 'a::b' and every boot set the default again, and it lowered
// everything, so 'Active' matched 'active' and a changed default was never seen.
func TestColumnDefaultsCompareByMeaning(t *testing.T) {
	for _, tt := range []struct {
		left, right string
		same        bool
	}{
		{"'USD'", "'USD'::character varying", true},
		{"'a::b'", "'a::b'::text", true},
		{"'it''s'", "'it''s'::text", true},
		{"CURRENT_TIMESTAMP", "current_timestamp", true},
		{"CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP(6)", true}, // MySQL on a DATETIME(6) column
		{"'USD'", "USD", true},                              // MySQL reads a string default back unquoted
		{"'Active'", "'active'::text", false},
		{"'a'", "'b'", false},
		{"true", "1", true}, // MySQL reads a boolean default back as a number
		{"FALSE", "0", true},
		{"0", "0.00", true}, // and a decimal one with its scale
		{"1.5", "1.50", true},
		{"1", "0", false},
		{"0", "false", true},
		{"('not_set')", "'not_set'", true}, // MySQL takes a TEXT default as an expression
		{"(UTC_TIMESTAMP(6))", "utc_timestamp(6)", true},
		{"(CURRENT_TIMESTAMP AT TIME ZONE 'UTC')", "timezone('UTC'::text, CURRENT_TIMESTAMP)", true},
		// A local current time where TSQ declares UTC, as a table created before it did.
		{"(UTC_TIMESTAMP(6))", "CURRENT_TIMESTAMP(6)", false},
		{"(CURRENT_TIMESTAMP AT TIME ZONE 'UTC')", "CURRENT_TIMESTAMP", false},
		{"(a) + (b)", "a) + (b", false},
		// MySQL's literal defaults, quoted again by mysqlDefault: read bare, the
		// first was an expression in parentheses and the second lost its padding.
		{"'(none)'", "'(none)'", true},
		{"'  pad  '", "'  pad  '", true},
		{"'  pad  '", "'pad'", false},
		// MySQL reads the default of a DATETIME(6) back with its six zeros.
		{"'2020-01-02 03:04:05'", "'2020-01-02 03:04:05.000000'", true},
		{"'2020-01-02 03:04:05'", "'2020-01-02 03:04:06.000000'", false},
	} {
		if got := SameDefault(tt.left, tt.right); got != tt.same {
			t.Errorf("SameDefault(%q, %q) = %v, want %v", tt.left, tt.right, got, tt.same)
		}
	}
}

// TestZeroTimeLiteralIsOneEachDialectTakes covers the value a migration writes
// into a time column that becomes NOT NULL. MySQL takes a zone offset in a literal
// only within the range of TIMESTAMP: '0001-01-01 00:00:00+00:00' was an error
// under an explicit session time zone and was stored as 0000-00-00, without a
// word, under the default one, after which every ALTER that copies the table
// failed on the row.
func TestZeroTimeLiteralIsOneEachDialectTakes(t *testing.T) {
	for d, want := range map[Dialect]string{
		MySQLDialect{}:    "'0001-01-01 00:00:00'",
		PostgresDialect{}: "'0001-01-01 00:00:00'",
		SQLiteDialect{}:   "'0001-01-01 00:00:00+00:00'",
	} {
		if got, ok := ZeroLiteral(d, ColumnType{Kind: KindTime}); !ok || got != want {
			t.Errorf("%s zero time = %s, %v; want %s", d.Name(), got, ok, want)
		}
	}
}

// TestAddColumnFillsTheRowsPresent covers a NOT NULL column without a default
// added to a table with rows, which PostgreSQL refuses for every type and MySQL for
// a time: it is added with the zero value as a default, dropped again. On MySQL a
// TEXT or BLOB takes a default only as an expression (error 1101 for a literal).
func TestAddColumnFillsTheRowsPresent(t *testing.T) {
	for _, c := range []struct {
		dialect Dialect
		column  ColumnSpec
		want    string
	}{
		{
			PostgresDialect{},
			ColumnSpec{Name: "n", Type: ColumnType{Kind: KindInt, Bits: 32}},
			`ALTER TABLE "t" ADD COLUMN "n" INTEGER NOT NULL DEFAULT 0; | ALTER TABLE "t" ALTER COLUMN "n" DROP DEFAULT;`,
		},
		{
			PostgresDialect{},
			ColumnSpec{Name: "ok", Type: ColumnType{Kind: KindBool}},
			`ALTER TABLE "t" ADD COLUMN "ok" BOOLEAN NOT NULL DEFAULT FALSE; | ALTER TABLE "t" ALTER COLUMN "ok" DROP DEFAULT;`,
		},
		{
			MySQLDialect{},
			ColumnSpec{Name: "at", Type: ColumnType{Kind: KindTime}},
			"ALTER TABLE `t` ADD COLUMN `at` DATETIME(6) NOT NULL DEFAULT '0001-01-01 00:00:00'; | ALTER TABLE `t` ALTER COLUMN `at` DROP DEFAULT;",
		},
		{
			MySQLDialect{},
			ColumnSpec{Name: "bio", Type: ColumnType{Kind: KindString, Size: 100000}},
			"ALTER TABLE `t` ADD COLUMN `bio` MEDIUMTEXT NOT NULL DEFAULT (''); | ALTER TABLE `t` ALTER COLUMN `bio` DROP DEFAULT;",
		},
		{
			MySQLDialect{},
			ColumnSpec{Name: "photo", Type: ColumnType{Kind: KindBytes}},
			"ALTER TABLE `t` ADD COLUMN `photo` BLOB NOT NULL DEFAULT (X''); | ALTER TABLE `t` ALTER COLUMN `photo` DROP DEFAULT;",
		},
		// Nothing to fill: a nullable column, one with a default, one of a raw type.
		{
			PostgresDialect{},
			ColumnSpec{Name: "n", Type: ColumnType{Kind: KindInt, Bits: 32, Nullable: true}},
			`ALTER TABLE "t" ADD COLUMN "n" INTEGER;`,
		},
		{
			PostgresDialect{},
			ColumnSpec{Name: "n", Type: ColumnType{Kind: KindInt, Bits: 32}, Default: "7"},
			`ALTER TABLE "t" ADD COLUMN "n" INTEGER NOT NULL DEFAULT 7;`,
		},
		{
			PostgresDialect{},
			ColumnSpec{Name: "doc", Type: ColumnType{RawType: "JSONB"}},
			`ALTER TABLE "t" ADD COLUMN "doc" JSONB NOT NULL;`,
		},
	} {
		statements, err := AddColumnSQL(c.dialect, "t", c.column)
		if got := strings.Join(statements, " | "); err != nil || got != c.want {
			t.Errorf("%s add %s:\n got  %s (%v)\n want %s", c.dialect.Name(), c.column.Name, got, err, c.want)
		}
	}

	// SQLite has no DROP DEFAULT: such a column is added by rebuilding the table.
	for column, rebuild := range map[ColumnSpec]bool{
		{Name: "n", Type: ColumnType{Kind: KindInt}}:                                                 true,
		{Name: "at", Type: ColumnType{Kind: KindTime, Nullable: true}, Default: "CURRENT_TIMESTAMP"}: true,
		{Name: "n", Type: ColumnType{Kind: KindInt, Nullable: true}}:                                 false,
		{Name: "n", Type: ColumnType{Kind: KindInt}, Default: "7"}:                                   false,
	} {
		if got := AddNeedsRebuild(SQLiteDialect{}, column); got != rebuild {
			t.Errorf("sqlite rebuilds to add %+v = %v, want %v", column, got, rebuild)
		}

		if AddNeedsRebuild(PostgresDialect{}, column) {
			t.Errorf("postgres rebuilds to add %+v", column)
		}
	}
}

// TestSQLiteRetypeConvertsOrRefuses covers the rule a SQLite rebuild carries a
// column into another type by. SQLite stores any value under any declared type, so
// a copy of the column as it was "succeeded" over 1.5 in an integer column, 2 in a
// boolean one and 'Hello' in either, and every later read of the table failed.
func TestSQLiteRetypeConvertsOrRefuses(t *testing.T) {
	kind := func(k ColumnKind) ColumnType { return ColumnType{Kind: k} }

	for _, c := range []struct {
		before, after ColumnKind
		want          Retype
	}{
		{KindFloat, KindInt, RetypeConvert},
		{KindInt, KindBool, RetypeConvert},
		{KindFloat, KindBool, RetypeConvert},
		{KindString, KindInt, RetypeMayFail},
		{KindString, KindFloat, RetypeMayFail},
		{KindString, KindBool, RetypeMayFail},
		{KindString, KindTime, RetypeMayFail},
		{KindInt, KindTime, RetypeMayFail},
		{KindBytes, KindInt, RetypeMayFail},
		{KindBool, KindInt, RetypeAsIs},
		{KindInt, KindFloat, RetypeAsIs},
		{KindInt, KindString, RetypeAsIs},
		{KindTime, KindString, RetypeAsIs},
		{KindString, KindBytes, RetypeAsIs},
		{KindInt, KindInt, RetypeAsIs},
	} {
		if got := SQLiteRetype(kind(c.before), kind(c.after)); got != c.want {
			t.Errorf("SQLiteRetype(%s, %s) = %d, want %d", c.before, c.after, got, c.want)
		}
	}

	// A raw type is the declaration's own business on either side.
	if got := SQLiteRetype(ColumnType{Kind: KindString, RawType: "JSON"}, kind(KindInt)); got != RetypeAsIs {
		t.Errorf("a raw type is classified %d", got)
	}

	if got := SQLiteRetypeSource(`"c"`, kind(KindInt)); got != `CASE typeof("c") WHEN 'real' THEN CAST(ROUND("c") AS INTEGER) ELSE "c" END` {
		t.Errorf("integer source = %s", got)
	}

	if got := SQLiteRetypeSource(`"c"`, kind(KindBool)); got != `CASE WHEN typeof("c") IN ('integer', 'real') THEN "c" <> 0 ELSE "c" END` {
		t.Errorf("boolean source = %s", got)
	}

	if got := SQLiteRetypeSource(`"c"`, kind(KindString)); got != `"c"` {
		t.Errorf("text source = %s", got)
	}

	for k, word := range map[ColumnKind]string{KindInt: "an integer", KindFloat: "a number", KindBool: "a boolean", KindTime: "a time", KindString: "", KindBytes: ""} {
		condition, got := SQLiteMisfit(`"c"`, kind(k))
		if got != word || (condition == "") != (word == "") {
			t.Errorf("SQLiteMisfit(%s) = %q, %q", k, condition, got)
		}
	}
}

// TestAdoptSpellingTakesTheEnginesWord covers the comparison that settles a type
// or a default the engine reports in a spelling of its own: where the engine
// spells the declaration as it spells the column, the column is described as
// declared, and where it does not, the difference stays.
func TestAdoptSpellingTakesTheEnginesWord(t *testing.T) {
	d := MySQLDialect{}
	declared := ColumnSpec{Name: "c", Type: ColumnType{Kind: KindString, RawType: "DECIMAL(10)", Nullable: true}, Default: "(1+1)"}
	inspected := Column{Name: "c", Type: ColumnType{RawType: "decimal(10,0)", Nullable: true}, Default: "1 + 1", NativeType: "decimal(10,0)"}

	if SameColumnType(d, inspected, declared) || SameDefault(inspected.Default, DefaultSQL(d, declared)) {
		t.Fatal("the fixture compares equal as text; it no longer tests the probe")
	}

	same := AdoptSpelling(d, inspected, Column{NativeType: "DECIMAL(10,0)", Default: "1 + 1"}, declared)
	if !SameColumnType(d, same, declared) || !SameDefault(same.Default, DefaultSQL(d, declared)) {
		t.Errorf("the engine's own spelling of the declaration was not taken: %+v", same)
	}

	other := AdoptSpelling(d, inspected, Column{NativeType: "decimal(12,0)", Default: "1 + 2"}, declared)
	if SameColumnType(d, other, declared) || SameDefault(other.Default, DefaultSQL(d, declared)) {
		t.Errorf("another type and default were taken for the declared ones: %+v", other)
	}

	// Nullability is not a matter of spelling and stays as inspected.
	notNull := inspected
	notNull.Type.Nullable = false

	if got := AdoptSpelling(d, notNull, Column{NativeType: "decimal(10,0)", Default: "1 + 1"}, declared); got.Type.Nullable {
		t.Error("the declared nullability replaced the inspected one")
	}
}
