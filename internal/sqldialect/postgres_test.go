package sqldialect

import (
	"database/sql"
	"slices"
	"strings"
	"testing"
)

// TestPostgresUnsignedIntegersFit covers unsigned types, which PostgreSQL does not
// have: uint16 was a SMALLINT, uint32 an INTEGER and uint64 a BIGINT, each too
// narrow for the upper half of the Go type. They take the next wider type, and
// read back as what was declared; a NUMERIC or DATE of another shape does not
// pass for a declared float or time.
func TestPostgresUnsignedIntegersFit(t *testing.T) {
	d := PostgresDialect{}

	for bits, want := range map[int]string{8: "SMALLINT", 16: "INTEGER", 32: "BIGINT", 64: "NUMERIC(20)"} {
		declared := ColumnSpec{Name: "n", Type: ColumnType{Kind: KindInt, Bits: bits, Unsigned: true}}
		if got := d.ColumnTypeSQL(declared.Type); got != want {
			t.Errorf("uint%d = %s, want %s", bits, got, want)
		}
	}

	back, err := parsePostgresColumnType("numeric", "numeric", "numeric(20,0)", sql.NullInt64{})
	if err != nil || !SameColumnType(d, Column{Name: "n", Type: back}, ColumnSpec{Name: "n", Type: ColumnType{Kind: KindInt, Bits: 64, Unsigned: true}}) {
		t.Errorf("numeric(20,0) = %+v, %v; want it to match uint64", back, err)
	}

	for data, formatted := range map[string]string{"numeric": "numeric(10,2)", "date": "date"} {
		back, err := parsePostgresColumnType(data, data, formatted, sql.NullInt64{})
		if err != nil {
			t.Fatal(err)
		}

		for _, declared := range []ColumnType{{Kind: KindFloat, Bits: 64}, {Kind: KindTime}} {
			if SameColumnType(d, Column{Name: "n", Type: back}, ColumnSpec{Name: "n", Type: declared}) {
				t.Errorf("%s matched a declared %+v", formatted, declared)
			}
		}
	}
}

// TestPostgresUnsignedKeysAreSerialsOfTheColumnTheyCompareTo covers an unsigned
// auto-increment key, a `uint` ID above all: the column was created a SERIAL of
// the Go type's own width and compared as the next wider type, so the table TSQ had
// just created failed validation on the next start. The key is as wide as the
// column would be, and a uint64 one is a BIGINT, the widest a sequence counts.
func TestPostgresUnsignedKeysAreSerialsOfTheColumnTheyCompareTo(t *testing.T) {
	d := PostgresDialect{}

	for _, c := range []struct {
		bits            int
		created, stored string
		storedBits      int
	}{
		{8, "SMALLSERIAL", "smallint", 16},
		{16, "SERIAL", "integer", 32},
		{32, "BIGSERIAL", "bigint", 64},
		{64, "BIGSERIAL", "bigint", 64},
	} {
		declared := ColumnSpec{Name: "id", Type: ColumnType{Kind: KindInt, Bits: c.bits, Unsigned: true}, PrimaryKey: true, AutoIncrement: true}

		definition, err := ColumnDefinitionSQL(d, declared)
		if err != nil || definition != `"id" `+c.created+` PRIMARY KEY` {
			t.Errorf("uint%d key = %s, %v; want %s", c.bits, definition, err, c.created)
		}

		live := Column{Name: "id", Type: ColumnType{Kind: KindInt, Bits: c.storedBits}, PrimaryKey: true, AutoIncrement: true}
		if !SameColumnType(d, live, declared) {
			t.Errorf("uint%d key created as %s does not match its own declaration", c.bits, c.created)
		}
	}

	// A uint64 column that is not a key is still the NUMERIC(20) that holds its range.
	plain := ColumnSpec{Name: "n", Type: ColumnType{Kind: KindInt, Bits: 64, Unsigned: true}}
	if SameColumnType(d, Column{Name: "n", Type: ColumnType{Kind: KindInt, Bits: 64}}, plain) {
		t.Error("a BIGINT passed for a uint64 column that is not an auto-increment key")
	}
}

// TestPostgresTypeChangeLeavesTheLengthToTheAssignment covers a type change
// between kinds, or to or from a raw type, which was written USING col::T: a cast
// to VARCHAR(5) cuts 'abcdefghij' to 'abcde' without an error, in a migration
// nobody was told changed data. A character target takes the value as TEXT, and
// the assignment refuses one that does not fit.
func TestPostgresTypeChangeLeavesTheLengthToTheAssignment(t *testing.T) {
	d := PostgresDialect{}
	text := func(size int) ColumnType { return ColumnType{Kind: KindString, Size: size} }

	for _, c := range []struct {
		before, after ColumnType
		want          string
	}{
		{ColumnType{RawType: "TEXT"}, text(5), `TYPE VARCHAR(5) USING "c"::TEXT;`},
		{text(40), ColumnType{RawType: "CHAR(3)"}, `TYPE CHAR(3) USING "c"::TEXT;`},
		{ColumnType{Kind: KindInt, Bits: 64}, text(1), `TYPE VARCHAR(1) USING "c"::TEXT;`},
		{ColumnType{Kind: KindTime}, text(10), `TYPE VARCHAR(10) USING "c"::TEXT;`},
		{text(40), ColumnType{Kind: KindInt, Bits: 64}, `TYPE BIGINT USING "c"::BIGINT;`},
		{text(40), ColumnType{RawType: "TEXT[]"}, `TYPE TEXT[] USING "c"::TEXT[];`},
		// BOOLEAN casts to INTEGER only, and no integer casts to it.
		{ColumnType{Kind: KindBool}, ColumnType{Kind: KindInt, Bits: 64}, `TYPE BIGINT USING "c"::INTEGER;`},
		{ColumnType{Kind: KindInt, Bits: 64}, ColumnType{Kind: KindBool}, `TYPE BOOLEAN USING "c" <> 0;`},
		{ColumnType{Kind: KindFloat, Bits: 64}, ColumnType{Kind: KindBool}, `TYPE BOOLEAN USING "c" <> 0;`},
		// Bytes and text carry their bytes over: bytea::TEXT is the hex spelling
		// ('\x616263' for abc) and text::BYTEA reads the text as an escape string.
		{ColumnType{Kind: KindBytes}, text(40), `TYPE VARCHAR(40) USING convert_from("c", 'UTF8');`},
		{ColumnType{RawType: "BYTEA"}, ColumnType{RawType: "TEXT"}, `TYPE TEXT USING convert_from("c", 'UTF8');`},
		{text(40), ColumnType{Kind: KindBytes}, `TYPE BYTEA USING convert_to("c", 'UTF8');`},
		{ColumnType{RawType: "TEXT"}, ColumnType{RawType: "BYTEA"}, `TYPE BYTEA USING convert_to("c", 'UTF8');`},
		{ColumnType{Kind: KindInt, Bits: 64}, ColumnType{Kind: KindBytes}, `TYPE BYTEA USING "c"::BYTEA;`},
	} {
		statements := d.AlterColumnSQL("t", Column{Name: "c", Type: c.before}, ColumnSpec{Name: "c", Type: c.after})
		if len(statements) != 1 || !strings.HasSuffix(statements[0], c.want) {
			t.Errorf("%+v to %+v = %v; want ... %s", c.before, c.after, statements, c.want)
		}
	}
}

// TestPostgresOversizedStringsAreText covers a size: above the longest VARCHAR
// PostgreSQL declares, which failed CREATE TABLE where MySQL takes a LONGTEXT.
func TestPostgresOversizedStringsAreText(t *testing.T) {
	d := PostgresDialect{}

	for size, want := range map[int]string{10485760: "VARCHAR(10485760)", 10485761: "TEXT", 1 << 30: "TEXT"} {
		if got := d.ColumnTypeSQL(ColumnType{Kind: KindString, Size: size}); got != want {
			t.Errorf("string of %d = %s, want %s", size, got, want)
		}
	}

	live, err := parsePostgresColumnType("text", "text", "text", sql.NullInt64{})
	if err != nil || !SameColumnType(d, Column{Name: "s", Type: live}, ColumnSpec{Name: "s", Type: ColumnType{Kind: KindString, Size: 1 << 30}}) {
		t.Errorf("a live TEXT (%+v, %v) does not match the oversized string it was created for", live, err)
	}
}

// TestKeySequenceAdvanceQueryIsPostgresOnly covers the statement that moves a
// column's sequence past a key a row wrote: PostgreSQL alone needs one, it
// names the column as pg_get_serial_sequence wants it (the table quoted, the
// column bare), checks the privilege instead of failing, and refuses an
// identifier the dialect would.
func TestKeySequenceAdvanceQueryIsPostgresOnly(t *testing.T) {
	t.Parallel()

	query, err := PostgresDialect{}.KeySequenceAdvanceQuery("order", "id")
	if err != nil {
		t.Fatal(err)
	}

	for _, want := range []string{
		`pg_get_serial_sequence('"order"', 'id')`,
		"has_sequence_privilege(seq::regclass, 'UPDATE')",
		"setval(seq::regclass, GREATEST($1::bigint, COALESCE(pg_sequence_last_value(seq::regclass), 0)), true)",
	} {
		if !strings.Contains(query, want) {
			t.Errorf("query %q lacks %q", query, want)
		}
	}

	if _, err := (PostgresDialect{}).KeySequenceAdvanceQuery("order", "id; DROP"); err == nil {
		t.Error("an invalid column name was accepted")
	}

	for _, d := range []Dialect{MySQLDialect{}, SQLiteDialect{}} {
		query, err := d.KeySequenceAdvanceQuery("order", "id")
		if err != nil || query != "" {
			t.Errorf("%s: query %q, err %v; want none, the counter follows a written key", d.Name(), query, err)
		}
	}
}

// TestPostgresAddsAnIdentityToAKeyThatGeneratesNothing covers a table another
// tool created with a plain primary key where the declaration generates its
// keys: PostgreSQL refused the change ("manual change required") while MySQL
// added AUTO_INCREMENT. The key gets an identity that starts past the rows
// present; a narrower key is widened first, and the sequence it does not have
// yet is not widened. The other direction drops the generator, an identity or
// a SERIAL's default; a key that stops being the key stays a migration to write.
func TestPostgresAddsAnIdentityToAKeyThatGeneratesNothing(t *testing.T) {
	t.Parallel()

	d := PostgresDialect{}
	declared := ColumnSpec{Name: "id", Type: ColumnType{Kind: KindInt, Bits: 64}, PrimaryKey: true, AutoIncrement: true}

	plain := Column{Name: "id", Type: ColumnType{Kind: KindInt, Bits: 64}, PrimaryKey: true}
	want := []string{
		`ALTER TABLE "order" ALTER COLUMN "id" ADD GENERATED BY DEFAULT AS IDENTITY;`,
		`SELECT setval(pg_get_serial_sequence('"order"', 'id'), COALESCE((SELECT MAX("id") FROM "order"), 0) + 1, false);`,
	}

	if got := d.AlterColumnSQL("order", plain, declared); !slices.Equal(got, want) {
		t.Errorf("plain BIGINT key:\n got %q\nwant %q", got, want)
	}

	narrow := Column{Name: "id", Type: ColumnType{Kind: KindInt, Bits: 32}, PrimaryKey: true}
	if got := d.AlterColumnSQL("order", narrow, declared); len(got) != 3 || got[0] != `ALTER TABLE "order" ALTER COLUMN "id" TYPE BIGINT;` || !slices.Equal(got[1:], want) {
		t.Errorf("plain INTEGER key:\n got %q", got)
	}

	generating := Column{Name: "id", Type: ColumnType{Kind: KindInt, Bits: 64}, PrimaryKey: true, AutoIncrement: true}
	assigned := ColumnSpec{Name: "id", Type: ColumnType{Kind: KindInt, Bits: 64}, PrimaryKey: true}
	dropped := []string{
		`ALTER TABLE "order" ALTER COLUMN "id" DROP IDENTITY IF EXISTS;`,
		`ALTER TABLE "order" ALTER COLUMN "id" DROP DEFAULT;`,
	}

	if got := d.AlterColumnSQL("order", generating, assigned); !slices.Equal(got, dropped) {
		t.Errorf("dropping the generator:\n got %q\nwant %q", got, dropped)
	}

	notAKey := ColumnSpec{Name: "id", Type: ColumnType{Kind: KindInt, Bits: 64}}
	if got := d.AlterColumnSQL("order", generating, notAKey); got != nil {
		t.Errorf("a key that stops being one rendered %q; want a refusal", got)
	}
}
