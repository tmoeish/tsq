package sqldialect

import (
	"database/sql"
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
