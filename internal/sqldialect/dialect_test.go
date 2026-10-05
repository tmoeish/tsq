package sqldialect

import (
	"strings"
	"testing"
)

// TestBatchInsertStartID covers the LastInsertId-based half of primary-key backfill.
// MySQL reports the first generated id of a multi-row insert and SQLite reports the
// last, so the same interface method has to mean different arithmetic per dialect.
// PostgreSQL opts out entirely: it backfills through INSERT ... RETURNING instead.
func TestBatchInsertStartID(t *testing.T) {
	start, ok := SQLiteDialect{}.BatchInsertStartID(7, 3, 1)
	if !ok || start != 5 {
		t.Fatalf("sqlite BatchInsertStartID = (%d, %t), want (5, true)", start, ok)
	}

	start, ok = MySQLDialect{}.BatchInsertStartID(7, 3, 1)
	if !ok || start != 7 {
		t.Fatalf("mysql BatchInsertStartID = (%d, %t), want (7, true)", start, ok)
	}

	// SQLite counts back from the last key by the step.
	if start, ok = (SQLiteDialect{}).BatchInsertStartID(11, 3, 2); !ok || start != 7 {
		t.Fatalf("sqlite BatchInsertStartID with step 2 = (%d, %t), want (7, true)", start, ok)
	}

	if (MySQLDialect{}).InsertIDStepQuery() == "" || (SQLiteDialect{}).InsertIDStepQuery() != "" {
		t.Fatal("only MySQL reads the step between generated keys")
	}

	if _, ok = (PostgresDialect{}).BatchInsertStartID(7, 3, 1); ok {
		t.Fatal("postgres should not derive multi-row insert IDs from LastInsertId")
	}
}

// TestOnlyPostgresReturnsInsertIDsThroughReturning pairs with the test above: the two
// backfill paths are chosen by whether this suffix is empty, and a dialect must be on
// exactly one of them. ReturningClause (then ReturningClause) sat in the interface, implemented
// and never called, for six releases; asserting the split keeps both paths honest.
func TestOnlyPostgresReturnsInsertIDsThroughReturning(t *testing.T) {
	if suffix := (PostgresDialect{}).ReturningClause("id"); suffix == "" {
		t.Fatal("postgres should backfill primary keys through a RETURNING clause")
	}

	for name, dialect := range map[Name]Dialect{MySQL: MySQLDialect{}, SQLite: SQLiteDialect{}} {
		if suffix := dialect.ReturningClause("id"); suffix != "" {
			t.Errorf("dialect %s returned RETURNING suffix %q; it backfills through LastInsertId", name, suffix)
		}
	}
}

// TestColumnDefinitionRefusesAColumnItCannotWrite covers a generated column with
// no expression. It used to be written as a plain NOT NULL column, so a runtime
// that created the table made every INSERT fail on it.
func TestColumnDefinitionRefusesAColumnItCannotWrite(t *testing.T) {
	column := ColumnSpec{Name: "slug", Type: ColumnType{Kind: KindString, Size: 64}, Fill: FillGenerated}

	for _, d := range []Dialect{MySQLDialect{}, PostgresDialect{}, SQLiteDialect{}} {
		if _, err := ColumnDefinitionSQL(d, column); err == nil || !strings.Contains(err.Error(), "slug is computed by the database") {
			t.Errorf("%s: ColumnDefinitionSQL = %v; want the column refused", d.Name(), err)
		}
	}
}

// TestIndexColumnsCompareWithoutCase covers an index a database reports over
// "Code" for a declared column code: MySQL and SQLite take them for one column,
// and Validate refused the index while Reconcile rebuilt it on every start.
func TestIndexColumnsCompareWithoutCase(t *testing.T) {
	existing := Index{Table: "things", Fields: []string{"Code"}}
	if err := ValidateIndex("things", false, "idx_things_code", []string{"code"}, existing); err != nil {
		t.Fatalf("ValidateIndex = %v", err)
	}
}

// TestRawTypesCompareUnderOneName covers a raw type (db:"...,type:X") against the
// spelling the database reports for it. A pair that never compared equal failed
// Validate on the table TSQ had created and had Reconcile run the same ALTER on
// every start, a rewrite of the table each time on PostgreSQL.
func TestRawTypesCompareUnderOneName(t *testing.T) {
	for _, c := range []struct {
		dialect            Name
		declared, reported string
		same               bool
	}{
		{Postgres, "TIMESTAMP(3)", "timestamp(3) without time zone", true},
		{Postgres, "TIME", "time without time zone", true},
		{Postgres, "FLOAT", "double precision", true},
		{Postgres, "FLOAT8", "double precision", true},
		{Postgres, "INT4", "integer", true},
		{Postgres, "INT8", "bigint", true},
		{Postgres, "NUMERIC(12, 4)", "numeric(12,4)", true},
		{Postgres, "CHARACTER VARYING(20)", "character varying(20)", true},
		{Postgres, "REAL", "double precision", false}, // a float4 there
		{Postgres, "TIMESTAMP(3)", "timestamp(6) without time zone", false},
		{MySQL, "REAL", "double", true},
		{MySQL, "DOUBLE PRECISION", "double", true},
		{MySQL, "BOOL", "tinyint(1)", true},
		{MySQL, "INT(11)", "int", true},
		{MySQL, "DECIMAL", "decimal(10,0)", true},
		{MySQL, "NUMERIC", "decimal(10,0)", true},
		{MySQL, "DECIMAL(10,2)", "decimal(10,2)", true},
		{MySQL, "VARCHAR(20) COLLATE utf8mb4_bin", "varchar(20)", true},
		{MySQL, "BIGINT UNSIGNED", "bigint unsigned", true},
		{MySQL, "DECIMAL(10,2)", "decimal(10,4)", false},
		{MySQL, "VARCHAR(20)", "varchar(40)", false},
		{MySQL, "BIGINT", "bigint unsigned", false},
	} {
		got := normalizeDDLNativeTypeName(c.dialect, c.declared) == normalizeDDLNativeTypeName(c.dialect, c.reported)
		if got != c.same {
			t.Errorf("%s: declared %q against reported %q = %v, want %v (%q, %q)", c.dialect, c.declared, c.reported, got, c.same,
				normalizeDDLNativeTypeName(c.dialect, c.declared), normalizeDDLNativeTypeName(c.dialect, c.reported))
		}
	}
}
