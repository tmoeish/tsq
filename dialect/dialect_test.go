package dialect

import "testing"

// TestBatchInsertStartID covers the LastInsertId-based half of primary-key backfill.
// MySQL reports the first generated id of a multi-row insert and SQLite reports the
// last, so the same interface method has to mean different arithmetic per dialect.
// PostgreSQL opts out entirely: it backfills through INSERT ... RETURNING instead.
func TestBatchInsertStartID(t *testing.T) {
	start, ok := SQLiteDialect{}.BatchInsertStartID(7, 3)
	if !ok || start != 5 {
		t.Fatalf("sqlite BatchInsertStartID = (%d, %t), want (5, true)", start, ok)
	}

	start, ok = MySQLDialect{}.BatchInsertStartID(7, 3)
	if !ok || start != 7 {
		t.Fatalf("mysql BatchInsertStartID = (%d, %t), want (7, true)", start, ok)
	}

	if _, ok = (PostgresDialect{}).BatchInsertStartID(7, 3); ok {
		t.Fatal("postgres should not derive multi-row insert IDs from LastInsertId")
	}
}

// TestOnlyPostgresReturnsInsertIDsThroughReturning pairs with the test above: the two
// backfill paths are chosen by whether this suffix is empty, and a dialect must be on
// exactly one of them. LastInsertIdReturningSuffix sat in the interface, implemented
// and never called, for six releases; asserting the split keeps both paths honest.
func TestOnlyPostgresReturnsInsertIDsThroughReturning(t *testing.T) {
	if suffix := (PostgresDialect{}).LastInsertIdReturningSuffix("users", "id"); suffix == "" {
		t.Fatal("postgres should backfill primary keys through a RETURNING clause")
	}

	for name, dialect := range map[Name]Dialect{MySQL: MySQLDialect{}, SQLite: SQLiteDialect{}} {
		if suffix := dialect.LastInsertIdReturningSuffix("users", "id"); suffix != "" {
			t.Errorf("dialect %s returned RETURNING suffix %q; it backfills through LastInsertId", name, suffix)
		}
	}
}
