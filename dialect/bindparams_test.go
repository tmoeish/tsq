package dialect

import "testing"

// allDialects lists every concrete dialect in this package. A dialect belongs here the
// moment it exists, which is what makes the checks below cover the whole package
// rather than a chosen subset.
var allDialects = []Dialect{MySQLDialect{}, PostgresDialect{}, SQLiteDialect{}}

// TestEveryDialectDeclaresABindParamLimit is the gate behind the rule that a bind
// parameter ceiling is per dialect, not a single number shared by all of them.
//
// tsq used to hard-code 65535 in the chunking helpers with a comment calling it "the
// tightest ceiling among the supported databases". It is not: SQLite stops at 32766,
// so a wide-table batch sized against 65535 failed on the one database the unit suite
// actually runs against.
func TestEveryDialectDeclaresABindParamLimit(t *testing.T) {
	for _, dialect := range allDialects {
		limit, declared := maxBindParams[dialect.Name()]
		if !declared {
			t.Errorf("dialect %s does not declare a bind parameter limit; add an entry to maxBindParams", dialect.Name())
			continue
		}

		if limit <= 0 {
			t.Errorf("dialect %s declares a non-positive bind parameter limit %d", dialect.Name(), limit)
		}

		if got := MaxBindParams(dialect); got != limit {
			t.Errorf("MaxBindParams(%s) = %d, want %d", dialect.Name(), got, limit)
		}
	}

	if len(maxBindParams) != len(allDialects) {
		t.Errorf("maxBindParams has %d entries for %d dialects; it names a dialect this package does not implement",
			len(maxBindParams), len(allDialects))
	}
}

// TestMaxBindParamsFallsBackToTheTightestLimit pins the unknown-dialect answer. An
// executor wrapped around a bare *sql.DB reports no dialect, and guessing high there
// is the failure mode this fallback exists to avoid.
func TestMaxBindParamsFallsBackToTheTightestLimit(t *testing.T) {
	for _, limit := range maxBindParams {
		if minBindParamsLimit > limit {
			t.Fatalf("minBindParamsLimit %d is larger than a declared limit %d", minBindParamsLimit, limit)
		}
	}

	if got := MaxBindParams(nil); got != minBindParamsLimit {
		t.Errorf("MaxBindParams(nil) = %d, want the tightest declared limit %d", got, minBindParamsLimit)
	}
}
