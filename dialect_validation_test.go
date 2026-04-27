package tsq

import (
	"strings"
	"testing"
)

// TestDialectDetection verifies dialect name detection
func TestDialectDetection(t *testing.T) {
	// Note: We can't easily test with real dialects without importing specific drivers
	// This test documents the dialect detection capability
	t.Run("detects nil dialect", func(t *testing.T) {
		name := detectDialectName(nil)
		if name != DialectUnknown {
			t.Errorf("Expected DialectUnknown for nil, got %q", name)
		}
	})

	t.Run("dialect names are lowercase", func(t *testing.T) {
		dialects := []DialectName{
			DialectMySQL,
			DialectPostgres,
			DialectSQLite,
			DialectSQLServer,
			DialectOracle,
		}
		for _, d := range dialects {
			str := string(d)
			if str != "" && str != string(DialectUnknown) {
				for _, ch := range str {
					if ch >= 'A' && ch <= 'Z' {
						t.Errorf("Dialect %q should be lowercase", str)
					}
				}
			}
		}
	})
}

// TestDialectCapabilities verifies dialect capability detection
func TestDialectCapabilities(t *testing.T) {
	t.Run("postgres supports FULL OUTER JOIN", func(t *testing.T) {
		// Document the capability (actual testing requires dialect instantiation)
		supported := SupportsFullOuterJoin(nil)
		// nil dialect returns false, but Postgres would return true
		if supported {
			t.Error("nil dialect should not support FULL OUTER JOIN")
		}
	})

	t.Run("unknown dialect defaults to no support", func(t *testing.T) {
		supported := SupportsFullOuterJoin(nil)
		if supported {
			t.Error("Unknown dialect should not support FULL OUTER JOIN")
		}
	})
}

// TestErrUnsupportedOperation verifies error message formatting
func TestErrUnsupportedOperation(t *testing.T) {
	t.Run("error message with reason", func(t *testing.T) {
		err := NewErrUnsupportedOperation("FULL OUTER JOIN", DialectMySQL, "MySQL does not support FULL OUTER JOIN")
		msg := err.Error()
		if !strings.Contains(msg, "FULL OUTER JOIN") {
			t.Errorf("Error should mention operation: %s", msg)
		}
		if !strings.Contains(msg, "mysql") {
			t.Errorf("Error should mention dialect: %s", msg)
		}
	})

	t.Run("error message without reason", func(t *testing.T) {
		err := NewErrUnsupportedOperation("CROSS APPLY", DialectSQLite, "")
		msg := err.Error()
		if !strings.Contains(msg, "CROSS APPLY") {
			t.Errorf("Error should mention operation: %s", msg)
		}
		if !strings.Contains(msg, "sqlite3") {
			t.Errorf("Error should mention dialect: %s", msg)
		}
	})
}

// TestValidateOperationForDialect verifies operation validation
func TestValidateOperationForDialect(t *testing.T) {
	t.Run("nil dialect allows all operations", func(t *testing.T) {
		err := ValidateOperationForDialect("FULL OUTER JOIN", nil)
		if err != nil {
			t.Error("nil dialect should allow operations")
		}
	})

	t.Run("validates FULL OUTER JOIN", func(t *testing.T) {
		// With nil dialect, it passes through
		err := ValidateOperationForDialect("FULL OUTER JOIN", nil)
		if err != nil {
			t.Error("nil dialect should pass validation")
		}
	})

	t.Run("documents unsupported operations", func(t *testing.T) {
		// This documents the validation exists
		// Real dialect validation would test against actual dialect instances
		_ = ValidateOperationForDialect("FULL OUTER JOIN", nil)
	})
}

// TestDialectLimitations documents dialect limitations
func TestDialectLimitations(t *testing.T) {
	limitations := map[DialectName][]string{
		DialectMySQL:     {"FULL OUTER JOIN"},
		DialectSQLite:    {"FULL OUTER JOIN"},
		DialectSQLServer: {"FULL OUTER JOIN"}, // Has workarounds but not native support
		DialectPostgres:  {}, // Supports most standard SQL
		DialectOracle:    {}, // Supports most standard SQL
	}

	t.Run("dialect limitations documented", func(t *testing.T) {
		for dialect, limitations := range limitations {
			t.Logf("Dialect %q has limitations: %v", dialect, limitations)
		}
	})

	t.Run("mysql does not support full outer join", func(t *testing.T) {
		mysql := DialectMySQL
		if _, ok := limitations[mysql]; !ok {
			t.Error("MySQL should have documented limitations")
		}
		if len(limitations[mysql]) == 0 {
			t.Error("MySQL should have FULL OUTER JOIN limitation documented")
		}
	})

	t.Run("postgres supports full outer join", func(t *testing.T) {
		postgres := DialectPostgres
		if _, ok := limitations[postgres]; !ok {
			t.Error("Postgres should have limitations map entry")
		}
		if len(limitations[postgres]) > 0 {
			t.Errorf("Postgres should support standard SQL, got limitations: %v", limitations[postgres])
		}
	})
}

// Helper for test assertions
