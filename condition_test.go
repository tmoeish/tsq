package tsq

import (
	"database/sql"
	"database/sql/driver"
	"math"
	"testing"
	"time"
)

func TestSqlValue(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected string
		hasError bool
	}{
		// Nil values
		{"nil", nil, "NULL", false},

		// String types
		{"string", "hello", "'hello'", false},
		{"string with quotes", "it's a test", "'it''s a test'", false},
		{"string with backslash", "path\\to\\file", "'path\\\\to\\\\file'", false},
		{"empty string", "", "''", false},

		// Byte types
		{"[]byte", []byte("hello"), "'hello'", false},
		{"[]byte with quotes", []byte("it's a test"), "'it''s a test'", false},
		{"sql.RawBytes", sql.RawBytes("raw data"), "'raw data'", false},

		// Integer types
		{"int", int(42), "42", false},
		{"int8", int8(-128), "-128", false},
		{"int16", int16(32767), "32767", false},
		{"int32", int32(-2147483648), "-2147483648", false},
		{"int64", int64(9223372036854775807), "9223372036854775807", false},

		// Unsigned integer types
		{"uint", uint(42), "42", false},
		{"uint8", uint8(255), "255", false},
		{"uint16", uint16(65535), "65535", false},
		{"uint32", uint32(4294967295), "4294967295", false},
		{"uint64", uint64(18446744073709551615), "18446744073709551615", false},

		// Floating point types
		{"float32", float32(3.14), "3.14", false},
		{"float64", float64(2.718281828), "2.718281828", false},
		{"float32 NaN", float32(math.NaN()), "NULL", false},
		{"float64 NaN", math.NaN(), "NULL", false},
		{"float32 +Inf", float32(math.Inf(1)), "NULL", false},
		{"float64 +Inf", math.Inf(1), "NULL", false},
		{"float32 -Inf", float32(math.Inf(-1)), "NULL", false},
		{"float64 -Inf", math.Inf(-1), "NULL", false},

		// Boolean types
		{"bool true", true, "TRUE", false},
		{"bool false", false, "FALSE", false},

		// Time types
		{"time.Time", time.Date(2023, 12, 25, 15, 30, 45, 0, time.UTC), "'2023-12-25 15:30:45'", false},
		{"time.Time zero", time.Time{}, "NULL", false},

		// Pointer types
		{"*string", stringPtr("hello"), "'hello'", false},
		{"*string nil", (*string)(nil), "NULL", false},
		{"*int", intPtr(42), "42", false},
		{"*int nil", (*int)(nil), "NULL", false},

		// sql.Null* types (through driver.Valuer interface)
		{"sql.NullString valid", sql.NullString{String: "hello", Valid: true}, "'hello'", false},
		{"sql.NullString invalid", sql.NullString{String: "hello", Valid: false}, "NULL", false},
		{"sql.NullInt64 valid", sql.NullInt64{Int64: 42, Valid: true}, "42", false},
		{"sql.NullInt64 invalid", sql.NullInt64{Int64: 42, Valid: false}, "NULL", false},
		{"sql.NullFloat64 valid", sql.NullFloat64{Float64: 3.14, Valid: true}, "3.14", false},
		{"sql.NullFloat64 invalid", sql.NullFloat64{Float64: 3.14, Valid: false}, "NULL", false},
		{"sql.NullBool valid true", sql.NullBool{Bool: true, Valid: true}, "TRUE", false},
		{"sql.NullBool valid false", sql.NullBool{Bool: false, Valid: true}, "FALSE", false},
		{"sql.NullBool invalid", sql.NullBool{Bool: true, Valid: false}, "NULL", false},
		{"sql.NullTime valid", sql.NullTime{Time: time.Date(2023, 12, 25, 15, 30, 45, 0, time.UTC), Valid: true}, "'2023-12-25 15:30:45'", false},
		{"sql.NullTime invalid", sql.NullTime{Time: time.Now(), Valid: false}, "NULL", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := sqlValue(tt.input)

			if tt.hasError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}

				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if result != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestSqlValueReflect(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected string
		hasError bool
	}{
		// Byte arrays
		{"[4]byte", [4]byte{'t', 'e', 's', 't'}, "'test'", false},
		{"[]uint8", []uint8{116, 101, 115, 116}, "'test'", false},

		// Unsupported slice types
		{"[]int", []int{1, 2, 3}, "", true},
		{"[]string", []string{"a", "b"}, "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := sqlValue(tt.input)

			if tt.hasError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}

				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if result != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestSqlEscapeString(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"simple string", "hello", "'hello'"},
		{"string with single quote", "it's", "'it''s'"},
		{"string with multiple quotes", "it's a 'test'", "'it''s a ''test'''"},
		{"string with backslash", "path\\file", "'path\\\\file'"},
		{"string with both", "it's a \\test\\", "'it''s a \\\\test\\\\'"},
		{"empty string", "", "''"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sqlEscapeString(tt.input)
			if result != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, result)
			}
		})
	}
}

// Custom type implementing driver.Valuer
type customValuer struct {
	value string
	valid bool
}

func (c customValuer) Value() (driver.Value, error) {
	if !c.valid {
		return nil, nil
	}

	return c.value, nil
}

func TestSqlValueCustomValuer(t *testing.T) {
	tests := []struct {
		name     string
		input    customValuer
		expected string
	}{
		{"valid custom valuer", customValuer{value: "custom", valid: true}, "'custom'"},
		{"invalid custom valuer", customValuer{value: "custom", valid: false}, "NULL"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := sqlValue(tt.input)
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if result != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, result)
			}
		})
	}
}

// Helper functions for pointer tests
func stringPtr(s string) *string {
	return &s
}

func intPtr(i int) *int {
	return &i
}
