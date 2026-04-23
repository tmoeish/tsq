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
		{"string with backslash", "path\\to\\file", "", true},
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

func TestSqlValueRejectsUnsupportedCompositeTypes(t *testing.T) {
	tests := []struct {
		name  string
		input any
	}{
		{name: "struct", input: struct{ ID int }{ID: 1}},
		{name: "map", input: map[string]int{"id": 1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := sqlValue(tt.input); err == nil {
				t.Fatal("expected unsupported value type to return an error")
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
		{"empty string", "", "''"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := sqlEscapeString(tt.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestSqlEscapeStringRejectsBackslashes(t *testing.T) {
	if _, err := sqlEscapeString(`path\file`); err == nil {
		t.Fatal("expected backslash-containing string literal to return an error")
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

type pointerValuer struct {
	value string
}

func (p *pointerValuer) Value() (driver.Value, error) {
	if p == nil {
		return "unexpected", nil
	}

	return p.value, nil
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

func TestSqlValueTreatsTypedNilPointerValuerAsNull(t *testing.T) {
	var value *pointerValuer

	result, err := sqlValue(value)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result != "NULL" {
		t.Fatalf("expected typed nil pointer valuer to map to NULL, got %q", result)
	}
}

// Helper functions for pointer tests
func stringPtr(s string) *string {
	return &s
}

func intPtr(i int) *int {
	return &i
}

func TestCondition_EmptyInShortCircuits(t *testing.T) {
	col := NewCol[int](newMockTable("users"), "id", "id", nil)

	if got := col.In().Clause(); got != "1 = 0" {
		t.Fatalf("expected empty IN to short-circuit to false predicate, got %q", got)
	}
	if len(col.In().Tables()) != 0 {
		t.Fatalf("expected empty IN short-circuit to avoid leaking source tables")
	}

	if got := col.NIn().Clause(); got != "1 = 1" {
		t.Fatalf("expected empty NOT IN to short-circuit to true predicate, got %q", got)
	}
	if len(col.NIn().Tables()) != 0 {
		t.Fatalf("expected empty NOT IN short-circuit to avoid leaking source tables")
	}
}

func TestCondition_EmptyAndOrShortCircuit(t *testing.T) {
	if got := And().Clause(); got != "1 = 1" {
		t.Fatalf("expected empty And to short-circuit to true predicate, got %q", got)
	}

	if got := Or().Clause(); got != "1 = 0" {
		t.Fatalf("expected empty Or to short-circuit to false predicate, got %q", got)
	}
}

func TestCondition_NullLiteralPredicatesFailFast(t *testing.T) {
	ptrCol := NewCol[*string](newMockTable("users"), "name", "name", nil)
	nullableCol := NewCol[sql.NullString](newMockTable("users"), "nickname", "nickname", nil)

	for _, fn := range []func(){
		func() { _ = ptrCol.EQ(nil) },
		func() { _ = ptrCol.In(nil) },
		func() { _ = nullableCol.EQ(sql.NullString{}) },
	} {
		func() {
			defer func() {
				if r := recover(); r == nil {
					t.Fatal("expected null literal predicate to panic")
				}
			}()

			fn()
		}()
	}
}

func TestCondition_NilCompositeInputsFailFast(t *testing.T) {
	var nilCond Condition

	for _, fn := range []func(){
		func() { _ = And(nilCond) },
		func() { _ = Or(nilCond) },
	} {
		func() {
			defer func() {
				if r := recover(); r == nil {
					t.Fatal("expected nil condition to panic")
				}
			}()

			fn()
		}()
	}
}

func TestCondition_PortabilitySensitiveLikePredicatesFailFast(t *testing.T) {
	users := newMockTable("users")
	nameCol := NewCol[string](users, "name", "name", nil)
	patternCol := NewCol[string](users, "pattern", "pattern", nil)

	for _, fn := range []func(){
		func() { _ = nameCol.StartWithVar() },
		func() { _ = nameCol.StartWithCol(patternCol) },
	} {
		func() {
			defer func() {
				if r := recover(); r == nil {
					t.Fatal("expected predicate helper to panic for non-portable SQL")
				}
			}()

			fn()
		}()
	}
}

func TestCondition_StringLiteralsRejectBackslashes(t *testing.T) {
	col := NewCol[string](newMockTable("users"), "name", "name", nil)

	for name, fn := range map[string]func(){
		"EQ": func() { _ = col.EQ(`path\file`) },
		"IN": func() { _ = col.In(`path\file`) },
	} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatal("expected backslash-containing string literal to panic")
				}
			}()

			fn()
		})
	}
}

func TestCondition_ExistsSubIsStandalonePredicate(t *testing.T) {
	col := NewCol[int](newMockTable("users"), "id", "id", nil)
	orderID := NewCol[int](newMockTable("orders"), "id", "id", nil)
	subquery := Select(orderID).MustBuild()

	got := renderCanonicalSQL(col.ExistsSub(subquery).Clause())
	want := `EXISTS (SELECT "orders"."id" FROM "orders")`
	if got != want {
		t.Fatalf("expected exists clause %q, got %q", want, got)
	}
}

func TestCondition_UnbuiltSubqueryFailsFast(t *testing.T) {
	col := NewCol[int](newMockTable("users"), "id", "id", nil)

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected unbuilt subquery to panic")
		}
	}()

	_ = col.InSub(&Query{})
}

func TestCondition_EmptyClauseFailsFast(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected empty condition clause to panic")
		}
	}()

	_ = And(Cond{})
}

func TestCondition_PredicateRejectsInvalidFormat(t *testing.T) {
	col := NewCol[int](newMockTable("users"), "id", "id", nil)

	tests := []struct {
		name string
		op   string
		args []any
	}{
		{name: "empty format", op: "", args: nil},
		{name: "missing placeholders", op: "id = 1", args: nil},
		{name: "placeholder count mismatch", op: "%s = %s", args: []any{1, 2}},
		{name: "unsupported verb", op: "%s = %d", args: []any{1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatal("expected Predicate to panic for invalid format")
				}
			}()

			_ = col.Predicate(tt.op, tt.args...)
		})
	}
}

func TestCondition_PredicateAllowsEscapedPercentLiterals(t *testing.T) {
	col := NewCol[string](newMockTable("users"), "name", "name", nil)

	clause := renderCanonicalSQL(col.Predicate("%s LIKE '%%s'").Clause())
	if clause != `"users"."name" LIKE '%s'` {
		t.Fatalf("expected escaped percent literal to be preserved, got %q", clause)
	}
}

func TestCondition_UniqueSubqueryPredicatesFailFast(t *testing.T) {
	col := NewCol[int](newMockTable("users"), "id", "id", nil)
	subquery := &Query{listSQL: "SELECT 1"}

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected Unique to panic for unsupported predicate")
		}
	}()

	_ = col.Unique(subquery)
}
