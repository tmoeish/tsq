package tsq

import (
	"testing"
)

func TestCol_Fn(t *testing.T) {
	table := newMockTable("users")
	col := NewCol[string](table, "name", "name", nil)

	// Test custom function format
	result := col.Fn("UPPER(%s)")

	if result.Name() != "name" {
		t.Errorf("Expected name 'name', got '%s'", result.Name())
	}

	if result.Table().Table() != "users" {
		t.Errorf("Expected table 'users', got '%s'", result.Table().Table())
	}

	expectedQualified := "UPPER(`users`.`name`)"
	if result.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, result.QualifiedName())
	}

	if result.JSONFieldName() != "name" {
		t.Errorf("Expected JSON field name 'name', got '%s'", result.JSONFieldName())
	}
}

func TestCol_Count(t *testing.T) {
	table := newMockTable("users")
	col := NewCol[int](table, "id", "id", nil)

	result := col.Count()

	expectedQualified := "COUNT(`users`.`id`)"
	if result.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, result.QualifiedName())
	}
}

func TestCol_Sum(t *testing.T) {
	table := newMockTable("orders")
	col := NewCol[float64](table, "amount", "amount", nil)

	result := col.Sum()

	expectedQualified := "SUM(`orders`.`amount`)"
	if result.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, result.QualifiedName())
	}
}

func TestCol_Avg(t *testing.T) {
	table := newMockTable("products")
	col := NewCol[float64](table, "price", "price", nil)

	result := col.Avg()

	expectedQualified := "AVG(`products`.`price`)"
	if result.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, result.QualifiedName())
	}
}

func TestCol_Max(t *testing.T) {
	table := newMockTable("users")
	col := NewCol[int](table, "age", "age", nil)

	result := col.Max()

	expectedQualified := "MAX(`users`.`age`)"
	if result.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, result.QualifiedName())
	}
}

func TestCol_Min(t *testing.T) {
	table := newMockTable("users")
	col := NewCol[int](table, "age", "age", nil)

	result := col.Min()

	expectedQualified := "MIN(`users`.`age`)"
	if result.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, result.QualifiedName())
	}
}

func TestCol_Distinct(t *testing.T) {
	table := newMockTable("users")
	col := NewCol[string](table, "department", "department", nil)

	result := col.Distinct()

	expectedQualified := "DISTINCT(`users`.`department`)"
	if result.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, result.QualifiedName())
	}
}

func TestCol_Upper(t *testing.T) {
	table := newMockTable("users")
	col := NewCol[string](table, "name", "name", nil)

	result := col.Upper()

	expectedQualified := "UPPER(`users`.`name`)"
	if result.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, result.QualifiedName())
	}
}

func TestCol_Lower(t *testing.T) {
	table := newMockTable("users")
	col := NewCol[string](table, "name", "name", nil)

	result := col.Lower()

	expectedQualified := "LOWER(`users`.`name`)"
	if result.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, result.QualifiedName())
	}
}

func TestCol_Substring(t *testing.T) {
	table := newMockTable("users")
	col := NewCol[string](table, "description", "description", nil)

	result := col.Substring(1, 10)

	expectedQualified := "SUBSTRING(`users`.`description`, 1, 10)"
	if result.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, result.QualifiedName())
	}
}

func TestCol_Length(t *testing.T) {
	table := newMockTable("users")
	col := NewCol[string](table, "name", "name", nil)

	result := col.Length()

	expectedQualified := "LENGTH(`users`.`name`)"
	if result.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, result.QualifiedName())
	}
}

func TestCol_Trim(t *testing.T) {
	table := newMockTable("users")
	col := NewCol[string](table, "name", "name", nil)

	result := col.Trim()

	expectedQualified := "TRIM(`users`.`name`)"
	if result.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, result.QualifiedName())
	}
}

func TestCol_Concat(t *testing.T) {
	table := newMockTable("users")
	col := NewCol[string](table, "first_name", "first_name", nil)

	result := col.Concat(" Smith")

	expectedQualified := "CONCAT(`users`.`first_name`, ' Smith')"
	if result.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, result.QualifiedName())
	}
}

func TestCol_Now(t *testing.T) {
	table := newMockTable("users")
	col := NewCol[string](table, "created_at", "created_at", nil)

	result := col.Now()

	actual := result.QualifiedName()
	if actual != "NOW()" {
		t.Errorf("Expected qualified name to be 'NOW()', got '%s'", actual)
	}
}

func TestCol_Date(t *testing.T) {
	table := newMockTable("orders")
	col := NewCol[string](table, "created_at", "created_at", nil)

	result := col.Date()

	expectedQualified := "DATE(`orders`.`created_at`)"
	if result.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, result.QualifiedName())
	}
}

func TestCol_Year(t *testing.T) {
	table := newMockTable("orders")
	col := NewCol[string](table, "created_at", "created_at", nil)

	result := col.Year()

	expectedQualified := "YEAR(`orders`.`created_at`)"
	if result.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, result.QualifiedName())
	}
}

func TestCol_Month(t *testing.T) {
	table := newMockTable("orders")
	col := NewCol[string](table, "created_at", "created_at", nil)

	result := col.Month()

	expectedQualified := "MONTH(`orders`.`created_at`)"
	if result.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, result.QualifiedName())
	}
}

func TestCol_Day(t *testing.T) {
	table := newMockTable("orders")
	col := NewCol[string](table, "created_at", "created_at", nil)

	result := col.Day()

	expectedQualified := "DAY(`orders`.`created_at`)"
	if result.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, result.QualifiedName())
	}
}

func TestCol_Round(t *testing.T) {
	table := newMockTable("products")
	col := NewCol[float64](table, "price", "price", nil)

	result := col.Round(2)

	expectedQualified := "ROUND(`products`.`price`, 2)"
	if result.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, result.QualifiedName())
	}
}

func TestCol_Ceil(t *testing.T) {
	table := newMockTable("products")
	col := NewCol[float64](table, "price", "price", nil)

	result := col.Ceil()

	expectedQualified := "CEIL(`products`.`price`)"
	if result.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, result.QualifiedName())
	}
}

func TestCol_Floor(t *testing.T) {
	table := newMockTable("products")
	col := NewCol[float64](table, "price", "price", nil)

	result := col.Floor()

	expectedQualified := "FLOOR(`products`.`price`)"
	if result.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, result.QualifiedName())
	}
}

func TestCol_Abs(t *testing.T) {
	table := newMockTable("transactions")
	col := NewCol[float64](table, "amount", "amount", nil)

	result := col.Abs()

	expectedQualified := "ABS(`transactions`.`amount`)"
	if result.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, result.QualifiedName())
	}
}

func TestCol_Coalesce(t *testing.T) {
	table := newMockTable("users")
	col := NewCol[string](table, "nickname", "nickname", nil)

	result := col.Coalesce("Anonymous")

	expectedQualified := "COALESCE(`users`.`nickname`, 'Anonymous')"
	if result.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, result.QualifiedName())
	}
}

func TestCol_NullIf(t *testing.T) {
	table := newMockTable("users")
	col := NewCol[string](table, "status", "status", nil)

	result := col.NullIf("inactive")

	expectedQualified := "NULLIF(`users`.`status`, 'inactive')"
	if result.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, result.QualifiedName())
	}
}

func TestCol_ChainedFunctions(t *testing.T) {
	table := newMockTable("users")
	col := NewCol[string](table, "name", "name", nil)

	// Test chaining multiple functions
	result := col.Upper().Trim()

	expectedQualified := "TRIM(UPPER(`users`.`name`))"
	if result.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, result.QualifiedName())
	}

	// Verify original properties are preserved
	if result.Name() != "name" {
		t.Errorf("Expected name 'name', got '%s'", result.Name())
	}

	if result.JSONFieldName() != "name" {
		t.Errorf("Expected JSON field name 'name', got '%s'", result.JSONFieldName())
	}
}

func TestCol_ComplexFunctionChain(t *testing.T) {
	table := newMockTable("products")
	col := NewCol[float64](table, "price", "price", nil)

	// Test complex function chaining
	result := col.Round(2).Coalesce("0.00")

	expectedQualified := "COALESCE(ROUND(`products`.`price`, 2), '0.00')"
	if result.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, result.QualifiedName())
	}
}

func TestCol_FunctionPreservesMetadata(t *testing.T) {
	table := newMockTable("users")
	col := NewCol[string](table, "email", "user_email", func(holder any) any { return nil })

	result := col.Upper()

	// Check that metadata is preserved
	if result.Name() != "email" {
		t.Errorf("Expected name 'email', got '%s'", result.Name())
	}

	if result.JSONFieldName() != "user_email" {
		t.Errorf("Expected JSON field name 'user_email', got '%s'", result.JSONFieldName())
	}

	if result.Table().Table() != "users" {
		t.Errorf("Expected table 'users', got '%s'", result.Table().Table())
	}

	// Check that function pointer is preserved (can't compare directly, but can check it's not nil)
	if result.FieldPointer() == nil {
		t.Error("Expected field pointer to be preserved")
	}
}
