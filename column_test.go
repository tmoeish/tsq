package tsq

import (
	"testing"
)

func TestNewCol(t *testing.T) {
	table := newMockTable("users")
	fieldPointer := func(holder any) any {
		s := struct{ Name string }{}
		return &s.Name
	}

	col := NewCol[string](table, "name", "user_name", fieldPointer)

	if col.Name() != "name" {
		t.Errorf("Expected name 'name', got '%s'", col.Name())
	}

	if col.Table().Table() != "users" {
		t.Errorf("Expected table 'users', got '%s'", col.Table().Table())
	}

	expectedQualified := "`users`.`name`"
	if col.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, col.QualifiedName())
	}

	if col.JSONFieldName() != "user_name" {
		t.Errorf("Expected JSON field name 'user_name', got '%s'", col.JSONFieldName())
	}

	if col.FieldPointer() == nil {
		t.Error("Expected field pointer to be set")
	}
}

func TestCol_Table(t *testing.T) {
	table := newMockTable("products")
	col := NewCol[float64](table, "price", "price", nil)

	resultTable := col.Table()
	if resultTable.Table() != "products" {
		t.Errorf("Expected table 'products', got '%s'", resultTable.Table())
	}
}

func TestCol_Name(t *testing.T) {
	table := newMockTable("orders")
	col := NewCol[int](table, "id", "id", nil)

	if col.Name() != "id" {
		t.Errorf("Expected name 'id', got '%s'", col.Name())
	}
}

func TestCol_QualifiedName(t *testing.T) {
	tests := []struct {
		tableName    string
		columnName   string
		expectedName string
	}{
		{"users", "id", "`users`.`id`"},
		{"orders", "user_id", "`orders`.`user_id`"},
		{"products", "name", "`products`.`name`"},
		{"user_profiles", "avatar_url", "`user_profiles`.`avatar_url`"},
	}

	for _, tt := range tests {
		t.Run(tt.tableName+"_"+tt.columnName, func(t *testing.T) {
			table := newMockTable(tt.tableName)
			col := NewCol[string](table, tt.columnName, tt.columnName, nil)

			if col.QualifiedName() != tt.expectedName {
				t.Errorf("Expected qualified name '%s', got '%s'", tt.expectedName, col.QualifiedName())
			}
		})
	}
}

func TestCol_FieldPointer(t *testing.T) {
	table := newMockTable("users")

	// Test with nil field pointer
	col1 := NewCol[string](table, "name", "name", nil)
	if col1.FieldPointer() != nil {
		t.Error("Expected nil field pointer")
	}

	// Test with actual field pointer
	fieldPointer := func(holder any) any {
		s := struct{ Name string }{}
		return &s.Name
	}

	col2 := NewCol[string](table, "name", "name", fieldPointer)
	if col2.FieldPointer() == nil {
		t.Error("Expected non-nil field pointer")
	}
}

func TestCol_JSONFieldName(t *testing.T) {
	table := newMockTable("users")

	tests := []struct {
		name          string
		jsonFieldName string
	}{
		{"simple", "name"},
		{"snake_case", "user_name"},
		{"camelCase", "userName"},
		{"with_prefix", "user_full_name"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col := NewCol[string](table, "name", tt.jsonFieldName, nil)
			if col.JSONFieldName() != tt.jsonFieldName {
				t.Errorf("Expected JSON field name '%s', got '%s'", tt.jsonFieldName, col.JSONFieldName())
			}
		})
	}
}

func TestCol_Into(t *testing.T) {
	table := newMockTable("users")
	originalFieldPointer := func(holder any) any {
		s := struct{ Name string }{}
		return &s.Name
	}
	col := NewCol[string](table, "name", "original_name", originalFieldPointer)

	// Create new field pointer and JSON field name
	newFieldPointer := func(holder any) any {
		s := struct{ DisplayName string }{}
		return &s.DisplayName
	}
	newJSONFieldName := "display_name"

	newCol := col.Into(newFieldPointer, newJSONFieldName)

	// Check that the new column has updated properties
	if newCol.JSONFieldName() != newJSONFieldName {
		t.Errorf("Expected JSON field name '%s', got '%s'", newJSONFieldName, newCol.JSONFieldName())
	}

	if newCol.FieldPointer() == nil {
		t.Error("Expected non-nil field pointer")
	}

	// Check that original properties are preserved
	if newCol.Name() != "name" {
		t.Errorf("Expected name 'name', got '%s'", newCol.Name())
	}

	if newCol.Table().Table() != "users" {
		t.Errorf("Expected table 'users', got '%s'", newCol.Table().Table())
	}

	expectedQualified := "`users`.`name`"
	if newCol.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, newCol.QualifiedName())
	}

	// Check that original column is unchanged
	if col.JSONFieldName() != "original_name" {
		t.Errorf("Original column JSON field name should remain 'original_name', got '%s'", col.JSONFieldName())
	}
}

func TestCol_Into_NilPointer(t *testing.T) {
	table := newMockTable("users")
	col := NewCol[string](table, "name", "name", nil)

	newCol := col.Into(nil, "new_name")

	if newCol.FieldPointer() != nil {
		t.Error("Expected nil field pointer")
	}

	if newCol.JSONFieldName() != "new_name" {
		t.Errorf("Expected JSON field name 'new_name', got '%s'", newCol.JSONFieldName())
	}
}

func TestCol_TypeSafety(t *testing.T) {
	table := newMockTable("users")

	// Test different types
	stringCol := NewCol[string](table, "name", "name", nil)
	intCol := NewCol[int](table, "age", "age", nil)
	floatCol := NewCol[float64](table, "score", "score", nil)
	boolCol := NewCol[bool](table, "active", "active", nil)

	// All should have the same basic interface behavior
	columns := []Column{stringCol, intCol, floatCol, boolCol}

	for i, col := range columns {
		if col.Table().Table() != "users" {
			t.Errorf("Column %d: Expected table 'users', got '%s'", i, col.Table().Table())
		}
	}

	// Check specific names
	expectedNames := []string{"name", "age", "score", "active"}
	for i, col := range columns {
		if col.Name() != expectedNames[i] {
			t.Errorf("Column %d: Expected name '%s', got '%s'", i, expectedNames[i], col.Name())
		}
	}
}

func TestCol_QualifiedNameFormatting(t *testing.T) {
	// Test that qualified names are properly formatted with backticks
	table := newMockTable("user_profiles")
	col := NewCol[string](table, "profile_image_url", "profile_image_url", nil)

	expected := "`user_profiles`.`profile_image_url`"
	if col.QualifiedName() != expected {
		t.Errorf("Expected qualified name '%s', got '%s'", expected, col.QualifiedName())
	}
}

func TestCol_InterfaceCompliance(t *testing.T) {
	table := newMockTable("test_table")
	col := NewCol[string](table, "test_column", "test_column", nil)

	// Verify that Col implements Column interface
	var _ Column = col

	// Test all interface methods
	if col.Table() == nil {
		t.Error("Table() should not return nil")
	}

	if col.Name() == "" {
		t.Error("Name() should not return empty string")
	}

	if col.QualifiedName() == "" {
		t.Error("QualifiedName() should not return empty string")
	}

	if col.JSONFieldName() == "" {
		t.Error("JSONFieldName() should not return empty string")
	}
	// FieldPointer() can be nil, so we don't test for non-nil
}

func TestCol_ImmutabilityOfInto(t *testing.T) {
	table := newMockTable("users")
	originalCol := NewCol[string](table, "name", "original_name", nil)

	// Create a new column using Into
	newCol := originalCol.Into(nil, "new_name")

	// Verify that the original column is unchanged
	if originalCol.JSONFieldName() != "original_name" {
		t.Errorf("Original column should remain unchanged, got JSON field name '%s'", originalCol.JSONFieldName())
	}

	// Verify that the new column has the new properties
	if newCol.JSONFieldName() != "new_name" {
		t.Errorf("New column should have new JSON field name 'new_name', got '%s'", newCol.JSONFieldName())
	}

	// Verify that both columns share the same basic properties
	if originalCol.Name() != newCol.Name() {
		t.Error("Both columns should have the same name")
	}

	if originalCol.Table().Table() != newCol.Table().Table() {
		t.Error("Both columns should belong to the same table")
	}

	if originalCol.QualifiedName() != newCol.QualifiedName() {
		t.Error("Both columns should have the same qualified name")
	}
}
