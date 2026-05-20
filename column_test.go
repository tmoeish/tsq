package tsq

import "testing"

type newColOwner struct{ Name string }
type colProjection struct{ DisplayName string }

func (colProjection) TSQOwner() {
}
func (newColOwner) TSQOwner() {
}
func (newColOwner) Table() string {
	return "users"
}
func (newColOwner) Cols() []SQLColumn {
	return nil
}
func (newColOwner) SearchColumns() []SearchColumn {
	return nil
}
func (newColOwner) PrimaryKeys() []string {
	return nil
}
func (newColOwner) AutoIncrement() bool {
	return false
}
func (newColOwner) VersionColumn() string {
	return ""
}
func TestNewCol(t *testing.T) {
	col := NewCol[newColOwner, string]("name", "user_name", nil)
	var _ Column[newColOwner, string] = col
	var _ TypedColumn[newColOwner, string] = col
	var _ SQLColumn = col
	if col.Table().Table() != "users" {
		t.Errorf("Expected table 'users', got '%s'", col.Table().Table())
	}
	expectedQualified := `"users"."name"`
	if col.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, col.QualifiedName())
	}
	if col.FieldPointer() != nil {
		t.Error("Expected nil field pointer")
	}
}
func TestNewColWithExplicitTableInternal(t *testing.T) {
	table := newMockTable("users")
	col := newColForTable[newColOwner, string](table, "name", "user_name", toScanPointer(func(holder *newColOwner) *string {
		return &holder.Name
	}))
	if col.Name() != "name" {
		t.Errorf("Expected name 'name', got '%s'", col.Name())
	}
	if col.Table().Table() != "users" {
		t.Errorf("Expected table 'users', got '%s'", col.Table().Table())
	}
	expectedQualified := `"users"."name"`
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
func TestNewCol_RejectsNilTable(t *testing.T) {
	col := newColForTable[Table, string](nil, "name", "name", nil)
	if _, err := validateColumnInput(col); err == nil {
		t.Fatal("expected nil table to be captured as a build error")
	}
}
func TestCol_Table(t *testing.T) {
	table := newMockTable("products")
	col := newColForTable[Table, float64](table, "price", "price", nil)
	resultTable := col.Table()
	if resultTable.Table() != "products" {
		t.Errorf("Expected table 'products', got '%s'", resultTable.Table())
	}
}
func TestCol_Name(t *testing.T) {
	table := newMockTable("orders")
	col := newColForTable[Table, int](table, "id", "id", nil)
	if col.Name() != "id" {
		t.Errorf("Expected name 'id', got '%s'", col.Name())
	}
}
func TestCol_QualifiedName(t *testing.T) {
	tests := []struct {
		tableName    string
		columnName   string
		expectedName string
	}{{"users", "id", `"users"."id"`}, {"orders", "user_id", `"orders"."user_id"`}, {"products", "name", `"products"."name"`}, {"user_profiles", "avatar_url", `"user_profiles"."avatar_url"`}}
	for _, tt := range tests {
		t.Run(tt.tableName+"_"+tt.columnName, func(t *testing.T) {
			table := newMockTable(tt.tableName)
			col := newColForTable[Table, string](table, tt.columnName, tt.columnName, nil)
			if col.QualifiedName() != tt.expectedName {
				t.Errorf("Expected qualified name '%s', got '%s'", tt.expectedName, col.QualifiedName())
			}
		})
	}
}
func TestCol_FieldPointer(t *testing.T) {
	table := newMockTable("users")
	col1 := newColForTable[Table, string](table, "name", "name", nil)
	if col1.FieldPointer() != nil {
		t.Error("Expected nil field pointer")
	}
	col2 := newColForTable[newColOwner, string](table, "name", "name", toScanPointer(func(holder *newColOwner) *string {
		return &holder.Name
	}))
	if col2.FieldPointer() == nil {
		t.Error("Expected non-nil field pointer")
	}
}
func TestCol_JSONFieldName(t *testing.T) {
	table := newMockTable("users")
	tests := []struct {
		name          string
		jsonFieldName string
	}{{"simple", "name"}, {"snake_case", "user_name"}, {"camelCase", "userName"}, {"with_prefix", "user_full_name"}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col := newColForTable[Table, string](table, "name", tt.jsonFieldName, nil)
			if col.JSONFieldName() != tt.jsonFieldName {
				t.Errorf("Expected JSON field name '%s', got '%s'", tt.jsonFieldName, col.JSONFieldName())
			}
		})
	}
}
func TestCol_TypeSafety(t *testing.T) {
	table := newMockTable("users")
	stringCol := newColForTable[Table, string](table, "name", "name", nil)
	intCol := newColForTable[Table, int](table, "age", "age", nil)
	floatCol := newColForTable[Table, float64](table, "score", "score", nil)
	boolCol := newColForTable[Table, bool](table, "active", "active", nil)
	columns := []SQLColumn{stringCol, intCol, floatCol, boolCol}
	for i, col := range columns {
		if col.Table().Table() != "users" {
			t.Errorf("column %d: expected table 'users', got '%s'", i, col.Table().Table())
		}
	}
	expectedNames := []string{"name", "age", "score", "active"}
	for i, col := range columns {
		if col.Name() != expectedNames[i] {
			t.Errorf("column %d: expected name '%s', got '%s'", i, expectedNames[i], col.Name())
		}
	}
}
func TestCol_QualifiedNameFormatting(t *testing.T) {
	table := newMockTable("user_profiles")
	col := newColForTable[Table, string](table, "profile_image_url", "profile_image_url", nil)
	expected := `"user_profiles"."profile_image_url"`
	if col.QualifiedName() != expected {
		t.Errorf("Expected qualified name '%s', got '%s'", expected, col.QualifiedName())
	}
}
func TestCol_InterfaceCompliance(t *testing.T) {
	table := newMockTable("test_table")
	col := newColForTable[Table, string](table, "test_column", "test_column", nil)
	var _ SQLColumn = col
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
}
func TestCol_AsRebindsQualifiedName(t *testing.T) {
	table := newMockTable("users")
	col := newColForTable[Table, string](table, "name", "name", nil)
	aliased := col.As("manager")
	if got := aliased.Table().Table(); got != "manager" {
		t.Fatalf("expected aliased table name manager, got %q", got)
	}
	if got := aliased.QualifiedName(); got != `"manager"."name"` {
		t.Fatalf("expected aliased qualified name, got %q", got)
	}
}
func TestCol_AsRejectsTransformedColumn(t *testing.T) {
	table := newMockTable("users")
	col := newColForTable[Table, string](table, "name", "name", nil).Upper()
	aliased := col.As("manager")
	if _, err := validateColumnInput(aliased); err == nil {
		t.Fatal("expected transformed column rebinding to return a build error")
	}
}
