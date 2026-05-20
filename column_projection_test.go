package tsq

import (
	"reflect"
	"testing"
)

func TestCol_MapInto(t *testing.T) {
	table := newMockTable("users")
	col := newColForTable[newColOwner, string](table, "name", "original_name", toScanPointer(func(holder *newColOwner) *string {
		return &holder.Name
	}))
	newJSONFieldName := "display_name"
	newCol := MapInto[colProjection](col, func(holder *colProjection) *string {
		return &holder.DisplayName
	}, newJSONFieldName)
	var _ ResultColumn[colProjection, string] = newCol
	if newCol.JSONFieldName() != newJSONFieldName {
		t.Errorf("Expected JSON field name '%s', got '%s'", newJSONFieldName, newCol.JSONFieldName())
	}
	if newCol.scanPointer() == nil {
		t.Error("Expected non-nil field pointer")
	}
	if newCol.Name() != "name" {
		t.Errorf("Expected name 'name', got '%s'", newCol.Name())
	}
	if newCol.Table().Table() != "users" {
		t.Errorf("Expected table 'users', got '%s'", newCol.Table().Table())
	}
	expectedQualified := `"users"."name"`
	if newCol.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, newCol.QualifiedName())
	}
	if col.JSONFieldName() != "original_name" {
		t.Errorf("Original column JSON field name should remain 'original_name', got '%s'", col.JSONFieldName())
	}
}

func TestCol_MapIntoNilPointer(t *testing.T) {
	table := newMockTable("users")
	col := newColForTable[newColOwner, string](table, "name", "name", nil)
	next := MapInto[newColOwner](col, nil, "new_name")
	if _, err := validateColumnInput(next); err == nil {
		t.Fatal("expected nil field pointer to be captured as a build error")
	}
}

func TestCol_MapIntoDoesNotMutateOriginal(t *testing.T) {
	table := newMockTable("users")
	originalCol := newColForTable[newColOwner, string](table, "name", "original_name", toScanPointer(func(holder *newColOwner) *string {
		return &holder.Name
	}))
	newCol := MapInto[colProjection](originalCol, func(holder *colProjection) *string {
		return &holder.DisplayName
	}, "new_name")
	if originalCol.JSONFieldName() != "original_name" {
		t.Errorf("Original column should remain unchanged, got JSON field name '%s'", originalCol.JSONFieldName())
	}
	if newCol.JSONFieldName() != "new_name" {
		t.Errorf("New column should have new JSON field name 'new_name', got '%s'", newCol.JSONFieldName())
	}
	if originalCol.Name() != newCol.Name() {
		t.Error("Both columns should have the same name")
	}
	if originalCol.Table().Table() != newCol.Table().Table() {
		t.Error("Both columns should belong to the same table")
	}
	if originalCol.QualifiedName() != newCol.QualifiedName() {
		t.Error("Both columns should have the same qualified name")
	}
	if reflect.ValueOf(originalCol.scanPointer()).Pointer() == reflect.ValueOf(newCol.scanPointer()).Pointer() {
		t.Error("MapInto should not mutate the original field pointer")
	}
}
