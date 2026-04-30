package tsq

import (
	"errors"
	"testing"
)

// TestJoinValidation_DifferentTablesSucceeds ensures joins work with different tables
func TestJoinValidation_DifferentTablesSucceeds(t *testing.T) {
	table1 := newMockTable("users")
	table2 := newMockTable("orders")

	col1 := NewCol[int](table1, "id", "id", nil)
	col2 := NewCol[int](table2, "user_id", "user_id", nil)

	err := validateJoinColumns(col1, col2)
	if err != nil {
		t.Errorf("expected no error for different tables, got: %v", err)
	}
}

// TestJoinValidation_SameTablesRejectsJoin ensures joins reject columns from same table
func TestJoinValidation_SameTablesRejectsJoin(t *testing.T) {
	table := newMockTable("users")

	col1 := NewCol[int](table, "id", "id", nil)
	col2 := NewCol[int](table, "parent_id", "parent_id", nil)

	err := validateJoinColumns(col1, col2)
	if err == nil {
		t.Fatal("expected error for same table columns, got nil")
	}

	var rebindErr *ErrIncompatibleTableRebind
	if !errors.As(err, &rebindErr) {
		t.Errorf("expected ErrIncompatibleTableRebind, got %T: %v", err, err)
	}
}

// TestJoinValidation_ReboundColumnsDifferentTablesSucceed tests rebounding col1 to a different table
func TestJoinValidation_ReboundColumnsDifferentTablesSucceed(t *testing.T) {
	usersTable := newMockTable("users")
	ordersTable := newMockTable("orders")

	// Create column from users table
	col1 := NewCol[int](usersTable, "id", "id", nil)

	// Rebind to orders table
	col1Rebound := col1.WithTable(ordersTable)

	// Create separate column from orders table for joining
	col2 := NewCol[int](ordersTable, "user_id", "user_id", nil)

	// Join should succeed because col1Rebound and col2 now refer to same table
	// (which is allowed for self-joins), but without aliasing it will be caught
	err := validateJoinColumns(col1Rebound, col2)
	if err == nil {
		// Both columns now refer to "orders" table, so this should fail
		t.Fatal("expected error because both columns now refer to same table")
	}
}

// TestJoinValidation_NilColumnsRejectJoin ensures nil columns are caught
func TestJoinValidation_NilColumnsRejectJoin(t *testing.T) {
	table := newMockTable("users")
	col := NewCol[int](table, "id", "id", nil)

	var nilCol Column

	err := validateJoinColumns(nilCol, col)
	if err == nil {
		t.Fatal("expected error for nil column, got nil")
	}
}

// TestJoinValidation_AliasedTablesSucceed ensures aliases don't prevent joins
func TestJoinValidation_AliasedTablesSucceed(t *testing.T) {
	usersTable := newMockTable("users")
	ordersTable := newMockTable("orders")

	col1 := NewCol[int](usersTable, "id", "id", nil)
	col2 := NewCol[int](ordersTable, "user_id", "user_id", nil)

	// Alias both columns
	col1Alias := col1.As("u")
	col2Alias := col2.As("o")

	err := validateJoinColumns(col1Alias, col2Alias)
	if err != nil {
		t.Errorf("expected no error for aliased tables, got: %v", err)
	}
}

// TestJoinValidation_SelfJoinWithAliasSucceeds ensures self-joins work with aliases
func TestJoinValidation_SelfJoinWithAliasSucceeds(t *testing.T) {
	table := newMockTable("users")

	col := NewCol[int](table, "id", "id", nil)

	// Create a self-join with different aliases
	col1 := col.As("u1")
	col2 := col.As("u2")

	err := validateJoinColumns(col1, col2)
	if err != nil {
		t.Errorf("expected no error for self-join with aliases, got: %v", err)
	}
}

// TestJoinValidation_ErrorMessageClarity ensures error messages are clear
func TestJoinValidation_ErrorMessageClarity(t *testing.T) {
	table := newMockTable("users")

	col1 := NewCol[int](table, "id", "id", nil)
	col2 := NewCol[int](table, "parent_id", "parent_id", nil)

	err := validateJoinColumns(col1, col2)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	msg := err.Error()
	if msg == "" {
		t.Fatal("error message is empty")
	}

	// Message should mention both table names
	found := false
	for i := 0; i <= len(msg)-len("users"); i++ {
		if msg[i:i+len("users")] == "users" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("error message should mention table name, got: %s", msg)
	}
}
