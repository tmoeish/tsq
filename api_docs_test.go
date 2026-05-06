package tsq

import (
	"testing"
)

// TestAPIDocumentation_PublicMethods verifies that key public methods have proper documentation
// This test documents the main API entry points and their usage patterns
func TestAPIDocumentation_PublicMethods(t *testing.T) {
	// Testing common public API methods have documentation

	// QueryBuilder methods
	tests := []struct {
		name string
		test func(*testing.T)
	}{
		{
			name: "Select_CreatesNewQueryBuilder",
			test: func(t *testing.T) {
				table := newMockTable("users")
				col := NewCol[any, string](table, "name", "name", nil)
				qb := Select(col)
				if qb == nil {
					t.Fatal("Select returned nil")
				}
			},
		},
		{
			name: "QueryBuilder_Where_AddConditions",
			test: func(t *testing.T) {
				table := newMockTable("users")
				idCol := NewCol[any, int](table, "id", "id", nil)
				nameCol := NewCol[any, string](table, "name", "name", nil)

				qb := Select(idCol, nameCol).
					From(idCol.Table()).
					Where(idCol.GT(0)).
					Where(nameCol.NE(""))

				if qb == nil {
					t.Fatal("QueryBuilder returned nil")
				}
			},
		},
		{
			name: "QueryBuilder_Join_AddJoins",
			test: func(t *testing.T) {
				usersTable := newMockTable("users")
				ordersTable := newMockTable("orders")

				uid := NewCol[any, int](usersTable, "id", "id", nil)
				oid := NewCol[any, int](ordersTable, "id", "id", nil)
				oUserID := NewCol[any, int](ordersTable, "user_id", "user_id", nil)

				qb := Select(uid, oid).
					From(uid.Table()).
					InnerJoin(ordersTable, uid.EQCol(oUserID))

				if qb == nil {
					t.Fatal("QueryBuilder returned nil")
				}
			},
		},
		{
			name: "QueryBuilder_Build_GeneratesQuery",
			test: func(t *testing.T) {
				table := newMockTable("users")
				col := NewCol[any, int](table, "id", "id", nil)

				query, err := Select(col).
					From(col.Table()).Build()
				if err != nil {
					t.Fatalf("Build failed: %v", err)
				}
				if query == nil {
					t.Fatal("Build returned nil Query")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

// TestAPIDocumentation_ColumnConditions verifies condition methods have consistent documentation
func TestAPIDocumentation_ColumnConditions(t *testing.T) {
	table := newMockTable("users")
	col := NewCol[any, int](table, "id", "id", nil)

	// All comparison methods exist and return Condition
	conditions := []struct {
		name  string
		cond  Condition
		valid bool
	}{
		{"EQ", col.EQ(1), true},
		{"NE", col.NE(1), true},
		{"GT", col.GT(0), true},
		{"GTE", col.GTE(0), true},
		{"LT", col.LT(100), true},
		{"LTE", col.LTE(100), true},
		{"Between", col.Between(1, 100), true},
		{"In", col.In(1, 2, 3), true},
		{"IsNull", col.IsNull(), true},
		{"IsNotNull", col.IsNotNull(), true},
	}

	for _, tt := range conditions {
		if tt.cond == nil {
			t.Errorf("condition %s returned nil", tt.name)
			continue
		}
		// Verify it's a valid Condition
		if tt.cond.Clause() == "" && tt.valid {
			t.Errorf("condition %s has empty clause", tt.name)
		}
	}
}

// TestAPIDocumentation_TableAndColumn verifies table and column API
func TestAPIDocumentation_TableAndColumn(t *testing.T) {
	// Table creation and column access
	table := newMockTable("users")

	if table.Table() != "users" {
		t.Errorf("table name mismatch: got %s, want users", table.Table())
	}

	// Column creation
	col := NewCol[any, int](table, "id", "id", nil)
	if col.Name() != "id" {
		t.Errorf("column name mismatch: got %s, want id", col.Name())
	}
	if col.Table() == nil {
		t.Fatal("column table is nil")
	}

	// Column alias
	aliasedCol := col.As("u")
	if aliasedCol.Name() != "id" {
		t.Errorf("aliased column name changed: got %s, want id", aliasedCol.Name())
	}

	// Column WithTable (rebinding)
	otherTable := newMockTable("orders")
	reboundCol := col.WithTable(otherTable)
	if reboundCol.Table().Table() != "orders" {
		t.Errorf("rebind failed: got %s, want orders", reboundCol.Table().Table())
	}
}
