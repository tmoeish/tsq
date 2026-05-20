package tsq

import "testing"

// TestAPIContract_PublicEntryPoints verifies that common public builder entry
// points remain usable together.
func TestAPIContract_PublicEntryPoints(t *testing.T) {
	tests := []struct {
		name string
		test func(*testing.T)
	}{
		{
			name: "Select_CreatesNewQueryBuilder",
			test: func(t *testing.T) {
				table := newMockTable("users")
				col := newColForTable[Table, string](table, "name", "name", nil)
				qb := Select(col)
				if qb == nil {
					t.Fatal("Select returned nil")
				}
			},
		},
		{
			name: "QueryBuilder_Where_SetsAndConditions",
			test: func(t *testing.T) {
				table := newMockTable("users")
				idCol := newColForTable[Table, int](table, "id", "id", nil)
				nameCol := newColForTable[Table, string](table, "name", "name", nil)

				qb := Select(idCol, nameCol).
					From(idCol.Table()).
					Where(idCol.GT(0), nameCol.NE(""))

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

				uid := newColForTable[Table, int](usersTable, "id", "id", nil)
				oid := newColForTable[Table, int](ordersTable, "id", "id", nil)
				oUserID := newColForTable[Table, int](ordersTable, "user_id", "user_id", nil)

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
				col := newColForTable[Table, int](table, "id", "id", nil)

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

// TestAPIContract_ColumnConditions verifies that common typed condition helpers
// produce usable predicates.
func TestAPIContract_ColumnConditions(t *testing.T) {
	table := newMockTable("users")
	col := newColForTable[Table, int](table, "id", "id", nil)

	conditions := []struct {
		name string
		cond Condition
	}{
		{"EQ", col.EQ(1)},
		{"NE", col.NE(1)},
		{"GT", col.GT(0)},
		{"GTE", col.GTE(0)},
		{"LT", col.LT(100)},
		{"LTE", col.LTE(100)},
		{"Between", col.Between(1, 100)},
		{"In", col.In(1, 2, 3)},
		{"IsNull", col.IsNull()},
		{"IsNotNull", col.IsNotNull()},
	}

	for _, tt := range conditions {
		if tt.cond == nil {
			t.Errorf("condition %s returned nil", tt.name)
			continue
		}
		if tt.cond.Clause() == "" {
			t.Errorf("condition %s has empty clause", tt.name)
		}
	}
}

// TestAPIContract_TableAndColumn verifies basic table and column metadata
// behavior used by public query construction.
func TestAPIContract_TableAndColumn(t *testing.T) {
	table := newMockTable("users")

	if table.Table() != "users" {
		t.Errorf("table name mismatch: got %s, want users", table.Table())
	}

	col := newColForTable[Table, int](table, "id", "id", nil)
	if col.Name() != "id" {
		t.Errorf("column name mismatch: got %s, want id", col.Name())
	}
	if col.Table() == nil {
		t.Fatal("column table is nil")
	}

	aliasedCol := col.As("u")
	if aliasedCol.Name() != "id" {
		t.Errorf("aliased column name changed: got %s, want id", aliasedCol.Name())
	}
	if got := aliasedCol.Table().Table(); got != "u" {
		t.Errorf("aliased column table mismatch: got %s, want u", got)
	}
	if got := aliasedCol.QualifiedName(); got != `"u"."id"` {
		t.Errorf("aliased column qualified name mismatch: got %s, want %s", got, `"u"."id"`)
	}

	otherTable := newMockTable("orders")
	reboundCol := col.WithTable(otherTable)
	if reboundCol.Table().Table() != "orders" {
		t.Errorf("rebind failed: got %s, want orders", reboundCol.Table().Table())
	}
}
