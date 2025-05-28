package tsq

import (
	"testing"
)

func TestOrder_Constants(t *testing.T) {
	tests := []struct {
		order    Order
		expected string
	}{
		{ASC, "ASC"},
		{DESC, "DESC"},
	}

	for _, tt := range tests {
		t.Run(string(tt.order), func(t *testing.T) {
			if string(tt.order) != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, string(tt.order))
			}
		})
	}
}

func TestOrderBy_Expr(t *testing.T) {
	table := newMockTable("users")
	col := newMockColumn(table, "name")

	tests := []struct {
		name     string
		orderBy  OrderBy
		expected string
	}{
		{
			name: "ASC order",
			orderBy: OrderBy{
				field: col,
				order: ASC,
			},
			expected: "`users`.`name` ASC",
		},
		{
			name: "DESC order",
			orderBy: OrderBy{
				field: col,
				order: DESC,
			},
			expected: "`users`.`name` DESC",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.orderBy.Expr()
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestOrderBy_Field(t *testing.T) {
	table := newMockTable("users")
	col := newMockColumn(table, "name")

	orderBy := OrderBy{
		field: col,
		order: ASC,
	}

	field := orderBy.Field()
	if field.Name() != "name" {
		t.Errorf("Expected field name 'name', got '%s'", field.Name())
	}

	if field.Table().Table() != "users" {
		t.Errorf("Expected table 'users', got '%s'", field.Table().Table())
	}
}

func TestOrderBy_Order(t *testing.T) {
	table := newMockTable("users")
	col := newMockColumn(table, "name")

	tests := []struct {
		name     string
		order    Order
		expected Order
	}{
		{"ASC order", ASC, ASC},
		{"DESC order", DESC, DESC},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			orderBy := OrderBy{
				field: col,
				order: tt.order,
			}

			result := orderBy.Order()
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestCol_Asc(t *testing.T) {
	table := newMockTable("users")
	col := NewCol[string](table, "name", "name", nil)

	orderBy := col.Asc()

	if orderBy.Field().Name() != "name" {
		t.Errorf("Expected field name 'name', got '%s'", orderBy.Field().Name())
	}

	if orderBy.Order() != ASC {
		t.Errorf("Expected ASC order, got %s", orderBy.Order())
	}

	expectedExpr := "`users`.`name` ASC"
	if orderBy.Expr() != expectedExpr {
		t.Errorf("Expected expression '%s', got '%s'", expectedExpr, orderBy.Expr())
	}
}

func TestCol_Desc(t *testing.T) {
	table := newMockTable("users")
	col := NewCol[string](table, "name", "name", nil)

	orderBy := col.Desc()

	if orderBy.Field().Name() != "name" {
		t.Errorf("Expected field name 'name', got '%s'", orderBy.Field().Name())
	}

	if orderBy.Order() != DESC {
		t.Errorf("Expected DESC order, got %s", orderBy.Order())
	}

	expectedExpr := "`users`.`name` DESC"
	if orderBy.Expr() != expectedExpr {
		t.Errorf("Expected expression '%s', got '%s'", expectedExpr, orderBy.Expr())
	}
}

func TestOrderByMultiple(t *testing.T) {
	table := newMockTable("users")
	col1 := NewCol[string](table, "name", "name", nil)
	col2 := NewCol[int](table, "age", "age", nil)

	orderBy1 := col1.Asc()
	orderBy2 := col2.Desc()

	expressions := OrderByMultiple(orderBy1, orderBy2)

	if len(expressions) != 2 {
		t.Errorf("Expected 2 expressions, got %d", len(expressions))
	}

	expected1 := "`users`.`name` ASC"
	if expressions[0] != expected1 {
		t.Errorf("Expected first expression '%s', got '%s'", expected1, expressions[0])
	}

	expected2 := "`users`.`age` DESC"
	if expressions[1] != expected2 {
		t.Errorf("Expected second expression '%s', got '%s'", expected2, expressions[1])
	}
}

func TestOrderByMultiple_Empty(t *testing.T) {
	expressions := OrderByMultiple()

	if len(expressions) != 0 {
		t.Errorf("Expected 0 expressions, got %d", len(expressions))
	}
}

func TestReverseOrder(t *testing.T) {
	tests := []struct {
		name     string
		input    Order
		expected Order
	}{
		{"ASC to DESC", ASC, DESC},
		{"DESC to ASC", DESC, ASC},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ReverseOrder(tt.input)
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestOrderBy_ComplexScenario(t *testing.T) {
	// Test a complex scenario with multiple tables and columns
	usersTable := newMockTable("users")
	ordersTable := newMockTable("orders")

	userNameCol := NewCol[string](usersTable, "name", "name", nil)
	userAgeCol := NewCol[int](usersTable, "age", "age", nil)
	orderDateCol := NewCol[string](ordersTable, "created_at", "created_at", nil)

	orderBys := []OrderBy{
		userNameCol.Asc(),
		userAgeCol.Desc(),
		orderDateCol.Asc(),
	}

	expressions := OrderByMultiple(orderBys...)

	expectedExpressions := []string{
		"`users`.`name` ASC",
		"`users`.`age` DESC",
		"`orders`.`created_at` ASC",
	}

	if len(expressions) != len(expectedExpressions) {
		t.Errorf("Expected %d expressions, got %d", len(expectedExpressions), len(expressions))
	}

	for i, expected := range expectedExpressions {
		if expressions[i] != expected {
			t.Errorf("Expected expression[%d] '%s', got '%s'", i, expected, expressions[i])
		}
	}
}

func TestOrderBy_FieldAndOrderConsistency(t *testing.T) {
	table := newMockTable("products")
	col := NewCol[float64](table, "price", "price", nil)

	// Test that Asc() creates consistent OrderBy
	ascOrderBy := col.Asc()
	if ascOrderBy.Field().Name() != col.Name() || ascOrderBy.Field().Table().Table() != col.Table().Table() {
		t.Error("Asc() should preserve the original column")
	}

	if ascOrderBy.Order() != ASC {
		t.Error("Asc() should set order to ASC")
	}

	// Test that Desc() creates consistent OrderBy
	descOrderBy := col.Desc()
	if descOrderBy.Field().Name() != col.Name() || descOrderBy.Field().Table().Table() != col.Table().Table() {
		t.Error("Desc() should preserve the original column")
	}

	if descOrderBy.Order() != DESC {
		t.Error("Desc() should set order to DESC")
	}

	// Test that both create different OrderBy instances
	if ascOrderBy.Order() == descOrderBy.Order() {
		t.Error("Asc() and Desc() should create different order directions")
	}
}
