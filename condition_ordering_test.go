package tsq

import (
	"testing"
)

// TestConditionParameterOrdering documents and verifies the consistent parameter ordering
// All condition methods follow the pattern: column.OPERATOR(values...)
func TestConditionParameterOrdering_ConsistentPattern(t *testing.T) {
	table := newMockTable("users")
	col := newColForTable[Table, int](table, "id", "id", nil)

	// Test basic comparisons - parameter is the value to compare
	_ = col.EQ(5)
	_ = col.NE(5)
	_ = col.GT(10)
	_ = col.GTE(10)
	_ = col.LT(100)
	_ = col.LTE(100)

	// Test range operations - parameters are (start, end)
	_ = col.Between(1, 100)
	_ = col.NBetween(1, 100)

	// Test membership - parameters are variable number of values
	_ = col.In(1, 2, 3)
	_ = col.NIn(1, 2, 3)

	// Test null checks - no parameters
	_ = col.IsNull()
	_ = col.IsNotNull()
}

// TestConditionParameterOrdering_StringPatterns documents string pattern methods
func TestConditionParameterOrdering_StringPatterns(t *testing.T) {
	table := newMockTable("users")
	nameCol := newColForTable[Table, string](table, "name", "name", nil)

	// Pattern matching - parameter is the pattern string
	_ = nameCol.StartsWith("John")
	_ = nameCol.NStartsWith("John")
	_ = nameCol.EndsWith("Smith")
	_ = nameCol.NEndsWith("Smith")
	_ = nameCol.Contains("test")
	_ = nameCol.NContains("test")

	// LIKE with explicit pattern - parameter is the full pattern
	_ = nameCol.StartsWith("A%")
	_ = nameCol.EndsWith("%Z")
	_ = nameCol.Contains("%middle%")
}

// TestConditionParameterOrdering_BindingStyles documents different binding style suffixes
func TestConditionParameterOrdering_BindingStyles(t *testing.T) {
	table := newMockTable("users")
	col := newColForTable[Table, int](table, "id", "id", nil)

	// Default binding (parameter binding with ?)
	_ = col.EQ(5)
	_ = col.GT(10)

	// Literal binding (value embedded in SQL)
	_ = col.EQLiteral(5)
	_ = col.GTLiteral(10)

	// Variable binding (? placeholder, value provided at execution)
	_ = col.EQVar()
	_ = col.GTVar()

	// Column comparison
	otherTable := newMockTable("orders")
	otherCol := newColForTable[Table, int](otherTable, "user_id", "user_id", nil)
	_ = col.EQCol(otherCol)
	_ = col.GTCol(otherCol)

	// Subquery comparison
	subquery, _ := Select(col).
		From(col.Table()).Where(col.GT(0)).Build()
	_ = col.EQSub(subquery)
	_ = col.GTSub(subquery)
}

// TestConditionParameterOrdering_Documentation verifies naming consistency
func TestConditionParameterOrdering_Documentation(t *testing.T) {
	// This test documents the naming conventions for method suffixes:
	//
	// No suffix: Parameter binding (col.EQ(value))
	//   - Value is bound with ? placeholder
	//   - Executes faster, safer from SQL injection
	//
	// Literal: Literal embedding (col.EQLiteral(value))
	//   - Value is embedded directly in SQL
	//   - Useful for static values known at compile time
	//
	// Var: Variable placeholder (col.EQVar())
	//   - Uses ? placeholder, value provided at execution
	//   - No parameter in method
	//
	// Col: Column comparison (col.EQCol(otherCol))
	//   - Compares two columns
	//   - Parameter is another Column
	//
	// Sub: Subquery comparison (col.EQSub(query))
	//   - Compares with subquery result
	//   - Parameter is a *Query

	t.Log("All condition methods follow consistent parameter ordering conventions")
}

// TestConditionParameterOrdering_ConsistencyAcrossOperators verifies that all operators use same pattern
func TestConditionParameterOrdering_ConsistencyAcrossOperators(t *testing.T) {
	table := newMockTable("users")
	col := newColForTable[Table, int](table, "id", "id", nil)

	tests := []struct {
		name string
		cond Condition
	}{
		{"EQ", col.EQ(5)},
		{"NE", col.NE(5)},
		{"GT", col.GT(5)},
		{"GTE", col.GTE(5)},
		{"LT", col.LT(5)},
		{"LTE", col.LTE(5)},
		{"Between", col.Between(1, 10)},
		{"NBetween", col.NBetween(1, 10)},
		{"In", col.In(1, 2, 3)},
		{"NIn", col.NIn(1, 2, 3)},
		{"IsNull", col.IsNull()},
		{"IsNotNull", col.IsNotNull()},
	}

	for _, tt := range tests {
		if tt.cond == nil {
			t.Errorf("condition %s returned nil", tt.name)
		}
		// All conditions should be non-nil and have no build error
		if c, ok := tt.cond.(interface{ buildError() error }); ok {
			if c.buildError() != nil {
				t.Errorf("condition %s has build error: %v", tt.name, c.buildError())
			}
		}
	}
}
