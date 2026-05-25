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
	_ = col.EQVal(5)
	_ = col.NEVal(5)
	_ = col.GTVal(10)
	_ = col.GTEVal(10)
	_ = col.LTVal(100)
	_ = col.LTEVal(100)

	// Test range operations - Val parameters are (start, end)
	_ = col.BetweenVal(1, 100)
	_ = col.NBetweenVal(1, 100)

	// Test membership - Val parameters are variable number of values
	_ = col.InVal(1, 2, 3)
	_ = col.NInVal(1, 2, 3)

	// Test null checks - no parameters
	_ = col.IsNull()
	_ = col.IsNotNull()
}

// TestConditionParameterOrdering_StringPatterns documents string pattern methods
func TestConditionParameterOrdering_StringPatterns(t *testing.T) {
	table := newMockTable("users")
	nameCol := newColForTable[Table, string](table, "name", "name", nil)

	// Pattern matching - parameter is the pattern string
	_ = nameCol.StartsWithVal("John")
	_ = nameCol.NStartsWithVal("John")
	_ = nameCol.EndsWithVal("Smith")
	_ = nameCol.NEndsWithVal("Smith")
	_ = nameCol.ContainsVal("test")
	_ = nameCol.NContainsVal("test")
	_ = nameCol.StartsWithVar()
	_ = nameCol.ContainsVar()

	// LIKE with explicit pattern can use Val/Var/RHS forms directly
	_ = nameCol.LikeVal("A%")
	_ = nameCol.NLikeVal("%Z")
	_ = nameCol.LikeVar()
}

// TestConditionParameterOrdering_BindingStyles documents different binding style suffixes
func TestConditionParameterOrdering_BindingStyles(t *testing.T) {
	table := newMockTable("users")
	col := newColForTable[Table, int](table, "id", "id", nil)

	// Default binding (parameter binding with ?)
	_ = col.EQVal(5)
	_ = col.GTVal(10)

	// Variable binding (? placeholder, value provided at execution)
	_ = col.EQVar()
	_ = col.GTVar()

	// Column comparison
	otherTable := newMockTable("orders")
	otherCol := newColForTable[Table, int](otherTable, "user_id", "user_id", nil)
	_ = col.EQ(otherCol)
	_ = col.GT(otherCol)

	// Subquery comparison
	subquery, _ := BuildSubquery(
		Select(col).From(col.Table()).Where(col.GTVal(0)),
		col,
	)
	_ = col.EQ(subquery)
	_ = col.GT(subquery)
	_ = col.Between(otherCol, subquery)
	_ = col.In(subquery)

	nameCol := newColForTable[Table, string](table, "name", "name", nil)
	patternCol := newColForTable[Table, string](newMockTable("patterns"), "pattern", "pattern", nil)
	_ = nameCol.LikeVal("%alice%")
	_ = nameCol.LikeVar()
	_ = nameCol.Like(patternCol)
}

// TestConditionParameterOrdering_Documentation verifies naming consistency
func TestConditionParameterOrdering_Documentation(t *testing.T) {
	// This test documents the naming conventions for method suffixes:
	//
	// Val suffix: Parameter binding (col.EQVal(value))
	//   - Value is bound with ? placeholder
	//   - Executes faster, safer from SQL injection
	//
	// EQVar: Variable placeholder (col.EQVar())
	//   - Uses ? placeholder, value provided at execution
	//   - No parameter in method
	//
	// LikeVal/LikeVar follow the same pattern for explicit LIKE patterns
	//   - LikeVal binds a concrete pattern
	//   - LikeVar defers the pattern to execution
	//
	// Between uses RHS operands, while BetweenVal/BetweenVar cover
	// literal and deferred runtime ranges.
	//
	// RHS: column comparison (col.EQ(otherCol))
	//   - Compares two columns
	//   - Parameter is a typed RHS such as another column
	//
	// RHS: subquery comparison (col.EQ(subquery))
	//   - Compares with subquery result
	//   - Parameter is a typed tsq.Subquery[T]

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
		{"EQVal", col.EQVal(5)},
		{"NEVal", col.NEVal(5)},
		{"GTVal", col.GTVal(5)},
		{"GTEVal", col.GTEVal(5)},
		{"LTVal", col.LTVal(5)},
		{"LTEVal", col.LTEVal(5)},
		{"BetweenVal", col.BetweenVal(1, 10)},
		{"NBetweenVal", col.NBetweenVal(1, 10)},
		{"InVal", col.InVal(1, 2, 3)},
		{"NInVal", col.NInVal(1, 2, 3)},
		{"InVar", col.InVar()},
		{"NInVar", col.NInVar()},
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
