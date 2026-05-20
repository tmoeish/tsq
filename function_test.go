package tsq

import (
	"testing"
)

func TestCol_Expr(t *testing.T) {
	table := newMockTable("users")
	col := newColForTable[Table, string](table, "name", "name", nil)

	// Test custom function format
	result := col.Expr("UPPER(%s)")

	if result.Name() != "name" {
		t.Errorf("Expected name 'name', got '%s'", result.Name())
	}

	if result.Table().Table() != "users" {
		t.Errorf("Expected table 'users', got '%s'", result.Table().Table())
	}

	expectedQualified := `UPPER("users"."name")`
	if result.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, result.QualifiedName())
	}

	if result.JSONFieldName() != "name" {
		t.Errorf("Expected JSON field name 'name', got '%s'", result.JSONFieldName())
	}
}

func TestCol_ExprRejectsInvalidFormat(t *testing.T) {
	table := newMockTable("users")
	col := newColForTable[Table, string](table, "name", "name", nil)

	tests := []struct {
		name   string
		format string
	}{
		{name: "empty", format: ""},
		{name: "missing placeholder", format: "UPPER(name)"},
		{name: "multiple placeholders", format: "CONCAT(%s, %s)"},
		{name: "unsupported verb", format: "FORMAT(%d)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := validateColumnInput(col.Expr(tt.format)); err == nil {
				t.Fatal("expected Expr to return a build error for invalid format")
			}
		})
	}
}

func TestCol_ExprAllowsEscapedPercentLiterals(t *testing.T) {
	table := newMockTable("users")
	col := newColForTable[Table, string](table, "name", "name", nil)

	result := col.Expr("CONCAT(%s, '%%s')")
	if got := result.QualifiedName(); got != `CONCAT("users"."name", '%s')` {
		t.Fatalf("expected escaped percent literal to be preserved, got %s", got)
	}
}

func TestCol_Count(t *testing.T) {
	table := newMockTable("users")
	col := newColForTable[Table, int](table, "id", "id", nil)

	result := col.Count()

	expectedQualified := `COUNT("users"."id")`
	if result.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, result.QualifiedName())
	}
}

func TestCol_Sum(t *testing.T) {
	table := newMockTable("orders")
	col := newColForTable[Table, float64](table, "amount", "amount", nil)

	result := col.Sum()

	expectedQualified := `SUM("orders"."amount")`
	if result.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, result.QualifiedName())
	}
}

func TestCol_Avg(t *testing.T) {
	table := newMockTable("products")
	col := newColForTable[Table, float64](table, "price", "price", nil)

	result := col.Avg()

	expectedQualified := `AVG("products"."price")`
	if result.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, result.QualifiedName())
	}
}

func TestCol_Max(t *testing.T) {
	table := newMockTable("users")
	col := newColForTable[Table, int](table, "age", "age", nil)

	result := col.Max()

	expectedQualified := `MAX("users"."age")`
	if result.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, result.QualifiedName())
	}
}

func TestCol_Min(t *testing.T) {
	table := newMockTable("users")
	col := newColForTable[Table, int](table, "age", "age", nil)

	result := col.Min()

	expectedQualified := `MIN("users"."age")`
	if result.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, result.QualifiedName())
	}
}

func TestCol_Distinct(t *testing.T) {
	table := newMockTable("users")
	col := newColForTable[Table, string](table, "department", "department", nil)

	result := col.Distinct()

	expectedQualified := `DISTINCT("users"."department")`
	if result.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, result.QualifiedName())
	}
}

func TestCol_Upper(t *testing.T) {
	table := newMockTable("users")
	col := newColForTable[Table, string](table, "name", "name", nil)

	result := col.Upper()

	expectedQualified := `UPPER("users"."name")`
	if result.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, result.QualifiedName())
	}
}

func TestCol_Lower(t *testing.T) {
	table := newMockTable("users")
	col := newColForTable[Table, string](table, "name", "name", nil)

	result := col.Lower()

	expectedQualified := `LOWER("users"."name")`
	if result.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, result.QualifiedName())
	}
}

func TestCol_Substring(t *testing.T) {
	table := newMockTable("users")
	col := newColForTable[Table, string](table, "description", "description", nil)

	result := col.Substring(1, 10)

	expectedQualified := `SUBSTRING("users"."description", 1, 10)`
	if result.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, result.QualifiedName())
	}
}

func TestCol_Length(t *testing.T) {
	table := newMockTable("users")
	col := newColForTable[Table, string](table, "name", "name", nil)

	result := col.Length()

	expectedQualified := `LENGTH("users"."name")`
	if result.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, result.QualifiedName())
	}
}

func TestCol_Trim(t *testing.T) {
	table := newMockTable("users")
	col := newColForTable[Table, string](table, "name", "name", nil)

	result := col.Trim()

	expectedQualified := `TRIM("users"."name")`
	if result.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, result.QualifiedName())
	}
}

func TestCol_Concat(t *testing.T) {
	table := newMockTable("users")
	col := newColForTable[Table, string](table, "first_name", "first_name", nil)

	if _, err := validateColumnInput(col.Concat(" Smith")); err == nil {
		t.Fatal("expected Concat to return a build error for non-portable SQL")
	}
}

func TestCaseBuilder_End(t *testing.T) {
	users := newMockTable("users")
	orgID := newColForTable[Table, int](users, "org_id", "org_id", nil)

	result := Case[string]().
		When(orgID.EQ(1), "internal").
		Else("external").
		End()

	expectedQualified := `CASE WHEN "users"."org_id" = ? THEN ? ELSE ? END`
	if result.QualifiedName() != expectedQualified {
		t.Fatalf("expected qualified name %q, got %q", expectedQualified, result.QualifiedName())
	}

	args := result.(interface{ expressionArgs() []any }).expressionArgs()
	if len(args) != 3 || args[0] != 1 || args[1] != "internal" || args[2] != "external" {
		t.Fatalf("expected bound args [1 internal external], got %#v", args)
	}
}

func TestCaseBuilder_RequiresWhenBranch(t *testing.T) {
	if _, err := validateColumnInput(Case[string]().End()); err == nil {
		t.Fatal("expected empty CASE builder to fail")
	}
}

func TestCol_ExprfTracksReferencedTables(t *testing.T) {
	users := newMockTable("users")
	orgs := newMockTable("orgs")
	userName := newColForTable[Table, string](users, "name", "name", nil)
	orgName := newColForTable[Table, string](orgs, "name", "name", nil)

	result := userName.Exprf("COALESCE(%s, %s)", orgName)

	refs := result.(interface{ referencedTables() map[string]Table }).referencedTables()
	if len(refs) != 2 || refs["orgs"] == nil || refs["users"] == nil {
		t.Fatalf("expected Exprf to track referenced tables, got %#v", refs)
	}
}

func TestCol_Now(t *testing.T) {
	table := newMockTable("users")
	col := newColForTable[Table, string](table, "created_at", "created_at", nil)

	result := col.Now()

	actual := result.QualifiedName()
	if actual != "CURRENT_TIMESTAMP" {
		t.Errorf("Expected qualified name to be 'CURRENT_TIMESTAMP', got '%s'", actual)
	}
}

func TestCol_Date(t *testing.T) {
	table := newMockTable("orders")
	col := newColForTable[Table, string](table, "created_at", "created_at", nil)

	result := col.Date()

	expectedQualified := `DATE("orders"."created_at")`
	if result.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, result.QualifiedName())
	}
}

func TestCol_Year(t *testing.T) {
	table := newMockTable("orders")
	col := newColForTable[Table, string](table, "created_at", "created_at", nil)

	result := col.Year()

	expectedQualified := `SUBSTR(DATE("orders"."created_at"), 1, 4)`
	if result.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, result.QualifiedName())
	}
}

func TestCol_Month(t *testing.T) {
	table := newMockTable("orders")
	col := newColForTable[Table, string](table, "created_at", "created_at", nil)

	result := col.Month()

	expectedQualified := `SUBSTR(DATE("orders"."created_at"), 6, 2)`
	if result.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, result.QualifiedName())
	}
}

func TestCol_Day(t *testing.T) {
	table := newMockTable("orders")
	col := newColForTable[Table, string](table, "created_at", "created_at", nil)

	result := col.Day()

	expectedQualified := `SUBSTR(DATE("orders"."created_at"), 9, 2)`
	if result.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, result.QualifiedName())
	}
}

func TestCol_Round(t *testing.T) {
	table := newMockTable("products")
	col := newColForTable[Table, float64](table, "price", "price", nil)

	result := col.Round(2)

	expectedQualified := `ROUND("products"."price", 2)`
	if result.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, result.QualifiedName())
	}
}

func TestCol_RoundPreservesRequestedPrecision(t *testing.T) {
	table := newMockTable("products")
	col := newColForTable[Table, float64](table, "price", "price", nil)

	largePrecision := col.Round(42)
	if got := largePrecision.QualifiedName(); got != `ROUND("products"."price", 42)` {
		t.Fatalf("expected large precision to be preserved, got %s", got)
	}
}

func TestCol_RoundRejectsNegativePrecision(t *testing.T) {
	table := newMockTable("products")
	col := newColForTable[Table, float64](table, "price", "price", nil)

	if _, err := validateColumnInput(col.Round(-2)); err == nil {
		t.Fatal("expected Round to return a build error for negative precision")
	}
}

func TestCol_Ceil(t *testing.T) {
	table := newMockTable("products")
	col := newColForTable[Table, float64](table, "price", "price", nil)

	result := col.Ceil()

	expectedQualified := `CEIL("products"."price")`
	if result.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, result.QualifiedName())
	}
}

func TestCol_Floor(t *testing.T) {
	table := newMockTable("products")
	col := newColForTable[Table, float64](table, "price", "price", nil)

	result := col.Floor()

	expectedQualified := `FLOOR("products"."price")`
	if result.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, result.QualifiedName())
	}
}

func TestCol_Abs(t *testing.T) {
	table := newMockTable("transactions")
	col := newColForTable[Table, float64](table, "amount", "amount", nil)

	result := col.Abs()

	expectedQualified := `ABS("transactions"."amount")`
	if result.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, result.QualifiedName())
	}
}

func TestCol_Coalesce(t *testing.T) {
	table := newMockTable("users")
	col := newColForTable[Table, string](table, "nickname", "nickname", nil)

	result := col.Coalesce("Anonymous")

	expectedQualified := `COALESCE("users"."nickname", ?)`
	if result.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, result.QualifiedName())
	}

	args := result.(interface{ expressionArgs() []any }).expressionArgs()
	if len(args) != 1 || args[0] != "Anonymous" {
		t.Fatalf("expected bound args [Anonymous], got %#v", args)
	}
}

func TestCol_NullIf(t *testing.T) {
	table := newMockTable("users")
	col := newColForTable[Table, string](table, "status", "status", nil)

	result := col.NullIf("inactive")

	expectedQualified := `NULLIF("users"."status", ?)`
	if result.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, result.QualifiedName())
	}

	args := result.(interface{ expressionArgs() []any }).expressionArgs()
	if len(args) != 1 || args[0] != "inactive" {
		t.Fatalf("expected bound args [inactive], got %#v", args)
	}
}

func TestCol_StringFunctionHelpersBindBackslashesSafely(t *testing.T) {
	table := newMockTable("users")
	col := newColForTable[Table, string](table, "nickname", "nickname", nil)

	for name, column := range map[string]SQLColumn{
		"Coalesce": col.Coalesce(`path\file`),
		"NullIf":   col.NullIf(`path\file`),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := validateColumnInput(column); err != nil {
				t.Fatalf("expected helper to keep backslash strings parameterized, got %v", err)
			}

			exprArgs := column.(interface{ expressionArgs() []any }).expressionArgs()
			if len(exprArgs) != 1 || exprArgs[0] != `path\file` {
				t.Fatalf("expected bound backslash arg, got %#v", exprArgs)
			}
		})
	}
}

func TestCol_ChainedFunctions(t *testing.T) {
	table := newMockTable("users")
	col := newColForTable[Table, string](table, "name", "name", nil)

	// Test chaining multiple functions
	result := col.Upper().Trim()

	expectedQualified := `TRIM(UPPER("users"."name"))`
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
	col := newColForTable[Table, float64](table, "price", "price", nil)

	// Test complex function chaining
	result := col.Round(2).Coalesce("0.00")

	expectedQualified := `COALESCE(ROUND("products"."price", 2), ?)`
	if result.QualifiedName() != expectedQualified {
		t.Errorf("Expected qualified name '%s', got '%s'", expectedQualified, result.QualifiedName())
	}

	args := result.(interface{ expressionArgs() []any }).expressionArgs()
	if len(args) != 1 || args[0] != "0.00" {
		t.Fatalf("expected bound args [0.00], got %#v", args)
	}
}

func TestCol_FunctionPreservesMetadata(t *testing.T) {
	table := newMockTable("users")
	col := newColForTable[Table, string](table, "email", "user_email", func(holder any) any { return nil })

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
	if result.scanPointer() == nil {
		t.Error("Expected field pointer to be preserved")
	}
}
