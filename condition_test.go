package tsq

import (
	"database/sql"
	"database/sql/driver"
	"reflect"
	"strings"
	"testing"
	"time"
)

type customValuer struct {
	value string
	valid bool
}

func (c customValuer) Value() (driver.Value, error) {
	if !c.valid {
		return nil, nil
	}

	return c.value, nil
}

type pointerValuer struct {
	value string
}

func (p *pointerValuer) Value() (driver.Value, error) {
	if p == nil {
		return nil, nil
	}

	return p.value, nil
}

func stringPtr(s string) *string {
	return &s
}

func intPtr(i int) *int {
	return &i
}

func TestBindUsesPlaceholdersForDatabaseCompatibleValues(t *testing.T) {
	now := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	tests := []struct {
		name  string
		input any
	}{
		{name: "string", input: `path\file`},
		{name: "bytes", input: []byte("hello")},
		{name: "time", input: now},
		{name: "valuer", input: customValuer{value: "custom", valid: true}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr := Bind(tt.input)
			if err := expressionBuildError(expr); err != nil {
				t.Fatalf("unexpected bind error: %v", err)
			}

			if got := expr.Expr(); got != "?" {
				t.Fatalf("expected bind expression to use placeholder, got %q", got)
			}

			args := expr.Args()
			if len(args) != 1 {
				t.Fatalf("expected one bound arg, got %#v", args)
			}

			if !reflect.DeepEqual(args[0], tt.input) {
				t.Fatalf("expected bound arg %#v, got %#v", tt.input, args[0])
			}
		})
	}
}

func TestBindRejectsNullAndUnsupportedPredicateValues(t *testing.T) {
	var nilPointerValuer *pointerValuer

	tests := []struct {
		name  string
		input any
	}{
		{name: "nil", input: nil},
		{name: "null string", input: sql.NullString{}},
		{name: "invalid valuer", input: customValuer{value: "custom", valid: false}},
		{name: "typed nil valuer", input: nilPointerValuer},
		{name: "slice", input: []int{1, 2, 3}},
		{name: "map", input: map[string]int{"id": 1}},
		{name: "struct", input: struct{ ID int }{ID: 1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := expressionBuildError(Bind(tt.input)); err == nil {
				t.Fatal("expected bind to reject value")
			}
		})
	}
}

func TestArgumentToExpressionDefaultsToBind(t *testing.T) {
	expr := argumentToExpression(`it's a test`)
	if err := expressionBuildError(expr); err != nil {
		t.Fatalf("unexpected expression error: %v", err)
	}

	if got := expr.Expr(); got != "?" {
		t.Fatalf("expected default expression conversion to bind values, got %q", got)
	}

	args := expr.Args()
	if len(args) != 1 || args[0] != `it's a test` {
		t.Fatalf("expected bound args [it's a test], got %#v", args)
	}
}

func TestCondition_EmptyInShortCircuits(t *testing.T) {
	col := newColForTable[Table, int](newMockTable("users"), "id", "id", nil)

	if got := col.In().Clause(); got != "1 = 0" {
		t.Fatalf("expected empty IN to short-circuit to false predicate, got %q", got)
	}

	if len(col.In().Tables()) != 0 {
		t.Fatalf("expected empty IN short-circuit to avoid leaking source tables")
	}

	if got := col.NIn().Clause(); got != "1 = 1" {
		t.Fatalf("expected empty NOT IN to short-circuit to true predicate, got %q", got)
	}

	if len(col.NIn().Tables()) != 0 {
		t.Fatalf("expected empty NOT IN short-circuit to avoid leaking source tables")
	}
}

func TestCondition_InVarDefersSliceBindingToExecution(t *testing.T) {
	col := newColForTable[Table, int](newMockTable("users"), "id", "id", nil)
	cond := col.InVar()

	if got := cond.Clause(); got != `"users"."id" IN (?)` {
		t.Fatalf("expected IN var clause template to keep a single placeholder, got %q", got)
	}

	args := cond.Args()
	if len(args) != 1 {
		t.Fatalf("expected exactly one deferred arg marker, got %#v", args)
	}

	if _, ok := args[0].(externalSliceArgMarker); !ok {
		t.Fatalf("expected deferred arg marker to be externalSliceArgMarker, got %T", args[0])
	}
}

func TestConditionClauseRendersCanonicalSQL(t *testing.T) {
	col := newColForTable[Table, int](newMockTable("users"), "id", "id", nil)

	cond := col.EQ(1)
	if got := cond.Clause(); got != `"users"."id" = ?` {
		t.Fatalf("expected public condition clause to render canonical SQL, got %q", got)
	}

	if got := cond.Args(); len(got) != 1 || got[0] != 1 {
		t.Fatalf("expected parameterized predicate args [1], got %#v", got)
	}
}

func TestCondition_EmptyAndOrShortCircuit(t *testing.T) {
	if got := And().Clause(); got != "1 = 1" {
		t.Fatalf("expected empty And to short-circuit to true predicate, got %q", got)
	}

	if got := Or().Clause(); got != "1 = 0" {
		t.Fatalf("expected empty Or to short-circuit to false predicate, got %q", got)
	}
}

func TestCondition_TablesReturnsDefensiveCopy(t *testing.T) {
	users := newMockTable("users")
	cond := newColForTable[Table, int](users, "id", "id", nil).EQ(1)

	tables := cond.Tables()
	delete(tables, "users")
	tables["other"] = newMockTable("other")

	fresh := cond.Tables()
	if len(fresh) != 1 || fresh["users"] == nil {
		t.Fatalf("expected condition tables to stay intact, got %#v", fresh)
	}
}

func TestCondition_NullPredicateValuesFailFast(t *testing.T) {
	ptrCol := newColForTable[Table, *string](newMockTable("users"), "name", "name", nil)
	nullableCol := newColForTable[Table, sql.NullString](newMockTable("users"), "nickname", "nickname", nil)

	for _, cond := range []Condition{
		ptrCol.EQ(nil),
		ptrCol.In(nil),
		nullableCol.EQ(sql.NullString{}),
	} {
		if _, _, _, err := validateConditionInput(cond); err == nil {
			t.Fatal("expected null predicate value to return a build error")
		}
	}
}

func TestCondition_NilCompositeInputsFailFast(t *testing.T) {
	var nilCond Condition

	for _, cond := range []Cond{And(nilCond), Or(nilCond)} {
		if _, _, _, err := validateConditionInput(cond); err == nil {
			t.Fatal("expected nil condition to be captured as a build error")
		}
	}
}

func TestCondition_PortabilitySensitiveLikePredicatesFailFast(t *testing.T) {
	users := newMockTable("users")
	nameCol := newColForTable[Table, string](users, "name", "name", nil)
	patternCol := newColForTable[Table, string](users, "pattern", "pattern", nil)

	for _, cond := range []Condition{
		nameCol.StartsWithVar(),
		nameCol.StartsWithCol(patternCol),
	} {
		if _, _, _, err := validateConditionInput(cond); err == nil {
			t.Fatal("expected predicate helper to return a build error for non-portable SQL")
		}
	}
}

func TestCondition_StringPredicatesBindBackslashesSafely(t *testing.T) {
	col := newColForTable[Table, string](newMockTable("users"), "name", "name", nil)

	for name, cond := range map[string]Condition{
		"EQ": col.EQ(`path\file`),
		"IN": col.In(`path\file`),
	} {
		t.Run(name, func(t *testing.T) {
			clause, _, args, err := validateConditionInput(cond)
			if err != nil {
				t.Fatalf("expected backslash-containing string to bind successfully, got %v", err)
			}

			if !strings.Contains(renderCanonicalSQL(clause), "?") {
				t.Fatalf("expected predicate to stay parameterized, got %q", renderCanonicalSQL(clause))
			}

			if len(args) == 0 || args[0] != `path\file` {
				t.Fatalf("expected bound backslash arg, got %#v", args)
			}
		})
	}
}

func TestCondition_ExistsSubIsStandalonePredicate(t *testing.T) {
	col := newColForTable[Table, int](newMockTable("users"), "id", "id", nil)
	orderID := newColForTable[Table, int](newMockTable("orders"), "id", "id", nil)
	subquery := mustBuild(Select(orderID).From(orderID.Table()))

	got := renderCanonicalSQL(col.ExistsSub(subquery).Clause())
	want := `EXISTS (SELECT "orders"."id" FROM "orders")`

	if got != want {
		t.Fatalf("expected exists clause %q, got %q", want, got)
	}
}

func TestCondition_UnbuiltSubqueryFailsFast(t *testing.T) {
	col := newColForTable[Table, int](newMockTable("users"), "id", "id", nil)

	if _, _, _, err := validateConditionInput(col.InSub(&Query[queryOwner]{})); err == nil {
		t.Fatal("expected unbuilt subquery to be captured as a build error")
	} else if !strings.Contains(err.Error(), "subquery is not built") {
		t.Fatalf("expected unbuilt subquery error, got %v", err)
	}
}

func TestCondition_ScalarSubqueryRejectsMultipleColumns(t *testing.T) {
	users := newMockTable("users")
	orders := newMockTable("orders")
	userID := newColForTable[Table, int](users, "id", "id", nil)
	orderID := newColForTable[Table, int](orders, "id", "id", nil)
	orderUserID := newColForTable[Table, int](orders, "user_id", "user_id", nil)
	subquery := mustBuild(Select(orderID, orderUserID).From(orders))

	if _, _, _, err := validateConditionInput(userID.EQSub(subquery)); err == nil {
		t.Fatal("expected scalar subquery to reject multiple columns")
	} else if !strings.Contains(err.Error(), "scalar subquery must select exactly one column") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCondition_InSubRejectsMultipleColumns(t *testing.T) {
	users := newMockTable("users")
	orders := newMockTable("orders")
	userID := newColForTable[Table, int](users, "id", "id", nil)
	orderID := newColForTable[Table, int](orders, "id", "id", nil)
	orderUserID := newColForTable[Table, int](orders, "user_id", "user_id", nil)
	subquery := mustBuild(Select(orderID, orderUserID).From(orders))

	if _, _, _, err := validateConditionInput(userID.InSub(subquery)); err == nil {
		t.Fatal("expected IN subquery to reject multiple columns")
	} else if !strings.Contains(err.Error(), "in subquery must select exactly one column") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCondition_ExistsSubAllowsMultipleColumnsAndKeepsArgs(t *testing.T) {
	users := newMockTable("users")
	orders := newMockTable("orders")
	userID := newColForTable[Table, int](users, "id", "id", nil)
	orderID := newColForTable[Table, int](orders, "id", "id", nil)
	orderUserID := newColForTable[Table, int](orders, "user_id", "user_id", nil)
	subquery := mustBuild(Select(orderID, orderUserID).From(orders).Where(orderUserID.EQ(1)))

	clause, _, args, err := validateConditionInput(userID.ExistsSub(subquery))
	if err != nil {
		t.Fatalf("expected EXISTS subquery to allow multiple columns, got %v", err)
	}

	wantClause := `EXISTS (SELECT "orders"."id", "orders"."user_id" FROM "orders" WHERE "orders"."user_id" = ?)`
	if got := renderCanonicalSQL(clause); got != wantClause {
		t.Fatalf("expected exists clause %q, got %q", wantClause, got)
	}

	if len(args) != 1 || args[0] != 1 {
		t.Fatalf("expected EXISTS subquery args [1], got %#v", args)
	}
}

func TestCondition_EmptyClauseFailsFast(t *testing.T) {
	if _, _, _, err := validateConditionInput(And(Cond{})); err == nil {
		t.Fatal("expected empty condition clause to be captured as a build error")
	}
}

func TestCondition_PredicateRejectsInvalidFormat(t *testing.T) {
	col := newColForTable[Table, int](newMockTable("users"), "id", "id", nil)

	tests := []struct {
		name string
		op   string
		args []any
	}{
		{name: "empty format", op: "", args: nil},
		{name: "missing placeholders", op: "id = 1", args: nil},
		{name: "placeholder count mismatch", op: "%s = %s", args: []any{1, 2}},
		{name: "unsupported verb", op: "%s = %d", args: []any{1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, _, _, err := validateConditionInput(col.Predicate(tt.op, tt.args...)); err == nil {
				t.Fatal("expected Predicate to return a build error for invalid format")
			}
		})
	}
}

func TestCondition_PredicateAllowsEscapedPercentLiterals(t *testing.T) {
	col := newColForTable[Table, string](newMockTable("users"), "name", "name", nil)

	clause := renderCanonicalSQL(col.Predicate("%s LIKE '%%s'").Clause())
	if clause != `"users"."name" LIKE '%s'` {
		t.Fatalf("expected escaped percent literal to be preserved, got %q", clause)
	}
}

func TestCondition_PredicateRejectsRawSubqueryArguments(t *testing.T) {
	col := newColForTable[Table, int](newMockTable("users"), "id", "id", nil)
	subquery := mustBuild(Select(col).From(col.Table()))

	if _, _, _, err := validateConditionInput(col.Predicate("%s = %s", subquery)); err == nil {
		t.Fatal("expected Predicate to reject raw subquery arguments")
	} else if !strings.Contains(err.Error(), "raw subqueries are not allowed in Predicate") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCondition_UniqueSubqueryPredicatesFailFast(t *testing.T) {
	col := newColForTable[Table, int](newMockTable("users"), "id", "id", nil)
	subquery := &Query[queryOwner]{listSQL: "SELECT 1"}

	if _, _, _, err := validateConditionInput(col.Unique(subquery)); err == nil {
		t.Fatal("expected Unique to return a build error for unsupported predicate")
	}
}

// TestUnsupportedPatternPredicatesDefer Error tests that unsupported pattern predicates
// return deferred errors (not immediate panics) that are reported at Build() time.
func TestUnsupportedPatternPredicatesDeferred(t *testing.T) {
	col := newColForTable[Table, string](newMockTable("users"), "name", "name", nil)

	tests := []struct {
		name string
		cond Condition
	}{
		{"StartsWithVar", col.StartsWithVar()},
		{"EndsWithVar", col.EndsWithVar()},
		{"ContainsVar", col.ContainsVar()},
		{"NStartsWithVar", col.NStartsWithVar()},
		{"NEndsWithVar", col.NEndsWithVar()},
		{"NContainsVar", col.NContainsVar()},
		{"StartsWithCol", col.StartsWithCol(col)},
		{"EndsWithCol", col.EndsWithCol(col)},
		{"ContainsCol", col.ContainsCol(col)},
		{"NStartsWithCol", col.NStartsWithCol(col)},
		{"NEndsWithCol", col.NEndsWithCol(col)},
		{"NContainsCol", col.NContainsCol(col)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Error should be deferred, not immediate
			// Calling the method shouldn't panic
			_, _, _, err := validateConditionInput(tt.cond)
			if err == nil {
				t.Fatalf("expected %s to have deferred error", tt.name)
			}
			if !strings.Contains(err.Error(), "not portable") {
				t.Fatalf("expected error to mention portability, got: %v", err)
			}
		})
	}
}

// TestUnsupportedSubqueryPredicatesDeferred tests that unsupported subquery predicates
// return deferred errors at Build() time, not immediate panics.
func TestUnsupportedSubqueryPredicatesDeferred(t *testing.T) {
	col := newColForTable[Table, int](newMockTable("users"), "id", "id", nil)
	query := &Query[queryOwner]{listSQL: "SELECT 1"}

	tests := []struct {
		name string
		cond Condition
	}{
		{"Unique", col.Unique(query)},
		{"NUnique", col.NUnique(query)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Error should be deferred, not immediate
			_, _, _, err := validateConditionInput(tt.cond)
			if err == nil {
				t.Fatalf("expected %s to have deferred error", tt.name)
			}
			if !strings.Contains(err.Error(), "subquery") {
				t.Fatalf("expected error to mention subquery, got: %v", err)
			}
		})
	}
}
