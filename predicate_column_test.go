package tsq

import (
	"database/sql"
	"strings"
	"testing"
)

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
func TestCondition_NullPredicateValuesFailFast(t *testing.T) {
	ptrCol := newColForTable[Table, *string](newMockTable("users"), "name", "name", nil)
	nullableCol := newColForTable[Table, sql.NullString](newMockTable("users"), "nickname", "nickname", nil)
	for _, cond := range []Condition{ptrCol.EQ(nil), ptrCol.In(nil), nullableCol.EQ(sql.NullString{})} {
		if _, _, _, err := validateConditionInput(cond); err == nil {
			t.Fatal("expected null predicate value to return a build error")
		}
	}
}
func TestCondition_PortabilitySensitiveLikePredicatesFailFast(t *testing.T) {
	users := newMockTable("users")
	nameCol := newColForTable[Table, string](users, "name", "name", nil)
	patternCol := newColForTable[Table, string](users, "pattern", "pattern", nil)
	for _, cond := range []Condition{nameCol.StartsWithVar(), nameCol.StartsWithCol(patternCol)} {
		if _, _, _, err := validateConditionInput(cond); err == nil {
			t.Fatal("expected predicate helper to return a build error for non-portable SQL")
		}
	}
}
func TestCondition_StringPredicatesBindBackslashesSafely(t *testing.T) {
	col := newColForTable[Table, string](newMockTable("users"), "name", "name", nil)
	for name, cond := range map[string]Condition{"EQ": col.EQ(`path\file`), "IN": col.In(`path\file`)} {
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
func TestCondition_PredRejectsInvalidFormat(t *testing.T) {
	col := newColForTable[Table, int](newMockTable("users"), "id", "id", nil)
	tests := []struct {
		name string
		op   string
		args []any
	}{{name: "empty format", op: "", args: nil}, {name: "missing placeholders", op: "id = 1", args: nil}, {name: "placeholder count mismatch", op: "%s = %s", args: []any{1, 2}}, {name: "unsupported verb", op: "%s = %d", args: []any{1}}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, _, _, err := validateConditionInput(col.Pred(tt.op, tt.args...)); err == nil {
				t.Fatal("expected Pred to return a build error for invalid format")
			}
		})
	}
}
func TestCondition_PredAllowsEscapedPercentLiterals(t *testing.T) {
	col := newColForTable[Table, string](newMockTable("users"), "name", "name", nil)
	clause := renderCanonicalSQL(col.Pred("%s LIKE '%%s'").Clause())
	if clause != `"users"."name" LIKE '%s'` {
		t.Fatalf("expected escaped percent literal to be preserved, got %q", clause)
	}
}
func TestCondition_PredRejectsRawSubqueryArguments(t *testing.T) {
	col := newColForTable[Table, int](newMockTable("users"), "id", "id", nil)
	subquery := mustBuild(Select(col).From(col.Table()))
	if _, _, _, err := validateConditionInput(col.Pred("%s = %s", subquery)); err == nil {
		t.Fatal("expected Pred to reject raw subquery arguments")
	} else if !strings.Contains(err.Error(), "raw subqueries are not allowed in Pred") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestUnsupportedPatternPredicatesDeferred(t *testing.T) {
	col := newColForTable[Table, string](newMockTable("users"), "name", "name", nil)
	tests := []struct// TestUnsupportedPatternPredicatesDefer Error tests that unsupported pattern predicates
	// return deferred errors (not immediate panics) that are reported at Build() time.
	{
		name string
		cond Condition
	}{{"StartsWithVar", col.StartsWithVar()}, {"EndsWithVar", col.EndsWithVar()}, {"ContainsVar", col.ContainsVar()}, {"NStartsWithVar", col.NStartsWithVar()}, {"NEndsWithVar", col.NEndsWithVar()}, {"NContainsVar", col.NContainsVar()}, {"StartsWithCol", col.StartsWithCol(col)}, {"EndsWithCol", col.EndsWithCol(col)}, {"ContainsCol", col.ContainsCol(col)}, {"NStartsWithCol", col.NStartsWithCol(col)}, {"NEndsWithCol", col.NEndsWithCol(col)}, {"NContainsCol", col.NContainsCol(col)}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
