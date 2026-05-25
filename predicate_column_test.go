package tsq

import (
	"database/sql"
	"strings"
	"testing"
)

func TestCondition_EmptyInShortCircuits(t *testing.T) {
	col := newColForTable[Table, int](newMockTable("users"), "id", "id", nil)
	if got := col.InVal().Clause(); got != "1 = 0" {
		t.Fatalf("expected empty IN to short-circuit to false predicate, got %q", got)
	}
	if len(col.InVal().Tables()) != 0 {
		t.Fatalf("expected empty IN short-circuit to avoid leaking source tables")
	}
	if got := col.NInVal().Clause(); got != "1 = 1" {
		t.Fatalf("expected empty NOT IN to short-circuit to true predicate, got %q", got)
	}
	if len(col.NInVal().Tables()) != 0 {
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

func TestCondition_NInVarDefersSliceBindingToExecution(t *testing.T) {
	col := newColForTable[Table, int](newMockTable("users"), "id", "id", nil)
	cond := col.NInVar()
	if got := cond.Clause(); got != `"users"."id" NOT IN (?)` {
		t.Fatalf("expected NOT IN var clause template to keep a single placeholder, got %q", got)
	}
	args := cond.Args()
	if len(args) != 1 {
		t.Fatalf("expected exactly one deferred arg marker, got %#v", args)
	}
	if _, ok := args[0].(externalNotInSliceArgMarker); !ok {
		t.Fatalf("expected deferred arg marker to be externalNotInSliceArgMarker, got %T", args[0])
	}
}

func TestCondition_NullPredicateValuesFailFast(t *testing.T) {
	ptrCol := newColForTable[Table, *string](newMockTable("users"), "name", "name", nil)
	nullableCol := newColForTable[Table, sql.NullString](newMockTable("users"), "nickname", "nickname", nil)
	for _, cond := range []Condition{ptrCol.EQVal(nil), ptrCol.InVal(nil), nullableCol.EQVal(sql.NullString{})} {
		if _, _, _, err := validateConditionInput(cond); err == nil {
			t.Fatal("expected null predicate value to return a build error")
		}
	}
}

func TestCondition_LikeSupportsValVarAndColumnRHS(t *testing.T) {
	users := newMockTable("users")
	nameCol := newColForTable[Table, string](users, "name", "name", nil)
	patternCol := newColForTable[Table, string](users, "pattern", "pattern", nil)

	tests := []struct {
		name       string
		cond       Condition
		wantClause string
		wantArg    any
	}{
		{name: "LikeVal", cond: nameCol.LikeVal("%alice%"), wantClause: `"users"."name" LIKE ?`, wantArg: "%alice%"},
		{name: "NLikeVal", cond: nameCol.NLikeVal("%alice%"), wantClause: `"users"."name" NOT LIKE ?`, wantArg: "%alice%"},
		{name: "LikeRHS", cond: nameCol.Like(patternCol), wantClause: `"users"."name" LIKE "users"."pattern"`},
		{name: "NLikeRHS", cond: nameCol.NLike(patternCol), wantClause: `"users"."name" NOT LIKE "users"."pattern"`},
		{name: "LikeVar", cond: nameCol.LikeVar(), wantClause: `"users"."name" LIKE ?`, wantArg: externalArgMarker},
		{name: "NLikeVar", cond: nameCol.NLikeVar(), wantClause: `"users"."name" NOT LIKE ?`, wantArg: externalArgMarker},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clause, _, args, err := validateConditionInput(tt.cond)
			if err != nil {
				t.Fatalf("expected LIKE helper to validate, got %v", err)
			}
			if got := renderCanonicalSQL(clause); got != tt.wantClause {
				t.Fatalf("expected clause %q, got %q", tt.wantClause, got)
			}
			if tt.wantArg == nil {
				if len(args) != 0 {
					t.Fatalf("expected no args, got %#v", args)
				}
				return
			}
			if len(args) != 1 || args[0] != tt.wantArg {
				t.Fatalf("expected args [%#v], got %#v", tt.wantArg, args)
			}
		})
	}
}

func TestCondition_StringPredicatesBindBackslashesSafely(t *testing.T) {
	col := newColForTable[Table, string](newMockTable("users"), "name", "name", nil)
	for name, cond := range map[string]Condition{"EQVal": col.EQVal(`path\file`), "InVal": col.InVal(`path\file`)} {
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

func TestCondition_StringPatternHelpersUseOpValAndOpVarForms(t *testing.T) {
	col := newColForTable[Table, string](newMockTable("users"), "name", "name", nil)
	tests := []struct {
		name       string
		cond       Condition
		wantClause string
		wantArg    any
	}{
		{name: "StartsWithVal", cond: col.StartsWithVal("al"), wantClause: `"users"."name" LIKE ?`, wantArg: "al%"},
		{name: "NStartsWithVal", cond: col.NStartsWithVal("al"), wantClause: `"users"."name" NOT LIKE ?`, wantArg: "al%"},
		{name: "EndsWithVal", cond: col.EndsWithVal("ce"), wantClause: `"users"."name" LIKE ?`, wantArg: "%ce"},
		{name: "NEndsWithVal", cond: col.NEndsWithVal("ce"), wantClause: `"users"."name" NOT LIKE ?`, wantArg: "%ce"},
		{name: "ContainsVal", cond: col.ContainsVal("lic"), wantClause: `"users"."name" LIKE ?`, wantArg: "%lic%"},
		{name: "NContainsVal", cond: col.NContainsVal("lic"), wantClause: `"users"."name" NOT LIKE ?`, wantArg: "%lic%"},
		{name: "StartsWithVar", cond: col.StartsWithVar(), wantClause: `"users"."name" LIKE ?`, wantArg: externalStartsWithMarker},
		{name: "NStartsWithVar", cond: col.NStartsWithVar(), wantClause: `"users"."name" NOT LIKE ?`, wantArg: externalStartsWithMarker},
		{name: "EndsWithVar", cond: col.EndsWithVar(), wantClause: `"users"."name" LIKE ?`, wantArg: externalEndsWithMarker},
		{name: "NEndsWithVar", cond: col.NEndsWithVar(), wantClause: `"users"."name" NOT LIKE ?`, wantArg: externalEndsWithMarker},
		{name: "ContainsVar", cond: col.ContainsVar(), wantClause: `"users"."name" LIKE ?`, wantArg: externalContainsMarker},
		{name: "NContainsVar", cond: col.NContainsVar(), wantClause: `"users"."name" NOT LIKE ?`, wantArg: externalContainsMarker},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clause, _, args, err := validateConditionInput(tt.cond)
			if err != nil {
				t.Fatalf("expected string helper to validate, got %v", err)
			}
			if got := renderCanonicalSQL(clause); got != tt.wantClause {
				t.Fatalf("expected clause %q, got %q", tt.wantClause, got)
			}
			if len(args) != 1 || args[0] != tt.wantArg {
				t.Fatalf("expected args [%#v], got %#v", tt.wantArg, args)
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
