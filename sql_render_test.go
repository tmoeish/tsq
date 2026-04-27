package tsq

import (
	"testing"
)

func TestRenderCanonicalSQLPreservesIdentifierMarkersInsideStringLiterals(t *testing.T) {
	raw := "SELECT " + rawQualifiedIdentifier("users", "name") +
		" WHERE " + rawQualifiedIdentifier("users", "name") +
		" = '__tsq_ident__(literal_name)'"

	got := renderCanonicalSQL(raw)
	want := `SELECT "users"."name" WHERE "users"."name" = '__tsq_ident__(literal_name)'`

	if got != want {
		t.Fatalf("expected canonical SQL %q, got %q", want, got)
	}
}

func TestRenderSQLForDialectPreservesIdentifierMarkersInsideEscapedStringLiterals(t *testing.T) {
	raw := "SELECT " + rawQualifiedIdentifier("users", "name") +
		" WHERE note = 'it''s __tsq_ident__(literal_name)?' AND " +
		rawQualifiedIdentifier("users", "id") + " = ?"

	got := renderSQLForDialect(raw, PostgresDialect{})
	want := `SELECT "users"."name" WHERE note = 'it''s __tsq_ident__(literal_name)?' AND "users"."id" = $1`

	if got != want {
		t.Fatalf("expected postgres SQL %q, got %q", want, got)
	}
}

func TestRenderCanonicalSQLPreservesIdentifierMarkersInsideComments(t *testing.T) {
	raw := "SELECT " + rawQualifiedIdentifier("users", "name") +
		" /* keep __tsq_ident__(ignored_name) */" +
		" WHERE " + rawQualifiedIdentifier("users", "id") + " = ?" +
		" -- keep __tsq_ident__(ignored_tail)\n"

	got := renderCanonicalSQL(raw)
	want := `SELECT "users"."name" /* keep __tsq_ident__(ignored_name) */ WHERE "users"."id" = ? -- keep __tsq_ident__(ignored_tail)` + "\n"

	if got != want {
		t.Fatalf("expected canonical SQL %q, got %q", want, got)
	}
}

func TestRenderSQLForDialectPreservesQuestionMarksInsideComments(t *testing.T) {
	raw := "SELECT " + rawQualifiedIdentifier("users", "name") +
		" /* comment ? __tsq_ident__(ignored_name) */" +
		" WHERE " + rawQualifiedIdentifier("users", "id") + " = ?" +
		" -- trailing ? __tsq_ident__(ignored_tail)\n"

	got := renderSQLForDialect(raw, PostgresDialect{})
	want := `SELECT "users"."name" /* comment ? __tsq_ident__(ignored_name) */ WHERE "users"."id" = $1 -- trailing ? __tsq_ident__(ignored_tail)` + "\n"

	if got != want {
		t.Fatalf("expected postgres SQL %q, got %q", want, got)
	}
}

func TestRenderCanonicalSQLPreservesIdentifierMarkersInsideDollarQuotedStrings(t *testing.T) {
	raw := "SELECT $$__tsq_ident__(ignored_name)$$ AS note, " +
		rawQualifiedIdentifier("users", "name") +
		" FROM " + rawIdentifier("users")

	got := renderCanonicalSQL(raw)
	want := `SELECT $$__tsq_ident__(ignored_name)$$ AS note, "users"."name" FROM "users"`

	if got != want {
		t.Fatalf("expected canonical SQL %q, got %q", want, got)
	}
}

func TestRenderSQLForDialectPreservesQuestionMarksInsideDollarQuotedStrings(t *testing.T) {
	raw := "SELECT $body$? __tsq_ident__(ignored_name)$body$ AS note, " +
		rawQualifiedIdentifier("users", "id") + " FROM " + rawIdentifier("users") +
		" WHERE " + rawQualifiedIdentifier("users", "id") + " = ?"

	got := renderSQLForDialect(raw, PostgresDialect{})
	want := `SELECT $body$? __tsq_ident__(ignored_name)$body$ AS note, "users"."id" FROM "users" WHERE "users"."id" = $1`

	if got != want {
		t.Fatalf("expected postgres SQL %q, got %q", want, got)
	}
}

func TestRenderCanonicalSQLHandlesIdentifiersContainingMarkerSuffix(t *testing.T) {
	raw := "SELECT " + rawQualifiedIdentifier("team)", "name)") +
		" FROM " + rawIdentifier("team)")

	got := renderCanonicalSQL(raw)
	want := `SELECT "team)"."name)" FROM "team)"`

	if got != want {
		t.Fatalf("expected canonical SQL %q, got %q", want, got)
	}
}

func TestRenderSQLForDialectHandlesIdentifiersContainingMarkerSuffix(t *testing.T) {
	raw := "SELECT " + rawQualifiedIdentifier("team)", "id)") +
		" FROM " + rawIdentifier("team)") +
		" WHERE " + rawQualifiedIdentifier("team)", "id)") + " = ?"

	got := renderSQLForDialect(raw, PostgresDialect{})
	want := `SELECT "team)"."id)" FROM "team)" WHERE "team)"."id)" = $1`

	if got != want {
		t.Fatalf("expected postgres SQL %q, got %q", want, got)
	}
}

func TestContainsIdentifierMarkersNeedingRenderIgnoresStringsAndComments(t *testing.T) {
	raw := "SELECT 1" +
		" /* __tsq_ident__(ignored_comment) */" +
		" WHERE note = '__tsq_ident__(ignored_string)' " +
		" -- __tsq_ident__(ignored_tail)\n"

	if containsIdentifierMarkersNeedingRender(raw) {
		t.Fatal("expected markers in comments and strings to be ignored")
	}
}

func TestContainsIdentifierMarkersNeedingRenderIgnoresDollarQuotedStrings(t *testing.T) {
	raw := "SELECT $$__tsq_ident__(ignored_marker)$$, $tag$still ? ignored$tag$"

	if containsIdentifierMarkersNeedingRender(raw) {
		t.Fatal("expected markers in dollar-quoted strings to be ignored")
	}
}

func TestContainsIdentifierMarkersNeedingRenderDetectsRealIdentifiers(t *testing.T) {
	raw := "SELECT " + rawQualifiedIdentifier("users", "id")

	if !containsIdentifierMarkersNeedingRender(raw) {
		t.Fatal("expected real identifier markers to be detected")
	}
}

func TestContainsBindVarsNeedingDialectIgnoresStringsCommentsAndDollarQuotes(t *testing.T) {
	raw := "SELECT 1" +
		" /* ? ignored_comment */" +
		" WHERE note = '?'" +
		" AND body = $$?$$" +
		" -- ? ignored_tail\n"

	if containsBindVarsNeedingDialect(raw) {
		t.Fatal("expected bind vars inside strings/comments to be ignored")
	}
}

func TestContainsBindVarsNeedingDialectDetectsRealPlaceholders(t *testing.T) {
	raw := `SELECT "users"."id" FROM "users" WHERE "users"."id" = ?`

	if !containsBindVarsNeedingDialect(raw) {
		t.Fatal("expected real bind vars to be detected")
	}
}
