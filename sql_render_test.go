package tsq

import (
	"testing"

	"gopkg.in/gorp.v2"
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

	got := renderSQLForDialect(raw, gorp.PostgresDialect{})
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

	got := renderSQLForDialect(raw, gorp.PostgresDialect{})
	want := `SELECT "users"."name" /* comment ? __tsq_ident__(ignored_name) */ WHERE "users"."id" = $1 -- trailing ? __tsq_ident__(ignored_tail)` + "\n"
	if got != want {
		t.Fatalf("expected postgres SQL %q, got %q", want, got)
	}
}
