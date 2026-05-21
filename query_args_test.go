package tsq

import (
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestQuery_BuildKeywordQueriesTrackDedicatedMarkers(t *testing.T) {
	users := newMockTable("users")
	userID := newMockColumn(users, "id")
	userName := newMockColumn(users, "name")
	query := mustBuild(Select(userID, userName).From(userID.Table()).Search(userID, userName))
	if got := len(query.kwListArgs); got != 2 {
		t.Fatalf("expected 2 keyword list args, got %d", got)
	}
	if got := len(query.kwCntArgs); got != 2 {
		t.Fatalf("expected 2 keyword count args, got %d", got)
	}
	args, err := resolveQueryArgs(query.kwListArgs, nil, "demo")
	if err != nil {
		t.Fatalf("expected keyword markers to resolve, got %v", err)
	}
	if len(args) != 2 || args[0] != "%demo%" || args[1] != "%demo%" {
		t.Fatalf("unexpected resolved keyword args: %#v", args)
	}
}

func TestResolveQueryExpandsExternalSliceArgs(t *testing.T) {
	sqlText, args, err := resolveQuery(`SELECT * FROM "users" WHERE "users"."id" IN (?) AND "users"."name" = ?`, []any{externalSliceArgMarker{}, externalArgMarker}, []any{[]int64{1, 3, 5}, "alice"}, "")
	if err != nil {
		t.Fatalf("expected resolveQuery to expand slice args, got %v", err)
	}
	wantSQL := `SELECT * FROM "users" WHERE "users"."id" IN (?, ?, ?) AND "users"."name" = ?`
	if sqlText != wantSQL {
		t.Fatalf("expected SQL %q, got %q", wantSQL, sqlText)
	}
	if want := []any{int64(1), int64(3), int64(5), "alice"}; len(args) != len(want) || args[0] != want[0] || args[1] != want[1] || args[2] != want[2] || args[3] != want[3] {
		t.Fatalf("unexpected resolved args: %#v", args)
	}
}

func TestResolveQueryExpandsEmptyExternalSliceArgsToNull(t *testing.T) {
	sqlText, args, err := resolveQuery(`SELECT * FROM "users" WHERE "users"."id" IN (?)`, []any{externalSliceArgMarker{}}, []any{[]int64{}}, "")
	if err != nil {
		t.Fatalf("expected empty slice to resolve, got %v", err)
	}
	if sqlText != `SELECT * FROM "users" WHERE "users"."id" IN (NULL)` {
		t.Fatalf("unexpected SQL for empty slice: %q", sqlText)
	}
	if len(args) != 0 {
		t.Fatalf("expected empty slice to contribute no args, got %#v", args)
	}
}

func TestResolveQueryResolvesExternalArgsWithoutRewritingSQL(t *testing.T) {
	sqlText, args, err := resolveQuery(`SELECT * FROM "users" WHERE "users"."id" = ? AND "users"."name" LIKE ?`, []any{externalArgMarker, keywordArgMarker}, []any{int64(7)}, "alice")
	if err != nil {
		t.Fatalf("expected resolveQuery to resolve non-slice markers, got %v", err)
	}
	if want := `SELECT * FROM "users" WHERE "users"."id" = ? AND "users"."name" LIKE ?`; sqlText != want {
		t.Fatalf("expected SQL %q, got %q", want, sqlText)
	}
	if want := []any{int64(7), "%alice%"}; len(args) != len(want) || args[0] != want[0] || args[1] != want[1] {
		t.Fatalf("unexpected resolved args: %#v", args)
	}
}

func TestFlattenExternalSliceArgFastPaths(t *testing.T) {
	values, err := flattenExternalSliceArg([]int64{1, 2, 3})
	if err != nil {
		t.Fatalf("expected []int64 fast path to succeed, got %v", err)
	}
	if want := []any{int64(1), int64(2), int64(3)}; len(values) != len(want) || values[0] != want[0] || values[1] != want[1] || values[2] != want[2] {
		t.Fatalf("unexpected flattened []int64 values: %#v", values)
	}
	values, err = flattenExternalSliceArg([]string{"a", "b"})
	if err != nil {
		t.Fatalf("expected []string fast path to succeed, got %v", err)
	}
	if want := []any{"a", "b"}; len(values) != len(want) || values[0] != want[0] || values[1] != want[1] {
		t.Fatalf("unexpected flattened []string values: %#v", values)
	}
	ints := []int{4, 5}
	values, err = flattenExternalSliceArg(&ints)
	if err != nil {
		t.Fatalf("expected *[]int fast path to succeed, got %v", err)
	}
	if want := []any{4, 5}; len(values) != len(want) || values[0] != want[0] || values[1] != want[1] {
		t.Fatalf("unexpected flattened *[]int values: %#v", values)
	}
}

func TestExpandSlicePlaceholdersUsesCache(t *testing.T) {
	if got := expandSlicePlaceholders(0); got != "NULL" {
		t.Fatalf("expected NULL placeholder for empty slice, got %q", got)
	}
	if got := expandSlicePlaceholders(3); got != "?, ?, ?" {
		t.Fatalf("expected cached placeholder expansion, got %q", got)
	}
	large := expandSlicePlaceholders(slicePlaceholderCacheMax + 1)
	if strings.Count(large, "?") != slicePlaceholderCacheMax+1 {
		t.Fatalf("expected %d placeholders, got %q", slicePlaceholderCacheMax+1, large)
	}
}

func TestEscapeKeywordSearch(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{{name: "empty string", input: "", expected: ""}, {name: "no special chars", input: "hello world", expected: "hello world"}, {name: "percent wildcard", input: "100%", expected: "100\\%"}, {name: "underscore wildcard", input: "user_name", expected: "user\\_name"}, {name: "both wildcards", input: "%value_", expected: "\\%value\\_"}, {name: "backslash", input: "path\\file", expected: "path\\\\file"}, {name: "backslash before percent", input: "100\\% cotton", expected: "100\\\\\\% cotton"}, {name: "real world example", input: "50% off_sale", expected: "50\\% off\\_sale"}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := escapeKeywordSearch(tt.input)
			if result != tt.expected {
				t.Errorf("escapeKeywordSearch(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}
