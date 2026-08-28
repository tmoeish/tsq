package tsq

import (
	"context"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

// keywordSearchColumn returns the "name" column as the search column used by the
// keyword-search tests.
func keywordSearchColumn(t *testing.T) SearchColumn {
	t.Helper()

	col, ok := batchMutationUserColumns()[1].(SearchColumn)
	if !ok {
		t.Fatalf("expected the name column to be a SearchColumn")
	}

	return col
}

func seedKeywordSearchRows(t *testing.T, runtime *Runtime, names ...string) {
	t.Helper()

	for i, name := range names {
		if _, err := runtime.DB().ExecContext(
			context.Background(),
			`INSERT INTO users (name, email) VALUES (?, ?)`,
			name,
			// The unique email column only needs distinct values here.
			strings.Join([]string{"user", string(rune('a' + i))}, "")+"@example.com",
		); err != nil {
			t.Fatalf("seed row %q: %v", name, err)
		}
	}
}

// TestPageKeywordEscapesWildcardsAgainstSQLite pins the promise that
// PageRequest.Keyword is matched literally. Escaping the keyword is only half of
// it: without the ESCAPE clause in the rendered predicate the escape character is
// an ordinary character on SQLite, which has no default LIKE escape, and the query
// silently returns nothing.
func TestPageKeywordEscapesWildcardsAgainstSQLite(t *testing.T) {
	tests := []struct {
		name    string
		keyword string
		want    string
	}{
		{name: "underscore is literal", keyword: "a_b", want: "a_b"},
		{name: "percent is literal", keyword: "100%", want: "100%"},
		{name: "escape char is literal", keyword: "c~d", want: "c~d"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runtime := newBatchMutationEngine(t)
			exec := requireInitializedRuntime(t, runtime)
			seedKeywordSearchRows(t, runtime, "a_b", "axb", "100%", "100x", "c~d", "cxd")

			query := Select(batchMutationUserColumns()...).
				From(batchMutationUser{}).
				Search(keywordSearchColumn(t)).
				MustBuild()

			resp, err := query.Page(context.Background(), exec, &PageRequest{
				Page:    1,
				Size:    10,
				Keyword: tt.keyword,
			})
			if err != nil {
				t.Fatalf("page query: %v", err)
			}

			if resp.Total != 1 {
				names := make([]string, 0, len(resp.Data))
				for _, row := range resp.Data {
					names = append(names, row.Name)
				}

				t.Fatalf("expected keyword %q to match exactly one row, got %d: %v", tt.keyword, resp.Total, names)
			}

			if resp.Data[0].Name != tt.want {
				t.Fatalf("expected keyword %q to match %q, got %q", tt.keyword, tt.want, resp.Data[0].Name)
			}
		})
	}
}

// TestPageKeywordStillMatchesSubstrings guards the surrounding wildcards: escaping
// the keyword must not turn a substring search into an equality search.
func TestPageKeywordStillMatchesSubstrings(t *testing.T) {
	runtime := newBatchMutationEngine(t)
	exec := requireInitializedRuntime(t, runtime)
	seedKeywordSearchRows(t, runtime, "alice", "malice", "bob")

	query := Select(batchMutationUserColumns()...).
		From(batchMutationUser{}).
		Search(keywordSearchColumn(t)).
		MustBuild()

	resp, err := query.Page(context.Background(), exec, &PageRequest{Page: 1, Size: 10, Keyword: "lic"})
	if err != nil {
		t.Fatalf("page query: %v", err)
	}

	if resp.Total != 2 {
		t.Fatalf("expected substring keyword to match two rows, got %d", resp.Total)
	}
}

// TestKeywordPredicateCarriesEscapeClause pins the rendered SQL so that the escape
// clause cannot be dropped from the predicate while the keyword is still escaped.
func TestKeywordPredicateCarriesEscapeClause(t *testing.T) {
	query := Select(batchMutationUserColumns()...).
		From(batchMutationUser{}).
		Search(keywordSearchColumn(t)).
		MustBuild()

	for _, sqlText := range []string{query.KeywordListSQL(), query.KeywordCountSQL()} {
		if !strings.Contains(sqlText, "LIKE ?"+keywordLikeEscapeClause) {
			t.Fatalf("expected keyword predicate to declare its escape character, got %q", sqlText)
		}
	}
}
