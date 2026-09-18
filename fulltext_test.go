package tsq

import (
	"strings"
	"testing"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
)

func TestMatchesIsSpelledPerDialect(t *testing.T) {
	q := Select(Note_ID).From(Notes).Where(Matches(Notes.FullText(), Val("hello world"))).MustBuild()

	want := map[tsqdialect.Name]string{
		tsqdialect.MySQL:    "MATCH(`notes`.`title`, `notes`.`body`) AGAINST (? IN NATURAL LANGUAGE MODE)",
		tsqdialect.Postgres: `to_tsvector('simple', coalesce("notes"."title", '') || ' ' || coalesce("notes"."body", '')) @@ plainto_tsquery('simple', $1)`,
		tsqdialect.SQLite:   `("notes"."title" LIKE ? ESCAPE '~' OR "notes"."body" LIKE ? ESCAPE '~')`,
	}

	for _, d := range []tsqdialect.Dialect{onMySQL, onPostgres, onSQLite} {
		sql, args, err := q.SQL(d)
		if err != nil {
			t.Fatalf("%s: %v", d.Name(), err)
		}

		if !strings.HasSuffix(sql, want[d.Name()]) {
			t.Errorf("%s:\n%s\nwant suffix\n%s", d.Name(), sql, want[d.Name()])
		}

		// The term is bound, and the substring fallback escapes it.
		if d.Name() == tsqdialect.SQLite {
			if len(args) != 2 || args[0] != "%hello world%" {
				t.Errorf("sqlite args = %v", args)
			}
		} else if len(args) != 1 || args[0] != "hello world" {
			t.Errorf("%s args = %v", d.Name(), args)
		}
	}
}

func TestFullTextRequiresADeclaredIndex(t *testing.T) {
	// Users declares no full-text index.
	if _, err := Select(User_ID).From(Users).Where(Matches(Users.FullText(), Val("x"))).Build(); err == nil ||
		!strings.Contains(err.Error(), "//tsq:fulltext") {
		t.Fatalf("missing index = %v", err)
	}

	if _, err := Select(Note_ID).From(Notes).Where(Matches(Notes.FullText("nope"), Val("x"))).Build(); err == nil ||
		!strings.Contains(err.Error(), "no full-text index named nope") {
		t.Fatalf("unknown name = %v", err)
	}

	// A Param works as the term, and the query knows which table it belongs to.
	term := NewParam[string]("term")
	if _, err := Select(Note_ID).From(Notes).Where(Matches(Notes.FullText(), term)).Build(); err != nil {
		t.Fatalf("param term = %v", err)
	}

	if _, err := Select(User_ID).From(Users).Where(Matches(Notes.FullText(), Val("x"))).Build(); err == nil {
		t.Fatal("expected the index's table to be required in the query")
	}
}
