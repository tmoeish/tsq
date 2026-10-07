package tsq

import (
	"encoding/json"
	"fmt"
	"reflect"
	"unicode/utf8"

	sqld "github.com/tmoeish/tsq/v5/internal/sqldialect"
)

// valueFit is what a value written to a column must fit: the column, and the
// characters it holds where that is enforced.
type valueFit struct {
	column string
	size   int
}

// fitValue refuses, before the statement runs, a value that not every engine
// takes for the column, so that what passes on a SQLite development database
// passes in production too: a json.RawMessage that is not JSON, which MySQL and
// PostgreSQL refuse and SQLite stores as text; and on SQLite a string longer than
// the column, which MySQL and PostgreSQL refuse (strict mode on MySQL) where
// SQLite enforces no length at all. A comparison is not held to the column: a
// longer value there matches nothing, and that is an answer.
func fitValue(d sqld.Dialect, fit *valueFit, v any) error {
	bound := bindValue(v)

	if raw, ok := bound.(json.RawMessage); ok && len(raw) > 0 && !json.Valid(raw) {
		return fmt.Errorf("%s is not valid JSON: %s", fitTarget(fit), truncated(string(raw)))
	}

	if fit == nil || fit.size <= 0 || d == nil || d.Name() != sqld.SQLite {
		return nil
	}

	rv := reflect.ValueOf(bound)
	if rv.Kind() != reflect.String {
		return nil
	}

	if n := utf8.RuneCountInString(rv.String()); n > fit.size {
		return fmt.Errorf("the value for %s is %d characters and the column holds %d; SQLite would store it, MySQL and PostgreSQL refuse it",
			fit.column, n, fit.size)
	}

	return nil
}

func fitTarget(fit *valueFit) string {
	if fit == nil || fit.column == "" {
		return "the value"
	}

	return "the value for " + fit.column
}

func truncated(s string) string {
	if len(s) > 40 {
		return s[:40] + "…"
	}

	return s
}
