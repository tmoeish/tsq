package tsq

import (
	"errors"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
	sqld "github.com/tmoeish/tsq/v5/internal/sqldialect"
)

// FullTextIndex is a table's declared full-text index, from TableOf.FullText.
type FullTextIndex struct {
	table Table
	index TableIndex
	err   error
}

// MatchTerm is the term Matches looks for: a Val or a Param of a string.
type MatchTerm interface {
	Pattern[string]
	RHS[string]
}

// Matches is the full-text predicate of index: rows whose indexed columns match
// term.
//
// What "match" means is the dialect's own: MySQL runs MATCH ... AGAINST in natural
// language mode, PostgreSQL compares to_tsvector against plainto_tsquery (every
// word must appear), and SQLite, which has no full-text index TSQ can manage,
// matches term as a substring of any indexed column. Ranking and operator syntax
// are not portable; dialect.CapabilityFullTextSearch reports which kind a runtime
// gets.
func Matches(index FullTextIndex, term MatchTerm) Condition {
	switch {
	case index.err != nil:
		return conditionError(index.err)
	case isNilValue(index.table):
		return conditionError(errors.New("full-text index cannot be nil"))
	case isNilValue(term):
		return conditionError(errors.New("search term cannot be nil"))
	}

	info := exprInfo{tables: map[string]Table{index.table.TableName(): index.table}}
	raw := term.operand()
	like := term.patternOperand(paramContains)

	if raw.err != nil || like.err != nil {
		return conditionError(errors.Join(raw.err, like.err))
	}

	// The spelling is built with the dialect in use, so the quoting and the indexed
	// expression come from it rather than from a dialect this package picked.
	return newCondition(info.merge(raw).merge(like).withSQL(
		sqlForDialect("full-text search", func(d sqld.Dialect) (sqlExpr, bool) {
			switch d.Name() {
			case tsqdialect.MySQL:
				return matchAgainst(index, raw.sql), true
			case tsqdialect.Postgres:
				return textSearchMatch(d, index, raw.sql), true
			case tsqdialect.SQLite:
				return substringMatch(index, like.sql), true
			default:
				return sqlExpr{}, false
			}
		})))
}

// matchAgainst is MySQL's MATCH(cols) AGAINST (term), which needs the FULLTEXT
// index over exactly those columns.
func matchAgainst(index FullTextIndex, term sqlExpr) sqlExpr {
	cols := make([]sqlExpr, 0, len(index.index.Fields))
	for _, name := range index.index.Fields {
		cols = append(cols, columnRef(index.table, name))
	}

	return sqlJoin(sqlText("MATCH("), sqlList(", ", cols), sqlText(") AGAINST ("), term, sqlText(" IN NATURAL LANGUAGE MODE)"))
}

// textSearchMatch repeats the expression the GIN index holds, which is what lets
// PostgreSQL use it.
func textSearchMatch(d sqld.Dialect, index FullTextIndex, term sqlExpr) sqlExpr {
	quoted := make([]string, 0, len(index.index.Fields))
	for _, name := range index.index.Fields {
		quoted = append(quoted, d.QuoteIdent(index.table.TableName())+"."+d.QuoteIdent(name))
	}

	return sqlJoin(sqlText(d.FullTextVectorSQL(quoted)+" @@ plainto_tsquery('simple', "), term, sqlText(")"))
}

// substringMatch is the fallback where the dialect has no full-text index: the term
// has to appear in one of the columns, wildcards escaped.
func substringMatch(index FullTextIndex, pattern sqlExpr) sqlExpr {
	terms := make([]sqlExpr, 0, len(index.index.Fields))
	for _, name := range index.index.Fields {
		terms = append(terms, sqlJoin(columnRef(index.table, name), sqlText(" LIKE "), pattern))
	}

	if len(terms) == 1 {
		return terms[0]
	}

	return sqlJoin(sqlText("("), sqlList(" OR ", terms), sqlText(")"))
}
