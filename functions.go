package tsq

import (
	"database/sql"
	"errors"
	"fmt"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
)

// Text is the set of column value types the text functions accept.
type Text interface {
	~string | sql.NullString
}

// Number is the set of column value types the numeric functions accept.
type Number interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~float32 | ~float64 |
		sql.NullInt16 | sql.NullInt32 | sql.NullInt64 | sql.NullByte | sql.NullFloat64
}

func derived[O, T any](col SQLColumn, info exprInfo) Column[O, T] {
	if isNilValue(col) || col.core() == nil {
		return columnImpl[O, T]{c: &columnCore{info: exprInfo{err: errors.New("column cannot be nil")}}}
	}

	next := *col.core()
	next.info = info
	next.plain = false

	return columnImpl[O, T]{c: &next}
}

func wrapped[O, T any](col SQLColumn, open, close string, aggregate bool) Column[O, T] {
	info := columnInfo(col)
	info = info.withSQL(sqlJoin(sqlText(open), info.sql, sqlText(close)))
	info.aggregate = info.aggregate || aggregate

	return derived[O, T](col, info)
}

func byDialect[O, T any](col SQLColumn, feature string, spell func(x sqlExpr) map[tsqdialect.Name]sqlExpr) Column[O, T] {
	info := columnInfo(col)

	return derived[O, T](col, info.withSQL(sqlByDialect(feature, spell(info.sql))))
}

// Count counts the non-NULL values of col.
func Count[O, T any](col Column[O, T]) Column[O, int64] {
	return wrapped[O, int64](col, "COUNT(", ")", true)
}

// CountDistinct counts the distinct non-NULL values of col. For a whole DISTINCT
// query use SelectDistinct.
func CountDistinct[O, T any](col Column[O, T]) Column[O, int64] {
	return wrapped[O, int64](col, "COUNT(DISTINCT ", ")", true)
}

// Max is the largest value of col.
func Max[O, T any](col Column[O, T]) Column[O, T] { return wrapped[O, T](col, "MAX(", ")", true) }

// Min is the smallest value of col.
func Min[O, T any](col Column[O, T]) Column[O, T] { return wrapped[O, T](col, "MIN(", ")", true) }

// Sum adds up col.
func Sum[O any, N Number](col Column[O, N]) Column[O, N] {
	return wrapped[O, N](col, "SUM(", ")", true)
}

// Avg is the mean of col.
func Avg[O any, N Number](col Column[O, N]) Column[O, float64] {
	return wrapped[O, float64](col, "AVG(", ")", true)
}

// Round rounds col to precision decimal places; PostgreSQL rounds through NUMERIC.
func Round[O any, N Number](col Column[O, N], precision int) Column[O, N] {
	if precision < 0 {
		return derived[O, N](col, exprInfo{err: errors.New("round precision cannot be negative")})
	}

	n := sqlText(fmt.Sprintf(", %d)", precision))

	return byDialect[O, N](col, "round", func(x sqlExpr) map[tsqdialect.Name]sqlExpr {
		return map[tsqdialect.Name]sqlExpr{
			tsqdialect.MySQL:    sqlJoin(sqlText("ROUND("), x, n),
			tsqdialect.Postgres: sqlJoin(sqlText("ROUND(CAST("), x, sqlText(" AS NUMERIC)"), n),
			tsqdialect.SQLite:   sqlJoin(sqlText("ROUND("), x, n),
		}
	})
}

// Ceil rounds col up.
func Ceil[O any, N Number](col Column[O, N]) Column[O, N] {
	return wrapped[O, N](col, "CEIL(", ")", false)
}

// Floor rounds col down.
func Floor[O any, N Number](col Column[O, N]) Column[O, N] {
	return wrapped[O, N](col, "FLOOR(", ")", false)
}

// Abs is the absolute value of col.
func Abs[O any, N Number](col Column[O, N]) Column[O, N] {
	return wrapped[O, N](col, "ABS(", ")", false)
}

// Upper upper-cases col. SQLite changes ASCII letters only.
func Upper[O any, S Text](col Column[O, S]) Column[O, S] {
	return wrapped[O, S](col, "UPPER(", ")", false)
}

// Lower lower-cases col. SQLite changes ASCII letters only.
func Lower[O any, S Text](col Column[O, S]) Column[O, S] {
	return wrapped[O, S](col, "LOWER(", ")", false)
}

// Trim removes leading and trailing spaces from col.
func Trim[O any, S Text](col Column[O, S]) Column[O, S] {
	return wrapped[O, S](col, "TRIM(", ")", false)
}

// Length counts the characters of col (CHAR_LENGTH on MySQL, where LENGTH counts
// bytes).
func Length[O any, S Text](col Column[O, S]) Column[O, int64] {
	return byDialect[O, int64](col, "length", func(x sqlExpr) map[tsqdialect.Name]sqlExpr {
		return map[tsqdialect.Name]sqlExpr{
			tsqdialect.MySQL:    sqlJoin(sqlText("CHAR_LENGTH("), x, sqlText(")")),
			tsqdialect.Postgres: sqlJoin(sqlText("LENGTH("), x, sqlText(")")),
			tsqdialect.SQLite:   sqlJoin(sqlText("LENGTH("), x, sqlText(")")),
		}
	})
}

// Substring takes length characters of col from the 1-based start.
func Substring[O any, S Text](col Column[O, S], start, length int) Column[O, S] {
	if start < 1 || length < 0 {
		return derived[O, S](col, exprInfo{err: fmt.Errorf("invalid substring range start=%d length=%d", start, length)})
	}

	// The bounds come from the program, so they are written into the SQL: bound,
	// PostgreSQL cannot always pick a substring overload for them.
	return wrapped[O, S](col, "SUBSTR(", fmt.Sprintf(", %d, %d)", start, length), false)
}

// sqliteTimeText keeps the "YYYY-MM-DD HH:MM:SS" prefix of a stored time. The
// modernc driver writes time.Time in Go's String format by default
// ("2026-03-04 05:06:07 +0000 UTC"), which SQLite's date functions reject; its
// "sqlite" format and ISO text share the same prefix, so this reads all of them.
func sqliteTimeText(x sqlExpr) sqlExpr {
	return sqlJoin(sqlText("SUBSTR("), x, sqlText(", 1, 19)"))
}

// Date formats the date part of col as 'YYYY-MM-DD' on every dialect.
func Date[O, T any](col Column[O, T]) Column[O, string] {
	return byDialect[O, string](col, "date", func(x sqlExpr) map[tsqdialect.Name]sqlExpr {
		return map[tsqdialect.Name]sqlExpr{
			tsqdialect.MySQL:    sqlJoin(sqlText("DATE_FORMAT("), x, sqlText(", '%Y-%m-%d')")),
			tsqdialect.Postgres: sqlJoin(sqlText("TO_CHAR("), x, sqlText(", 'YYYY-MM-DD')")),
			tsqdialect.SQLite:   sqlJoin(sqlText("DATE("), sqliteTimeText(x), sqlText(")")),
		}
	})
}

// Year extracts the year of col as an integer.
func Year[O, T any](col Column[O, T]) Column[O, int64] { return datePart(col, "year", "YEAR", "%Y") }

// Month extracts the month of col as an integer.
func Month[O, T any](col Column[O, T]) Column[O, int64] {
	return datePart(col, "month", "MONTH", "%m")
}

// Day extracts the day of the month of col as an integer.
func Day[O, T any](col Column[O, T]) Column[O, int64] { return datePart(col, "day", "DAY", "%d") }

func datePart[O, T any](col Column[O, T], part, sqlPart, strftime string) Column[O, int64] {
	return byDialect[O, int64](col, part+" extraction", func(x sqlExpr) map[tsqdialect.Name]sqlExpr {
		return map[tsqdialect.Name]sqlExpr{
			tsqdialect.MySQL:    sqlJoin(sqlText(sqlPart+"("), x, sqlText(")")),
			tsqdialect.Postgres: sqlJoin(sqlText("CAST(EXTRACT("+sqlPart+" FROM "), x, sqlText(") AS BIGINT)")),
			tsqdialect.SQLite:   sqlJoin(sqlText("CAST(strftime('"+strftime+"', "), sqliteTimeText(x), sqlText(") AS INTEGER)")),
		}
	})
}

// Coalesce is col, or fallback where col is NULL: a column, Param, Val or subquery.
func Coalesce[O, T any](col Column[O, T], fallback RHS[T]) Column[O, T] {
	return combined[O, T](col, "COALESCE(", fallback)
}

// NullIf is col, or NULL where col equals value.
func NullIf[O, T any](col Column[O, T], value RHS[T]) Column[O, T] {
	return combined[O, T](col, "NULLIF(", value)
}

func combined[O, T any](col Column[O, T], open string, rhs RHS[T]) Column[O, T] {
	return combinedInfo[O, T](col, open, rhsInfo(rhs))
}

func combinedInfo[O, T any](col Column[O, T], open string, right exprInfo) Column[O, T] {
	left := columnInfo(col)
	info := left.merge(right).withSQL(sqlJoin(sqlText(open), left.sql, sqlText(", "), right.sql, sqlText(")")))

	return derived[O, T](col, info)
}

func pattern[O any, S ~string](col Column[O, S], op string, right exprInfo) Condition {
	left := columnInfo(col)

	return newCondition(left.merge(right).withSQL(sqlJoin(left.sql, sqlText(" "+op+" "), right.sql)))
}

func patternValue(s string, mode paramMode) exprInfo {
	s = escapeLikePattern(s)

	switch mode {
	case paramPrefix:
		s += "%"
	case paramSuffix:
		s = "%" + s
	default:
		s = "%" + s + "%"
	}

	return exprInfo{sql: sqlJoin(sqlValue(s), sqlText(likeEscapeClause))}
}

// Pattern is the text StartsWith, EndsWith and Contains match literally: a Val or
// a Param of the column's type. Its % and _ are escaped, so they match themselves.
type Pattern[S ~string] interface {
	patternText(S)
	patternOperand(mode paramMode) exprInfo
}

func patternMatch[O any, S ~string](col Column[O, S], op string, text Pattern[S], mode paramMode) Condition {
	if isNilValue(text) {
		return conditionError(errors.New("pattern cannot be nil"))
	}

	return pattern(col, op, text.patternOperand(mode))
}

// StartsWith matches values of col beginning with prefix.
func StartsWith[O any, S ~string](col Column[O, S], prefix Pattern[S]) Condition {
	return patternMatch(col, "LIKE", prefix, paramPrefix)
}

// NotStartsWith matches values of col not beginning with prefix.
func NotStartsWith[O any, S ~string](col Column[O, S], prefix Pattern[S]) Condition {
	return patternMatch(col, "NOT LIKE", prefix, paramPrefix)
}

// EndsWith matches values of col ending with suffix.
func EndsWith[O any, S ~string](col Column[O, S], suffix Pattern[S]) Condition {
	return patternMatch(col, "LIKE", suffix, paramSuffix)
}

// NotEndsWith matches values of col not ending with suffix.
func NotEndsWith[O any, S ~string](col Column[O, S], suffix Pattern[S]) Condition {
	return patternMatch(col, "NOT LIKE", suffix, paramSuffix)
}

// Contains matches values of col containing part.
func Contains[O any, S ~string](col Column[O, S], part Pattern[S]) Condition {
	return patternMatch(col, "LIKE", part, paramContains)
}

// NotContains matches values of col not containing part.
func NotContains[O any, S ~string](col Column[O, S], part Pattern[S]) Condition {
	return patternMatch(col, "NOT LIKE", part, paramContains)
}

type searchColumn struct {
	SQLColumn
}

func (searchColumn) searchable() {}

// Searchable marks a text column for keyword search (TableSpec.Search, Search).
func Searchable[O any, S ~string](col Column[O, S]) SearchColumn {
	return searchColumn{SQLColumn: col}
}
