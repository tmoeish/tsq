package tsq

import (
	"errors"
	"fmt"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
)

// Text is the set of column value types the text functions accept. A nullable
// column's value type is its non-NULL type, so NullColumn[O, string] is text too.
type Text interface {
	~string
}

// Number is the set of column value types the numeric functions accept.
type Number interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~float32 | ~float64
}

// derived builds the expression that wraps col. It keeps no scan target: what the
// expression holds is not what col's field holds, so it is not selectable on its
// own (MapInto or SelectValue says where it goes).
func derived[T any](col SQLColumn, info exprInfo) Expression[T] {
	if isNilValue(col) || col.core() == nil {
		return exprImpl[T]{c: &columnCore{info: exprInfo{err: errors.New("column cannot be nil")}}}
	}

	next := *col.core()
	next.info = info
	next.plain = false
	next.scan = nil
	next.nullable = false

	return exprImpl[T]{c: &next}
}

func wrapped[T any](col SQLColumn, open, close string, aggregate bool) Expression[T] {
	info := columnInfo(col)
	info = info.withSQL(sqlJoin(sqlText(open), info.sql, sqlText(close)))
	info.aggregate = info.aggregate || aggregate

	// SUM, AVG, MAX and MIN are NULL over no rows; COUNT is 0.
	info.null.emptyGroup = info.null.emptyGroup || aggregate

	return derived[T](col, info)
}

func counted[T any](col SQLColumn, open string) Expression[int64] {
	info := columnInfo(col)
	info = info.withSQL(sqlJoin(sqlText(open), info.sql, sqlText(")")))
	info.aggregate = true
	info.null = nullness{}

	return derived[int64](col, info)
}

func byDialect[T any](col SQLColumn, feature string, spell func(x sqlExpr) map[tsqdialect.Name]sqlExpr) Expression[T] {
	info := columnInfo(col)

	return derived[T](col, info.withSQL(sqlByDialect(feature, spell(info.sql))))
}

// Count counts the non-NULL values of col.
func Count[T any](col Expression[T]) Expression[int64] {
	return counted[T](col, "COUNT(")
}

// CountDistinct counts the distinct non-NULL values of col. For a whole DISTINCT
// query use SelectDistinct.
func CountDistinct[T any](col Expression[T]) Expression[int64] {
	return counted[T](col, "COUNT(DISTINCT ")
}

// Max is the largest value of col.
func Max[T any](col Expression[T]) Expression[T] { return wrapped[T](col, "MAX(", ")", true) }

// Min is the smallest value of col.
func Min[T any](col Expression[T]) Expression[T] { return wrapped[T](col, "MIN(", ")", true) }

// Sum adds up col.
func Sum[N Number](col Expression[N]) Expression[N] {
	return wrapped[N](col, "SUM(", ")", true)
}

// Avg is the mean of col.
func Avg[N Number](col Expression[N]) Expression[float64] {
	return wrapped[float64](col, "AVG(", ")", true)
}

// Round rounds col to precision decimal places; PostgreSQL rounds through NUMERIC.
func Round[N Number](col Expression[N], precision int) Expression[N] {
	if precision < 0 {
		return derived[N](col, exprInfo{err: errors.New("round precision cannot be negative")})
	}

	n := sqlText(fmt.Sprintf(", %d)", precision))

	return byDialect[N](col, "round", func(x sqlExpr) map[tsqdialect.Name]sqlExpr {
		return map[tsqdialect.Name]sqlExpr{
			tsqdialect.MySQL:    sqlJoin(sqlText("ROUND("), x, n),
			tsqdialect.Postgres: sqlJoin(sqlText("ROUND(CAST("), x, sqlText(" AS NUMERIC)"), n),
			tsqdialect.SQLite:   sqlJoin(sqlText("ROUND("), x, n),
		}
	})
}

// Ceil rounds col up.
func Ceil[N Number](col Expression[N]) Expression[N] {
	return wrapped[N](col, "CEIL(", ")", false)
}

// Floor rounds col down.
func Floor[N Number](col Expression[N]) Expression[N] {
	return wrapped[N](col, "FLOOR(", ")", false)
}

// Abs is the absolute value of col.
func Abs[N Number](col Expression[N]) Expression[N] {
	return wrapped[N](col, "ABS(", ")", false)
}

// Upper upper-cases col. SQLite changes ASCII letters only.
func Upper[S Text](col Expression[S]) Expression[S] {
	return wrapped[S](col, "UPPER(", ")", false)
}

// Lower lower-cases col. SQLite changes ASCII letters only.
func Lower[S Text](col Expression[S]) Expression[S] {
	return wrapped[S](col, "LOWER(", ")", false)
}

// Trim removes leading and trailing spaces from col.
func Trim[S Text](col Expression[S]) Expression[S] {
	return wrapped[S](col, "TRIM(", ")", false)
}

// Length counts the characters of col (CHAR_LENGTH on MySQL, where LENGTH counts
// bytes).
func Length[S Text](col Expression[S]) Expression[int64] {
	return byDialect[int64](col, "length", func(x sqlExpr) map[tsqdialect.Name]sqlExpr {
		return map[tsqdialect.Name]sqlExpr{
			tsqdialect.MySQL:    sqlJoin(sqlText("CHAR_LENGTH("), x, sqlText(")")),
			tsqdialect.Postgres: sqlJoin(sqlText("LENGTH("), x, sqlText(")")),
			tsqdialect.SQLite:   sqlJoin(sqlText("LENGTH("), x, sqlText(")")),
		}
	})
}

// Substring takes length characters of col from the 1-based start.
func Substring[S Text](col Expression[S], start, length int) Expression[S] {
	if start < 1 || length < 0 {
		return derived[S](col, exprInfo{err: fmt.Errorf("invalid substring range start=%d length=%d", start, length)})
	}

	// The bounds come from the program, so they are written into the SQL: bound,
	// PostgreSQL cannot always pick a substring overload for them.
	return wrapped[S](col, "SUBSTR(", fmt.Sprintf(", %d, %d)", start, length), false)
}

// sqliteTimeText keeps the "YYYY-MM-DD HH:MM:SS" prefix of a stored time. The
// modernc driver writes time.Time in Go's String format by default
// ("2026-03-04 05:06:07 +0000 UTC"), which SQLite's date functions reject; its
// "sqlite" format and ISO text share the same prefix, so this reads all of them.
func sqliteTimeText(x sqlExpr) sqlExpr {
	return sqlJoin(sqlText("SUBSTR("), x, sqlText(", 1, 19)"))
}

// Date formats the date part of col as 'YYYY-MM-DD' on every dialect.
func Date[T any](col Expression[T]) Expression[string] {
	return byDialect[string](col, "date", func(x sqlExpr) map[tsqdialect.Name]sqlExpr {
		return map[tsqdialect.Name]sqlExpr{
			tsqdialect.MySQL:    sqlJoin(sqlText("DATE_FORMAT("), x, sqlText(", '%Y-%m-%d')")),
			tsqdialect.Postgres: sqlJoin(sqlText("TO_CHAR("), x, sqlText(", 'YYYY-MM-DD')")),
			tsqdialect.SQLite:   sqlJoin(sqlText("DATE("), sqliteTimeText(x), sqlText(")")),
		}
	})
}

// Year extracts the year of col as an integer.
func Year[T any](col Expression[T]) Expression[int64] { return datePart(col, "year", "YEAR", "%Y") }

// Month extracts the month of col as an integer.
func Month[T any](col Expression[T]) Expression[int64] {
	return datePart(col, "month", "MONTH", "%m")
}

// Day extracts the day of the month of col as an integer.
func Day[T any](col Expression[T]) Expression[int64] { return datePart(col, "day", "DAY", "%d") }

func datePart[T any](col Expression[T], part, sqlPart, strftime string) Expression[int64] {
	return byDialect[int64](col, part+" extraction", func(x sqlExpr) map[tsqdialect.Name]sqlExpr {
		return map[tsqdialect.Name]sqlExpr{
			tsqdialect.MySQL:    sqlJoin(sqlText(sqlPart+"("), x, sqlText(")")),
			tsqdialect.Postgres: sqlJoin(sqlText("CAST(EXTRACT("+sqlPart+" FROM "), x, sqlText(") AS BIGINT)")),
			tsqdialect.SQLite:   sqlJoin(sqlText("CAST(strftime('"+strftime+"', "), sqliteTimeText(x), sqlText(") AS INTEGER)")),
		}
	})
}

// Coalesce is col, or fallback where col is NULL: a column, Param, Val or subquery.
// It is NULL only if both can be, so Coalesce(col, tsq.Val(x)) reads into a field
// that cannot hold NULL.
func Coalesce[T any](col Expression[T], fallback Operand[T]) Expression[T] {
	return combined[T](col, "COALESCE(", fallback, func(left, right nullness) nullness {
		if left.never() || right.never() {
			return nullness{}
		}

		return left.or(right)
	})
}

// NullIf is col, or NULL where col equals value.
func NullIf[T any](col Expression[T], value Operand[T]) Expression[T] {
	return combined[T](col, "NULLIF(", value, func(left, right nullness) nullness {
		n := left.or(right)
		n.always = true

		return n
	})
}

func combined[T any](col Expression[T], open string, rhs Operand[T], null func(left, right nullness) nullness) Expression[T] {
	left := columnInfo(col)
	right := rhsInfo(rhs)
	info := left.merge(right).withSQL(sqlJoin(sqlText(open), left.sql, sqlText(", "), right.sql, sqlText(")")))
	info.null = null(left.null, right.null)

	return derived[T](col, info)
}

func pattern[S ~string](col Expression[S], op string, right exprInfo) Condition {
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
	needsTsqVal()
	patternText(S)
	patternOperand(mode paramMode) exprInfo
}

func patternMatch[S ~string](col Expression[S], op string, text Pattern[S], mode paramMode) Condition {
	if isNilValue(text) {
		return conditionError(errors.New("pattern cannot be nil"))
	}

	return pattern(col, op, text.patternOperand(mode))
}

// StartsWith matches values of col beginning with prefix.
func StartsWith[S ~string](col Expression[S], prefix Pattern[S]) Condition {
	return patternMatch(col, "LIKE", prefix, paramPrefix)
}

// NotStartsWith matches values of col not beginning with prefix.
func NotStartsWith[S ~string](col Expression[S], prefix Pattern[S]) Condition {
	return patternMatch(col, "NOT LIKE", prefix, paramPrefix)
}

// EndsWith matches values of col ending with suffix.
func EndsWith[S ~string](col Expression[S], suffix Pattern[S]) Condition {
	return patternMatch(col, "LIKE", suffix, paramSuffix)
}

// NotEndsWith matches values of col not ending with suffix.
func NotEndsWith[S ~string](col Expression[S], suffix Pattern[S]) Condition {
	return patternMatch(col, "NOT LIKE", suffix, paramSuffix)
}

// Contains matches values of col containing part.
func Contains[S ~string](col Expression[S], part Pattern[S]) Condition {
	return patternMatch(col, "LIKE", part, paramContains)
}

// NotContains matches values of col not containing part.
func NotContains[S ~string](col Expression[S], part Pattern[S]) Condition {
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
