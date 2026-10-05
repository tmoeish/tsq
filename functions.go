package tsq

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

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
	next.bare = false
	next.scan = nil
	next.adapt = nil
	next.nullable = false

	return exprImpl[T]{c: &next}
}

func wrapped[T any](col SQLColumn, open, close string, aggregate bool) Expression[T] {
	info := columnInfo(col)
	if aggregate && info.aggregate && info.err == nil {
		info.err = fmt.Errorf("%s cannot aggregate an aggregate; aggregate in a subquery or CTE first", strings.TrimSuffix(open, "("))
	}

	info = info.withSQL(sqlJoin(sqlText(open), info.sql, sqlText(close)))

	info.aggregate = info.aggregate || aggregate
	if aggregate {
		info.bare = nil
	}

	// SUM, AVG, MAX and MIN are NULL over no rows; COUNT is 0.
	info.null.emptyGroup = info.null.emptyGroup || aggregate

	return derived[T](col, info)
}

func counted[T any](col SQLColumn, open string) Expression[int64] {
	info := columnInfo(col)
	if info.aggregate && info.err == nil {
		info.err = fmt.Errorf("%s cannot aggregate an aggregate; aggregate in a subquery or CTE first", strings.TrimSuffix(open, "("))
	}

	info = info.withSQL(sqlJoin(sqlText(open), info.sql, sqlText(")")))
	info.aggregate = true
	info.bare = nil
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

// Max is the largest value of col; over a boolean, whether any row holds true.
func Max[T any](col Expression[T]) Expression[T] { return extreme[T](col, "MAX(", "BOOL_OR(") }

// Min is the smallest value of col; over a boolean, whether every row holds true.
func Min[T any](col Expression[T]) Expression[T] { return extreme[T](col, "MIN(", "BOOL_AND(") }

// extreme is MAX or MIN of col. PostgreSQL has neither over a boolean, where
// MySQL and SQLite order false before true: there the largest of a boolean is
// BOOL_OR and the smallest BOOL_AND, which give the same answer.
func extreme[T any](col Expression[T], open, overBool string) Expression[T] {
	plain := wrapped[T](col, open, ")", true)
	if reflect.TypeFor[T]().Kind() != reflect.Bool {
		return plain
	}

	x := columnInfo(col).sql

	return derived[T](plain, columnInfo(plain).withSQL(sqlByDialect("the largest or smallest of a boolean", map[tsqdialect.Name]sqlExpr{
		tsqdialect.MySQL:    sqlJoin(sqlText(open), x, sqlText(")")),
		tsqdialect.Postgres: sqlJoin(sqlText(overBool), x, sqlText(")")),
		tsqdialect.SQLite:   sqlJoin(sqlText(open), x, sqlText(")")),
	})))
}

// Sum adds up col.
func Sum[N Number](col Expression[N]) Expression[N] {
	return wrapped[N](col, "SUM(", ")", true)
}

// Avg is the mean of col. MySQL averages an integer column as a DECIMAL with four
// more digits (AVG of 1, 2, 2 is 1.6667); the column is averaged as a DOUBLE
// there, as PostgreSQL and SQLite give it.
func Avg[N Number](col Expression[N]) Expression[float64] {
	if isFloat[N]() {
		return wrapped[float64](col, "AVG(", ")", true)
	}

	floating := byDialect[N](col, "avg", func(x sqlExpr) map[tsqdialect.Name]sqlExpr {
		return map[tsqdialect.Name]sqlExpr{
			tsqdialect.MySQL:    sqlJoin(sqlText("("), x, sqlText(" + 0E0)")),
			tsqdialect.Postgres: x,
			tsqdialect.SQLite:   x,
		}
	})

	return wrapped[float64](floating, "AVG(", ")", true)
}

// isFloat reports a floating-point N.
func isFloat[N Number]() bool {
	kind := reflect.TypeFor[N]().Kind()

	return kind == reflect.Float32 || kind == reflect.Float64
}

// Round rounds col to precision decimal places, a tie away from zero: 2.5 is 3 and
// -2.5 is -3 on every dialect. PostgreSQL and MySQL round a floating-point value
// through an exact decimal: PostgreSQL has no ROUND(double precision, integer),
// and MySQL rounds a DOUBLE to the nearest even digit (2.5 is 2), so the same
// query gave another answer there. A value that is a tie only as it is written
// (1.005 is stored as 1.00499999999999989) is rounded up by the two, which round
// what is written, and down by SQLite, which rounds what is stored.
//
// An integer has nothing to round, and is the value of col as it is.
func Round[N Number](col Expression[N], precision int) Expression[N] {
	if precision < 0 {
		return derived[N](col, exprInfo{err: errors.New("round precision cannot be negative")})
	}

	if !isFloat[N]() {
		return whole(col)
	}

	n := sqlText(fmt.Sprintf(", %d)", precision))

	return byDialect[N](col, "round", func(x sqlExpr) map[tsqdialect.Name]sqlExpr {
		return map[tsqdialect.Name]sqlExpr{
			// A DECIMAL holds 35 digits before the point and CAST clamps a larger
			// value to it without an error; such a value has no fraction to round.
			tsqdialect.MySQL: sqlJoin(sqlText("(CASE WHEN ABS("), x, sqlText(") < 1E30 THEN ROUND(CAST("), x, sqlText(" AS DECIMAL(65,30))"), n,
				sqlText(" ELSE "), x, sqlText(" END)")),
			tsqdialect.Postgres: sqlJoin(sqlText("ROUND(CAST("), x, sqlText(" AS NUMERIC)"), n),
			tsqdialect.SQLite:   sqlJoin(sqlText("ROUND("), x, n),
		}
	})
}

// Ceil rounds col up. An integer is the value of col as it is.
func Ceil[N Number](col Expression[N]) Expression[N] {
	return towards[N](col, "CEIL(", "ceil", " + (", " > ")
}

// Floor rounds col down. An integer is the value of col as it is.
func Floor[N Number](col Expression[N]) Expression[N] {
	return towards[N](col, "FLOOR(", "floor", " - (", " < ")
}

// whole is Round, Ceil or Floor of an integer: the value itself. The engines'
// own functions answer in another type there (PostgreSQL's CEIL and SQLite's
// ROUND in a floating-point one, PostgreSQL's ROUND to two places as 7.00), which
// loses the digits of a large value, is not read back into an integer, and
// divides with a fraction.
func whole[N Number](col Expression[N]) Expression[N] {
	return derived[N](col, columnInfo(col))
}

// towards is CEIL or FLOOR of col. SQLite has the two only where it was built
// with its math functions, which mattn/go-sqlite3 leaves out by default ("no such
// function: CEIL"): there the value is cut to an integer and stepped by one where
// the cut moved it the other way, which every build computes. The answer is cast
// back, so that it divides as the floating-point value it is, and a value past
// 2^52, which has no fraction and can be past what an integer holds, is its own.
func towards[N Number](col Expression[N], open, feature, step, moved string) Expression[N] {
	if !isFloat[N]() {
		return whole(col)
	}

	return byDialect[N](col, feature, func(x sqlExpr) map[tsqdialect.Name]sqlExpr {
		plain := sqlJoin(sqlText(open), x, sqlText(")"))
		cut := sqlJoin(sqlText("CAST("), x, sqlText(" AS INTEGER)"))

		return map[tsqdialect.Name]sqlExpr{
			tsqdialect.MySQL:    plain,
			tsqdialect.Postgres: plain,
			tsqdialect.SQLite: sqlJoin(sqlText("(CASE WHEN ABS("), x, sqlText(") < 4503599627370496 THEN CAST("), cut, sqlText(step), x, sqlText(moved), cut,
				sqlText(") AS REAL) ELSE "), x, sqlText(" END)")),
		}
	})
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

// Date formats the date part of col as 'YYYY-MM-DD' on every dialect. The date
// functions take a time column, nullable or not.
func Date(col Expression[time.Time]) Expression[string] {
	return byDialect[string](col, "date", func(x sqlExpr) map[tsqdialect.Name]sqlExpr {
		return map[tsqdialect.Name]sqlExpr{
			tsqdialect.MySQL:    sqlJoin(sqlText("DATE_FORMAT("), x, sqlText(", '%Y-%m-%d')")),
			tsqdialect.Postgres: sqlJoin(sqlText("TO_CHAR("), x, sqlText(", 'YYYY-MM-DD')")),
			tsqdialect.SQLite:   sqlJoin(sqlText("DATE("), sqliteTimeText(x), sqlText(")")),
		}
	})
}

// Year extracts the year of col as an integer.
func Year(col Expression[time.Time]) Expression[int64] { return datePart(col, "year", "YEAR", "%Y") }

// Month extracts the month of col as an integer.
func Month(col Expression[time.Time]) Expression[int64] {
	return datePart(col, "month", "MONTH", "%m")
}

// Day extracts the day of the month of col as an integer.
func Day(col Expression[time.Time]) Expression[int64] { return datePart(col, "day", "DAY", "%d") }

func datePart(col Expression[time.Time], part, sqlPart, strftime string) Expression[int64] {
	return byDialect[int64](col, part+" extraction", func(x sqlExpr) map[tsqdialect.Name]sqlExpr {
		return map[tsqdialect.Name]sqlExpr{
			tsqdialect.MySQL:    sqlJoin(sqlText(sqlPart+"("), x, sqlText(")")),
			tsqdialect.Postgres: sqlJoin(sqlText("CAST(EXTRACT("+sqlPart+" FROM "), x, sqlText(") AS BIGINT)")),
			tsqdialect.SQLite:   sqlJoin(sqlText("CAST(strftime('"+strftime+"', "), sqliteTimeText(x), sqlText(") AS INTEGER)")),
		}
	})
}

// Coalesce is col, or fallback where col is NULL: a column, Param, Val or subquery.
// It is NULL only where both are, so Coalesce(col, tsq.Val(x)), and
// Coalesce(nullable, column) with a NOT NULL column, read into a field that
// cannot hold NULL.
func Coalesce[T any](col Expression[T], fallback Operand[T]) Expression[T] {
	return combined[T](col, "COALESCE(", fallback, func(left, right nullness) nullness {
		switch {
		case left.never() || right.never():
			return nullness{}
		case left.always:
			// NULL exactly where the fallback is: a nullable column over a NOT NULL
			// one was still called nullable, and the error said to use Coalesce.
			return right
		case right.always:
			return left
		default:
			return left.or(right)
		}
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

func patternOf[S Text](col Expression[S], op string, right exprInfo) Condition {
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
//
// The match is the database's LIKE, whose case sensitivity differs: SQLite ignores
// ASCII case, MySQL follows the column's collation (the default _ci ones ignore
// case), and PostgreSQL respects case. Keyword search (Search) matches the same
// way. Match Lower(col) against a lowercased pattern to get one answer on all
// three.
type Pattern[S Text] interface {
	needsTsqVal()
	patternText(S)
	patternOperand(mode paramMode) exprInfo
}

func patternMatch[S Text](col Expression[S], op string, text Pattern[S], mode paramMode) Condition {
	if isNilValue(text) {
		return conditionError(errors.New("pattern cannot be nil"))
	}

	return patternOf(col, op, text.patternOperand(mode))
}

// Like matches col against pattern as written: % and _ are wildcards, and how a
// wildcard is escaped is the database's (a backslash on MySQL and PostgreSQL,
// nothing on SQLite). StartsWith, EndsWith and Contains match text literally on
// every dialect.
func Like[S Text](col Expression[S], pattern Operand[S]) Condition {
	return patternOf(col, "LIKE", rhsInfo(pattern))
}

// NotLike matches values of col that do not match pattern; see Like.
func NotLike[S Text](col Expression[S], pattern Operand[S]) Condition {
	return patternOf(col, "NOT LIKE", rhsInfo(pattern))
}

// StartsWith matches values of col beginning with prefix.
func StartsWith[S Text](col Expression[S], prefix Pattern[S]) Condition {
	return patternMatch(col, "LIKE", prefix, paramPrefix)
}

// NotStartsWith matches values of col not beginning with prefix.
func NotStartsWith[S Text](col Expression[S], prefix Pattern[S]) Condition {
	return patternMatch(col, "NOT LIKE", prefix, paramPrefix)
}

// EndsWith matches values of col ending with suffix.
func EndsWith[S Text](col Expression[S], suffix Pattern[S]) Condition {
	return patternMatch(col, "LIKE", suffix, paramSuffix)
}

// NotEndsWith matches values of col not ending with suffix.
func NotEndsWith[S Text](col Expression[S], suffix Pattern[S]) Condition {
	return patternMatch(col, "NOT LIKE", suffix, paramSuffix)
}

// Contains matches values of col containing part.
func Contains[S Text](col Expression[S], part Pattern[S]) Condition {
	return patternMatch(col, "LIKE", part, paramContains)
}

// NotContains matches values of col not containing part.
func NotContains[S Text](col Expression[S], part Pattern[S]) Condition {
	return patternMatch(col, "NOT LIKE", part, paramContains)
}

type searchColumn struct {
	SQLColumn
}

func (searchColumn) searchable() {}

// Searchable marks a text column for keyword search (TableSpec.Search, Search).
func Searchable[O any, S Text](col Column[O, S]) SearchColumn {
	if isNilValue(col) {
		return searchColumn{SQLColumn: exprImpl[S]{c: &columnCore{info: exprInfo{err: errors.New("search column cannot be nil")}}}}
	}

	return searchColumn{SQLColumn: col}
}
