package tsq

import (
	"reflect"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
)

// Add is a + b.
func Add[N Number](a Expression[N], b Operand[N]) Expression[N] {
	return arithmetic(a, " + ", b)
}

// Sub is a - b: tsq.UpdateTable(t).Set(t.Stock, tsq.Sub(t.Stock, qty)).
func Sub[N Number](a Expression[N], b Operand[N]) Expression[N] {
	return arithmetic(a, " - ", b)
}

// Mul is a * b.
func Mul[N Number](a Expression[N], b Operand[N]) Expression[N] {
	return arithmetic(a, " * ", b)
}

// Div is a / b. An integer N divides as integers on every dialect, truncating
// toward zero: DIV on MySQL, where / returns a decimal, and DIV() on PostgreSQL,
// where an operand can be NUMERIC (SUM of an integer column, a uint64 column) and
// / keeps the fraction. A floating-point N divides exactly. Division by zero is NULL on MySQL and SQLite and an error on
// PostgreSQL, so the quotient can be NULL unless b is a non-zero tsq.Val: read it
// into a nullable field, or Coalesce it.
func Div[N Number](a Expression[N], b Operand[N]) Expression[N] {
	left := columnInfo(a)
	right := rhsInfo(b)
	info := left.merge(right)

	quotient := sqlJoin(sqlText("("), left.sql, sqlText(" / "), right.sql, sqlText(")"))

	var zero N
	if kind := reflect.TypeOf(zero).Kind(); kind != reflect.Float32 && kind != reflect.Float64 {
		quotient = sqlByDialect("integer division", map[tsqdialect.Name]sqlExpr{
			tsqdialect.MySQL:    sqlJoin(sqlText("("), left.sql, sqlText(" DIV "), right.sql, sqlText(")")),
			tsqdialect.Postgres: sqlJoin(sqlText("DIV("), left.sql, sqlText(", "), right.sql, sqlText(")")),
			tsqdialect.SQLite:   quotient,
		})
	}

	if v, ok := b.(Value[N]); !ok || v.v == 0 {
		info.null.always = true
	}

	return derived[N](a, info.withSQL(quotient))
}

func arithmetic[N Number](a Expression[N], op string, b Operand[N]) Expression[N] {
	left := columnInfo(a)
	right := rhsInfo(b)
	info := left.merge(right)

	return derived[N](a, info.withSQL(sqlJoin(sqlText("("), left.sql, sqlText(op), right.sql, sqlText(")"))))
}
