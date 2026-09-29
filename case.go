package tsq

import (
	"errors"
	"reflect"
	"slices"
	"time"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
)

// CaseStage builds a searched CASE expression holding a T.
type CaseStage[T any] interface {
	sealedStage()

	// When adds WHEN cond THEN result: a column, Param, Val or subquery.
	When(cond Condition, result Operand[T]) CaseStage[T]
	// Else sets the ELSE result; End follows it.
	Else(result Operand[T]) CaseElseStage[T]
	// End finishes the expression. Project it into a result with MapInto.
	End() Expression[T]
}

// CaseElseStage is a CASE expression with its ELSE result, which only ends.
type CaseElseStage[T any] interface {
	sealedStage()

	// End finishes the expression.
	End() Expression[T]
}

// Case starts a searched CASE expression with its first branch, WHEN cond THEN
// result; T is the result's type.
func Case[T any](cond Condition, result Operand[T]) CaseStage[T] {
	return caseBuilder[T]{}.When(cond, result)
}

type caseBuilder[T any] struct {
	info     exprInfo
	whens    []caseWhen
	elseExpr *sqlExpr
	// typed reports a result that gives the CASE its type: one that reads a
	// column or a subquery, not only bound values.
	typed bool
}

type caseWhen struct{ cond, result sqlExpr }

func (caseBuilder[T]) sealedStage() {}

func (b caseBuilder[T]) branch(cond Condition, result exprInfo) CaseStage[T] {
	ci := conditionInfo(cond)
	ci.null = nullness{} // a condition decides the branch; only results make the CASE NULL
	b.info = b.info.merge(ci).merge(result)
	b.whens = append(slices.Clone(b.whens), caseWhen{cond: ci.sql, result: result.sql})
	b.typed = b.typed || len(result.tables) > 0

	return b
}

func (b caseBuilder[T]) When(cond Condition, result Operand[T]) CaseStage[T] {
	return b.branch(cond, rhsInfo(result))
}

func (b caseBuilder[T]) otherwise(result exprInfo) CaseElseStage[T] {
	b.info = b.info.merge(result)
	b.elseExpr = &result.sql
	b.typed = b.typed || len(result.tables) > 0

	return b
}

func (b caseBuilder[T]) Else(result Operand[T]) CaseElseStage[T] { return b.otherwise(rhsInfo(result)) }

func (b caseBuilder[T]) End() Expression[T] {
	info := b.info
	if len(b.whens) == 0 {
		info.err = errors.Join(info.err, errors.New("case expression needs at least one WHEN"))
	}

	info.sql = b.render(func(result sqlExpr) sqlExpr { return result })

	// PostgreSQL types a CASE whose results are all bound values as text: 10
	// sorted before 9, and pgx cannot bind an int64 as text. The results are cast
	// to the type T holds there.
	if pg := postgresTypeOf(reflect.TypeFor[T]()); !b.typed && pg != "" {
		plain := info.sql
		cast := b.render(func(result sqlExpr) sqlExpr {
			return sqlJoin(sqlText("CAST("), result, sqlText(" AS "+pg+")"))
		})

		info.sql = sqlByDialect("case", map[tsqdialect.Name]sqlExpr{
			tsqdialect.MySQL:    plain,
			tsqdialect.Postgres: cast,
			tsqdialect.SQLite:   plain,
		})
	}

	// Without ELSE, a row no branch matches is NULL.
	if b.elseExpr == nil {
		info.null.always = true
	}

	core := &columnCore{name: "case", info: info}

	// The expression belongs to the first table it references, in name order, so
	// that the choice does not depend on map iteration.
	names := make([]string, 0, len(info.tables))
	for name := range info.tables {
		names = append(names, name)
	}

	slices.Sort(names)

	if len(names) > 0 {
		core.table = info.tables[names[0]]
	}

	return exprImpl[T]{c: core}
}

// render writes the CASE, passing each result through result.
func (b caseBuilder[T]) render(result func(sqlExpr) sqlExpr) sqlExpr {
	parts := []sqlExpr{sqlText("CASE")}
	for _, w := range b.whens {
		parts = append(parts, sqlText(" WHEN "), w.cond, sqlText(" THEN "), result(w.result))
	}

	if b.elseExpr != nil {
		parts = append(parts, sqlText(" ELSE "), result(*b.elseExpr))
	}

	return sqlJoin(append(parts, sqlText(" END"))...)
}

// postgresTypeOf is the PostgreSQL type a value of t binds as, or "" when TSQ does
// not know it (the result is then left to PostgreSQL).
func postgresTypeOf(t reflect.Type) string {
	switch {
	case t == reflect.TypeFor[time.Time]():
		return "TIMESTAMP"
	case t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8:
		return "BYTEA"
	}

	switch t.Kind() {
	case reflect.Bool:
		return "BOOLEAN"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return "BIGINT"
	case reflect.Uint, reflect.Uint64:
		return "NUMERIC(20)"
	case reflect.Float32, reflect.Float64:
		return "DOUBLE PRECISION"
	case reflect.String:
		return "TEXT"
	default:
		return ""
	}
}
