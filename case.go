package tsq

import (
	"errors"
	"slices"
)

// CaseStage builds a searched CASE expression holding a T.
type CaseStage[T any] interface {
	// When adds WHEN cond THEN result: a column, Param or subquery.
	When(cond Condition, result RHS[T]) CaseStage[T]
	// WhenVal adds WHEN cond THEN value, with value bound.
	WhenVal(cond Condition, value T) CaseStage[T]
	// Else sets the ELSE result.
	Else(result RHS[T]) CaseStage[T]
	// ElseVal sets the ELSE result to a bound value.
	ElseVal(value T) CaseStage[T]
	// End finishes the expression. Project it into a result with MapInto.
	End() ValueColumn[T]
}

// Case starts a searched CASE expression.
func Case[T any]() CaseStage[T] {
	return caseBuilder[T]{}
}

type caseBuilder[T any] struct {
	info     exprInfo
	branches []sqlExpr
	elseExpr *sqlExpr
}

func (b caseBuilder[T]) branch(cond Condition, result exprInfo) CaseStage[T] {
	ci := conditionInfo(cond)
	b.info = b.info.merge(ci).merge(result)
	b.branches = append(slices.Clone(b.branches),
		sqlJoin(sqlText(" WHEN "), ci.sql, sqlText(" THEN "), result.sql))

	return b
}

func (b caseBuilder[T]) When(cond Condition, result RHS[T]) CaseStage[T] {
	return b.branch(cond, rhsInfo(result))
}

func (b caseBuilder[T]) WhenVal(cond Condition, value T) CaseStage[T] {
	return b.branch(cond, operandOf(value))
}

func (b caseBuilder[T]) ElseVal(value T) CaseStage[T] { return b.otherwise(operandOf(value)) }

func (b caseBuilder[T]) otherwise(result exprInfo) CaseStage[T] {
	if b.elseExpr != nil {
		b.info.err = errors.Join(b.info.err, errors.New("case expression has two ELSE results"))
		return b
	}

	b.info = b.info.merge(result)
	b.elseExpr = &result.sql

	return b
}

func (b caseBuilder[T]) Else(result RHS[T]) CaseStage[T] { return b.otherwise(rhsInfo(result)) }

func (b caseBuilder[T]) End() ValueColumn[T] {
	info := b.info
	if len(b.branches) == 0 {
		info.err = errors.Join(info.err, errors.New("case expression needs at least one WHEN"))
	}

	parts := append([]sqlExpr{sqlText("CASE")}, b.branches...)
	if b.elseExpr != nil {
		parts = append(parts, sqlText(" ELSE "), *b.elseExpr)
	}

	parts = append(parts, sqlText(" END"))
	info.sql = sqlJoin(parts...)

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

	return columnImpl[struct{}, T]{c: core}
}
