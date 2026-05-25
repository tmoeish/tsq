package tsq

import "errors"

// RHS is a typed right-hand-side operand for scalar predicates such as
// EQ/NE/GT/GTE/LT/LTE/Like/NLike.
//
// Supported RHS values are TSQ typed columns/expressions and typed Subquery
// handles built with BuildSubquery or AsSubquery. Plain Go values intentionally
// use the dedicated EQVal/NEVal/GTVal/GTEVal/LTVal/LTEVal helpers, while
// runtime-bound placeholders continue to use EQVar/NEVar/GTVar/GTEVar/LTVar/LTEVar.
type RHS[T any] interface {
	rhsValue(T)
	rhsPredicateArg() any
}

func predicateRHSArg[T any](rhs RHS[T]) any {
	if isNilValue(rhs) {
		return expressionError{err: errors.New("rhs cannot be nil")}
	}

	return rhs.rhsPredicateArg()
}
