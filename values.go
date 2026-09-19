package tsq

import (
	"fmt"
	"reflect"
)

// Value is a Go value used as an expression of type T. It is always bound as a
// parameter, never written into the SQL text. Build one with Val.
type Value[T any] struct {
	v T
}

// Val wraps a Go value so it can stand wherever a column of the same type could:
// EQ, Between, Like, Set, Case().When, Coalesce, and the pattern of StartsWith,
// EndsWith and Contains.
//
// T is inferred from v alone, so an untyped constant gets its default type:
// Val(90) is a Value[int]. Against an int64 column write Val(int64(90)); the
// mismatch is a compile error, "does not implement tsq.Operand[int64]".
func Val[T any](v T) Value[T] { return Value[T]{v: v} }

func (Value[T]) valueOfType(T) {}
func (Value[T]) needsTsqVal()  {}

// operand renders the value for a comparison, where NULL never matches.
func (v Value[T]) operand() exprInfo {
	if err := valueError(v.v); err != nil {
		return exprInfo{err: err}
	}

	if err := validatePredicateValue(v.v); err != nil {
		return exprInfo{err: err}
	}

	return exprInfo{sql: sqlValue(v.v)}
}

func (Value[T]) patternText(T) {}

func (v Value[T]) patternOperand(mode paramMode) exprInfo {
	rv := reflect.ValueOf(v.v)
	if rv.Kind() != reflect.String {
		return exprInfo{err: fmt.Errorf("pattern must be text, got %T", v.v)}
	}

	return patternValue(rv.String(), mode)
}

func valueError(v any) error {
	switch v.(type) {
	case interface{ operand() exprInfo }, Condition, anySubquery, Arg:
		return fmt.Errorf("Val takes a Go value, not %T; pass the expression itself", v)
	}

	return nil
}

// ValueList is a list of Go values for In and NotIn, each bound as a parameter.
// Build one with Vals.
type ValueList[T any] struct {
	vs []T
}

// Vals wraps Go values for In and NotIn. No values is still a filter: In matches
// nothing and NotIn matches everything. As with Val, T is inferred from the
// values alone.
func Vals[T any](values ...T) ValueList[T] { return ValueList[T]{vs: values} }

func (ValueList[T]) valuesOfType(T) {}
func (ValueList[T]) needsTsqVals()  {}

func (l ValueList[T]) setOperand(negated bool) exprInfo {
	if len(l.vs) == 0 {
		if negated {
			return exprInfo{sql: sqlText("(SELECT 1 WHERE 1 = 0)")}
		}

		return exprInfo{sql: sqlText("(NULL)")}
	}

	var info exprInfo

	items := make([]sqlExpr, 0, len(l.vs))
	for _, v := range l.vs {
		item := Val(v).operand()
		if item.err != nil {
			return exprInfo{err: fmt.Errorf("list value: %w", item.err)}
		}

		info = info.merge(item)
		items = append(items, item.sql)
	}

	return info.withSQL(sqlJoin(sqlText("("), sqlList(", ", items), sqlText(")")))
}
