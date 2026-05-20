package tsq

import (
	"fmt"
	"strings"
)

// EQVar compares the column to a runtime-bound value with =.
func (c columnImpl[Owner, T]) EQVar() Predicate[Owner] { return c.Pred(`%s = %s`, varMarker) }

// NEVar compares the column to a runtime-bound value with <>.
func (c columnImpl[Owner, T]) NEVar() Predicate[Owner] { return c.Pred(`%s <> %s`, varMarker) }

// GTVar compares the column to a runtime-bound value with >.
func (c columnImpl[Owner, T]) GTVar() Predicate[Owner] { return c.Pred(`%s > %s`, varMarker) }

// GTEVar compares the column to a runtime-bound value with >=.
func (c columnImpl[Owner, T]) GTEVar() Predicate[Owner] { return c.Pred(`%s >= %s`, varMarker) }

// LTVar compares the column to a runtime-bound value with <.
func (c columnImpl[Owner, T]) LTVar() Predicate[Owner] { return c.Pred(`%s < %s`, varMarker) }

// LTEVar compares the column to a runtime-bound value with <=.
func (c columnImpl[Owner, T]) LTEVar() Predicate[Owner] { return c.Pred(`%s <= %s`, varMarker) }

// InVar binds a slice at execution time for IN predicates.
//
// TSQ intentionally treats nil and empty slices as an explicit "match nothing"
// filter. During execution the placeholder list is expanded from the runtime
// argument slice; when that slice is empty, TSQ renders IN (NULL), which keeps
// the query valid while producing zero matches across the supported built-in
// dialects. This is by design and lets callers express "no selected IDs" without
// adding custom branching around the query.
func (c columnImpl[Owner, T]) InVar() Predicate[Owner] {
	return c.Pred(`%s IN (%s)`, varSliceMarker)
}

// StartsWithVar defers a prefix match to execution time, which tsq rejects for portability.
func (c columnImpl[Owner, T]) StartsWithVar() Predicate[Owner] {
	return pred[Owner](unsupportedPatternPredicate("StartsWithVar"))
}

// NStartsWithVar defers a negated prefix match to execution time, which tsq rejects for portability.
func (c columnImpl[Owner, T]) NStartsWithVar() Predicate[Owner] {
	return pred[Owner](unsupportedPatternPredicate("NStartsWithVar"))
}

// EndsWithVar defers a suffix match to execution time, which tsq rejects for portability.
func (c columnImpl[Owner, T]) EndsWithVar() Predicate[Owner] {
	return pred[Owner](unsupportedPatternPredicate("EndsWithVar"))
}

// NEndsWithVar defers a negated suffix match to execution time, which tsq rejects for portability.
func (c columnImpl[Owner, T]) NEndsWithVar() Predicate[Owner] {
	return pred[Owner](unsupportedPatternPredicate("NEndsWithVar"))
}

// ContainsVar defers a contains match to execution time, which tsq rejects for portability.
func (c columnImpl[Owner, T]) ContainsVar() Predicate[Owner] {
	return pred[Owner](unsupportedPatternPredicate("ContainsVar"))
}

// NContainsVar defers a negated contains match to execution time, which tsq rejects for portability.
func (c columnImpl[Owner, T]) NContainsVar() Predicate[Owner] {
	return pred[Owner](unsupportedPatternPredicate("NContainsVar"))
}

// BetweenVar compares the column to two runtime-bound values with BETWEEN.
func (c columnImpl[Owner, T]) BetweenVar() Predicate[Owner] {
	return c.Pred(`%s BETWEEN %s AND %s`, varMarker, varMarker)
}

// NBetweenVar compares the column to two runtime-bound values with NOT BETWEEN.
func (c columnImpl[Owner, T]) NBetweenVar() Predicate[Owner] {
	return c.Pred(`%s NOT BETWEEN %s AND %s`, varMarker, varMarker)
}

// EQ compares the column to arg with =.
func (c columnImpl[Owner, T]) EQ(arg T) Predicate[Owner] {
	return c.Pred(`%s = %s`, Bind(arg))
}

// NE compares the column to arg with <>.
func (c columnImpl[Owner, T]) NE(arg T) Predicate[Owner] {
	return c.Pred(`%s <> %s`, Bind(arg))
}

// GT compares the column to arg with >.
func (c columnImpl[Owner, T]) GT(arg T) Predicate[Owner] {
	return c.Pred(`%s > %s`, Bind(arg))
}

// GTE compares the column to arg with >=.
func (c columnImpl[Owner, T]) GTE(arg T) Predicate[Owner] {
	return c.Pred(`%s >= %s`, Bind(arg))
}

// LT compares the column to arg with <.
func (c columnImpl[Owner, T]) LT(arg T) Predicate[Owner] {
	return c.Pred(`%s < %s`, Bind(arg))
}

// LTE compares the column to arg with <=.
func (c columnImpl[Owner, T]) LTE(arg T) Predicate[Owner] {
	return c.Pred(`%s <= %s`, Bind(arg))
}

// StartsWith compares the column to a bound prefix pattern.
func (c columnImpl[Owner, T]) StartsWith(str string) Predicate[Owner] {
	return c.Pred(`%s LIKE %s`, Bind(str+"%"))
}

// NStartsWith compares the column to a negated bound prefix pattern.
func (c columnImpl[Owner, T]) NStartsWith(str string) Predicate[Owner] {
	return c.Pred(`%s NOT LIKE %s`, Bind(str+"%"))
}

// EndsWith compares the column to a bound suffix pattern.
func (c columnImpl[Owner, T]) EndsWith(str string) Predicate[Owner] {
	return c.Pred(`%s LIKE %s`, Bind("%"+str))
}

// NEndsWith compares the column to a negated bound suffix pattern.
func (c columnImpl[Owner, T]) NEndsWith(str string) Predicate[Owner] {
	return c.Pred(`%s NOT LIKE %s`, Bind("%"+str))
}

// Contains compares the column to a bound contains pattern.
func (c columnImpl[Owner, T]) Contains(str string) Predicate[Owner] {
	return c.Pred(`%s LIKE %s`, Bind("%"+str+"%"))
}

// NContains compares the column to a negated bound contains pattern.
func (c columnImpl[Owner, T]) NContains(str string) Predicate[Owner] {
	return c.Pred(`%s NOT LIKE %s`, Bind("%"+str+"%"))
}

// Between compares the column to an inclusive range.
func (c columnImpl[Owner, T]) Between(start, end T) Predicate[Owner] {
	return c.Pred(`%s BETWEEN %s AND %s`, Bind(start), Bind(end))
}

// NBetween compares the column to values outside an inclusive range.
func (c columnImpl[Owner, T]) NBetween(start, end T) Predicate[Owner] {
	return c.Pred(`%s NOT BETWEEN %s AND %s`, Bind(start), Bind(end))
}

// In compares the column to an explicit list of bound values.
func (c columnImpl[Owner, T]) In(args ...T) Predicate[Owner] {
	if len(args) == 0 {
		return pred[Owner](rawCondition("1 = 0"))
	}

	return c.Pred(`%s IN (%s)`, BindSlice(args))
}

// NIn compares the column to a negated list of bound values.
func (c columnImpl[Owner, T]) NIn(args ...T) Predicate[Owner] {
	if len(args) == 0 {
		return pred[Owner](rawCondition("1 = 1"))
	}

	return c.Pred(`%s NOT IN (%s)`, BindSlice(args))
}

// IsNull checks whether the column value is NULL.
func (c columnImpl[Owner, T]) IsNull() Predicate[Owner] {
	return c.Pred(`%s IS NULL`)
}

// IsNotNull checks whether the column value is not NULL.
func (c columnImpl[Owner, T]) IsNotNull() Predicate[Owner] {
	return c.Pred(`%s IS NOT NULL`)
}

// EQCol compares the column to another column with =.
func (c columnImpl[Owner, T]) EQCol(other typedColumnInternal[T]) Predicate[Owner] {
	return c.Pred(`%s = %s`, other)
}

// NECol compares the column to another column with <>.
func (c columnImpl[Owner, T]) NECol(other typedColumnInternal[T]) Predicate[Owner] {
	return c.Pred(`%s <> %s`, other)
}

// GTCol compares the column to another column with >.
func (c columnImpl[Owner, T]) GTCol(other typedColumnInternal[T]) Predicate[Owner] {
	return c.Pred(`%s > %s`, other)
}

// GTECol compares the column to another column with >=.
func (c columnImpl[Owner, T]) GTECol(other typedColumnInternal[T]) Predicate[Owner] {
	return c.Pred(`%s >= %s`, other)
}

// LTCol compares the column to another column with <.
func (c columnImpl[Owner, T]) LTCol(other typedColumnInternal[T]) Predicate[Owner] {
	return c.Pred(`%s < %s`, other)
}

// LTECol compares the column to another column with <=.
func (c columnImpl[Owner, T]) LTECol(other typedColumnInternal[T]) Predicate[Owner] {
	return c.Pred(`%s <= %s`, other)
}

// StartsWithCol compares the column to another column with a prefix match, which tsq rejects for portability.
func (c columnImpl[Owner, T]) StartsWithCol(_ typedColumnInternal[T]) Predicate[Owner] {
	return pred[Owner](unsupportedPatternPredicate("StartsWithCol"))
}

// NStartsWithCol compares the column to another column with a negated prefix match, which tsq rejects for portability.
func (c columnImpl[Owner, T]) NStartsWithCol(_ typedColumnInternal[T]) Predicate[Owner] {
	return pred[Owner](unsupportedPatternPredicate("NStartsWithCol"))
}

// EndsWithCol compares the column to another column with a suffix match, which tsq rejects for portability.
func (c columnImpl[Owner, T]) EndsWithCol(_ typedColumnInternal[T]) Predicate[Owner] {
	return pred[Owner](unsupportedPatternPredicate("EndsWithCol"))
}

// NEndsWithCol compares the column to another column with a negated suffix match, which tsq rejects for portability.
func (c columnImpl[Owner, T]) NEndsWithCol(_ typedColumnInternal[T]) Predicate[Owner] {
	return pred[Owner](unsupportedPatternPredicate("NEndsWithCol"))
}

// ContainsCol compares the column to another column with a contains match, which tsq rejects for portability.
func (c columnImpl[Owner, T]) ContainsCol(_ typedColumnInternal[T]) Predicate[Owner] {
	return pred[Owner](unsupportedPatternPredicate("ContainsCol"))
}

// NContainsCol compares the column to another column with a negated contains match, which tsq rejects for portability.
func (c columnImpl[Owner, T]) NContainsCol(_ typedColumnInternal[T]) Predicate[Owner] {
	return pred[Owner](unsupportedPatternPredicate("NContainsCol"))
}

// Pred formats a custom predicate template around the receiver column.
func (c columnImpl[Owner, T]) Pred(format string, args ...any) Predicate[Owner] {
	if err := validatePredicateFormat(format, len(args)+1); err != nil {
		return pred[Owner](conditionImpl{buildErr: err})
	}

	baseTable, err := validateColumnInput(c)
	if err != nil {
		return pred[Owner](conditionImpl{buildErr: err})
	}

	tables := map[string]Table{baseTable.Table(): baseTable}

	for _, arg := range args {
		if col, ok := arg.(SQLColumn); ok {
			table, err := validateColumnInput(col)
			if err != nil {
				return pred[Owner](conditionImpl{buildErr: err})
			}

			tables[table.Table()] = table
		}
	}

	formatArgs := make([]any, 0, len(args)+1)
	formatArgs = append(formatArgs, c.rawQualifiedName())

	for _, arg := range args {
		expr := argumentToExpression(arg)
		if err := expressionBuildError(expr); err != nil {
			return pred[Owner](conditionImpl{buildErr: err})
		}

		formatArgs = append(formatArgs, expr.Expr())
	}

	return pred[Owner](conditionImpl{
		tables: tables,
		expr:   fmt.Sprintf(format, formatArgs...),
		args:   collectExpressionArgs(args...),
	})
}

func (c columnImpl[Owner, T]) rawCondition(expr string) Predicate[Owner] {
	table, err := validateColumnInput(c)
	if err != nil {
		return pred[Owner](conditionImpl{buildErr: err})
	}

	return pred[Owner](conditionImpl{
		tables: map[string]Table{table.Table(): table},
		expr:   expr,
	})
}

// unsupportedPatternPredicate returns a condition with a deferred error indicating
// that this pattern predicate is not portable across TSQ's built-in dialects.
// The error will be returned when Build() is called, not immediately.
// Users should use LIKE with an explicit pattern instead.
func unsupportedPatternPredicate(name string) conditionImpl {
	return conditionImpl{buildErr: fmt.Errorf(
		"%s is not portable across TSQ's built-in dialects; use LIKE with an explicit pattern instead",
		name,
	)}
}

func validatePredicateFormat(format string, placeholderCount int) error {
	if strings.TrimSpace(format) == "" {
		return fmt.Errorf("predicate format cannot be empty")
	}

	actual, err := countStringFormatPlaceholders(format)
	if err != nil {
		return err
	}

	if actual != placeholderCount {
		return fmt.Errorf(
			"predicate format placeholder count mismatch: expected %d, got %d",
			placeholderCount,
			actual,
		)
	}

	return nil
}
