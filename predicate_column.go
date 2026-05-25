package tsq

import (
	"fmt"
	"strings"
)

// EQVar compares the column to a runtime-bound value with =.
func (c columnImpl[Owner, T]) EQVar() Condition { return c.Pred(`%s = %s`, varMarker) }

// NEVar compares the column to a runtime-bound value with <>.
func (c columnImpl[Owner, T]) NEVar() Condition { return c.Pred(`%s <> %s`, varMarker) }

// GTVar compares the column to a runtime-bound value with >.
func (c columnImpl[Owner, T]) GTVar() Condition { return c.Pred(`%s > %s`, varMarker) }

// GTEVar compares the column to a runtime-bound value with >=.
func (c columnImpl[Owner, T]) GTEVar() Condition { return c.Pred(`%s >= %s`, varMarker) }

// LTVar compares the column to a runtime-bound value with <.
func (c columnImpl[Owner, T]) LTVar() Condition { return c.Pred(`%s < %s`, varMarker) }

// LTEVar compares the column to a runtime-bound value with <=.
func (c columnImpl[Owner, T]) LTEVar() Condition { return c.Pred(`%s <= %s`, varMarker) }

// LikeVar compares the column to a runtime-bound pattern with LIKE.
func (c columnImpl[Owner, T]) LikeVar() Condition { return c.Pred(`%s LIKE %s`, varMarker) }

// NLikeVar compares the column to a runtime-bound pattern with NOT LIKE.
func (c columnImpl[Owner, T]) NLikeVar() Condition { return c.Pred(`%s NOT LIKE %s`, varMarker) }

// StartsWithVar compares the column to a runtime-bound prefix pattern.
func (c columnImpl[Owner, T]) StartsWithVar() Condition {
	return c.Pred(`%s LIKE %s`, varStartsWithMarker)
}

// NStartsWithVar compares the column to a negated runtime-bound prefix pattern.
func (c columnImpl[Owner, T]) NStartsWithVar() Condition {
	return c.Pred(`%s NOT LIKE %s`, varStartsWithMarker)
}

// EndsWithVar compares the column to a runtime-bound suffix pattern.
func (c columnImpl[Owner, T]) EndsWithVar() Condition { return c.Pred(`%s LIKE %s`, varEndsWithMarker) }

// NEndsWithVar compares the column to a negated runtime-bound suffix pattern.
func (c columnImpl[Owner, T]) NEndsWithVar() Condition {
	return c.Pred(`%s NOT LIKE %s`, varEndsWithMarker)
}

// ContainsVar compares the column to a runtime-bound contains pattern.
func (c columnImpl[Owner, T]) ContainsVar() Condition { return c.Pred(`%s LIKE %s`, varContainsMarker) }

// NContainsVar compares the column to a negated runtime-bound contains pattern.
func (c columnImpl[Owner, T]) NContainsVar() Condition {
	return c.Pred(`%s NOT LIKE %s`, varContainsMarker)
}

// InVar binds a slice at execution time for IN predicates.
//
// TSQ intentionally treats nil and empty slices as an explicit "match nothing"
// filter. During execution the placeholder list is expanded from the runtime
// argument slice; when that slice is empty, TSQ renders IN (NULL), which keeps
// the query valid while producing zero matches across the supported built-in
// dialects. This is by design and lets callers express "no selected IDs" without
// adding custom branching around the query.
func (c columnImpl[Owner, T]) InVar() Condition {
	return c.Pred(`%s IN (%s)`, varSliceMarker)
}

// NInVar binds a slice at execution time for NOT IN predicates.
//
// For nil and empty slices, TSQ renders the empty-set form `NOT IN (SELECT 1
// WHERE 1 = 0)`, which preserves the explicit "match everything" meaning of an
// empty NOT IN list across the built-in dialects.
func (c columnImpl[Owner, T]) NInVar() Condition {
	return c.Pred(`%s NOT IN (%s)`, varNotInSliceMarker)
}

// BetweenVar compares the column to two runtime-bound values with BETWEEN.
func (c columnImpl[Owner, T]) BetweenVar() Condition {
	return c.Pred(`%s BETWEEN %s AND %s`, varMarker, varMarker)
}

// NBetweenVar compares the column to two runtime-bound values with NOT BETWEEN.
func (c columnImpl[Owner, T]) NBetweenVar() Condition {
	return c.Pred(`%s NOT BETWEEN %s AND %s`, varMarker, varMarker)
}

// EQ compares the column to rhs with =.
func (c columnImpl[Owner, T]) EQ(rhs RHS[T]) Condition {
	return c.Pred(`%s = %s`, predicateRHSArg(rhs))
}

// NE compares the column to rhs with <>.
func (c columnImpl[Owner, T]) NE(rhs RHS[T]) Condition {
	return c.Pred(`%s <> %s`, predicateRHSArg(rhs))
}

// GT compares the column to rhs with >.
func (c columnImpl[Owner, T]) GT(rhs RHS[T]) Condition {
	return c.Pred(`%s > %s`, predicateRHSArg(rhs))
}

// GTE compares the column to rhs with >=.
func (c columnImpl[Owner, T]) GTE(rhs RHS[T]) Condition {
	return c.Pred(`%s >= %s`, predicateRHSArg(rhs))
}

// LT compares the column to rhs with <.
func (c columnImpl[Owner, T]) LT(rhs RHS[T]) Condition {
	return c.Pred(`%s < %s`, predicateRHSArg(rhs))
}

// LTE compares the column to rhs with <=.
func (c columnImpl[Owner, T]) LTE(rhs RHS[T]) Condition {
	return c.Pred(`%s <= %s`, predicateRHSArg(rhs))
}

// Like compares the column to rhs with LIKE.
func (c columnImpl[Owner, T]) Like(rhs RHS[T]) Condition {
	return c.Pred(`%s LIKE %s`, predicateRHSArg(rhs))
}

// NLike compares the column to rhs with NOT LIKE.
func (c columnImpl[Owner, T]) NLike(rhs RHS[T]) Condition {
	return c.Pred(`%s NOT LIKE %s`, predicateRHSArg(rhs))
}

// Between compares the column to an inclusive RHS range.
func (c columnImpl[Owner, T]) Between(start, end RHS[T]) Condition {
	return c.Pred(`%s BETWEEN %s AND %s`, predicateRHSArg(start), predicateRHSArg(end))
}

// NBetween compares the column to values outside an inclusive RHS range.
func (c columnImpl[Owner, T]) NBetween(start, end RHS[T]) Condition {
	return c.Pred(`%s NOT BETWEEN %s AND %s`, predicateRHSArg(start), predicateRHSArg(end))
}

// EQVal compares the column to arg with =.
func (c columnImpl[Owner, T]) EQVal(arg T) Condition {
	return c.Pred(`%s = %s`, Bind(arg))
}

// NEVal compares the column to arg with <>.
func (c columnImpl[Owner, T]) NEVal(arg T) Condition {
	return c.Pred(`%s <> %s`, Bind(arg))
}

// GTVal compares the column to arg with >.
func (c columnImpl[Owner, T]) GTVal(arg T) Condition {
	return c.Pred(`%s > %s`, Bind(arg))
}

// GTEVal compares the column to arg with >=.
func (c columnImpl[Owner, T]) GTEVal(arg T) Condition {
	return c.Pred(`%s >= %s`, Bind(arg))
}

// LTVal compares the column to arg with <.
func (c columnImpl[Owner, T]) LTVal(arg T) Condition {
	return c.Pred(`%s < %s`, Bind(arg))
}

// LTEVal compares the column to arg with <=.
func (c columnImpl[Owner, T]) LTEVal(arg T) Condition {
	return c.Pred(`%s <= %s`, Bind(arg))
}

// LikeVal compares the column to arg with LIKE.
func (c columnImpl[Owner, T]) LikeVal(arg T) Condition {
	return c.Pred(`%s LIKE %s`, Bind(arg))
}

// NLikeVal compares the column to arg with NOT LIKE.
func (c columnImpl[Owner, T]) NLikeVal(arg T) Condition {
	return c.Pred(`%s NOT LIKE %s`, Bind(arg))
}

// BetweenVal compares the column to an inclusive literal range.
func (c columnImpl[Owner, T]) BetweenVal(start, end T) Condition {
	return c.Pred(`%s BETWEEN %s AND %s`, Bind(start), Bind(end))
}

// NBetweenVal compares the column to values outside an inclusive literal range.
func (c columnImpl[Owner, T]) NBetweenVal(start, end T) Condition {
	return c.Pred(`%s NOT BETWEEN %s AND %s`, Bind(start), Bind(end))
}

// StartsWithVal compares the column to a bound prefix pattern.
func (c columnImpl[Owner, T]) StartsWithVal(str string) Condition {
	return c.Pred(`%s LIKE %s`, Bind(str+"%"))
}

// NStartsWithVal compares the column to a negated bound prefix pattern.
func (c columnImpl[Owner, T]) NStartsWithVal(str string) Condition {
	return c.Pred(`%s NOT LIKE %s`, Bind(str+"%"))
}

// EndsWithVal compares the column to a bound suffix pattern.
func (c columnImpl[Owner, T]) EndsWithVal(str string) Condition {
	return c.Pred(`%s LIKE %s`, Bind("%"+str))
}

// NEndsWithVal compares the column to a negated bound suffix pattern.
func (c columnImpl[Owner, T]) NEndsWithVal(str string) Condition {
	return c.Pred(`%s NOT LIKE %s`, Bind("%"+str))
}

// ContainsVal compares the column to a bound contains pattern.
func (c columnImpl[Owner, T]) ContainsVal(str string) Condition {
	return c.Pred(`%s LIKE %s`, Bind("%"+str+"%"))
}

// NContainsVal compares the column to a negated bound contains pattern.
func (c columnImpl[Owner, T]) NContainsVal(str string) Condition {
	return c.Pred(`%s NOT LIKE %s`, Bind("%"+str+"%"))
}

// InVal compares the column to an explicit list of bound values.
func (c columnImpl[Owner, T]) InVal(args ...T) Condition {
	if len(args) == 0 {
		return pred[Owner](rawCondition("1 = 0"))
	}

	return c.Pred(`%s IN (%s)`, BindSlice(args))
}

// NInVal compares the column to a negated list of bound values.
func (c columnImpl[Owner, T]) NInVal(args ...T) Condition {
	if len(args) == 0 {
		return pred[Owner](rawCondition("1 = 1"))
	}

	return c.Pred(`%s NOT IN (%s)`, BindSlice(args))
}

// IsNull checks whether the column value is NULL.
func (c columnImpl[Owner, T]) IsNull() Condition {
	return c.Pred(`%s IS NULL`)
}

// IsNotNull checks whether the column value is not NULL.
func (c columnImpl[Owner, T]) IsNotNull() Condition {
	return c.Pred(`%s IS NOT NULL`)
}

// Pred formats a custom predicate template around the receiver column.
func (c columnImpl[Owner, T]) Pred(format string, args ...any) Condition {
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

func (c columnImpl[Owner, T]) rawCondition(expr string) Condition {
	table, err := validateColumnInput(c)
	if err != nil {
		return pred[Owner](conditionImpl{buildErr: err})
	}

	return pred[Owner](conditionImpl{
		tables: map[string]Table{table.Table(): table},
		expr:   expr,
	})
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
