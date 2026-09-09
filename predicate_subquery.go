package tsq

import (
	"errors"
	"fmt"
	"strings"
)

// AnySubquery is a subquery used where the selected columns do not matter, such
// as EXISTS. A built *Query and a typed Subquery both satisfy it.
//
// Its methods are unexported, so it cannot be implemented outside this package.
// It is exported anyway because a parameter type callers cannot name is a type
// they cannot write a helper around.
type AnySubquery interface {
	subquerySQL() string
	subqueryArgs() []any
	subquerySelectCount() int
}

type subqueryUsage string

const (
	scalarSubqueryUsage     subqueryUsage = "scalar"
	membershipSubqueryUsage subqueryUsage = "membership"
	existsSubqueryUsage     subqueryUsage = "exists"
)

// In compares the column to a membership subquery with IN.
func (c columnImpl[Owner, T]) In(sq Subquery[T]) Condition {
	return c.Pred(`%s IN %s`, membershipSubquery(sq))
}

// NIn compares the column to a membership subquery with NOT IN.
func (c columnImpl[Owner, T]) NIn(sq Subquery[T]) Condition {
	return c.Pred(`%s NOT IN %s`, membershipSubquery(sq))
}

type validatedSubquery struct {
	query AnySubquery
	usage subqueryUsage
}

func scalarSubquery(q AnySubquery) validatedSubquery {
	return validatedSubquery{query: q, usage: scalarSubqueryUsage}
}

func membershipSubquery(q AnySubquery) validatedSubquery {
	return validatedSubquery{query: q, usage: membershipSubqueryUsage}
}

func buildSubqueryExpression(q AnySubquery, usage subqueryUsage) (string, []any, error) {
	if q == nil {
		return "", nil, errors.New("subquery cannot be nil")
	}

	sqlText := strings.TrimSpace(q.subquerySQL())
	if sqlText == "" {
		return "", nil, errors.New("subquery is not built")
	}

	selectCount := q.subquerySelectCount()
	if selectCount == 0 {
		return "", nil, errors.New("subquery metadata is unavailable; build the subquery with tsq.Select(...).Build()")
	}

	switch usage {
	case scalarSubqueryUsage:
		if selectCount != 1 {
			return "", nil, fmt.Errorf("scalar subquery must select exactly one column, got %d", selectCount)
		}
	case membershipSubqueryUsage:
		if selectCount != 1 {
			return "", nil, fmt.Errorf("in subquery must select exactly one column, got %d", selectCount)
		}
	case existsSubqueryUsage:
	default:
		return "", nil, fmt.Errorf("unknown subquery usage %q", usage)
	}

	return fmt.Sprintf("(%s)", sqlText), q.subqueryArgs(), nil
}

// Exists builds an EXISTS (subquery) predicate.
//
// EXISTS asks whether the subquery returns any row at all, so it belongs to no
// column. It used to be a method on every column, which forced the caller to
// pick an arbitrary one and name a type they could not spell.
func Exists(sq AnySubquery) Condition {
	return existsCondition(sq, "EXISTS ")
}

// NotExists builds a NOT EXISTS (subquery) predicate.
func NotExists(sq AnySubquery) Condition {
	return existsCondition(sq, "NOT EXISTS ")
}

func existsCondition(sq AnySubquery, keyword string) Condition {
	subquery, args, err := buildSubqueryExpression(sq, existsSubqueryUsage)
	if err != nil {
		return conditionImpl{buildErr: err}
	}

	return conditionImpl{
		tables: map[string]Table{},
		expr:   keyword + subquery,
		args:   args,
	}
}
