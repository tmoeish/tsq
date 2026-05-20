package tsq

import (
	"errors"
	"fmt"
	"strings"
)

type subquery interface {
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

// EQSub compares the column to a scalar subquery with =.
func (c columnImpl[Owner, T]) EQSub(sq subquery) Condition {
	return c.Pred(`%s = %s`, scalarSubquery(sq))
}

// NESub compares the column to a scalar subquery with <>.
func (c columnImpl[Owner, T]) NESub(sq subquery) Condition {
	return c.Pred(`%s <> %s`, scalarSubquery(sq))
}

// GTSub compares the column to a scalar subquery with >.
func (c columnImpl[Owner, T]) GTSub(sq subquery) Condition {
	return c.Pred(`%s > %s`, scalarSubquery(sq))
}

// GTESub compares the column to a scalar subquery with >=.
func (c columnImpl[Owner, T]) GTESub(sq subquery) Condition {
	return c.Pred(`%s >= %s`, scalarSubquery(sq))
}

// LTSub compares the column to a scalar subquery with <.
func (c columnImpl[Owner, T]) LTSub(sq subquery) Condition {
	return c.Pred(`%s < %s`, scalarSubquery(sq))
}

// LTESub compares the column to a scalar subquery with <=.
func (c columnImpl[Owner, T]) LTESub(sq subquery) Condition {
	return c.Pred(`%s <= %s`, scalarSubquery(sq))
}

// LikeSub compares the column to a scalar subquery with LIKE.
func (c columnImpl[Owner, T]) LikeSub(sq subquery) Condition {
	return c.Pred(`%s LIKE %s`, scalarSubquery(sq))
}

// NLikeSub compares the column to a scalar subquery with NOT LIKE.
func (c columnImpl[Owner, T]) NLikeSub(sq subquery) Condition {
	return c.Pred(`%s NOT LIKE %s`, scalarSubquery(sq))
}

// InSub compares the column to a membership subquery with IN.
func (c columnImpl[Owner, T]) InSub(sq subquery) Condition {
	return c.Pred(`%s IN %s`, membershipSubquery(sq))
}

// NInSub compares the column to a membership subquery with NOT IN.
func (c columnImpl[Owner, T]) NInSub(sq subquery) Condition {
	return c.Pred(`%s NOT IN %s`, membershipSubquery(sq))
}

// ExistsSub returns an EXISTS predicate for the supplied subquery.
func (c columnImpl[Owner, T]) ExistsSub(sq subquery) Condition {
	subquery, args, err := buildSubqueryExpression(sq, existsSubqueryUsage)
	if err != nil {
		return pred[Owner](conditionImpl{buildErr: err})
	}

	return pred[Owner](conditionImpl{
		tables: map[string]Table{},
		expr:   "EXISTS " + subquery,
		args:   args,
	})
}

// NExistsSub returns a NOT EXISTS predicate for the supplied subquery.
func (c columnImpl[Owner, T]) NExistsSub(sq subquery) Condition {
	subquery, args, err := buildSubqueryExpression(sq, existsSubqueryUsage)
	if err != nil {
		return pred[Owner](conditionImpl{buildErr: err})
	}

	return pred[Owner](conditionImpl{
		tables: map[string]Table{},
		expr:   "NOT EXISTS " + subquery,
		args:   args,
	})
}

// Unique returns a deferred portability error because UNIQUE subquery predicates are not supported.
func (c columnImpl[Owner, T]) Unique(_ subquery) Condition {
	return pred[Owner](unsupportedSubqueryPredicate("UNIQUE"))
}

// NUnique returns a deferred portability error because NOT UNIQUE subquery predicates are not supported.
func (c columnImpl[Owner, T]) NUnique(_ subquery) Condition {
	return pred[Owner](unsupportedSubqueryPredicate("NOT UNIQUE"))
}

// unsupportedSubqueryPredicate returns a condition with a deferred error indicating
// that this predicate uses subqueries, which are not supported by TSQ's built-in dialects.
// The error will be returned when Build() is called, not immediately.
func unsupportedSubqueryPredicate(name string) conditionImpl {
	return conditionImpl{buildErr: fmt.Errorf("%s subquery predicate is not supported by TSQ's built-in dialects", name)}
}

type validatedSubquery struct {
	query subquery
	usage subqueryUsage
}

func scalarSubquery(q subquery) validatedSubquery {
	return validatedSubquery{query: q, usage: scalarSubqueryUsage}
}

func membershipSubquery(q subquery) validatedSubquery {
	return validatedSubquery{query: q, usage: membershipSubqueryUsage}
}

func buildSubqueryExpression(q subquery, usage subqueryUsage) (string, []any, error) {
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
