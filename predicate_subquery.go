package tsq

import (
	"errors"
	"fmt"
	"strings"
)

type rawSubquery interface {
	subquerySQL() string
	subqueryArgs() []any
	subquerySelectCount() int
	// subqueryShape reports whether the query sets LIMIT/OFFSET and whether it
	// was built with keyword search.
	subqueryShape() (limited, searched bool)
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

// ExistsSub returns an EXISTS predicate for the supplied subquery.
func (c columnImpl[Owner, T]) ExistsSub(sq rawSubquery) Condition {
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
func (c columnImpl[Owner, T]) NExistsSub(sq rawSubquery) Condition {
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
func (c columnImpl[Owner, T]) Unique(_ rawSubquery) Condition {
	return pred[Owner](unsupportedSubqueryPredicate("UNIQUE"))
}

// NUnique returns a deferred portability error because NOT UNIQUE subquery predicates are not supported.
func (c columnImpl[Owner, T]) NUnique(_ rawSubquery) Condition {
	return pred[Owner](unsupportedSubqueryPredicate("NOT UNIQUE"))
}

// unsupportedSubqueryPredicate returns a condition with a deferred error indicating
// that this predicate uses subqueries, which are not supported by TSQ's built-in dialects.
// The error will be returned when Build() is called, not immediately.
func unsupportedSubqueryPredicate(name string) conditionImpl {
	return conditionImpl{buildErr: fmt.Errorf("%s subquery predicate is not supported by TSQ's built-in dialects", name)}
}

type validatedSubquery struct {
	query rawSubquery
	usage subqueryUsage
}

func scalarSubquery(q rawSubquery) validatedSubquery {
	return validatedSubquery{query: q, usage: scalarSubqueryUsage}
}

func membershipSubquery(q rawSubquery) validatedSubquery {
	return validatedSubquery{query: q, usage: membershipSubqueryUsage}
}

func buildSubqueryExpression(q rawSubquery, usage subqueryUsage) (string, []any, error) {
	if q == nil {
		return "", nil, errors.New("subquery cannot be nil")
	}

	sqlText := strings.TrimSpace(q.subquerySQL())
	if sqlText == "" {
		return "", nil, errors.New("subquery is not built")
	}

	// The keyword is bound only when the statement that runs was built with
	// Search; a subquery never receives it, so its search predicate was dropped.
	limited, searched := q.subqueryShape()
	if searched {
		return "", nil, errors.New("a subquery cannot use keyword search; filter it with Where")
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

	// MySQL refuses LIMIT in an IN subquery (error 1235) but not in a derived
	// table, and the derived table means the same everywhere.
	if usage == membershipSubqueryUsage && limited {
		return fmt.Sprintf("(SELECT * FROM (%s) AS tsq_in)", sqlText), q.subqueryArgs(), nil
	}

	return fmt.Sprintf("(%s)", sqlText), q.subqueryArgs(), nil
}
