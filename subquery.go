package tsq

import (
	"errors"
	"fmt"
	"strings"
)

// Subquery is an opaque typed handle for a built single-column subquery.
// Construct it with BuildSubquery or AsSubquery.
type Subquery[T any] interface {
	rawSubquery
	RHS[T]
	subqueryValue(T)
}

type typedSubquery[O Owner, T any] struct {
	query *Query[O]
}

func (sq *typedSubquery[O, T]) subquerySQL() string {
	if sq == nil {
		return ""
	}

	return sq.query.subquerySQL()
}

func (sq *typedSubquery[O, T]) subqueryArgs() []any {
	if sq == nil {
		return nil
	}

	return sq.query.subqueryArgs()
}

func (sq *typedSubquery[O, T]) subquerySelectCount() int {
	if sq == nil {
		return 0
	}

	return sq.query.subquerySelectCount()
}

func (*typedSubquery[O, T]) subqueryValue(T) {}

func (*typedSubquery[O, T]) rhsValue(T) {}

func (sq *typedSubquery[O, T]) rhsPredicateArg() any { return scalarSubquery(sq) }

// BuildSubquery builds qb and validates that it returns exactly one column with
// the same typed expression metadata as selected.
func BuildSubquery[O Owner, T any](qb QueryStage[O], selected TypedColumn[O, T]) (Subquery[T], error) {
	if qb == nil {
		return nil, errors.New("subquery builder cannot be nil")
	}

	query, err := qb.Build()
	if err != nil {
		return nil, err
	}

	return AsSubquery(query, selected)
}

// AsSubquery validates that query is a built single-column query whose selected
// column matches selected, then returns a typed subquery handle suitable for
// RHS comparisons such as EQ/NE/GT/GTE/LT/LTE/Like/Between and for In/NIn.
func AsSubquery[O Owner, T any](query *Query[O], selected TypedColumn[O, T]) (Subquery[T], error) {
	if query == nil {
		return nil, errors.New("subquery cannot be nil")
	}

	if isNilValue(selected) {
		return nil, errors.New("subquery selected column cannot be nil")
	}

	if strings.TrimSpace(query.subquerySQL()) == "" {
		return nil, errors.New("subquery is not built")
	}

	if len(query.selectCols) != 1 {
		return nil, fmt.Errorf("subquery must select exactly one column, got %d", len(query.selectCols))
	}

	actual, ok := query.selectCols[0].(typedColumnInternal[T])
	if !ok {
		return nil, errors.New("subquery selected column type does not match the expected column type")
	}

	if !sameSubqueryColumn(selected, actual) {
		return nil, fmt.Errorf(
			"subquery selected %s but expected %s",
			describeSubqueryColumn(actual),
			describeSubqueryColumn(selected),
		)
	}

	return &typedSubquery[O, T]{query: query}, nil
}

func sameSubqueryColumn(a, b SQLColumn) bool {
	return renderCanonicalSQL(a.SQLExpr()) == renderCanonicalSQL(b.SQLExpr()) &&
		a.OutputName() == b.OutputName()
}

func describeSubqueryColumn(col SQLColumn) string {
	expr := renderCanonicalSQL(col.SQLExpr())

	name := strings.TrimSpace(col.OutputName())
	if name == "" || name == expr {
		return expr
	}

	return fmt.Sprintf("%s AS %s", expr, name)
}
