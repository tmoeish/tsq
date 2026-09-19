package tsq

import (
	"fmt"
	"strings"
)

// ================================================
// Sort direction values.
// ================================================

// sortOrder represents a SQL ORDER BY direction.
type sortOrder string

const (
	// orderAsc sorts rows in ascending order.
	orderAsc sortOrder = "ASC" // Ascending order
	// orderDesc sorts rows in descending order.
	orderDesc sortOrder = "DESC" // Descending order
)

// OrderBy is one ORDER BY term, made by Column.Asc and Column.Desc.
//
// Where the value can be NULL, NULLs sort as the smallest value on every dialect:
// first when ascending, last when descending. That is what MySQL and SQLite do;
// PostgreSQL is told so. NullsFirst and NullsLast choose otherwise.
type OrderBy struct {
	column    SQLColumn
	direction sortOrder
	nulls     nullsOrder
}

type nullsOrder uint8

const (
	nullsSmallest nullsOrder = iota
	nullsFirst
	nullsLast
)

// NullsFirst sorts NULLs before every value, whatever the direction.
func (ob OrderBy) NullsFirst() OrderBy {
	ob.nulls = nullsFirst
	return ob
}

// NullsLast sorts NULLs after every value, whatever the direction.
func (ob OrderBy) NullsLast() OrderBy {
	ob.nulls = nullsLast
	return ob
}

// first reports whether NULLs go first for direction.
func (n nullsOrder) first(direction sortOrder) bool {
	switch n {
	case nullsFirst:
		return true
	case nullsLast:
		return false
	default:
		return direction != orderDesc
	}
}

func parseOrder(value string) (sortOrder, error) {
	order := sortOrder(strings.ToUpper(strings.TrimSpace(value)))
	switch order {
	case orderAsc, orderDesc:
		return order, nil
	default:
		return "", &SortError{Field: value, Reason: "order must be asc or desc"}
	}
}

func normalizeSortOrders(values []string, expected int) ([]sortOrder, error) {
	if len(values) == 0 {
		orders := make([]sortOrder, expected)
		for i := range orders {
			orders[i] = orderAsc
		}

		return orders, nil
	}

	if len(values) != expected {
		return nil, &SortError{Reason: fmt.Sprintf("order_by lists %d fields but order lists %d directions", expected, len(values))}
	}

	orders := make([]sortOrder, 0, len(values))
	for _, value := range values {
		order, err := parseOrder(value)
		if err != nil {
			return nil, err
		}

		orders = append(orders, order)
	}

	return orders, nil
}
