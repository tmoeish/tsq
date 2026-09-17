package tsq

import (
	"fmt"
	"strings"
)

// ================================================
// Sort direction values.
// ================================================

// Order represents a SQL ORDER BY direction.
type Order string

const (
	// ASC sorts rows in ascending order.
	ASC Order = "ASC" // Ascending order
	// DESC sorts rows in descending order.
	DESC Order = "DESC" // Descending order
)

// OrderBy is one ORDER BY term, made by Column.Asc and Column.Desc.
type OrderBy struct {
	column    SQLColumn
	direction Order
}

// Column returns the ordered column.
func (ob OrderBy) Column() SQLColumn { return ob.column }

// Order returns the sort direction.
func (ob OrderBy) Order() Order { return ob.direction }

// Reverse returns the opposite sort direction. An unknown direction reverses to "".
func (o Order) Reverse() Order {
	switch o {
	case ASC:
		return DESC
	case DESC:
		return ASC
	default:
		return ""
	}
}

func parseOrder(value string) (Order, error) {
	order := Order(strings.ToUpper(strings.TrimSpace(value)))
	switch order {
	case ASC, DESC:
		return order, nil
	default:
		return "", fmt.Errorf("invalid order: %s", value)
	}
}

func normalizeSortOrders(values []string, expected int) ([]Order, error) {
	if len(values) == 0 {
		orders := make([]Order, expected)
		for i := range orders {
			orders[i] = ASC
		}

		return orders, nil
	}

	if len(values) != expected {
		return nil, &OrderCountMismatchError{Fields: expected, Directions: len(values)}
	}

	orders := make([]Order, 0, len(values))
	for _, value := range values {
		order, err := parseOrder(value)
		if err != nil {
			return nil, err
		}

		orders = append(orders, order)
	}

	return orders, nil
}
