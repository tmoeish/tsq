// Package tsq provides type-safe SQL query helpers and code generation utilities.
package tsq

type InputOrderMatch[T any, K comparable] struct {
	Ordered []*T
	Missing []K
}

func MatchByInputOrder[T any, K comparable](inputs []K, rows []*T, key func(*T) K) InputOrderMatch[T, K] {
	index := make(map[K]*T, len(rows))

	for _, row := range rows {
		if row == nil {
			continue
		}

		index[key(row)] = row
	}

	ordered := make([]*T, 0, len(inputs))
	missing := make([]K, 0)
	seenMissing := make(map[K]struct{})

	for _, input := range inputs {
		row, ok := index[input]
		if ok {
			ordered = append(ordered, row)
			continue
		}

		if _, exists := seenMissing[input]; exists {
			continue
		}

		seenMissing[input] = struct{}{}

		missing = append(missing, input)
	}

	return InputOrderMatch[T, K]{
		Ordered: ordered,
		Missing: missing,
	}
}
