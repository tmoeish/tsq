package tsq

import (
	"fmt"
)

func (f Column[T]) Fn(format string) Column[T] {
	return Column[T]{
		table:    f.table,
		fullName: fmt.Sprintf(format, f.FullName()),
		// baseName TODO
		// ptr TODO
	}
}

func (f Column[T]) Count() Column[T] {
	return f.Fn("COUNT(%s)")
}

func (f Column[T]) Sum() Column[T] {
	return f.Fn("SUM(%s)")
}

func (f Column[T]) Avg() Column[T] {
	return f.Fn("AVG(%s)")
}

func (f Column[T]) Max() Column[T] {
	return f.Fn("MAX(%s)")
}

func (f Column[T]) Min() Column[T] {
	return f.Fn("MIN(%s)")
}

func (f Column[T]) Distinct() Column[T] {
	return f.Fn("DISTINCT(%s)")
}

func (f Column[T]) AS(an string) Column[T] {
	return f.Fn("%s AS `" + an + "`")
}
