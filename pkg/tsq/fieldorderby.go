package tsq

func (f Column[T]) Asc() OrderBy {
	return OrderBy{
		field: f,
		order: ASC,
	}
}

func (f Column[T]) Desc() OrderBy {
	return OrderBy{
		field: f,
		order: DESC,
	}
}

type Order string

const (
	ASC  Order = "ASC"
	DESC Order = "DESC"
)

type OrderBy struct {
	field IColumn
	order Order
}

func (ob OrderBy) String() string {
	return ob.field.FullName() + " " + string(ob.order)
}
