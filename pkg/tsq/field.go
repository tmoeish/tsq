package tsq

// Ptr is a function that returns a pointer to a field in a struct.
type Ptr func(holder any) any

// IColumn is a column in a table.
type IColumn interface {
	Table() Table
	Name() string
	FullName() string
	Ptr() Ptr
}

// NewColumn creates a field in a table.
func NewColumn[T any](table Table, baseName string, ptr Ptr) Column[T] {
	return Column[T]{
		table:    table,
		name:     baseName,
		fullName: table.Table() + "." + baseName,
		ptr:      ptr,
	}
}

// Column is a field in a table.
type Column[T any] struct {
	table    Table
	name     string
	fullName string
	ptr      Ptr
}

func (f Column[T]) Table() Table {
	return f.table
}

func (f Column[T]) Name() string {
	return f.name
}

func (f Column[T]) FullName() string {
	return f.fullName
}

func (f Column[T]) Ptr() Ptr {
	return f.ptr
}
