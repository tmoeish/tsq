package tsq

// ================================================
// 排序方向枚举
// ================================================

// Order represents SQL ORDER BY direction
type Order string

const (
	ASC  Order = "ASC"  // Ascending order
	DESC Order = "DESC" // Descending order
)

// ================================================
// 排序结构体
// ================================================

// OrderBy represents an ORDER BY clause with field and direction
type OrderBy struct {
	field Column // The column to order by
	order Order  // The sort direction (ASC/DESC)
}

// Expr returns the SQL expression for this ORDER BY clause
func (ob OrderBy) Expr() string {
	return ob.field.QualifiedName() + " " + string(ob.order)
}

// Field returns the column being ordered
func (ob OrderBy) Field() Column {
	return ob.field
}

// Order returns the sort direction
func (ob OrderBy) Order() Order {
	return ob.order
}

// ================================================
// 列排序方法
// ================================================

// Asc creates an ascending ORDER BY clause for this column
func (c Col[T]) Asc() OrderBy {
	return OrderBy{
		field: c,
		order: ASC,
	}
}

// Desc creates a descending ORDER BY clause for this column
func (c Col[T]) Desc() OrderBy {
	return OrderBy{
		field: c,
		order: DESC,
	}
}

// ================================================
// 排序工具函数
// ================================================

// OrderByMultiple creates multiple ORDER BY clauses
func OrderByMultiple(orderBys ...OrderBy) []string {
	expressions := make([]string, 0, len(orderBys))
	for _, ob := range orderBys {
		expressions = append(expressions, ob.Expr())
	}

	return expressions
}

// ReverseOrder returns the opposite order direction
func ReverseOrder(order Order) Order {
	if order == ASC {
		return DESC
	}

	return ASC
}
