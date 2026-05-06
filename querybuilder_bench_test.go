package tsq

import (
	"testing"
)

// BenchmarkQueryBuilder_SimpleBuild measures basic query builder Build() performance
func BenchmarkQueryBuilder_SimpleBuild(b *testing.B) {
	table := newMockTable("users")
	col1 := NewCol[any, int](table, "id", "id", nil)
	col2 := NewCol[any, string](table, "name", "name", nil)
	col3 := NewCol[any, string](table, "email", "email", nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Select(col1, col2, col3).
			From(col1.Table()).
			Where(col1.GT(100)).
			Build()
	}
}

// BenchmarkQueryBuilder_JoinBuild measures Build() performance with joins
func BenchmarkQueryBuilder_JoinBuild(b *testing.B) {
	usersTable := newMockTable("users")
	ordersTable := newMockTable("orders")

	uid := NewCol[any, int](usersTable, "id", "id", nil)
	oid := NewCol[any, int](ordersTable, "id", "id", nil)
	oUserID := NewCol[any, int](ordersTable, "user_id", "user_id", nil)
	oTotal := NewCol[any, float64](ordersTable, "total", "total", nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Select(uid, oid, oTotal).
			From(uid.Table()).
			InnerJoin(ordersTable, uid.EQCol(oUserID)).
			Where(oTotal.GT(50.0)).
			Build()
	}
}

// BenchmarkQueryBuilder_ComplexBuild measures Build() with multiple conditions and clauses
func BenchmarkQueryBuilder_ComplexBuild(b *testing.B) {
	table := newMockTable("products")
	id := NewCol[any, int](table, "id", "id", nil)
	price := NewCol[any, float64](table, "price", "price", nil)
	stock := NewCol[any, int](table, "stock", "stock", nil)
	status := NewCol[any, string](table, "status", "status", nil)
	category := NewCol[any, string](table, "category", "category", nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Select(id, price, stock, status, category).
			From(id.Table()).
			Where(
				price.GT(10.0),
				price.LT(1000.0),
				stock.GTE(1),
				status.EQ("active"),
			).
			Build()
	}
}

// BenchmarkQueryBuilder_GroupByBuild measures Build() performance with GROUP BY
func BenchmarkQueryBuilder_GroupByBuild(b *testing.B) {
	table := newMockTable("sales")
	categoryCol := NewCol[any, string](table, "category", "category", nil)
	totalCol := NewCol[any, float64](table, "total", "total", nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Select(categoryCol, totalCol).
			From(categoryCol.Table()).
			GroupBy(categoryCol).
			Build()
	}
}

// BenchmarkQueryBuilder_AliasAndWhere measures Build() with column aliases and complex conditions
func BenchmarkQueryBuilder_AliasAndWhere(b *testing.B) {
	table := newMockTable("orders")
	id := NewCol[any, int](table, "id", "id", nil)
	userID := NewCol[any, int](table, "user_id", "user_id", nil)
	total := NewCol[any, float64](table, "total", "total", nil)
	status := NewCol[any, string](table, "status", "status", nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Select(
			id.As("order_id"),
			userID.As("uid"),
			total.As("order_total"),
		).
			Where(
				total.GT(100.0),
				status.NE("cancelled"),
			).
			Build()
	}
}
