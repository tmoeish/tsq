package tsq

import (
	"testing"
)

// BenchmarkQueryBuilder_SimpleBuild measures basic query builder Build() performance
func BenchmarkQueryBuilder_SimpleBuild(b *testing.B) {
	table := newMockTable("users")
	col1 := newColForTable[Table, int](table, "id", "id", nil)
	col2 := newColForTable[Table, string](table, "name", "name", nil)
	col3 := newColForTable[Table, string](table, "email", "email", nil)

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

	uid := newColForTable[Table, int](usersTable, "id", "id", nil)
	oid := newColForTable[Table, int](ordersTable, "id", "id", nil)
	oUserID := newColForTable[Table, int](ordersTable, "user_id", "user_id", nil)
	oTotal := newColForTable[Table, float64](ordersTable, "total", "total", nil)

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
	id := newColForTable[Table, int](table, "id", "id", nil)
	price := newColForTable[Table, float64](table, "price", "price", nil)
	stock := newColForTable[Table, int](table, "stock", "stock", nil)
	status := newColForTable[Table, string](table, "status", "status", nil)
	category := newColForTable[Table, string](table, "category", "category", nil)

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
	categoryCol := newColForTable[Table, string](table, "category", "category", nil)
	totalCol := newColForTable[Table, float64](table, "total", "total", nil)

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
	id := newColForTable[Table, int](table, "id", "id", nil)
	userID := newColForTable[Table, int](table, "user_id", "user_id", nil)
	total := newColForTable[Table, float64](table, "total", "total", nil)
	status := newColForTable[Table, string](table, "status", "status", nil)

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
