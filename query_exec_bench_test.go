package tsq

import (
	"testing"
)

// BenchmarkQueryBuilder_SimpleBuild measures basic query builder Build() performance
func BenchmarkQueryBuilder_SimpleWhere(b *testing.B) {
	table := newMockTable("users")
	idCol := NewCol[int](table, "id", "id", nil)
	nameCol := NewCol[string](table, "name", "name", nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Select(idCol, nameCol).
			Where(idCol.GT(100)).
			Build()
	}
}

// BenchmarkQueryBuilder_MultipleConditions measures WHERE with multiple conditions
func BenchmarkQueryBuilder_MultipleConditions(b *testing.B) {
	table := newMockTable("users")
	idCol := NewCol[int](table, "id", "id", nil)
	statusCol := NewCol[int](table, "status", "status", nil)
	nameCol := NewCol[string](table, "name", "name", nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Select(idCol, statusCol, nameCol).
			Where(
				idCol.GT(10),
				idCol.LT(1000),
				statusCol.EQ(1),
			).
			Build()
	}
}

// BenchmarkQueryBuilder_WithJoins measures Build() performance with joins
func BenchmarkQueryBuilder_WithJoins(b *testing.B) {
	usersTable := newMockTable("users")
	ordersTable := newMockTable("orders")

	uid := NewCol[int](usersTable, "id", "id", nil)
	oid := NewCol[int](ordersTable, "id", "id", nil)
	oUserID := NewCol[int](ordersTable, "user_id", "user_id", nil)
	oTotal := NewCol[float64](ordersTable, "total", "total", nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Select(uid, oid, oTotal).
			InnerJoin(uid, oUserID).
			Where(oTotal.GT(50.0)).
			Build()
	}
}

// BenchmarkQueryBuilder_GroupBy measures Build() with GROUP BY
func BenchmarkQueryBuilder_GroupBy(b *testing.B) {
	table := newMockTable("sales")
	categoryCol := NewCol[string](table, "category", "category", nil)
	totalCol := NewCol[float64](table, "total", "total", nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Select(categoryCol, totalCol).
			GroupBy(categoryCol).
			Build()
	}
}

// BenchmarkQueryBuilder_WithAliases measures Build() with column aliases
func BenchmarkQueryBuilder_WithAliases(b *testing.B) {
	table := newMockTable("orders")
	idCol := NewCol[int](table, "id", "id", nil)
	userIDCol := NewCol[int](table, "user_id", "user_id", nil)
	totalCol := NewCol[float64](table, "total", "total", nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Select(
			idCol.As("order_id"),
			userIDCol.As("uid"),
			totalCol.As("order_total"),
		).
			Where(totalCol.GT(100.0)).
			Build()
	}
}
