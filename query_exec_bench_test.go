package tsq

import (
	"testing"
)

// BenchmarkQueryBuilder_SimpleBuild measures basic query builder Build() performance
func BenchmarkQueryBuilder_SimpleWhere(b *testing.B) {
	table := newMockTable("users")
	idCol := newColForTable[Table, int](table, "id", "id", nil)
	nameCol := newColForTable[Table, string](table, "name", "name", nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Select(idCol, nameCol).
			From(idCol.Table()).
			Where(idCol.GT(100)).
			Build()
	}
}

// BenchmarkQueryBuilder_MultipleConditions measures WHERE with multiple conditions
func BenchmarkQueryBuilder_MultipleConditions(b *testing.B) {
	table := newMockTable("users")
	idCol := newColForTable[Table, int](table, "id", "id", nil)
	statusCol := newColForTable[Table, int](table, "status", "status", nil)
	nameCol := newColForTable[Table, string](table, "name", "name", nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Select(idCol, statusCol, nameCol).
			From(idCol.Table()).
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

// BenchmarkQueryBuilder_GroupBy measures Build() with GROUP BY
func BenchmarkQueryBuilder_GroupBy(b *testing.B) {
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

// BenchmarkQueryBuilder_WithAliases measures Build() with column aliases
func BenchmarkQueryBuilder_WithAliases(b *testing.B) {
	table := newMockTable("orders")
	idCol := newColForTable[Table, int](table, "id", "id", nil)
	userIDCol := newColForTable[Table, int](table, "user_id", "user_id", nil)
	totalCol := newColForTable[Table, float64](table, "total", "total", nil)

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
