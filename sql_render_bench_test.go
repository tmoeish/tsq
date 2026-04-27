package tsq

import (
	"testing"

	"gopkg.in/gorp.v2"
)

// BenchmarkRenderCanonicalSQL measures the performance of converting raw SQL with markers to canonical form
func BenchmarkRenderCanonicalSQL(b *testing.B) {
	raw := "SELECT " + rawQualifiedIdentifier("users", "id") +
		", " + rawQualifiedIdentifier("users", "name") +
		", " + rawQualifiedIdentifier("users", "email") +
		" FROM " + rawIdentifier("users") +
		" WHERE " + rawQualifiedIdentifier("users", "active") + " = ? AND " +
		rawQualifiedIdentifier("users", "deleted_at") + " IS NULL"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = renderCanonicalSQL(raw)
	}
}

// BenchmarkRenderSQLForDialect_Postgres measures rendering SQL for PostgreSQL dialect with parameter rebinding
func BenchmarkRenderSQLForDialect_Postgres(b *testing.B) {
	raw := "SELECT " + rawQualifiedIdentifier("users", "id") +
		", " + rawQualifiedIdentifier("users", "name") +
		", " + rawQualifiedIdentifier("users", "email") +
		" FROM " + rawIdentifier("users") +
		" WHERE " + rawQualifiedIdentifier("users", "active") + " = ? AND " +
		rawQualifiedIdentifier("users", "created_at") + " > ?"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = renderSQLForDialect(raw, gorp.PostgresDialect{})
	}
}

// BenchmarkRenderSQLForDialect_MySQL measures rendering SQL for MySQL dialect with parameter rebinding
func BenchmarkRenderSQLForDialect_MySQL(b *testing.B) {
	raw := "SELECT " + rawQualifiedIdentifier("orders", "id") +
		", " + rawQualifiedIdentifier("orders", "user_id") +
		", " + rawQualifiedIdentifier("orders", "total") +
		", " + rawQualifiedIdentifier("orders", "status") +
		" FROM " + rawIdentifier("orders") +
		" WHERE " + rawQualifiedIdentifier("orders", "status") + " = ? AND " +
		rawQualifiedIdentifier("orders", "total") + " > ?"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = renderSQLForDialect(raw, gorp.MySQLDialect{})
	}
}

// BenchmarkRawQualifiedIdentifier measures identifier encoding performance
func BenchmarkRawQualifiedIdentifier(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = rawQualifiedIdentifier("users", "email_address")
	}
}

// BenchmarkComplexSQLRender measures a complex query with many joins
func BenchmarkComplexSQLRender(b *testing.B) {
	raw := "SELECT " + rawQualifiedIdentifier("u", "id") +
		", " + rawQualifiedIdentifier("u", "name") +
		", " + rawQualifiedIdentifier("o", "id") +
		", " + rawQualifiedIdentifier("o", "total") +
		", " + rawQualifiedIdentifier("i", "id") +
		", " + rawQualifiedIdentifier("i", "product_name") +
		" FROM " + rawIdentifier("users") + " u" +
		" INNER JOIN " + rawIdentifier("orders") + " o ON " +
		rawQualifiedIdentifier("u", "id") + " = " + rawQualifiedIdentifier("o", "user_id") +
		" LEFT JOIN " + rawIdentifier("order_items") + " i ON " +
		rawQualifiedIdentifier("o", "id") + " = " + rawQualifiedIdentifier("i", "order_id") +
		" WHERE " + rawQualifiedIdentifier("u", "active") + " = ? AND " +
		rawQualifiedIdentifier("o", "created_at") + " > ?"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = renderSQLForDialect(raw, gorp.PostgresDialect{})
	}
}
