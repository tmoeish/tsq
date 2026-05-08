package tsq

import (
	"strings"
	"testing"
)

func TestQueryBuilder_MultiCall_Select(t *testing.T) {
	table := newMockTable("users")
	id := newMockColumn(table, "id")
	name := newMockColumn(table, "name")

	qb := Select(id).Select(name)
	_, err := qb.Build()
	if err == nil {
		t.Fatal("expected error when calling Select() twice")
	}

	if !strings.Contains(err.Error(), "Select() can only be called once") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestQueryBuilder_MultiCall_From(t *testing.T) {
	table1 := newMockTable("users")
	table2 := newMockTable("orders")
	id := newMockColumn(table1, "id")

	qb := Select(id).From(table1).From(table2)
	_, err := qb.Build()
	if err == nil {
		t.Fatal("expected error when calling From() twice")
	}

	if !strings.Contains(err.Error(), "From() can only be called once") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestQueryBuilder_MultiCall_Where(t *testing.T) {
	table := newMockTable("users")
	id := newMockColumn(table, "id")

	qb := Select(id).From(table).Where(id.EQ("1")).Where(id.EQ("2"))
	_, err := qb.Build()
	if err == nil {
		t.Fatal("expected error when calling Where() twice")
	}

	if !strings.Contains(err.Error(), "Where() or SetWhere() can only be called once") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestQueryBuilder_MultiCall_SetWhere(t *testing.T) {
	table := newMockTable("users")
	id := newMockColumn(table, "id")

	qb := Select(id).From(table).SetWhere(id.EQ("1")).SetWhere(id.EQ("2"))
	_, err := qb.Build()
	if err == nil {
		t.Fatal("expected error when calling SetWhere() twice")
	}

	if !strings.Contains(err.Error(), "Where() or SetWhere() can only be called once") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestQueryBuilder_MultiCall_GroupBy(t *testing.T) {
	table := newMockTable("users")
	id := newMockColumn(table, "id")
	name := newMockColumn(table, "name")

	qb := Select(id).From(table).GroupBy(id).GroupBy(name)
	_, err := qb.Build()
	if err == nil {
		t.Fatal("expected error when calling GroupBy() twice")
	}

	if !strings.Contains(err.Error(), "GroupBy() can only be called once") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestQueryBuilder_MultiCall_Having(t *testing.T) {
	table := newMockTable("users")
	id := newMockColumn(table, "id")

	qb := Select(id).From(table).GroupBy(id).Having(id.EQ("1")).Having(id.EQ("2"))
	_, err := qb.Build()
	if err == nil {
		t.Fatal("expected error when calling Having() twice")
	}

	if !strings.Contains(err.Error(), "Having() can only be called once") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestQueryBuilder_MultiCall_KwSearch(t *testing.T) {
	table := newMockTable("users")
	id := newMockColumn(table, "id")
	name := newMockColumn(table, "name")

	qb := Select(id).From(table).KwSearch(id).KwSearch(name)
	_, err := qb.Build()
	if err == nil {
		t.Fatal("expected error when calling KwSearch() twice")
	}

	if !strings.Contains(err.Error(), "KwSearch() or SetKwSearch() can only be called once") {
		t.Fatalf("unexpected error: %v", err)
	}
}
