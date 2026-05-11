package tsq

import (
	"strings"
	"testing"
)

func TestQueryBuilder_MultiCall_Select(t *testing.T) {
	table := newMockTable("users")
	id := newMockColumn(table, "id")
	name := newMockColumn(table, "name")

	sb := Select(id)
	qb := sb.From(table)
	second := sb.From(table)
	if qb == nil || second == nil {
		t.Fatal("expected initial Select/From to succeed")
	}
	_ = name
	_, err := second.Build()
	if err == nil {
		t.Fatal("expected stale SelectBuilder to reject a second From()")
	}

	if !strings.Contains(err.Error(), "From() is not available") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestQueryBuilder_MultiCall_From(t *testing.T) {
	table1 := newMockTable("users")
	table2 := newMockTable("orders")
	id := newMockColumn(table1, "id")

	fb := From[Table](table1)
	qb := fb.Select(id)
	second := fb.Select(id)
	if qb == nil || second == nil {
		t.Fatal("expected initial From/Select to succeed")
	}
	_ = table2
	_, err := second.Build()
	if err == nil {
		t.Fatal("expected stale FromBuilder to reject a second Select()")
	}

	if !strings.Contains(err.Error(), "Select() is not available") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestQueryBuilder_MultiCall_Where(t *testing.T) {
	table := newMockTable("users")
	id := newMockColumn(table, "id")

	qb := Select(id).From(table)
	first := qb.Where(id.EQ("1"))
	_, err := qb.Where(id.EQ("2")).Build()
	if err == nil {
		t.Fatal("expected stale QueryBuilder to reject a second Where()")
	}
	if first == nil {
		t.Fatal("expected initial Where() to succeed")
	}

	if !strings.Contains(err.Error(), "Where() is not available") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestQueryBuilder_MultiCall_GroupBy(t *testing.T) {
	table := newMockTable("users")
	id := newMockColumn(table, "id")
	name := newMockColumn(table, "name")

	qb := Select(id).From(table)
	first := qb.GroupBy(id)
	_, err := qb.GroupBy(name).Build()
	if err == nil {
		t.Fatal("expected stale QueryBuilder to reject a second GroupBy()")
	}
	if first == nil {
		t.Fatal("expected initial GroupBy() to succeed")
	}

	if !strings.Contains(err.Error(), "GroupBy() is not available") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestQueryBuilder_MultiCall_Having(t *testing.T) {
	table := newMockTable("users")
	id := newMockColumn(table, "id")

	qb := Select(id).From(table).GroupBy(id)
	first := qb.Having(id.EQ("1"))
	_, err := qb.Having(id.EQ("2")).Build()
	if err == nil {
		t.Fatal("expected stale GroupedQueryBuilder to reject a second Having()")
	}
	if first == nil {
		t.Fatal("expected initial Having() to succeed")
	}

	if !strings.Contains(err.Error(), "Having() is not available") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestQueryBuilder_MultiCall_KwSearch(t *testing.T) {
	table := newMockTable("users")
	id := newMockColumn(table, "id")
	name := newMockColumn(table, "name")

	qb := Select(id).From(table)
	first := qb.Search(id)
	_, err := qb.Search(name).Build()
	if err == nil {
		t.Fatal("expected stale QueryBuilder to reject a second Search()")
	}
	if first == nil {
		t.Fatal("expected initial Search() to succeed")
	}

	if !strings.Contains(err.Error(), "Search() is not available") {
		t.Fatalf("unexpected error: %v", err)
	}
}
