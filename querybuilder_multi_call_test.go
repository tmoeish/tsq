package tsq

import "testing"

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
	if _, err := second.Build(); err != nil {
		t.Fatalf("expected SelectBuilder reuse to branch cleanly, got %v", err)
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
	if _, err := second.Build(); err != nil {
		t.Fatalf("expected FromBuilder reuse to branch cleanly, got %v", err)
	}
}

func TestQueryBuilder_MultiCall_Where(t *testing.T) {
	table := newMockTable("users")
	id := newMockColumn(table, "id")

	qb := Select(id).From(table)
	first := qb.Where(id.EQVal("1"))
	second, err := qb.Where(id.EQVal("2")).Build()
	if first == nil {
		t.Fatal("expected initial Where() to succeed")
	}
	if err != nil {
		t.Fatalf("expected QueryBuilder reuse to branch cleanly, got %v", err)
	}
	if len(second.listArgs) != 1 || second.listArgs[0] != "2" {
		t.Fatalf("expected second branch args [2], got %#v", second.listArgs)
	}
}

func TestQueryBuilder_MultiCall_GroupBy(t *testing.T) {
	table := newMockTable("users")
	id := newMockColumn(table, "id")
	name := newMockColumn(table, "name")

	qb := Select(id).From(table)
	first := qb.GroupBy(id)
	second, err := qb.GroupBy(name).Build()
	if first == nil {
		t.Fatal("expected initial GroupBy() to succeed")
	}
	if err != nil {
		t.Fatalf("expected GroupBy reuse to branch cleanly, got %v", err)
	}
	if got := second.ListSQL(); got != `SELECT "users"."id" FROM "users" GROUP BY "users"."name"` {
		t.Fatalf("unexpected second branch SQL %q", got)
	}
}

func TestQueryBuilder_MultiCall_Having(t *testing.T) {
	table := newMockTable("users")
	id := newMockColumn(table, "id")

	qb := Select(id).From(table).GroupBy(id)
	first := qb.Having(id.EQVal("1"))
	second, err := qb.Having(id.EQVal("2")).Build()
	if first == nil {
		t.Fatal("expected initial Having() to succeed")
	}
	if err != nil {
		t.Fatalf("expected Having reuse to branch cleanly, got %v", err)
	}
	if len(second.listArgs) != 1 || second.listArgs[0] != "2" {
		t.Fatalf("expected second branch args [2], got %#v", second.listArgs)
	}
}

func TestQueryBuilder_MultiCall_KwSearch(t *testing.T) {
	table := newMockTable("users")
	id := newMockColumn(table, "id")
	name := newMockColumn(table, "name")

	qb := Select(id).From(table)
	first := qb.Search(id)
	second, err := qb.Search(name).Build()
	if first == nil {
		t.Fatal("expected initial Search() to succeed")
	}
	if err != nil {
		t.Fatalf("expected Search reuse to branch cleanly, got %v", err)
	}
	if len(second.kwCols) != 1 || second.kwCols[0].OutputName() != "name" {
		t.Fatalf("expected second branch keyword search to target name, got %#v", second.kwCols)
	}
}
