package tsq

import (
	"context"
	"errors"
	"strings"
	"testing"

	tsqdialect "github.com/tmoeish/tsq/v4/dialect"
)

func TestQueryBuilder_Union(t *testing.T) {
	users := newMockTable("users")
	orders := newMockTable("orders")
	userID := newMockColumn(users, "id")
	orderUserID := newMockColumn(orders, "user_id")
	qb := Select(userID).From(userID.Table()).Union(Select(orderUserID).From(orderUserID.Table()))
	core := mustBuilderCore[Table](t, qb)
	if len(core.spec.SetOps) != 1 {
		t.Fatalf("expected 1 set operation, got %d", len(core.spec.SetOps))
	}
	if core.spec.SetOps[0].op != unionType {
		t.Fatalf("expected UNION operation, got %s", core.spec.SetOps[0].op)
	}
}

func TestQueryBuilder_SetOperationRejectsMismatchedSelectCounts(t *testing.T) {
	users := newMockTable("users")
	id := newMockColumn(users, "id")
	name := newMockColumn(users, "name")
	_, err := Select(id).From(id.Table()).Union(Select(id, name).From(id.Table())).Build()
	if err == nil {
		t.Fatal("expected mismatched select counts to fail")
	}
	if !strings.Contains(err.Error(), "matching select column counts") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestQueryBuilder_SetOperationRejectsKeywordSearch(t *testing.T) {
	users := newMockTable("users")
	id := newMockColumn(users, "id")
	_, err := Select(id).From(id.Table()).Search(id).Union(Select(id).From(id.Table())).Build()
	if err == nil {
		t.Fatal("expected keyword search with set operations to fail")
	}
	if !strings.Contains(err.Error(), "do not support keyword search") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestQueryBuilder_SetOperationBuildsWrappedCountSQL(t *testing.T) {
	users := newMockTable("users")
	orders := newMockTable("orders")
	userID := newMockColumn(users, "id")
	orderUserID := newMockColumn(orders, "user_id")
	query := mustBuild(Select(userID).From(userID.Table()).UnionAll(Select(orderUserID).From(orderUserID.Table())))
	wantList := `SELECT "users"."id" FROM "users" UNION ALL SELECT "orders"."user_id" FROM "orders"`
	if query.ListSQL() != wantList {
		t.Fatalf("expected list SQL %q, got %q", wantList, query.ListSQL())
	}
	wantCount := `SELECT COUNT(1) FROM (` + wantList + `) AS _tsq_cnt`
	if query.CountSQL() != wantCount {
		t.Fatalf("expected count SQL %q, got %q", wantCount, query.CountSQL())
	}
}

func setOpUserCols() (columnImpl[inVarUser, int64], columnImpl[inVarUser, string]) {
	users := newMockTable("users")
	id := newColForTable[inVarUser, int64](users, "id", "userId", toScanPointer(func(h *inVarUser) *int64 { return &h.ID }))
	name := newColForTable[inVarUser, string](users, "name", "name", toScanPointer(func(h *inVarUser) *string { return &h.Name }))

	return id, name
}

// TestSetOperationChainsReadLeftToRight covers a flat chain mixing INTERSECT with
// UNION: SQLite evaluates it left to right and MySQL/PostgreSQL bind INTERSECT
// first, so the same query returned different rows per dialect. The part before
// the INTERSECT is now grouped as a derived table, and a combined operand is a
// derived table too (SQLite has no parenthesized compound SELECT).
func TestSetOperationChainsReadLeftToRight(t *testing.T) {
	id, _ := setOpUserCols()
	pick := func(v int64) *whereQueryBuilder[inVarUser] {
		return Select(id).From(id.Table()).Where(id.EQVal(v)).(*whereQueryBuilder[inVarUser])
	}

	q := mustBuild(pick(1).Union(pick(2)).Intersect(pick(2)))

	operand := `SELECT "users"."id" FROM "users" WHERE "users"."id" = ?`
	if want := `SELECT * FROM (` + operand + ` UNION ` + operand + `) AS tsq_set INTERSECT ` + operand; q.ListSQL() != want {
		t.Fatalf("list SQL =\n%s\nwant\n%s", q.ListSQL(), want)
	}

	rows, err := q.List(context.Background(), newInVarEngine(t))
	if err != nil || len(rows) != 1 || rows[0].ID != 2 {
		t.Fatalf("(1 ∪ 2) ∩ 2 = %v, %v; want [2]", rows, err)
	}

	nested := mustBuild(pick(1).Union(pick(2).UnionAll(pick(3))))
	if rows, err := nested.List(context.Background(), newInVarEngine(t)); err != nil || len(rows) != 3 {
		t.Fatalf("nested operand = %v, %v; want three rows on SQLite", rows, err)
	}
}

// TestSetOperationsWithAllAreRefusedOnSQLite covers INTERSECT ALL and EXCEPT
// ALL, which SQLite lacks although it has INTERSECT and EXCEPT: they reached it
// as a syntax error instead of ErrUnsupportedCapability.
func TestSetOperationsWithAllAreRefusedOnSQLite(t *testing.T) {
	id, _ := setOpUserCols()
	base := Select(id).From(id.Table())

	for name, q := range map[string]*Query[inVarUser]{
		"intersect all": mustBuild(base.IntersectAll(Select(id).From(id.Table()))),
		"except all":    mustBuild(base.ExceptAll(Select(id).From(id.Table()))),
	} {
		_, err := q.List(context.Background(), newInVarEngine(t))
		if _, ok := errors.AsType[*tsqdialect.ErrUnsupportedCapability](err); !ok {
			t.Errorf("%s on sqlite = %v; want ErrUnsupportedCapability", name, err)
		}
	}
}

// TestSetOperationsAreOrderedByOutputColumns covers ORDER BY on a set operation,
// which only accepts output column names. The builder wrote a table-qualified
// column (refused by PostgreSQL and MySQL) and accepted any column; Page sorted
// by a JSON name or an expression that is no output column.
func TestSetOperationsAreOrderedByOutputColumns(t *testing.T) {
	id, name := setOpUserCols()
	db := newInVarEngine(t)

	ordered := mustBuild(Select(id, name).From(id.Table()).Union(Select(id, name).From(id.Table())).OrderBy(name.Desc()))
	if !strings.HasSuffix(ordered.ListSQL(), `ORDER BY "name" DESC`) {
		t.Fatalf("list SQL = %s; want the output column name", ordered.ListSQL())
	}

	if rows, err := ordered.List(context.Background(), db); err != nil || len(rows) != 3 || rows[0].Name != "carol" {
		t.Fatalf("rows = %v, %v", rows, err)
	}

	for label, stage := range map[string]QueryStage[inVarUser]{
		"not selected": Select(id).From(id.Table()).Union(Select(id).From(id.Table())).OrderBy(name.Desc()),
		"expression":   Select(id, name).From(id.Table()).Union(Select(id, name).From(id.Table())).OrderBy(name.Upper().Asc()),
	} {
		if _, err := stage.Build(); err == nil || !strings.Contains(err.Error(), "ordered by its output columns") {
			t.Errorf("%s: Build = %v; want the term refused", label, err)
		}
	}

	compound := mustBuild(Select(id, name.Upper()).From(id.Table()).Union(Select(id, name).From(id.Table())))

	// The JSON name sorts by the output column it names.
	if page, err := compound.Page(context.Background(), db, &PageRequest{Page: 1, Size: 10, OrderBy: "userId", Order: "desc"}); err != nil || len(page.Data) == 0 || page.Data[0].ID != 3 {
		t.Fatalf("Page by JSON name = %+v, %v", page, err)
	}

	// An expression has no name TSQ knows, so it is not sortable.
	if _, err := compound.Page(context.Background(), db, &PageRequest{Page: 1, Size: 10, OrderBy: "name"}); err == nil {
		t.Fatal("expected Page to refuse sorting by an expression of a set operation")
	}
}

// TestCountOfAGroupedQueryHasDistinctColumnNames covers counting a grouped query
// that selects users.id and orders.id: the count wraps it as a derived table,
// where MySQL refuses two columns of one name (error 1060).
func TestCountOfAGroupedQueryHasDistinctColumnNames(t *testing.T) {
	users := newMockTable("users")
	orders := newMockTable("orders")
	uid := newColForTable[Table, int64](users, "id", "id", nil)
	oid := newColForTable[Table, int64](orders, "id", "id", nil)
	ouid := newColForTable[Table, int64](orders, "user_id", "user_id", nil)

	q := mustBuild(Select(uid, oid).From(users).InnerJoin(orders, ouid.EQ(uid)).GroupBy(uid, oid))
	if !strings.Contains(q.CountSQL(), `SELECT "users"."id", "orders"."id" AS "tsq_c2" FROM`) {
		t.Fatalf("count SQL = %s; want the repeated name replaced", q.CountSQL())
	}
}
