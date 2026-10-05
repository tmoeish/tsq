package tsq

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
)

func TestNullableOrderingIsSpelledPerDialect(t *testing.T) {
	render := func(d tsqdialect.Name, ob ...OrderBy) (string, error) {
		sql, _, err := Select(Notes.Columns()...).From(Notes).OrderBy(ob[0], ob[1:]...).MustBuild().SQL(d)
		if err != nil {
			return "", err
		}

		return sql[strings.Index(sql, " ORDER BY ")+len(" ORDER BY "):], nil
	}

	tests := []struct {
		name  string
		order OrderBy
		want  map[tsqdialect.Name]string
	}{
		{"not null column", Note_ID.Asc().NullsLast(), map[tsqdialect.Name]string{
			tsqdialect.MySQL:    "`notes`.`id` ASC",
			tsqdialect.Postgres: `"notes"."id" ASC`,
			tsqdialect.SQLite:   `"notes"."id" ASC`,
		}},
		{"default ascending", Note_Rating.Asc(), map[tsqdialect.Name]string{
			tsqdialect.MySQL:    "`notes`.`rating` ASC",
			tsqdialect.Postgres: `"notes"."rating" ASC NULLS FIRST`,
			tsqdialect.SQLite:   `"notes"."rating" ASC NULLS FIRST`,
		}},
		{"default descending", Note_Rating.Desc(), map[tsqdialect.Name]string{
			tsqdialect.MySQL:    "`notes`.`rating` DESC",
			tsqdialect.Postgres: `"notes"."rating" DESC NULLS LAST`,
			tsqdialect.SQLite:   `"notes"."rating" DESC NULLS LAST`,
		}},
		{"ascending, nulls last", Note_Rating.Asc().NullsLast(), map[tsqdialect.Name]string{
			tsqdialect.MySQL:    "`notes`.`rating` IS NULL ASC, `notes`.`rating` ASC",
			tsqdialect.Postgres: `"notes"."rating" ASC NULLS LAST`,
			tsqdialect.SQLite:   `"notes"."rating" ASC NULLS LAST`,
		}},
		{"descending, nulls first", Note_Rating.Desc().NullsFirst(), map[tsqdialect.Name]string{
			tsqdialect.MySQL:    "`notes`.`rating` IS NULL DESC, `notes`.`rating` DESC",
			tsqdialect.Postgres: `"notes"."rating" DESC NULLS FIRST`,
			tsqdialect.SQLite:   `"notes"."rating" DESC NULLS FIRST`,
		}},
	}

	for _, tt := range tests {
		for _, d := range []tsqdialect.Name{onMySQL, onPostgres, onSQLite} {
			got, err := render(d, tt.order)
			if err != nil || got != tt.want[d] {
				t.Errorf("%s on %s = %q, %v; want %q", tt.name, d, got, err, tt.want[d])
			}
		}
	}

	// A column of an outer-joined table can be NULL too.
	outer := Select(User_ID).From(Users).LeftJoin(Orders, Order_UserID.EQ(User_ID)).OrderBy(Order_Amount.Asc()).MustBuild()
	if sql, _, err := outer.SQL(onPostgres); err != nil || !strings.HasSuffix(sql, `"orders"."amount" ASC NULLS FIRST`) {
		t.Errorf("outer-joined order = %s, %v", sql, err)
	}

	// MySQL cannot order a set operation by an expression.
	union := Select(Note_Rating).From(Notes).Union(Select(Note_Rating).From(Notes))
	if _, _, err := union.OrderBy(Note_Rating.Asc().NullsLast()).MustBuild().SQL(onMySQL); err == nil {
		t.Error("expected NULLS LAST on a MySQL set operation to be refused")
	}

	if sql, _, err := union.OrderBy(Note_Rating.Asc()).MustBuild().SQL(onMySQL); err != nil || !strings.HasSuffix(sql, "ORDER BY `rating` ASC") {
		t.Errorf("default order of a MySQL set operation = %s, %v", sql, err)
	}
}

// TestExpressionsThatDifferInABoundValueAreDifferent covers GROUP BY and ORDER BY
// finding their select item by text in which bound values print as placeholders:
// CASE amount > 100 and CASE amount > 1000 matched, so the query ordered by the
// wrong item and grouped by an expression it did not select.
func TestExpressionsThatDifferInABoundValueAreDifferent(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)
	users := seedUsers(t, rt, "a")

	for _, amount := range []int64{50, 500, 5000} {
		if err := Orders.Insert(ctx, rt, &order{UserID: users[0].ID, Amount: amount, Note: "x"}); err != nil {
			t.Fatal(err)
		}
	}

	type row struct{ ID, Big, Huge int64 }

	big := Case(Order_Amount.GT(Val(int64(100))), Val(int64(1))).Else(Val(int64(0))).End()
	huge := Case(Order_Amount.GT(Val(int64(1000))), Val(int64(1))).Else(Val(int64(0))).End()

	rows, err := Select(
		MapInto(Order_ID, func(r *row) *int64 { return &r.ID }),
		MapInto(big, func(r *row) *int64 { return &r.Big }),
		MapInto(huge, func(r *row) *int64 { return &r.Huge }),
	).From(Orders).OrderBy(huge.Desc(), Order_ID.Asc()).MustBuild().List(ctx, rt)
	if err != nil || len(rows) != 3 || rows[0].Huge != 1 {
		t.Fatalf("rows = %+v, %v; want the only huge row first", rows, err)
	}

	if _, err := Select(
		MapInto(big, func(r *row) *int64 { return &r.Big }),
		MapInto(Count(Order_ID), func(r *row) *int64 { return &r.ID }),
	).From(Orders).GroupBy(huge).Build(); err == nil {
		t.Fatal("Build grouped by huge while selecting big: want it refused")
	}
}

// TestNullPlacementOfAPositionalTermOnMySQL covers an ordered expression with
// bound values, written as its select-list position: MySQL's IS NULL key became
// "2 IS NULL", a constant, and NullsLast was lost.
func TestNullPlacementOfAPositionalTermOnMySQL(t *testing.T) {
	type row struct {
		ID    int64
		Label sql.Null[string]
	}

	label := Case(Order_Amount.GT(Val(int64(100))), Val("big")).End()
	q := Select(
		MapInto(Order_ID, func(r *row) *int64 { return &r.ID }),
		MapIntoNull(label, func(r *row) *sql.Null[string] { return &r.Label }),
	).From(Orders).OrderBy(label.Asc().NullsLast()).MustBuild()

	got, _, err := q.SQL(tsqdialect.MySQL)
	if err != nil || strings.Contains(got, "2 IS NULL") || !strings.Contains(got, "END IS NULL ASC, 2 ASC") {
		t.Fatalf("MySQL = %s, %v; want the expression tested for NULL and the position ordered", got, err)
	}
}
