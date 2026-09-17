package tsq

import (
	"strings"
	"testing"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
)

func TestNullableOrderingIsSpelledPerDialect(t *testing.T) {
	render := func(d tsqdialect.Dialect, ob ...OrderBy) (string, error) {
		sql, _, err := Select(Notes.Columns()...).From(Notes).OrderBy(ob...).MustBuild().SQL(d)
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
		for _, d := range []tsqdialect.Dialect{onMySQL, onPostgres, onSQLite} {
			got, err := render(d, tt.order)
			if err != nil || got != tt.want[d.Name()] {
				t.Errorf("%s on %s = %q, %v; want %q", tt.name, d.Name(), got, err, tt.want[d.Name()])
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
