package tsq

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
	sqld "github.com/tmoeish/tsq/v5/internal/sqldialect"
)

func TestRenderQuotesAndNumbersPerDialect(t *testing.T) {
	name := NewParam[string]("name")
	q := Select(User_ID, User_Name).From(Users).
		Where(User_Name.EQ(name), User_Version.GT(Val(int64(2)))).
		MustBuild()

	tests := []struct {
		dialect tsqdialect.Name
		want    string
	}{
		{onSQLite, `SELECT "users"."id", "users"."name" FROM "users" WHERE ("users"."name" = ? AND "users"."version" > ? AND "users"."deleted_at" = 0)`},
		{onMySQL, "SELECT `users`.`id`, `users`.`name` FROM `users` WHERE (`users`.`name` = ? AND `users`.`version` > ? AND `users`.`deleted_at` = 0)"},
		{onPostgres, `SELECT "users"."id", "users"."name" FROM "users" WHERE ("users"."name" = $1 AND "users"."version" > $2 AND "users"."deleted_at" = 0)`},
	}

	for _, tt := range tests {
		t.Run(string(tt.dialect), func(t *testing.T) {
			sql, args := sqlOf(t, q, tt.dialect, name.Bind("amy"))
			if sql != tt.want {
				t.Fatalf("SQL =\n%s\nwant\n%s", sql, tt.want)
			}

			if !reflect.DeepEqual(args, []any{"amy", int64(2)}) {
				t.Fatalf("args = %#v", args)
			}
		})
	}
}

func TestListParamExpandsAndKeepsEmptyListsExplicit(t *testing.T) {
	in := Select(User_ID).From(Users.WithDeleted()).Where(User_ID.In(User_ID.ListParam())).MustBuild()
	notIn := Select(User_ID).From(Users.WithDeleted()).Where(User_ID.NotIn(User_ID.ListParam())).MustBuild()

	sql, args := sqlOf(t, in, onPostgres, User_ID.BindList(4, 5))
	if !strings.HasSuffix(sql, `"users"."id" IN ($1, $2)`) || len(args) != 2 {
		t.Fatalf("IN list rendered %s %v", sql, args)
	}

	// An empty IN matches nothing and an empty NOT IN matches everything; neither
	// drops the predicate.
	if sql, _ := sqlOf(t, in, onSQLite, User_ID.BindList()); !strings.HasSuffix(sql, `IN (NULL)`) {
		t.Fatalf("empty IN rendered %s", sql)
	}

	if sql, _ := sqlOf(t, notIn, onSQLite, User_ID.BindList()); !strings.HasSuffix(sql, `NOT IN (SELECT 1 WHERE 1 = 0)`) {
		t.Fatalf("empty NOT IN rendered %s", sql)
	}
}

func TestPatternsEscapeWildcards(t *testing.T) {
	prefix := NewParam[string]("prefix")
	q := Select(User_ID).From(Users).Where(StartsWith(User_Name, prefix), Contains(User_Email, Val("50%_off"))).MustBuild()

	sql, args := sqlOf(t, q, onSQLite, prefix.Bind("a~b"))
	if strings.Count(sql, "ESCAPE '~'") != 2 {
		t.Fatalf("every pattern must declare its escape character: %s", sql)
	}

	want := []any{"a~~b%", "%50~%~_off%"}
	if !reflect.DeepEqual(args, want) {
		t.Fatalf("args = %#v, want %#v", args, want)
	}
}

func TestDialectCapabilitiesAreCheckedWhenRendered(t *testing.T) {
	full := Select(User_ID).From(Users).FullJoin(Orders, Order_UserID.EQ(User_ID)).MustBuild()

	if _, _, err := full.SQL(onMySQL); !isUnsupported(err) {
		t.Fatalf("mysql FULL JOIN error = %v", err)
	}

	if _, _, err := full.SQL(onPostgres); err != nil {
		t.Fatalf("postgres FULL JOIN error = %v", err)
	}

	locked := Select(User_ID).From(Users).ForUpdate().MustBuild()
	if _, _, err := locked.SQL(onSQLite); !isUnsupported(err) {
		t.Fatalf("sqlite FOR UPDATE error = %v", err)
	}

	// A literal that merely contains the words is not a row lock.
	literal := Select(User_ID).From(Users).Where(User_Name.EQ(Val(" FOR UPDATE "))).MustBuild()
	if _, _, err := literal.SQL(onSQLite); err != nil {
		t.Fatalf("a string literal was mistaken for a capability: %v", err)
	}
}

func isUnsupported(err error) bool {
	_, ok := errors.AsType[*tsqdialect.UnsupportedCapabilityError](err)

	return ok
}

func TestDatePartsAreSpelledPerDialect(t *testing.T) {
	q := Select(MapInto(Year(User_CreatedAt), func(r *namedRow) *int64 { return &r.ID }).Named("year")).
		From(Users).MustBuild()

	for d, want := range map[tsqdialect.Name]string{
		onMySQL:    "YEAR(`users`.`created_at`)",
		onPostgres: `CAST(EXTRACT(YEAR FROM "users"."created_at") AS BIGINT)`,
		onSQLite:   `CAST(strftime('%Y', SUBSTR("users"."created_at", 1, 19)) AS INTEGER)`,
	} {
		if sql, _ := sqlOf(t, q, d); !strings.Contains(sql, want) {
			t.Fatalf("%s: %s does not contain %s", d, sql, want)
		}
	}
}

func TestIdentifiersAreValidatedForTheDialect(t *testing.T) {
	long := firstRejectedIdentifier(t, sqld.PostgresDialect{})
	table := namedTable(long)

	q := Select(table.Columns()...).From(table).MustBuild()
	if _, _, err := q.SQL(onPostgres); err == nil {
		t.Fatal("expected an identifier longer than postgres allows to be rejected")
	}

	if _, _, err := q.SQL(onMySQL); err != nil {
		t.Fatalf("mysql accepts %d characters: %v", len(long), err)
	}
}

func TestSetOperationsCTEAndSubqueries(t *testing.T) {
	big := Select(Order_UserID).From(Orders).Where(Order_Amount.GT(Val(int64(100))))
	cte := CTE("big_orders", big)
	bigUser := Order_UserID.WithTable(cte)

	q := Select(User_ID).From(Users.WithDeleted()).
		Join(cte, bigUser.EQ(User_ID)).
		Union(Select(User_ID).From(Users.WithDeleted()).Where(User_Name.EQ(Val("root")))).
		MustBuild()

	sql, args := sqlOf(t, q, onSQLite)
	want := `WITH "big_orders" AS (SELECT "orders"."user_id" FROM "orders" WHERE "orders"."amount" > ?) ` +
		`SELECT "users"."id" FROM "users" INNER JOIN "big_orders" ON "big_orders"."user_id" = "users"."id" ` +
		`UNION SELECT "users"."id" FROM "users" WHERE "users"."name" = ?`

	if sql != want {
		t.Fatalf("SQL =\n%s\nwant\n%s", sql, want)
	}

	if !reflect.DeepEqual(args, []any{int64(100), "root"}) {
		t.Fatalf("args = %#v", args)
	}

	intersect := Select(User_ID).From(Users).Intersect(Select(User_ID).From(Users).Where(User_Version.GT(Val(int64(1))))).MustBuild()
	if _, _, err := intersect.SQL(onSQLite); err != nil {
		t.Fatalf("sqlite supports INTERSECT: %v", err)
	}
}

func TestCorrelatedSubqueryCarriesItsParameters(t *testing.T) {
	min := NewParam[int64]("min")
	sub := Select(Order_ID).From(Orders).Correlate(Users).Where(Order_UserID.EQ(User_ID), Order_Amount.GTE(min))

	q := Select(User_ID).From(Users).Where(Exists(sub)).MustBuild()

	sql, args := sqlOf(t, q, onPostgres, min.Bind(10))
	if !strings.Contains(sql, `EXISTS (SELECT "orders"."id" FROM "orders" WHERE ("orders"."user_id" = "users"."id" AND "orders"."amount" >= $1))`) {
		t.Fatalf("SQL = %s", sql)
	}

	if !reflect.DeepEqual(args, []any{int64(10)}) {
		t.Fatalf("args = %#v", args)
	}

	// The outer query must provide the correlated table.
	orphan := Select(Order_ID).From(Orders).Where(Exists(sub))
	if _, err := orphan.Build(); err == nil || !strings.Contains(err.Error(), "users") {
		t.Fatalf("expected the missing outer table to be reported, got %v", err)
	}

	// And a correlated query cannot run on its own.
	inner := Select(Order_ID).From(Orders).Correlate(Users).Where(Order_UserID.EQ(User_ID)).MustBuild()
	if _, _, err := inner.SQL(onSQLite); err == nil {
		t.Fatal("expected a correlated query to be refused outside a subquery")
	}
}

func TestCaseRendersBranchesInOrder(t *testing.T) {
	label := Case[string]().
		When(User_Version.GT(Val(int64(10))), Val("hot")).
		When(User_Name.IsNull(), User_Email).
		Else(Val("cold")).
		End()

	q := Select(MapInto(label, func(r *namedRow) *string { return &r.Name }).Named("label")).From(Users.WithDeleted()).MustBuild()

	sql, args := sqlOf(t, q, onSQLite)
	want := `SELECT CASE WHEN "users"."version" > ? THEN ? WHEN "users"."name" IS NULL THEN "users"."email" ELSE ? END FROM "users"`

	if sql != want {
		t.Fatalf("SQL =\n%s\nwant\n%s", sql, want)
	}

	if !reflect.DeepEqual(args, []any{int64(10), "hot", "cold"}) {
		t.Fatalf("args = %#v", args)
	}
}

func TestQueryRenderingIsCachedPerDialect(t *testing.T) {
	q := Select(User_ID).From(Users).MustBuild()

	first, err := q.statement(sqld.SQLiteDialect{}, renderMode{})
	if err != nil {
		t.Fatal(err)
	}

	second, _ := q.statement(sqld.SQLiteDialect{}, renderMode{})
	other, _ := q.statement(sqld.MySQLDialect{}, renderMode{})

	if first != second {
		t.Fatal("expected the second render for the same dialect to hit the cache")
	}

	if first == other {
		t.Fatal("expected each dialect to render separately")
	}
}

// TestSingleRowReadsLimitBeforeTheLock guards the clause order Get, Find, Exists
// and Scalar rely on: every dialect wants LIMIT before FOR UPDATE / FOR SHARE.
func TestSingleRowReadsLimitBeforeTheLock(t *testing.T) {
	single := func(q *Query[user], d tsqdialect.Name) (string, []any) {
		t.Helper()

		impl, err := sqld.For(d)
		if err != nil {
			t.Fatal(err)
		}

		stmt, err := q.statement(impl, renderMode{single: true})
		if err != nil {
			t.Fatal(err)
		}

		sql, args, err := stmt.assemble(impl, argSet{})
		if err != nil {
			t.Fatal(err)
		}

		return sql, args
	}

	locked := Select(User_ID).From(Users.WithDeleted()).ForUpdate().MustBuild()
	for _, d := range []tsqdialect.Name{onMySQL, onPostgres} {
		if sql, _ := single(locked, d); !strings.HasSuffix(sql, " LIMIT 1 FOR UPDATE") {
			t.Errorf("%s: %s", d, sql)
		}
	}

	// A limit the builder set is kept rather than replaced.
	limited := Select(User_ID).From(Users.WithDeleted()).Limit(5).MustBuild()
	if sql, args := single(limited, onSQLite); !strings.HasSuffix(sql, " LIMIT ?") || len(args) != 1 || fmt.Sprint(args[0]) != "5" {
		t.Errorf("builder limit: %s %v", sql, args)
	}
}
