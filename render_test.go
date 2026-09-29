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
	if !strings.HasSuffix(sql, `("users"."id" IN ($1, $2))`) || len(args) != 2 {
		t.Fatalf("IN list rendered %s %v", sql, args)
	}

	// An empty IN matches nothing and an empty NOT IN matches everything; neither
	// drops the predicate. IN (NULL) alone is UNKNOWN, which Not would keep UNKNOWN,
	// so the guard makes it FALSE.
	if sql, _ := sqlOf(t, in, onSQLite, User_ID.BindList()); !strings.HasSuffix(sql, `("users"."id" IN (NULL) AND 1 = 0)`) {
		t.Fatalf("empty IN rendered %s", sql)
	}

	// NOT IN (NULL) alone matches nothing, so the guard carries the empty case;
	// no subquery whose column type PostgreSQL would compare with the column's.
	if sql, _ := sqlOf(t, notIn, onSQLite, User_ID.BindList()); !strings.HasSuffix(sql, `("users"."id" NOT IN (NULL) OR 1 = 1)`) {
		t.Fatalf("empty NOT IN rendered %s", sql)
	}

	if sql, args := sqlOf(t, notIn, onPostgres, User_ID.BindList(4)); !strings.HasSuffix(sql, `("users"."id" NOT IN ($1))`) || len(args) != 1 {
		t.Fatalf("NOT IN list rendered %s %v", sql, args)
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
	table := namedTableOf(long, func(r *longNamedRow) (*int64, *string) { return &r.ID, &r.Name })

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
		InnerJoin(cte, bigUser.EQ(User_ID)).
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

// TestOneCTEPerName covers the WITH clause, which names every CTE of the statement
// once. The same CTE used in two branches is written once; two different CTEs
// under one name used to be written as the first, so the second branch read the
// wrong query and its parameter was dropped.
func TestOneCTEPerName(t *testing.T) {
	named := func(name string) Table {
		return CTE("t", Select(User_ID).From(Users).Where(User_Name.EQ(Val(name))))
	}

	a := named("a")

	shared := Select(User_ID.WithTable(a)).From(a).Union(Select(User_ID.WithTable(a)).From(a)).MustBuild()

	sql, args := sqlOf(t, shared, onSQLite)
	if strings.Count(sql, `"t" AS (`) != 1 || !reflect.DeepEqual(args, []any{"a"}) {
		t.Fatalf("SQL = %s, args = %#v; want the shared CTE written once", sql, args)
	}

	b := named("b")

	_, err := Select(User_ID.WithTable(a)).From(a).Union(Select(User_ID.WithTable(b)).From(b)).Build()
	if err == nil || !strings.Contains(err.Error(), "two different CTEs are named t") {
		t.Fatalf("Build = %v; want the name collision refused", err)
	}
}

// TestSetOperationsNameAndGroupTheirOperands covers the SQL a set operation relies
// on to find its output columns. A select item that is not a column reference is
// written with AS its name, so ORDER BY finds it on every dialect; an ORDER BY term
// that is not an output column is refused, where Upper(col) used to render as the
// column it wraps; and a combined operand is a derived table, since SQLite has no
// parenthesized compound SELECT.
func TestSetOperationsNameAndGroupTheirOperands(t *testing.T) {
	upper := MapInto(Upper(User_Name), func(r *string) *string { return r })
	left := Select[string](upper).From(Users.WithDeleted())
	right := Select[string](upper).From(Users.WithDeleted())

	sql, _ := sqlOf(t, left.Union(right).OrderBy(upper.Asc()).MustBuild(), onSQLite)
	if want := `SELECT UPPER("users"."name") AS "name" FROM "users" UNION SELECT UPPER("users"."name") AS "name" FROM "users" ORDER BY "name" ASC`; sql != want {
		t.Fatalf("SQL =\n%s\nwant\n%s", sql, want)
	}

	for name, stage := range map[string]OrderedResultStage[string]{
		"expression": left.Union(right).OrderBy(Upper(User_Name).Asc()),
		"not output": left.Union(right).OrderBy(User_Email.Asc()),
	} {
		if _, err := stage.Build(); err == nil || !strings.Contains(err.Error(), "ordered by its output columns") {
			t.Errorf("%s: Build = %v; want the term refused", name, err)
		}
	}

	id := func(name string) WhereStage[int64] {
		return SelectValue(User_ID).From(Users.WithDeleted()).Where(User_Name.EQ(Val(name)))
	}

	sql, _ = sqlOf(t, id("a").Union(id("b").UnionAll(id("c"))).MustBuild(), onSQLite)
	if want := `SELECT "users"."id" FROM "users" WHERE "users"."name" = ? UNION SELECT * FROM (SELECT "users"."id" FROM "users" WHERE "users"."name" = ? UNION ALL SELECT "users"."id" FROM "users" WHERE "users"."name" = ?) AS "tsq_set"`; sql != want {
		t.Fatalf("nested SQL =\n%s\nwant\n%s", sql, want)
	}
}

// TestSetOperationChainsReadLeftToRight covers a flat chain that mixes INTERSECT
// with UNION or EXCEPT. SQLite evaluates it left to right, MySQL and PostgreSQL
// bind INTERSECT tighter, so written flat the same query returned different rows
// per dialect. The part before the INTERSECT is grouped instead.
func TestSetOperationChainsReadLeftToRight(t *testing.T) {
	id := func(name string) WhereStage[int64] {
		return SelectValue(User_ID).From(Users.WithDeleted()).Where(User_Name.EQ(Val(name)))
	}

	const (
		a = `SELECT "users"."id" FROM "users" WHERE "users"."name" = ?`
		b = a
		c = a
	)

	for name, tt := range map[string]struct {
		q    *Query[int64]
		want string
	}{
		"union then intersect": {
			id("a").Union(id("b")).Intersect(id("c")).MustBuild(),
			`SELECT * FROM (` + a + ` UNION ` + b + `) AS "tsq_set" INTERSECT ` + c,
		},
		"intersect then union needs nothing": {
			id("a").Intersect(id("b")).Union(id("c")).MustBuild(),
			a + ` INTERSECT ` + b + ` UNION ` + c,
		},
		"except, intersect, union, intersect": {
			id("a").Except(id("b")).Intersect(id("c")).Union(id("d")).Intersect(id("e")).MustBuild(),
			`SELECT * FROM (SELECT * FROM (` + a + ` EXCEPT ` + a + `) AS "tsq_set" INTERSECT ` + a + ` UNION ` + a + `) AS "tsq_set" INTERSECT ` + a,
		},
	} {
		if sql, _ := sqlOf(t, tt.q, onSQLite); sql != tt.want {
			t.Errorf("%s: SQL =\n%s\nwant\n%s", name, sql, tt.want)
		}

		for _, d := range []tsqdialect.Name{onMySQL, onPostgres} {
			if _, _, err := tt.q.SQL(d); err != nil {
				t.Errorf("%s on %s: %v", name, d, err)
			}
		}
	}
}

// TestSetOperationsWithAllNeedTheirCapability covers INTERSECT ALL and EXCEPT
// ALL, which SQLite lacks although it has INTERSECT and EXCEPT: they used to
// reach SQLite as a syntax error instead of an UnsupportedCapabilityError.
func TestSetOperationsWithAllNeedTheirCapability(t *testing.T) {
	id := SelectValue(User_ID).From(Users)

	for want, q := range map[tsqdialect.Capability]*Query[int64]{
		tsqdialect.CapabilityIntersectAll: id.IntersectAll(id).MustBuild(),
		tsqdialect.CapabilityExceptAll:    id.ExceptAll(id).MustBuild(),
	} {
		_, _, err := q.SQL(onSQLite)
		if e, ok := errors.AsType[*tsqdialect.UnsupportedCapabilityError](err); !ok || e.Capability != want || e.Dialect != onSQLite {
			t.Errorf("%s on sqlite = %v; want UnsupportedCapabilityError", want, err)
		}

		for _, d := range []tsqdialect.Name{onMySQL, onPostgres} {
			if _, _, err := q.SQL(d); err != nil {
				t.Errorf("%s on %s: %v", want, d, err)
			}
		}
	}
}

// TestDerivedTablesHaveDistinctColumnNames covers a SELECT list with one name
// twice. It is read by position, so the name only matters once the query is a
// derived table (counting a grouped query, grouping a set operation), where MySQL
// refuses two columns of one name with error 1060.
func TestDerivedTablesHaveDistinctColumnNames(t *testing.T) {
	upper := MapInto(Upper(User_Name), func(u *user) *string { return &u.Email })
	q := SelectDistinct(User_Name, upper).From(Users).MustBuild()

	exec, err := WrapExecutor(noopExecutor{}, onMySQL)
	if err != nil {
		t.Fatal(err)
	}

	_, stmts, err := q.prepare(exec, nil, nil, renderMode{count: true})
	if err != nil {
		t.Fatal(err)
	}

	sql := stmts[0].sql

	if !strings.Contains(sql, "SELECT DISTINCT `users`.`name`, UPPER(`users`.`name`) AS `name_2`") {
		t.Fatalf("count SQL = %s; want the repeated name replaced", sql)
	}
}

// TestSubqueriesKeepTheirFilters covers two subquery shapes that used to lose or
// break their meaning: Search in a subquery never received the keyword, so its
// predicate was dropped, and MySQL refuses LIMIT inside IN (error 1235).
func TestSubqueriesKeepTheirFilters(t *testing.T) {
	searched := SelectValue(Order_UserID).From(Orders).Search(Searchable(Order_Note))
	if _, err := Select(User_ID).From(Users).Where(User_ID.In(searched)).Build(); err == nil || !strings.Contains(err.Error(), "keyword search") {
		t.Fatalf("Build = %v; want Search in a subquery refused", err)
	}

	top := SelectValue(Order_UserID).From(Orders).OrderBy(Order_Amount.Desc()).Limit(3)
	q := Select(User_ID).From(Users.WithDeleted()).Where(User_ID.In(top)).MustBuild()

	sql, _ := sqlOf(t, q, onMySQL)
	if !strings.Contains(sql, "IN (SELECT * FROM (SELECT `orders`.`user_id` FROM `orders` ORDER BY `orders`.`amount` DESC LIMIT ?) AS `tsq_in`)") {
		t.Fatalf("SQL = %s; want the limited subquery as a derived table", sql)
	}
}

// TestSetOperationOperandsReadIntoTheSameFields covers operands that select the
// same columns in another order: their rows were read through the first
// operand's columns by position, and name and email were swapped without a word.
func TestSetOperationOperandsReadIntoTheSameFields(t *testing.T) {
	_, err := Select(User_ID, User_Name, User_Email).From(Users).UnionAll(Select(User_ID, User_Email, User_Name).From(Users)).Build()
	if err == nil || !strings.Contains(err.Error(), "reads into another field") {
		t.Fatalf("Build = %v; want the swapped operand refused", err)
	}

	if _, err := Select(User_ID, User_Name).From(Users).UnionAll(Select(User_ID, User_Name).From(Users)).Build(); err != nil {
		t.Fatalf("matching operands = %v", err)
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
	label := Case[string](User_Version.GT(Val(int64(10))), Val("hot")).
		When(User_Name.IsNull(), User_Email).
		Else(Val("cold")).
		End()

	q := Select(MapInto(label, func(r *namedRow) *string { return &r.Name }).Named("label")).From(Users.WithDeleted()).MustBuild()

	sql, args := sqlOf(t, q, onSQLite)
	want := `SELECT CASE WHEN "users"."version" > ? THEN ? WHEN "users"."name" IS NULL THEN "users"."email" ELSE ? END AS "case" FROM "users"`

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
