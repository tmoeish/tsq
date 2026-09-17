package tsq

import (
	"reflect"
	"strings"
	"testing"
)

func TestValIsBoundAndNeverNullInComparisons(t *testing.T) {
	q := Select(User_ID).From(Users.WithDeleted()).Where(User_Name.EQ(Val("amy")), User_ID.Between(Val(int64(1)), Val(int64(9)))).MustBuild()

	sql, args := sqlOf(t, q, onSQLite)
	if !strings.HasSuffix(sql, `("users"."name" = ? AND "users"."id" BETWEEN ? AND ?)`) {
		t.Fatalf("SQL = %s", sql)
	}

	if !reflect.DeepEqual(args, []any{"amy", int64(1), int64(9)}) {
		t.Fatalf("args = %#v", args)
	}

	// Val holds Go values; an expression goes in as itself.
	if _, err := Select(User_ID).From(Users).Where(User_ID.Pred("%s = %s", Val(User_Version))).Build(); err == nil {
		t.Fatal("expected Val of a column to be refused")
	}
}

func TestValsExpandAndKeepEmptyListsExplicit(t *testing.T) {
	render := func(cond Condition) string {
		t.Helper()

		sql, _ := sqlOf(t, Select(User_ID).From(Users.WithDeleted()).Where(cond).MustBuild(), onPostgres)

		return sql
	}

	if sql := render(User_ID.In(Vals[int64](4, 5))); !strings.HasSuffix(sql, `"users"."id" IN ($1, $2)`) {
		t.Fatalf("IN rendered %s", sql)
	}

	if sql := render(User_ID.In(Vals[int64]())); !strings.HasSuffix(sql, `IN (NULL)`) {
		t.Fatalf("empty IN rendered %s", sql)
	}

	if sql := render(User_ID.NotIn(Vals[int64]())); !strings.HasSuffix(sql, `NOT IN (SELECT 1 WHERE 1 = 0)`) {
		t.Fatalf("empty NOT IN rendered %s", sql)
	}
}
