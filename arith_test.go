package tsq

import (
	"context"
	"strings"
	"testing"

	tsqdialect "github.com/tmoeish/tsq/v5/dialect"
)

// TestArithmeticWritesAndReadsWithoutTheEscapeHatch covers stock = stock - ?, which
// needed Exprf and lost the parameter's type on the way.
func TestArithmeticWritesAndReadsWithoutTheEscapeHatch(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)
	users := seedUsers(t, rt, "a")

	order := &order{UserID: users[0].ID, Amount: 10, Note: "x"}
	if err := Orders.Insert(ctx, rt, order); err != nil {
		t.Fatal(err)
	}

	take := NewParam[int64]("take")
	stmt := UpdateTable(Orders).
		Set(Order_Amount, Sub(Order_Amount, take)).
		Where(Order_ID.EQ(Val(order.ID)), Order_Amount.GTE(take)).
		MustBuild()

	if n, err := stmt.Exec(ctx, rt, take.Bind(4)); err != nil || n != 1 {
		t.Fatalf("Exec = %d, %v", n, err)
	}

	if n, err := stmt.Exec(ctx, rt, take.Bind(40)); err != nil || n != 0 {
		t.Fatalf("Exec past the amount = %d, %v; want no row", n, err)
	}

	got, err := SelectValue(Add(Mul(Order_Amount, Val(int64(3))), Val(int64(1)))).From(Orders).MustBuild().Get(ctx, rt)
	if err != nil || *got != 19 {
		t.Fatalf("amount * 3 + 1 = %v, %v; want 19", got, err)
	}

	half, err := SelectValue(Div(Order_Amount, Val(int64(4)))).From(Orders).MustBuild().Get(ctx, rt)
	if err != nil || *half != 1 {
		t.Fatalf("amount / 4 = %v, %v; want the integer quotient 1", half, err)
	}

	// A divisor that is not a non-zero value can be zero, which is NULL.
	divisor := NewParam[int64]("divisor")
	if _, err := SelectValue(Div(Order_Amount, divisor)).From(Orders).MustBuild().Get(ctx, rt, divisor.Bind(2)); err == nil {
		t.Fatal("SelectValue of a division by a parameter: want it refused, the quotient can be NULL")
	}

	byZero, err := SelectNullValue(Div(Order_Amount, divisor)).From(Orders).MustBuild().Get(ctx, rt, divisor.Bind(0))
	if err != nil || byZero.Valid {
		t.Fatalf("amount / 0 = %v, %v; want NULL on SQLite", byZero, err)
	}

	q := SelectNullValue(Div(Order_Amount, divisor)).From(Orders).MustBuild()

	// A parameter is typed there too: next to a DECIMAL (a SUM of integers), an
	// untyped one gives the result thirty decimal places.
	mysql, _, err := q.SQL(tsqdialect.MySQL, divisor.Bind(2))
	if err != nil || !strings.Contains(mysql, "`amount` DIV CAST(? AS SIGNED)") {
		t.Fatalf("MySQL = %s, %v; want DIV, whose / returns a decimal, over a typed parameter", mysql, err)
	}

	pg, _, err := q.SQL(tsqdialect.Postgres, divisor.Bind(2))
	if err != nil || !strings.Contains(pg, `DIV("orders"."amount", $1)`) {
		t.Fatalf("PostgreSQL = %s, %v", pg, err)
	}
}

// TestArithmeticOfCustomSQLKeepsItsPrecedence covers an Exprf operand spliced into
// arithmetic without parentheses: (amount + 1) * 2 rendered as amount + 1 * 2.
func TestArithmeticOfCustomSQLKeepsItsPrecedence(t *testing.T) {
	ctx := context.Background()
	rt := newSQLite(t)
	users := seedUsers(t, rt, "a")

	if err := Orders.Insert(ctx, rt, &order{UserID: users[0].ID, Amount: 10, Note: "x"}); err != nil {
		t.Fatal(err)
	}

	got, err := SelectValue(Mul(Order_Amount.Exprf("%s + %s", int64(1)), Val(int64(2)))).From(Orders).MustBuild().Get(ctx, rt)
	if err != nil || *got != 22 {
		t.Fatalf("(amount + 1) * 2 = %v, %v; want 22", got, err)
	}
}
