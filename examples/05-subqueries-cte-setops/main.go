// 第 5 章：子查询、关联子查询、CTE 和集合运算。
//
// 运行：go run ./examples/05-subqueries-cte-setops
package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/tmoeish/tsq/v5"
	"github.com/tmoeish/tsq/v5/examples/internal/show"
	"github.com/tmoeish/tsq/v5/examples/shop"
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

var (
	product  = shop.TableProduct
	customer = shop.TableCustomer
	order    = shop.TableOrder
)

type spender struct {
	Name  string
	Spent int64
}

func run(ctx context.Context, w io.Writer) error {
	db, cleanup, err := shop.Open(ctx, w)
	if err != nil {
		return err
	}
	defer cleanup()

	// ---------------------------------------------------------------------
	show.Step(w, "5.1 IN 子查询：有已付款订单的顾客")
	// SelectValue 的阶段本身就是一个子查询，类型就是它选的那一列的类型：
	// 这里是 int64 的集合，只能放在 int64 列的 In 里。它不需要单独 Build，
	// 外层查询 Build 时一起检查。
	paidCustomerIDs := tsq.
		SelectValue(order.CustomerID).
		From(order).
		Where(order.Status.EQ(tsq.Val(shop.OrderPaid)))

	names, err := tsq.
		SelectValue(customer.Name).
		From(customer).
		Where(customer.ID.In(paidCustomerIDs)).
		OrderBy(customer.Name.Asc()).
		List(ctx, db)
	if err != nil {
		return err
	}

	for _, n := range names {
		show.Resultf(w, "%s", *n)
	}

	// ---------------------------------------------------------------------
	show.Step(w, "5.2 标量子查询：最贵的商品")
	// 只返回一个值的子查询可以放在比较的右边。
	mostExpensive, err := tsq.
		Select(product.Columns()...).
		From(product).
		Where(product.PriceCents.EQ(tsq.SelectValue(tsq.Max(product.PriceCents)).From(product))).
		Get(ctx, db)
	if err != nil {
		return err
	}

	show.Resultf(w, "%s %s", mostExpensive.Name, show.Yuan(mostExpensive.PriceCents))

	// ---------------------------------------------------------------------
	show.Step(w, "5.3 关联子查询：NOT EXISTS 找出没下过单的顾客")
	// 子查询引用外层查询的表（customers），必须用 Correlate 声明，
	// 否则 Build 报错"表被引用了却不在 FROM 里"——防止你以为关联了，其实没有。
	hasOrder := tsq.
		SelectValue(order.ID).
		From(order).
		Correlate(customer).
		Where(order.CustomerID.EQ(customer.ID))

	names, err = tsq.
		SelectValue(customer.Name).
		From(customer).
		Where(tsq.NotExists(hasOrder)).
		List(ctx, db)
	if err != nil {
		return err
	}

	for _, n := range names {
		show.Resultf(w, "%s", *n)
	}

	// ---------------------------------------------------------------------
	show.Step(w, "5.4 CTE（WITH 子句）：先算每个顾客的有效消费，再连回顾客表")
	// CTE 的主体是一个普通查询。这里读进 shop.Order 的两个字段：
	// customer_id 原样选出，SUM(total_cents) 映射回 TotalCents 字段、输出列名也是 total_cents。
	spend := tsq.CTE("spend", tsq.
		Select(
			order.CustomerID,
			tsq.MapInto(tsq.Sum(order.TotalCents), func(r *shop.Order) *int64 { return &r.TotalCents }),
		).
		From(order).
		Where(order.Status.NE(tsq.Val(shop.OrderCancelled))).
		GroupBy(order.CustomerID))

	// col.Rebind(cte) 把列重新绑定到 CTE 上：按名字找 CTE 的输出列。
	spendCustomer := order.CustomerID.Rebind(spend)
	spendTotal := order.TotalCents.Rebind(spend)

	spenders, err := tsq.
		Select(
			tsq.MapInto(customer.Name, func(r *spender) *string { return &r.Name }),
			tsq.MapInto(spendTotal, func(r *spender) *int64 { return &r.Spent }),
		).
		From(customer).
		InnerJoin(spend, spendCustomer.EQ(customer.ID)).
		OrderBy(spendTotal.Desc()).
		List(ctx, db)
	if err != nil {
		return err
	}

	for _, s := range spenders {
		show.Resultf(w, "%s %s", s.Name, show.Yuan(s.Spent))
	}

	// ---------------------------------------------------------------------
	show.Step(w, "5.5 集合运算：UNION、INTERSECT、EXCEPT")
	// 两边必须读进同一种类型、同样顺序的字段——这里都是 SelectValue(customer.Name)。
	vip := tsq.SelectValue(customer.Name).From(customer).Where(customer.Level.EQ(tsq.Val("vip")))
	buyers := tsq.SelectValue(customer.Name).From(customer).Where(customer.ID.In(paidCustomerIDs))

	for _, op := range []struct {
		name string
		q    tsq.CompoundStage[string]
	}{
		{"VIP 或 有已付款订单", vip.Union(buyers)},
		{"VIP 且 有已付款订单", vip.Intersect(buyers)},
		{"VIP 但 没有已付款订单", vip.Except(buyers)},
	} {
		// 集合运算的结果整体排序：OrderBy 引用输出列。
		names, err := op.q.OrderBy(customer.Name.Asc()).List(ctx, db)
		if err != nil {
			return err
		}

		list := make([]string, len(names))
		for i, n := range names {
			list[i] = *n
		}

		show.Resultf(w, "%s：%v", op.name, list)
	}

	// 链式集合运算从左到右求值，和读代码的顺序一致：a.Union(b).Intersect(c) 是 (a ∪ b) ∩ c，
	// 三种方言结果相同。要 a ∪ (b ∩ c)，把组合好的操作数传进去：a.Union(b.Intersect(c))。

	return nil
}
