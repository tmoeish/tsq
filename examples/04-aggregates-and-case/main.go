// 第 4 章：聚合、分组、CASE、列函数，以及自定义 SQL 片段。
//
// 运行：go run ./examples/04-aggregates-and-case
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
	category  = shop.TableCategory
	product   = shop.TableProduct
	customer  = shop.TableCustomer
	order     = shop.TableOrder
	orderItem = shop.TableOrderItem
)

type categoryStats struct {
	Category string
	Products int64
	Stock    int64
	AvgPrice float64
	MaxPrice int64
}

type customerSpend struct {
	Name   string
	Orders int64
	Spent  int64
}

type bucket struct {
	Label    string
	Products int64
}

func run(ctx context.Context, w io.Writer) error {
	db, cleanup, err := shop.Open(ctx, w)
	if err != nil {
		return err
	}
	defer cleanup()

	// ---------------------------------------------------------------------
	show.Step(w, "4.1 GROUP BY + 聚合函数：每个分类的商品数、总库存、均价、最高价")
	// 聚合函数是包级泛型函数：tsq.Sum 只接受数值列，tsq.Avg 返回 float64，
	// 对字符串列求和编译不过。聚合的结果是表达式，用 MapInto 读进结果字段。
	stats, err := tsq.
		Select(
			tsq.MapInto(category.Name, func(r *categoryStats) *string { return &r.Category }),
			tsq.MapInto(tsq.Count(product.ID), func(r *categoryStats) *int64 { return &r.Products }),
			tsq.MapInto(tsq.Sum(product.Stock), func(r *categoryStats) *int64 { return &r.Stock }),
			tsq.MapInto(tsq.Avg(product.PriceCents), func(r *categoryStats) *float64 { return &r.AvgPrice }),
			tsq.MapInto(tsq.Max(product.PriceCents), func(r *categoryStats) *int64 { return &r.MaxPrice }),
		).
		From(product).
		InnerJoin(category, product.CategoryID.EQ(category.ID)).
		// 按分类主键分组，分类表的其他列（Name）就可以直接选；
		// 选一个既没分组也没聚合的列，Build 会拒绝。
		GroupBy(category.ID).
		OrderBy(category.ID.Asc()).
		List(ctx, db)
	if err != nil {
		return err
	}

	for _, s := range stats {
		show.Resultf(w, "%s：%d 件，库存 %d，均价 %s，最高 %s",
			s.Category, s.Products, s.Stock, show.Yuan(int64(s.AvgPrice)), show.Yuan(s.MaxPrice))
	}

	// ---------------------------------------------------------------------
	show.Step(w, "4.2 HAVING：有效订单（已付款或已发货）总额超过 ¥1000 的顾客")
	spent := tsq.Sum(order.TotalCents)

	spenders, err := tsq.
		Select(
			tsq.MapInto(customer.Name, func(r *customerSpend) *string { return &r.Name }),
			tsq.MapInto(tsq.Count(order.ID), func(r *customerSpend) *int64 { return &r.Orders }),
			tsq.MapInto(spent, func(r *customerSpend) *int64 { return &r.Spent }),
		).
		From(customer).
		InnerJoin(order, order.CustomerID.EQ(customer.ID)).
		Where(order.Status.In(tsq.Vals(shop.OrderPaid, shop.OrderShipped))).
		GroupBy(customer.ID).
		Having(spent.GT(tsq.Val(int64(100000)))).
		OrderBy(spent.Desc()).
		List(ctx, db)
	if err != nil {
		return err
	}

	for _, s := range spenders {
		show.Resultf(w, "%s：%d 单，共 %s", s.Name, s.Orders, show.Yuan(s.Spent))
	}

	// ---------------------------------------------------------------------
	show.Step(w, "4.3 只要一个聚合值：SelectValue 和 SelectNullValue")
	// COUNT 永远不是 NULL，用 SelectValue 读成 *int64。
	distinct, err := tsq.SelectValue(tsq.CountDistinct(orderItem.ProductID)).From(orderItem).Get(ctx, db)
	if err != nil {
		return err
	}

	show.Resultf(w, "被买过的不同商品：%d 种", *distinct)

	// 没有 GROUP BY 的 SUM 在没有行时是 NULL，SelectValue 会拒绝它；
	// SelectNullValue 读成 sql.Null[int64]，没有行时 Valid 为 false。
	revenue := tsq.
		SelectNullValue(tsq.Sum(order.TotalCents)).
		From(order).
		Where(order.Status.EQ(order.Status.Param()))

	for _, status := range []shop.OrderStatus{shop.OrderPaid, shop.OrderStatus("refunded")} {
		total, err := revenue.Get(ctx, db, order.Status.Bind(status))
		if err != nil {
			return err
		}

		if total.Valid {
			show.Resultf(w, "%s 订单总额 %s", status, show.Yuan(total.V))
		} else {
			show.Resultf(w, "%s 订单总额：NULL（没有这种订单）", status)
		}
	}

	// ---------------------------------------------------------------------
	show.Step(w, "4.4 CASE：按价格分档，再按档位分组计数")
	// Case(条件, 结果).When(...).Else(...).End()。第一个分支决定结果类型，
	// 其他分支类型不同就编译不过。Else 之后只能 End。
	label := tsq.
		Case(product.PriceCents.LT(tsq.Val(int64(10000))), tsq.Val("¥100 以下")).
		When(product.PriceCents.LT(tsq.Val(int64(500000))), tsq.Val("¥100～¥5000")).
		Else(tsq.Val("¥5000 以上")).
		End()

	buckets, err := tsq.
		Select(
			tsq.MapInto(label, func(r *bucket) *string { return &r.Label }),
			tsq.MapInto(tsq.Count(product.ID), func(r *bucket) *int64 { return &r.Products }),
		).
		From(product).
		// 带绑定值的表达式同时出现在 SELECT 和 GROUP BY / ORDER BY 里时，
		// TSQ 把后两者写成列序号（GROUP BY 1），否则 PostgreSQL 认不出它们是同一个表达式。
		GroupBy(label).
		OrderBy(label.Asc()).
		List(ctx, db)
	if err != nil {
		return err
	}

	for _, b := range buckets {
		show.Resultf(w, "%s：%d 件", b.Label, b.Products)
	}

	// ---------------------------------------------------------------------
	show.Step(w, "4.5 列函数：名字是三个字符的顾客，邮箱转大写、算长度、取前缀")
	// 列函数同样按类型约束：Upper 只接受字符串列，Round 只接受数值列。
	// 三种方言写法不同的地方（比如 MySQL 的 LENGTH 数字节）由 TSQ 按方言改写。
	type nameInfo struct {
		Email  string
		Chars  int64
		Prefix string
	}

	infos, err := tsq.
		Select(
			tsq.MapInto(tsq.Upper(customer.Email), func(r *nameInfo) *string { return &r.Email }),
			tsq.MapInto(tsq.Length(customer.Email), func(r *nameInfo) *int64 { return &r.Chars }),
			tsq.MapInto(tsq.Substring(customer.Email, 1, 3), func(r *nameInfo) *string { return &r.Prefix }),
		).
		From(customer).
		Where(tsq.Length(customer.Name).EQ(tsq.Val(int64(3)))).
		OrderBy(customer.ID.Asc()).
		Limit(2).
		List(ctx, db)
	if err != nil {
		return err
	}

	for _, i := range infos {
		show.Resultf(w, "%s（%d 个字符，前三个是 %s）", i.Email, i.Chars, i.Prefix)
	}

	// ---------------------------------------------------------------------
	show.Step(w, "4.6 SELECT DISTINCT：订单里出现过哪些状态")
	type statusRow struct{ Status shop.OrderStatus }

	statuses, err := tsq.
		SelectDistinct(tsq.MapInto(order.Status, func(r *statusRow) *shop.OrderStatus { return &r.Status })).
		From(order).
		OrderBy(order.Status.Asc()).
		List(ctx, db)
	if err != nil {
		return err
	}

	for _, s := range statuses {
		show.Resultf(w, "%s", s.Status)
	}

	// ---------------------------------------------------------------------
	show.Step(w, "4.7 算术和逃生舱：价格尾数是 99 元的商品打九折")
	// 加减乘除是 tsq.Add / Sub / Mul / Div，和列函数一样按类型约束。
	// Div 的除数不是非零的 tsq.Val 时结果可能是 NULL（除以零），就得读进可空字段。
	type discounted struct {
		Name  string
		Price int64
	}

	nineTenths := tsq.Div(tsq.Mul(product.PriceCents, tsq.Val(int64(90))), tsq.Val(int64(100)))

	deals, err := tsq.
		Select(
			tsq.MapInto(product.Name, func(r *discounted) *string { return &r.Name }),
			tsq.MapInto(nineTenths, func(r *discounted) *int64 { return &r.Price }),
		).
		From(product).
		// TSQ 没有封装的 SQL 用 Pred / Exprf 写：格式串里第一个 %s 是列本身，之后每个 %s
		// 依次取一个参数（值会被绑定）。格式串原样发给每种方言，可移植性由你负责。
		// %% 是字面的百分号（取模运算符）。
		Where(product.PriceCents.Pred("%s %% %s = %s", int64(10000), int64(9900))).
		OrderBy(product.ID.Asc()).
		List(ctx, db)
	if err != nil {
		return err
	}

	for _, d := range deals {
		show.Resultf(w, "%s 九折价 %s", d.Name, show.Yuan(d.Price))
	}

	return nil
}
