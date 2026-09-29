// 第 3 章：多表。连接、结果投影、别名，以及可能为 NULL 的值怎么读。
//
// 运行：go run ./examples/03-joins-and-results
package main

import (
	"context"
	"database/sql"
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

// orderLine 是只在这个文件里用的结果形状。结果形状稳定、多处复用时，
// 用 //tsq:result 让 tsq gen 生成（见 shop/results.go）；临时用一次，就用 tsq.MapInto 现场映射。
type orderLine struct {
	OrderID   int64
	Customer  string
	Product   string
	Quantity  int64
	LineTotal int64
}

// categoryPath 的 Parent 可能不存在（顶级分类），所以是 sql.Null[string]。
type categoryPath struct {
	Name   string
	Parent sql.Null[string]
}

func run(ctx context.Context, w io.Writer) error {
	db, cleanup, err := shop.Open(ctx, w)
	if err != nil {
		return err
	}
	defer cleanup()

	// ---------------------------------------------------------------------
	show.Step(w, "3.1 内连接 + 生成的结果投影：商品和它的分类名")
	// shop.ProductListing 带 //tsq:result 注解，每个字段写明来自哪张表的哪一列；
	// tsq gen 生成了 ResultProductListing，Select 它的 Columns() 就读进 ProductListing。
	listings, err := tsq.
		Select(shop.ResultProductListing.Columns()...).
		From(product).
		InnerJoin(category, product.CategoryID.EQ(category.ID)).
		Where(product.Status.EQ(tsq.Val(shop.ProductOnSale))).
		OrderBy(category.Name.Asc(), product.PriceCents.Desc()).
		List(ctx, db)
	if err != nil {
		return err
	}

	for _, l := range listings {
		show.Resultf(w, "[%s] %s %s", l.CategoryName, l.ProductName, show.Yuan(l.PriceCents))
	}

	// ---------------------------------------------------------------------
	show.Step(w, "3.2 多表连接 + MapInto：订单明细")
	// MapInto(列, 字段指针) 把任意列或表达式读进你自己的结构体的一个字段。
	// 字段类型必须和列的值类型一致，不一致编译不过。
	lines, err := tsq.
		Select(
			tsq.MapInto(order.ID, func(r *orderLine) *int64 { return &r.OrderID }),
			tsq.MapInto(customer.Name, func(r *orderLine) *string { return &r.Customer }),
			tsq.MapInto(product.Name, func(r *orderLine) *string { return &r.Product }),
			tsq.MapInto(orderItem.Quantity, func(r *orderLine) *int64 { return &r.Quantity }),
			tsq.MapInto(orderItem.LineTotalCents, func(r *orderLine) *int64 { return &r.LineTotal }),
		).
		From(order).
		InnerJoin(customer, order.CustomerID.EQ(customer.ID)).
		InnerJoin(orderItem, orderItem.OrderID.EQ(order.ID)).
		InnerJoin(product, product.ID.EQ(orderItem.ProductID)).
		Where(order.Status.In(tsq.Vals(shop.OrderPaid, shop.OrderShipped))).
		OrderBy(order.ID.Asc(), product.ID.Asc()).
		List(ctx, db)
	if err != nil {
		return err
	}

	for _, l := range lines {
		show.Resultf(w, "订单 %d  %s  %s × %d = %s", l.OrderID, l.Customer, l.Product, l.Quantity, show.Yuan(l.LineTotal))
	}

	// ---------------------------------------------------------------------
	show.Step(w, "3.3 左连接：每个顾客和他的订单，没下过单的也列出来")
	// 左连接右侧的列可能为 NULL（Dan 没有订单）。shop.CustomerOrder 把这两个字段声明成
	// sql.Null[int64]，tsq gen 就用 MapIntoNull 投影它们。
	rows, err := tsq.
		Select(shop.ResultCustomerOrder.Columns()...).
		From(customer).
		LeftJoin(order, order.CustomerID.EQ(customer.ID)).
		OrderBy(customer.ID.Asc(), order.ID.Asc()).
		List(ctx, db)
	if err != nil {
		return err
	}

	for _, r := range rows {
		if !r.OrderID.Valid {
			show.Resultf(w, "%s：没有订单", r.CustomerName)

			continue
		}

		show.Resultf(w, "%s：订单 %d，%s", r.CustomerName, r.OrderID.V, show.Yuan(r.TotalCents.V))
	}

	// ---------------------------------------------------------------------
	show.Step(w, "3.4 读 NULL 的字段必须能存 NULL：这个错误在执行前就报出来")
	// 把左连接右侧的列读进 int64，TSQ 知道这个值可能是 NULL，
	// 不等扫描到第一行 NULL 才失败，而是在发出 SQL 之前就拒绝，并说明原因。
	type wrong struct{ OrderID int64 }

	_, err = tsq.
		Select(tsq.MapInto(order.ID, func(r *wrong) *int64 { return &r.OrderID })).
		From(customer).
		LeftJoin(order, order.CustomerID.EQ(customer.ID)).
		List(ctx, db)
	show.Resultf(w, "错误：%v", err)

	// ---------------------------------------------------------------------
	show.Step(w, "3.5 别名和自连接：分类和它的上级分类")
	// 同一张表在一个查询里出现两次，第二次用 As 起别名；别名表的每一列都绑定到别名上。
	parent := category.As("parent")

	paths, err := tsq.
		Select(
			tsq.MapInto(category.Name, func(r *categoryPath) *string { return &r.Name }),
			// 顶级分类没有上级，左连接出来是 NULL：用 MapIntoNull 读进 sql.Null[string]。
			tsq.MapIntoNull(parent.Name, func(r *categoryPath) *sql.Null[string] { return &r.Parent }),
		).
		From(category).
		LeftJoin(parent, category.ParentID.EQ(parent.ID)).
		OrderBy(category.ID.Asc()).
		List(ctx, db)
	if err != nil {
		return err
	}

	for _, p := range paths {
		if p.Parent.Valid {
			show.Resultf(w, "%s / %s", p.Parent.V, p.Name)
		} else {
			show.Resultf(w, "%s（顶级）", p.Name)
		}
	}

	// ---------------------------------------------------------------------
	show.Step(w, "3.6 Coalesce：给 NULL 一个默认值，就能读进普通字段")
	type labeled struct{ Name, Parent string }

	labels, err := tsq.
		Select(
			tsq.MapInto(category.Name, func(r *labeled) *string { return &r.Name }),
			tsq.MapInto(tsq.Coalesce(parent.Name, tsq.Val("—")), func(r *labeled) *string { return &r.Parent }),
		).
		From(category).
		LeftJoin(parent, category.ParentID.EQ(parent.ID)).
		OrderBy(category.ID.Asc()).
		List(ctx, db)
	if err != nil {
		return err
	}

	for _, l := range labels {
		show.Resultf(w, "%s ← %s", l.Name, l.Parent)
	}

	return nil
}
