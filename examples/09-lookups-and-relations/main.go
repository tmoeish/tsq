// 第 9 章：按键查找和加载关联数据。主键和唯一键的生成方法、批量查找、不产生 N+1 的关联加载。
//
// 运行：go run ./examples/09-lookups-and-relations
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

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
	product   = shop.TableProduct
	customer  = shop.TableCustomer
	order     = shop.TableOrder
	orderItem = shop.TableOrderItem
)

// 子查询由你来写：它的 Where、OrderBy 决定哪些子行算数、按什么顺序。
// 唯一的列表参数就是子键，AttachMany 会把父行的键填进去。
var (
	itemsOfOrders = tsq.
			Select(orderItem.Columns()...).
			From(orderItem).
			Where(orderItem.OrderID.In(orderItem.OrderID.ListParam())).
			OrderBy(orderItem.ID.Asc()).
			MustBuild()

	productsByID = tsq.
			Select(product.Columns()...).
			From(product).
			Where(product.ID.In(product.ID.ListParam())).
			MustBuild()
)

func run(ctx context.Context, w io.Writer) error {
	db, cleanup, err := shop.Open(ctx, w)
	if err != nil {
		return err
	}
	defer cleanup()

	// ---------------------------------------------------------------------
	show.Step(w, "9.1 按主键：Get、Find、Fetch")
	// 这三个方法在表上，类型跟着主键走：TableProduct 的主键是 int64，传字符串编译不过。
	p, err := product.Get(ctx, db, 3)
	if err != nil {
		return err
	}

	show.Resultf(w, "Get(3)：%s", p.Name)

	missing, err := product.Find(ctx, db, 404)
	if err != nil {
		return err
	}

	show.Resultf(w, "Find(404)：%v", missing)

	// Fetch 按给出的顺序返回；任何一个键不存在，整个调用返回包装了 sql.ErrNoRows 的错误。
	ps, err := product.Fetch(ctx, db, 5, 1, 3)
	if err != nil {
		return err
	}

	show.Resultf(w, "Fetch(5, 1, 3)：%s", names(ps))

	_, err = product.Fetch(ctx, db, 1, 404)
	show.Resultf(w, "Fetch(1, 404)：errors.Is(err, sql.ErrNoRows) = %t", errors.Is(err, sql.ErrNoRows))

	// ---------------------------------------------------------------------
	show.Step(w, "9.2 按唯一键：//tsq:unique 生成的 GetByX / FindByX / FetchByX")

	ada, err := customer.GetByEmail(ctx, db, "ada@example.com")
	if err != nil {
		return err
	}

	show.Resultf(w, "GetByEmail：%s", ada.Name)

	books, err := product.FetchBySKU(ctx, db, "P-3001", "P-3002")
	if err != nil {
		return err
	}

	show.Resultf(w, "FetchBySKU：%s", names(books))

	// 复合唯一键 OrderID,ProductID 生成 GetByOrderIDAndProductID。
	line, err := orderItem.GetByOrderIDAndProductID(ctx, db, 1, 5)
	if err != nil {
		return err
	}

	show.Resultf(w, "订单 1 里商品 5 的数量：%d", line.Quantity)

	// ---------------------------------------------------------------------
	show.Step(w, "9.3 GetBy 只接受唯一的列：不唯一的列会被拒绝，而不是随便返回一行")
	_, err = order.GetBy(ctx, db, order.Status, shop.OrderPaid)
	show.Resultf(w, "%v", err)

	// ---------------------------------------------------------------------
	show.Step(w, "9.4 AttachMany：一次查询给所有订单装上明细（不是每个订单一次）")

	orders, err := tsq.
		Select(order.Columns()...).
		From(order).
		Where(order.CustomerID.EQ(tsq.Val(ada.ID))).
		OrderBy(order.ID.Asc()).
		List(ctx, db)
	if err != nil {
		return err
	}

	// TSQ 没有关系 DSL，也不往你的结构体里塞字段：装到哪里由回调决定。
	items := map[int64][]*shop.OrderItem{}

	if err := tsq.AttachMany(ctx, db, orders, order.ID, itemsOfOrders, orderItem.OrderID,
		func(o *shop.Order, children []*shop.OrderItem) { items[o.ID] = children },
	); err != nil {
		return err
	}

	for _, o := range orders {
		show.Resultf(w, "订单 %d（%s）：%d 行明细", o.ID, o.Status, len(items[o.ID]))
	}

	// ---------------------------------------------------------------------
	show.Step(w, "9.5 AttachOne：顺着外键给每行明细找到它的商品")

	var all []*shop.OrderItem
	for _, o := range orders {
		all = append(all, items[o.ID]...)
	}

	productOf := map[int64]*shop.Product{}

	if err := tsq.AttachOne(ctx, db, all, orderItem.ProductID, productsByID, product.ID,
		func(i *shop.OrderItem, p *shop.Product) { productOf[i.ID] = p },
	); err != nil {
		return err
	}

	for _, i := range all {
		show.Resultf(w, "明细 %d：%s × %d", i.ID, productOf[i.ID].Name, i.Quantity)
	}

	// ---------------------------------------------------------------------
	show.Step(w, "9.6 ListIn：IN 列表再长也不怕")
	// 数据库对一条语句能绑定的参数个数有上限（SQLite 32766，MySQL/PostgreSQL 65535）。
	// ListIn 先去重，放得进一条语句就执行一条；放不下就按上限切成多条、在同一个快照里执行，
	// 再把结果拼起来。Fetch / FetchBy / AttachMany 内部用的都是它。
	ids := make([]int64, 0, 40000)
	for i := range int64(40000) {
		ids = append(ids, i%8+1)
	}

	rows, err := productsByID.ListIn(ctx, db, product.ID.ListParam(), ids)
	if err != nil {
		return err
	}

	show.Resultf(w, "4 万个键，去重后 8 个，一条语句 → %d 行", len(rows))

	return nil
}

func names(ps []*shop.Product) string {
	list := make([]string, len(ps))
	for i, p := range ps {
		list[i] = p.Name
	}

	return strings.Join(list, "、")
}
