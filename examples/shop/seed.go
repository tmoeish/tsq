package shop

import (
	"context"

	"github.com/tmoeish/tsq/v5"
)

// Seed 写入各章共用的种子数据。它本身也是一段 TSQ 代码：一个事务里的几次 BatchInsert。
//
// 库是新建的，所以主键从 1 开始按插入顺序分配（括号里是主键）：
//
//	分类：电子产品(1) ─┬─ 手机(2)
//	                  └─ 电脑(3)
//	      图书(4)、家居(5)
//	商品：P-1001(1) … P-4002(8) 共 8 件，按下面列出的顺序；P-2002 和 P-4002 已下架且缺货
//	顾客：Ada(1)、Bob(2)、Cai(3)、Dan(4)；Bob 和 Dan 没留手机号，Dan 没下过单
//	订单：Ada 已付款(1)、Ada 已发货(2)、Bob 待付款(3)、Cai 已付款(4)、Cai 已取消(5)
func Seed(ctx context.Context, rt *tsq.Runtime) error {
	return rt.WithTx(ctx, func(ctx context.Context, tx tsq.Executor) error {
		electronics := &Category{Name: "电子产品"}
		if err := electronics.Insert(ctx, tx); err != nil {
			return err
		}

		// Insert 把数据库分配的自增主键写回了 electronics.ID，下面可以直接用。
		phones := &Category{Name: "手机", ParentID: &electronics.ID}
		computers := &Category{Name: "电脑", ParentID: &electronics.ID}
		books := &Category{Name: "图书"}
		home := &Category{Name: "家居"}

		if err := TableCategory.BatchInsert(ctx, tx, []*Category{phones, computers, books, home}); err != nil {
			return err
		}

		product := func(c *Category, sku, name, desc string, price, stock int64, status ProductStatus) *Product {
			return &Product{CategoryID: c.ID, SKU: sku, Name: name, Description: desc, PriceCents: price, Stock: stock, Status: status}
		}

		products := []*Product{
			product(phones, "P-1001", "Aurora 手机", "6.1 英寸屏幕，双卡双待", 399900, 50, ProductOnSale),
			product(phones, "P-1002", "Nimbus 手机 Pro", "长续航旗舰手机", 599900, 20, ProductOnSale),
			product(computers, "P-2001", "Swift 笔记本", "轻薄 14 英寸笔记本电脑", 699900, 10, ProductOnSale),
			product(computers, "P-2002", "Titan 工作站", "32 核工作站", 1999900, 0, ProductOffSale),
			product(books, "P-3001", "Go 语言编程", "从入门到并发", 8900, 200, ProductOnSale),
			product(books, "P-3002", "SQL 必知必会", "数据库查询入门", 5900, 150, ProductOnSale),
			product(home, "P-4001", "陶瓷马克杯", "350ml 马克杯", 3900, 500, ProductOnSale),
			product(home, "P-4002", "北欧台灯", "暖光护眼台灯", 12900, 0, ProductOffSale),
		}

		if err := TableProduct.BatchInsert(ctx, tx, products); err != nil {
			return err
		}

		ada := &Customer{Email: "ada@example.com", Name: "Ada", Phone: new("13800000001"), Level: new("vip")}
		bob := &Customer{Email: "bob@example.com", Name: "Bob"}
		cai := &Customer{Email: "cai@example.com", Name: "Cai", Phone: new("13800000003")}
		dan := &Customer{Email: "dan@example.com", Name: "Dan", Level: new("vip")}

		// 逐行 Insert 而不是 BatchInsert：单行 Insert 会把数据库填的默认值读回 Level，
		// 批量插入不回读（那要每行一次查询）。
		for _, c := range []*Customer{ada, bob, cai, dan} {
			if err := c.Insert(ctx, tx); err != nil {
				return err
			}
		}

		sku := map[string]*Product{}
		for _, p := range products {
			sku[p.SKU] = p
		}

		type line struct {
			sku string
			qty int64
		}

		orders := []struct {
			customer *Customer
			status   OrderStatus
			note     *string
			lines    []line
		}{
			{ada, OrderPaid, nil, []line{{"P-1001", 1}, {"P-3001", 2}}},
			{ada, OrderShipped, nil, []line{{"P-4001", 4}}},
			{bob, OrderPending, new("请工作日送货"), []line{{"P-2001", 1}}},
			{cai, OrderPaid, nil, []line{{"P-3001", 1}, {"P-3002", 1}}},
			{cai, OrderCancelled, nil, []line{{"P-1002", 1}}},
		}

		for _, o := range orders {
			order := &Order{CustomerID: o.customer.ID, Status: o.status, Note: o.note}

			items := make([]*OrderItem, 0, len(o.lines))
			for _, l := range o.lines {
				p := sku[l.sku]
				order.TotalCents += p.PriceCents * l.qty
				items = append(items, &OrderItem{ProductID: p.ID, Quantity: l.qty, UnitPriceCents: p.PriceCents})
			}

			if err := order.Insert(ctx, tx); err != nil {
				return err
			}

			for _, item := range items {
				item.OrderID = order.ID
			}

			if err := TableOrderItem.BatchInsert(ctx, tx, items); err != nil {
				return err
			}
		}

		return nil
	})
}
