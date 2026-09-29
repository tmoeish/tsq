// 第 7 章：写数据。插入、更新、Upsert、按条件批量改删，以及事务。
//
// 运行：go run ./examples/07-writing-data
package main

import (
	"context"
	"errors"
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
	product   = shop.TableProduct
	customer  = shop.TableCustomer
	order     = shop.TableOrder
	orderItem = shop.TableOrderItem
)

// 按条件改数据的语句和查询一样：构建一次，执行时绑定参数。
// 扣库存：只在库存够的时候扣，受影响行数为 0 就说明库存不够。
var (
	quantity = tsq.NewParam[int64]("quantity")

	takeStock = tsq.
			UpdateTable(product).
			Set(product.Stock, product.Stock.Exprf("%s - %s", quantity)).
			Where(product.ID.EQ(product.ID.Param()), product.Stock.GTE(quantity)).
			MustBuild()
)

// errOutOfStock 让事务回滚。
var errOutOfStock = errors.New("库存不足")

func run(ctx context.Context, w io.Writer) error {
	db, cleanup, err := shop.Open(ctx, w)
	if err != nil {
		return err
	}
	defer cleanup()

	// ---------------------------------------------------------------------
	show.Step(w, "7.1 Insert：主键、时间戳、数据库默认值都会写回结构体")
	// Level 是 nil，INSERT 里就不写这一列，由数据库填默认值 'regular'；
	// 单行 Insert 随后把它读回来。
	eve := &shop.Customer{Email: "eve@example.com", Name: "Eve"}
	if err := eve.Insert(ctx, db); err != nil {
		return err
	}

	show.Resultf(w, "ID=%d，Level=%s，CreatedAt 已填：%t", eve.ID, *eve.Level, !eve.CreatedAt.IsZero())

	// 生成列 line_total_cents 由数据库计算，Insert 从不写它，但会读回来。
	item := &shop.OrderItem{OrderID: 3, ProductID: 7, Quantity: 3, UnitPriceCents: 3900}
	if err := item.Insert(ctx, db); err != nil {
		return err
	}

	show.Resultf(w, "3 × ¥39.00，数据库算出的小计 %s", show.Yuan(item.LineTotalCents))

	// ---------------------------------------------------------------------
	show.Step(w, "7.2 BatchInsert + WithSkipDuplicates：跳过唯一键冲突的行")
	// BatchInsert 平时一条语句插入多行，按方言的参数上限自动切分。
	// 加了 WithSkipDuplicates 就逐行插入，才知道是哪一行重复：它只跳过主键或唯一键
	// 重复的行，其他错误照样返回。被跳过的行保持原样（ID 还是 0）。
	batch := []*shop.Product{
		{CategoryID: 4, SKU: "P-3003", Name: "Go 并发实战", PriceCents: 9900, Stock: 30, Status: shop.ProductOnSale},
		{CategoryID: 4, SKU: "P-3001", Name: "重复的 SKU", PriceCents: 1, Stock: 1, Status: shop.ProductOnSale},
		{CategoryID: 4, SKU: "P-3004", Name: "数据密集型应用", PriceCents: 12900, Stock: 12, Status: shop.ProductOnSale},
	}
	if err := product.BatchInsert(ctx, db, batch, tsq.WithSkipDuplicates()); err != nil {
		return err
	}

	for _, p := range batch {
		if p.ID == 0 {
			show.Resultf(w, "%s %s → 跳过", p.SKU, p.Name)
		} else {
			show.Resultf(w, "%s %s → ID %d", p.SKU, p.Name, p.ID)
		}
	}

	// ---------------------------------------------------------------------
	show.Step(w, "7.3 Update：改完整行再保存")
	// products 声明了 version 和 updated_at：Update 按"主键 + 当前版本"匹配，
	// 成功后版本加一、updated_at 刷新（数据库和结构体里都是）。
	book, err := product.GetBySKU(ctx, db, "P-3003")
	if err != nil {
		return err
	}

	before := book.Version
	book.PriceCents = 8900

	if err := book.Update(ctx, db); err != nil {
		return err
	}

	show.Resultf(w, "version %d → %d", before, book.Version)

	// 只想写某几列时，把列传给 Update：只写 stock（外加 updated_at 和 version）。
	book.Stock = 25
	if err := book.Update(ctx, db, product.Stock); err != nil {
		return err
	}

	// ---------------------------------------------------------------------
	show.Step(w, "7.4 只读了几列的行不能整行保存")
	// 用窄 Select 读进 shop.Product，没读的列是零值。整行 Update 会把零值写回去，
	// 所以 TSQ 记住了这一行是怎么读的，拒绝整行保存；点名要写的列就可以。
	partial, err := tsq.
		Select(product.ID, product.Name, product.Version).
		From(product).
		Where(product.SKU.EQ(tsq.Val("P-3004"))).
		Get(ctx, db)
	if err != nil {
		return err
	}

	partial.Name = "数据密集型应用系统设计"
	show.Resultf(w, "整行 Update：%v", partial.Update(ctx, db))

	if err := partial.Update(ctx, db, product.Name); err != nil {
		return err
	}

	show.Resultf(w, "Update(ctx, db, product.Name)：成功")

	// ---------------------------------------------------------------------
	show.Step(w, "7.5 Upsert：按唯一键插入或更新")
	// 邮箱已存在就更新那一行，不存在就插入。按主键以外的唯一键冲突时，
	// 行会拿到被更新那一行的主键。
	// 注意 Upsert 写的是整行：这里 Phone 是 nil，Ada 原来的手机号会被改成 NULL。
	ada := &shop.Customer{Email: "ada@example.com", Name: "Ada Lovelace", Level: new("vip")}
	if err := customer.Upsert(ctx, db, ada, customer.Email); err != nil {
		return err
	}

	show.Resultf(w, "ada@example.com 已存在，更新后 ID 仍是 %d，名字 %s", ada.ID, ada.Name)

	newcomers := []*shop.Customer{
		{Email: "bob@example.com", Name: "Bob Builder", Level: new("regular")},
		{Email: "fay@example.com", Name: "Fay", Level: new("regular")},
	}
	if err := customer.BatchUpsert(ctx, db, newcomers, []tsq.BoundColumn[shop.Customer]{customer.Email}); err != nil {
		return err
	}

	// ---------------------------------------------------------------------
	show.Step(w, "7.6 按条件更新：UpdateTable")
	// 不需要先把行读出来。Set 的值可以是 tsq.Val、参数、列或表达式，类型在编译期检查。
	// Where 必须写且只能写一次；真要改全表，写 Where(tsq.And())，让意图明明白白。
	raised, err := tsq.
		UpdateTable(product).
		Set(product.PriceCents, product.PriceCents.Exprf("%s * 110 / 100")).
		Where(product.CategoryID.EQ(tsq.Val(int64(4)))).
		Exec(ctx, db)
	if err != nil {
		return err
	}

	show.Resultf(w, "图书涨价 10%%：%d 行", raised)

	// SetNull 只接受可空列：清掉订单备注。
	cleared, err := tsq.UpdateTable(order).SetNull(order.Note).Where(order.Note.IsNotNull()).Exec(ctx, db)
	if err != nil {
		return err
	}

	show.Resultf(w, "清空备注：%d 行", cleared)

	// ---------------------------------------------------------------------
	show.Step(w, "7.7 按条件删除：HardDeleteFrom")
	// order_items 没有 deleted_at，删除只能是真删，所以方法名里带 Hard。
	// 对没有软删除的表调用 tsq.DeleteFrom 编译不过。
	cancelled := tsq.SelectValue(order.ID).From(order).Where(order.Status.EQ(tsq.Val(shop.OrderCancelled)))

	removed, err := tsq.HardDeleteFrom(orderItem).Where(orderItem.OrderID.In(cancelled)).Exec(ctx, db)
	if err != nil {
		return err
	}

	show.Resultf(w, "删掉已取消订单的明细：%d 行", removed)

	// ---------------------------------------------------------------------
	show.Step(w, "7.8 事务：下单 = 扣库存 + 写订单 + 写明细，要么全成，要么全不成")

	placed, err := placeOrder(ctx, db, 1, 1, 2) // Ada 买 2 台 Aurora 手机
	if err != nil {
		return err
	}

	show.Resultf(w, "下单成功：订单 %d，金额 %s", placed.ID, show.Yuan(placed.TotalCents))

	_, err = placeOrder(ctx, db, 1, 3, 100) // 笔记本库存只有 10
	show.Resultf(w, "再下一单：%v", err)

	notebook, err := product.Get(ctx, db, 3)
	if err != nil {
		return err
	}

	show.Resultf(w, "回滚后笔记本库存仍是 %d", notebook.Stock)

	return nil
}

// placeOrder 在一个事务里下单。WithTxResult 把回调的返回值带出来；
// 回调返回错误就回滚。回调里只用它拿到的 tx，不要用外面的 db。
func placeOrder(ctx context.Context, db *tsq.Runtime, customerID, productID, qty int64) (*shop.Order, error) {
	return db.WithTxResult(ctx, func(ctx context.Context, tx tsq.Executor) (*shop.Order, error) {
		p, err := product.Get(ctx, tx, productID)
		if err != nil {
			return nil, err
		}

		n, err := takeStock.Exec(ctx, tx, product.ID.Bind(productID), quantity.Bind(qty))
		if err != nil {
			return nil, err
		}

		if n == 0 {
			return nil, fmt.Errorf("%s：想买 %d，只剩 %d：%w", p.Name, qty, p.Stock, errOutOfStock)
		}

		o := &shop.Order{CustomerID: customerID, Status: shop.OrderPending, TotalCents: p.PriceCents * qty}
		if err := o.Insert(ctx, tx); err != nil {
			return nil, err
		}

		line := &shop.OrderItem{OrderID: o.ID, ProductID: productID, Quantity: qty, UnitPriceCents: p.PriceCents}
		if err := line.Insert(ctx, tx); err != nil {
			return nil, err
		}

		return o, nil
	})
}
