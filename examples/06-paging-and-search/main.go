// 第 6 章：分页和搜索。页码分页、来自 HTTP 的分页请求、游标分页、关键词搜索、全文检索。
//
// 运行：go run ./examples/06-paging-and-search
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/tmoeish/tsq/v5"
	"github.com/tmoeish/tsq/v5/dialect"
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
	category = shop.TableCategory
	product  = shop.TableProduct
)

// listings 是一个带 Search 的查询：执行时传 tsq.Keyword(词)，
// 任一搜索列包含这个词的行就匹配。空词不过滤，所以搜索框的值可以原样传进来。
var listings = tsq.
	Select(shop.ResultProductListing.Columns()...).
	From(product).
	InnerJoin(category, product.CategoryID.EQ(category.ID)).
	Where(product.Status.EQ(tsq.Val(shop.ProductOnSale))).
	Search(tsq.Searchable(product.Name), tsq.Searchable(category.Name)).
	MustBuild()

func run(ctx context.Context, w io.Writer) error {
	db, cleanup, err := shop.Open(ctx, w)
	if err != nil {
		return err
	}
	defer cleanup()

	// ---------------------------------------------------------------------
	show.Step(w, "6.1 页码分页：按价格排序，每页 3 件，取第 2 页")
	// Page 在一个只读事务里先数总数、再取这一页，两者来自同一个快照。
	page, err := tsq.
		Select(product.Columns()...).
		From(product).
		Page(ctx, db, tsq.Paging{
			Page:    2,
			Size:    3,
			OrderBy: []tsq.OrderBy{product.PriceCents.Asc(), product.ID.Asc()},
		})
	if err != nil {
		return err
	}

	show.Resultf(w, "第 %d/%d 页，共 %d 件，还有下一页：%t", page.Page, page.TotalPages, page.Total, page.HasNext())

	for _, p := range page.Data {
		show.Resultf(w, "%s %s", p.Name, show.Yuan(p.PriceCents))
	}

	// ---------------------------------------------------------------------
	show.Step(w, "6.2 生成的关键词搜索：TableProduct.Query()")
	// //tsq:search Name,Description 让 TableProduct.Query() 搜这两列。
	// 关键词里的 % 和 _ 会被转义，按字面匹配。
	found, err := product.Query().List(ctx, db, tsq.Keyword("手机"))
	if err != nil {
		return err
	}

	for _, p := range found {
		show.Resultf(w, "%s", p.Name)
	}

	// ---------------------------------------------------------------------
	show.Step(w, "6.3 自己写的搜索：连接查询，同时搜商品名和分类名")

	for _, term := range []string{"图书", "马克杯", ""} {
		rows, err := listings.List(ctx, db, tsq.Keyword(term))
		if err != nil {
			return err
		}

		show.Resultf(w, "搜 %q：%d 件", term, len(rows))
	}

	// ---------------------------------------------------------------------
	show.Step(w, "6.4 HTTP 分页请求：PageRequest → Paging")
	// 接口收到的是字符串：?page=1&size=2&order_by=price_cents&order=desc&keyword=手机。
	// PageRequest 就是这个形状；Paging(可排序的列...) 把它转成 Paging，
	// 排序字段只能从你列出的列里选——在没有索引的列上排序是一种代价，要由接口自己决定。
	req := tsq.PageRequest{Page: 1, Size: 2, OrderBy: "price_cents", Order: "desc", Keyword: "手机"}

	paging, err := req.Paging(product.PriceCents, product.Name)
	if err != nil {
		return err
	}

	// 关键词随 Paging 一起带过来，Page 会用它搜索，不用再传 tsq.Keyword。
	resp, err := product.Query().Page(ctx, db, paging)
	if err != nil {
		return err
	}

	for _, p := range resp.Data {
		show.Resultf(w, "%s %s", p.Name, show.Yuan(p.PriceCents))
	}

	// 客户端要按一个不允许的字段排序：得到 *tsq.PageRequestError，接口返回 400。
	bad := tsq.PageRequest{OrderBy: "stock"}
	if _, err := bad.Paging(product.PriceCents, product.Name); err != nil {
		if reqErr, ok := errors.AsType[*tsq.PageRequestError](err); ok {
			show.Resultf(w, "400 Bad Request：字段 %s，%s", reqErr.Field, reqErr.Reason)
		}
	}

	// ---------------------------------------------------------------------
	show.Step(w, "6.5 游标分页（keyset）：翻得再深也不用跳过前面的行")
	// 排序列必须包含主键，保证每个位置唯一；Next 是一个不透明的游标，下一页传给 After。
	// 没有 Total——不数总数正是它快的原因。
	keyset := tsq.Keyset{Size: 3, OrderBy: []tsq.OrderBy{product.PriceCents.Desc(), product.ID.Asc()}}

	for n := 1; ; n++ {
		kp, err := product.Query().PageKeyset(ctx, db, keyset)
		if err != nil {
			return err
		}

		names := make([]string, len(kp.Data))
		for i, p := range kp.Data {
			names[i] = p.Name
		}

		show.Resultf(w, "第 %d 页：%v", n, names)

		if !kp.HasNext() {
			break
		}

		keyset.After = kp.Next
	}

	// ---------------------------------------------------------------------
	show.Step(w, "6.6 全文检索：tsq.Matches")
	// //tsq:fulltext Name,Description 声明了全文索引，生成 FullTextNameAndDescription()。
	// "匹配"的含义取决于方言：MySQL 是 MATCH ... AGAINST，PostgreSQL 是 to_tsvector，
	// SQLite 没有 TSQ 管理的全文索引，退化成子串匹配——所以测试可以在 SQLite 上跑同一段代码。
	show.Resultf(w, "当前方言 %s 支持真正的全文索引：%t", db.Dialect(),
		dialect.Supports(db.Dialect(), dialect.CapabilityFullTextSearch))

	hits, err := tsq.
		Select(product.Columns()...).
		From(product).
		Where(tsq.Matches(product.FullTextNameAndDescription(), tsq.Val("笔记本"))).
		List(ctx, db)
	if err != nil {
		return err
	}

	for _, p := range hits {
		show.Resultf(w, "%s：%s", p.Name, p.Description)
	}

	return nil
}
