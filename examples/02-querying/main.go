// 第 2 章：查询。条件、排序、分片、参数，以及读结果的几种方式。
//
// 运行：go run ./examples/02-querying
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

// 生成代码里每张表是一个值：TableProduct.Name 是 tsq.Column[shop.Product, string]。
// 起个短名字，下面的查询读起来更像 SQL。
var (
	product  = shop.TableProduct
	customer = shop.TableCustomer
)

// 查询构建一次、到处复用。值在执行时才知道的地方放参数：每一列都自带一个
// 同类型的参数 col.Param()，执行时用 col.Bind(值) 给它赋值。
var productsInCategory = tsq.
	Select(product.Columns()...).
	From(product).
	Where(product.CategoryID.EQ(product.CategoryID.Param())).
	OrderBy(product.PriceCents.Desc()).
	MustBuild()

// 同一列要用两个值时（区间的上下界），自己声明具名参数。
var (
	minPrice = tsq.NewParam[int64]("min_price")
	maxPrice = tsq.NewParam[int64]("max_price")

	productsInPriceRange = tsq.
				Select(product.Columns()...).
				From(product).
				Where(product.PriceCents.Between(minPrice, maxPrice)).
				OrderBy(product.PriceCents.Asc()).
				MustBuild()
)

// 列表参数给 In 用：col.ListParam() 声明，col.BindList(值...) 赋值。
var productsBySKU = tsq.
	Select(product.Columns()...).
	From(product).
	Where(product.SKU.In(product.SKU.ListParam())).
	OrderBy(product.SKU.Asc()).
	MustBuild()

func run(ctx context.Context, w io.Writer) error {
	db, cleanup, err := shop.Open(ctx, w)
	if err != nil {
		return err
	}
	defer cleanup()

	// ---------------------------------------------------------------------
	show.Step(w, "2.1 条件和排序：在售、价格高于 ¥50 的商品，贵的在前")
	// Select → From → Where → OrderBy，顺序和 SQL 一样。
	// 注意生成的 SQL 里多了 deleted_at = 0：products 声明了软删除，
	// 任何查询都自动只看没删除的行（第 8 章）。
	list, err := tsq.
		Select(product.Columns()...).
		From(product).
		Where(
			// 传给 Where 的多个条件是 AND 关系。
			product.Status.EQ(tsq.Val(shop.ProductOnSale)),
			// tsq.Val 包住一个 Go 值；它总是作为参数绑定，从不拼进 SQL 文本。
			// 类型要和列一致：PriceCents 是 int64，写 tsq.Val(5000) 会是 int，编译不过。
			product.PriceCents.GT(tsq.Val(int64(5000))),
		).
		OrderBy(product.PriceCents.Desc()).
		List(ctx, db)
	if err != nil {
		return err
	}

	printProducts(w, list)

	// ---------------------------------------------------------------------
	show.Step(w, "2.2 OR、IN、NOT：图书和家居类，或者名字里有“手机”的，但排除 P-1002 和缺货的")

	list, err = tsq.
		Select(product.Columns()...).
		From(product).
		Where(
			tsq.Or(
				// 图书和家居的主键是 4 和 5（见 shop/seed.go）。
				product.CategoryID.In(tsq.Vals[int64](4, 5)),
				tsq.Contains(product.Name, tsq.Val("手机")),
			),
			tsq.Not(product.SKU.EQ(tsq.Val("P-1002"))),
			product.Stock.NE(tsq.Val(int64(0))),
		).
		OrderBy(product.ID.Asc()).
		List(ctx, db)
	if err != nil {
		return err
	}

	printProducts(w, list)

	// ---------------------------------------------------------------------
	show.Step(w, "2.3 字符串匹配：StartsWith 会转义通配符，Like 按原样使用模式")
	// StartsWith / EndsWith / Contains 把 % 和 _ 当普通字符匹配，适合直接接用户输入。
	// Like 的模式按你写的原样使用，% 和 _ 是通配符。
	list, err = tsq.
		Select(product.Columns()...).
		From(product).
		Where(tsq.Or(
			tsq.StartsWith(product.SKU, tsq.Val("P-3")),
			tsq.Like(product.Name, tsq.Val("%台灯")),
		)).
		OrderBy(product.SKU.Asc()).
		List(ctx, db)
	if err != nil {
		return err
	}

	printProducts(w, list)

	// ---------------------------------------------------------------------
	show.Step(w, "2.4 可空列：没留手机号的顾客")
	// Phone 是 *string，生成的是 tsq.NullColumn。NULL 不等于任何值，用 IsNull / IsNotNull 找它。
	customers, err := tsq.
		Select(customer.Columns()...).
		From(customer).
		Where(customer.Phone.IsNull()).
		OrderBy(customer.ID.Asc()).
		List(ctx, db)
	if err != nil {
		return err
	}

	for _, c := range customers {
		show.Resultf(w, "%s <%s>", c.Name, c.Email)
	}

	// ---------------------------------------------------------------------
	show.Step(w, "2.5 参数：同一个查询，换参数执行两次")
	// 参数按"是哪个参数"匹配，不按位置；Bind 只接受列的类型。
	for _, categoryID := range []int64{2, 4} { // 手机、图书
		list, err := productsInCategory.List(ctx, db, product.CategoryID.Bind(categoryID))
		if err != nil {
			return err
		}

		show.Resultf(w, "分类 %d：%d 件商品", categoryID, len(list))
	}

	list, err = productsInPriceRange.List(ctx, db, minPrice.Bind(5000), maxPrice.Bind(10000))
	if err != nil {
		return err
	}

	show.Resultf(w, "价格在 ¥50～¥100 之间：")
	printProducts(w, list)

	list, err = productsBySKU.List(ctx, db, product.SKU.BindList("P-2001", "P-4001", "P-9999"))
	if err != nil {
		return err
	}

	show.Resultf(w, "按 SKU 列表取：")
	printProducts(w, list)

	// ---------------------------------------------------------------------
	show.Step(w, "2.6 Limit 和 Offset：按价格排序后的第 3、4 件")
	// OrderBy → Limit → Offset 各最多一次、顺序固定，Offset 只能跟在 Limit 后面——
	// 这些都由类型系统在编译期保证，写错了编译不过。
	list, err = tsq.
		Select(product.Columns()...).
		From(product).
		OrderBy(product.PriceCents.Asc()).
		Limit(2).
		Offset(2).
		List(ctx, db)
	if err != nil {
		return err
	}

	printProducts(w, list)

	// ---------------------------------------------------------------------
	show.Step(w, "2.7 只要一行、有没有、有几行：Get / Find / Exists / Count")
	byEmail := tsq.
		Select(customer.Columns()...).
		From(customer).
		Where(customer.Email.EQ(customer.Email.Param()))

	// Get 找不到时返回包装了 sql.ErrNoRows 的错误；Find 找不到时返回 nil, nil。
	ada, err := byEmail.Get(ctx, db, customer.Email.Bind("ada@example.com"))
	if err != nil {
		return err
	}

	show.Resultf(w, "Get: %s，等级 %s", ada.Name, *ada.Level)

	nobody, err := byEmail.Find(ctx, db, customer.Email.Bind("nobody@example.com"))
	if err != nil {
		return err
	}

	show.Resultf(w, "Find 一个不存在的邮箱: %v", nobody)

	exists, err := byEmail.Exists(ctx, db, customer.Email.Bind("bob@example.com"))
	if err != nil {
		return err
	}

	show.Resultf(w, "Exists: %t", exists)

	n, err := tsq.Select(product.Columns()...).From(product).Where(product.Stock.EQ(tsq.Val(int64(0)))).Count(ctx, db)
	if err != nil {
		return err
	}

	show.Resultf(w, "缺货商品 %d 件", n)

	// ---------------------------------------------------------------------
	show.Step(w, "2.8 只要一列：SelectValue")
	// Select 要求读进一个结构体；只要一列的值时用 SelectValue，结果是 []*string。
	names, err := tsq.
		SelectValue(product.Name).
		From(product).
		Where(product.Status.EQ(tsq.Val(shop.ProductOffSale))).
		OrderBy(product.Name.Asc()).
		List(ctx, db)
	if err != nil {
		return err
	}

	for _, name := range names {
		show.Resultf(w, "已下架：%s", *name)
	}

	// ---------------------------------------------------------------------
	show.Step(w, "2.9 逐行遍历：Iter 不把整个结果读进内存")
	// 导出、批处理这类可能很大的结果用 Iter；break 会停止查询。
	for p, err := range productsInCategory.Iter(ctx, db, product.CategoryID.Bind(5)) { // 家居
		if err != nil {
			return err
		}

		show.Resultf(w, "%s %s", p.SKU, p.Name)
	}

	return nil
}

func printProducts(w io.Writer, list []*shop.Product) {
	if len(list) == 0 {
		show.Resultf(w, "（没有结果）")
	}

	for _, p := range list {
		show.Resultf(w, "%s  %s  %s  库存 %d", p.SKU, p.Name, show.Yuan(p.PriceCents), p.Stock)
	}
}
