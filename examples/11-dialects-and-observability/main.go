// 第 11 章：方言和可观测性。同一个查询的三种 SQL、方言能力、追踪、包装外部连接、分页上限。
//
// 运行：go run ./examples/11-dialects-and-observability
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

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

// 一个 *Query 不绑定方言：执行时按执行器的方言渲染（并缓存），所以同一个查询可以跑在三种库上。
var cheapestInCategory = tsq.
	Select(shop.ResultProductListing.Columns()...).
	From(product).
	InnerJoin(category, product.CategoryID.EQ(category.ID)).
	Where(category.Name.EQ(category.Name.Param())).
	Search(tsq.Searchable(product.Name)).
	OrderBy(product.PriceCents.Asc()).
	Limit(3).
	MustBuild()

func run(ctx context.Context, w io.Writer) error {
	// ---------------------------------------------------------------------
	show.Step(w, "11.1 同一个查询，三种方言的 SQL")
	// query.SQL(方言, 参数...) 只渲染不执行：日志、测试、排查问题时用。
	// 注意占位符（? 和 $1）、标识符引号（" 和 `）的区别。
	for _, name := range []dialect.Name{dialect.SQLite, dialect.MySQL, dialect.Postgres} {
		sqlText, args, err := cheapestInCategory.SQL(name, category.Name.Bind("手机"), tsq.Keyword("Pro"))
		if err != nil {
			return err
		}

		show.Resultf(w, "%-8s %s   -- 参数 %v", name, sqlText, args)
	}

	// ---------------------------------------------------------------------
	show.Step(w, "11.2 方言能力：Build 只查结构，执行（或渲染）时才查方言")
	// FULL JOIN 在任何方言上都能 Build；渲染到 MySQL 时才报 *dialect.UnsupportedCapabilityError。
	everything := tsq.
		Select(shop.ResultProductListing.Columns()...).
		From(product).
		FullJoin(category, product.CategoryID.EQ(category.ID)).
		MustBuild()

	for _, name := range []dialect.Name{dialect.SQLite, dialect.MySQL, dialect.Postgres} {
		rendered, _, err := everything.SQL(name)
		if capErr, ok := errors.AsType[*dialect.UnsupportedCapabilityError](err); ok {
			show.Resultf(w, "%-8s 不支持 %s", capErr.Dialect, capErr.Capability)

			continue
		}

		if err != nil {
			return err
		}

		show.Resultf(w, "%-8s 渲染出 FULL JOIN：%t", name, strings.Contains(rendered, "FULL JOIN"))
	}

	// 要事先分支，用 dialect.Supports；要一个和执行时一样的错误，用 dialect.Check。
	for _, capability := range []dialect.Capability{
		dialect.CapabilityFullJoin, dialect.CapabilityIntersectAll, dialect.CapabilityForUpdate, dialect.CapabilityFullTextSearch,
	} {
		show.Resultf(w, "%-16s sqlite=%-5t mysql=%-5t postgres=%t", capability,
			dialect.Supports(dialect.SQLite, capability),
			dialect.Supports(dialect.MySQL, capability),
			dialect.Supports(dialect.Postgres, capability))
	}

	// ---------------------------------------------------------------------
	show.Step(w, "11.3 追踪：WithTracers 包住每一次操作")
	// Tracer 拿到 context、操作信息（Op 和 Table）和"继续执行"的函数，必须调用它并返回它的错误。
	// 接 OpenTelemetry 时就在这里开 span、记耗时：span 名用 info.Op + info.Table。
	// 追踪不带 SQL 文本（它包住的是整个操作，包括渲染），SQL 走 WithSQLLogging。
	var spans []string

	tracer := func(ctx context.Context, info tsq.TraceInfo, next func(context.Context) error) error {
		err := next(ctx)
		spans = append(spans, fmt.Sprintf("%s %s（%s）", info.Op, info.Table, outcome(err)))

		return err
	}

	// shop.Open 的额外参数就是 tsq.RuntimeOption，原样传给 tsq.Open。
	db, cleanup, err := shop.Open(ctx, w, tsq.WithTracers(tracer), tsq.WithMaxPageSize(5))
	if err != nil {
		return err
	}
	defer cleanup()

	spans = nil // 忽略种子数据的写入

	rows, err := cheapestInCategory.List(ctx, db, category.Name.Bind("手机"))
	if err != nil {
		return err
	}

	show.Resultf(w, "手机类最便宜的：%s", rows[0].ProductName)

	// 这个事务故意失败（主键 404 不存在），看失败的操作在 span 里什么样。
	_ = db.WithTx(ctx, func(ctx context.Context, tx tsq.Executor) error {
		_, err := product.Get(ctx, tx, 404)

		return err
	})

	for _, span := range spans {
		show.Resultf(w, "span：%s", span)
	}

	// ---------------------------------------------------------------------
	show.Step(w, "11.4 包装别处开的连接：WrapExecutor")
	// 手里只有 *sql.Tx / *sql.Conn（比如别的库开的事务），用 WrapExecutor 告诉 TSQ 方言。
	// 裸的 *sql.DB 不能直接传给 TSQ——编译不过，因为 TSQ 必须知道方言才能渲染。
	// 包装出来的执行器不属于任何 runtime，所以不打 SQL 日志、不走 tracer，分页上限是默认值。
	conn, err := db.DB().Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	exec, err := tsq.WrapExecutor(conn, dialect.SQLite)
	if err != nil {
		return err
	}

	n, err := tsq.Select(product.Columns()...).From(product).Count(ctx, exec)
	if err != nil {
		return err
	}

	show.Resultf(w, "经 *sql.Conn 数到 %d 件商品；DialectOf(exec) = %s", n, tsq.DialectOf(exec))

	// ---------------------------------------------------------------------
	show.Step(w, "11.5 WithMaxPageSize：客户端要 100 条，最多给 5 条")

	page, err := product.Query().Page(ctx, db, tsq.Paging{Size: 100, OrderBy: []tsq.OrderBy{product.ID.Asc()}})
	if err != nil {
		return err
	}

	show.Resultf(w, "Size=%d，本页 %d 条，共 %d 条", page.Size, len(page.Data), page.Total)

	return nil
}

func outcome(err error) string {
	if err != nil {
		return "失败：" + err.Error()
	}

	return "成功"
}
