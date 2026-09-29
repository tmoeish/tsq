package shop

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite" // 纯 Go 的 SQLite 驱动，注册为 "sqlite"

	"github.com/tmoeish/tsq/v5"
	"github.com/tmoeish/tsq/v5/examples/internal/show"
)

// 这个文件是各章共用的演示脚手架，不是 TSQ 的一部分：打开一个临时的 SQLite 库、
// 灌入种子数据，并把 TSQ 执行的每条 SQL 打印出来，让你看到每行 Go 代码背后跑了什么。

// Open 打开一个位于临时目录的 SQLite 数据库，按 TSQTables() 建表并灌入种子数据，
// 之后执行的每条 SQL 都以 "SQL>" 开头打印到 w。返回的 cleanup 关闭数据库并删掉临时目录。
//
// 真实项目里的写法见第 1 章和第 10 章：tsq.Open(ctx, 驱动名, DSN, 表清单, 选项...)。
func Open(ctx context.Context, w io.Writer, options ...tsq.RuntimeOption) (*tsq.Runtime, func(), error) {
	dir, err := os.MkdirTemp("", "tsq-shop-*")
	if err != nil {
		return nil, nil, err
	}

	printer := &show.SQLPrinter{W: w}
	options = append([]tsq.RuntimeOption{
		// 开发和演示用：缺的表和索引启动时自动建。生产环境保持默认的 Manual，见第 10 章。
		tsq.WithSchemaPolicy(tsq.SchemaPolicyCreateMissing),
		tsq.WithLogger(printer),
		// 把每条渲染好的 SQL 和它的参数以 debug 级别交给 logger。
		tsq.WithSQLLogging(),
	}, options...)

	dsn := filepath.Join(dir, "shop.db") + "?_time_format=sqlite"

	rt, err := tsq.Open(ctx, "sqlite", dsn, TSQTables(), options...)
	if err != nil {
		_ = os.RemoveAll(dir)

		return nil, nil, err
	}

	cleanup := func() {
		_ = rt.Close()
		_ = os.RemoveAll(dir)
	}

	if err := Seed(ctx, rt); err != nil {
		cleanup()

		return nil, nil, fmt.Errorf("seed: %w", err)
	}

	// 建表和种子数据不打印，从这里开始的 SQL 才是各章自己的。
	printer.On()

	return rt, cleanup, nil
}

// Ptr 返回 v 的指针，给 *string 这类可空字段赋值用。
//
//go:fix inline
func Ptr[T any](v T) *T { return new(v) }
