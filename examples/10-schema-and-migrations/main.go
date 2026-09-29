// 第 10 章：表结构。生成的 DDL、四种启动策略、结构漂移的检测与修复。
//
// 运行：go run ./examples/10-schema-and-migrations
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	_ "modernc.org/sqlite"

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

func run(ctx context.Context, w io.Writer) error {
	dir, err := os.MkdirTemp("", "tsq-schema-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(dir) }()

	printer := &show.SQLPrinter{W: w}
	printer.On()

	// ---------------------------------------------------------------------
	show.Step(w, "10.1 tsq gen 生成的 DDL：每种方言一份")
	// 同一个结构体在三种方言下的列类型、自增写法、索引语法各不相同，tsq gen 各写一份。
	for _, name := range []string{"sqlite.sql", "mysql.sql", "postgres.sql"} {
		text, err := shop.Schema.ReadFile(name)
		if err != nil {
			return err
		}

		// 各取 categories 表的主键那一行。
		show.Resultf(w, "%-13s %s", name, firstLine(string(text), `"id"`, "`id`"))
	}

	// ---------------------------------------------------------------------
	show.Step(w, "10.2 生产环境的做法：迁移工具执行 DDL，TSQ 启动时只校验")
	// 默认策略是 Manual：什么都不做，只记一条日志。生产环境用迁移工具管理表结构，
	// 启动时用 Validate 确认代码和库对得上，对不上就拒绝启动。
	prod, err := sql.Open("sqlite", filepath.Join(dir, "prod.db"))
	if err != nil {
		return err
	}
	defer func() { _ = prod.Close() }()

	schema, err := shop.Schema.ReadFile("sqlite.sql")
	if err != nil {
		return err
	}

	// 新库执行"完整 schema"那一段；已有的库执行它还没执行过的 Migration 段。
	full, _, hasMigrations := strings.Cut(string(schema), "\n-- Migration:")
	if _, err := prod.ExecContext(ctx, full); err != nil {
		return err
	}

	show.Resultf(w, "执行了 sqlite.sql 的完整 schema 段（文件里还有迁移段：%t）", hasMigrations)

	// 连接池是你自己开的（比如带了监控），就用 NewRuntime 包一层，而不是 tsq.Open。
	// NewRuntime 不接管这个池子：rt.Close() 不会关它。
	rt, err := tsq.NewRuntime(ctx, prod, dialect.SQLite, shop.TSQTables(),
		tsq.WithSchemaPolicy(tsq.SchemaPolicyValidate), tsq.WithLogger(printer))
	if err != nil {
		return err
	}

	show.Resultf(w, "Validate 通过：库里的表和代码里声明的一致")

	_ = rt.Close()

	// ---------------------------------------------------------------------
	show.Step(w, "10.3 空库 + Validate：缺表就拒绝启动")
	empty := filepath.Join(dir, "empty.db")

	_, err = tsq.Open(ctx, "sqlite", empty, shop.TSQTables(), tsq.WithSchemaPolicy(tsq.SchemaPolicyValidate))
	if missing, ok := errors.AsType[*tsq.MissingTableError](err); ok {
		show.Resultf(w, "*tsq.MissingTableError：缺表 %s", missing.Table)
	}

	// ---------------------------------------------------------------------
	show.Step(w, "10.4 结构漂移：库里的 customers 是旧版本，少了 phone 和 level 两列")

	old := filepath.Join(dir, "old.db")
	if err := execSQL(ctx, old, `CREATE TABLE customers (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		email VARCHAR(128) NOT NULL,
		name VARCHAR(64) NOT NULL,
		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`); err != nil {
		return err
	}

	customersOnly := []tsq.Table{shop.TableCustomer}

	_, err = tsq.Open(ctx, "sqlite", old, customersOnly, tsq.WithSchemaPolicy(tsq.SchemaPolicyValidate))
	if mismatch, ok := errors.AsType[*tsq.SchemaMismatchError](err); ok {
		show.Resultf(w, "*tsq.SchemaMismatchError，表 %s：", mismatch.Table)

		for _, change := range mismatch.Changes {
			show.Resultf(w, "  %s", change)
		}
	}

	// ---------------------------------------------------------------------
	show.Step(w, "10.5 CreateMissing：补上缺的表、列和索引，但不改已有的列")
	// 适合开发环境和测试。已有的列和声明不一致、或者库里多出声明里没有的列，照样拒绝启动。
	rt, err = tsq.Open(ctx, "sqlite", old, customersOnly,
		tsq.WithSchemaPolicy(tsq.SchemaPolicyCreateMissing), tsq.WithLogger(printer))
	if err != nil {
		return err
	}

	_ = rt.Close()

	// 再用 Validate 打开，已经没有差异了。
	rt, err = tsq.Open(ctx, "sqlite", old, customersOnly, tsq.WithSchemaPolicy(tsq.SchemaPolicyValidate))
	if err != nil {
		return err
	}

	_ = rt.Close()

	show.Resultf(w, "再次 Validate：通过")

	// Reconcile 更进一步：把列改回声明的样子，删掉声明里已经没有的列（连同数据）。
	// 它是原型阶段"改完结构体重启就行"的设置；任何策略都从不删表。

	// ---------------------------------------------------------------------
	show.Step(w, "10.6 表描述符里有完整的结构信息")

	for _, col := range shop.TableProduct.ColumnSpecs()[:4] {
		show.Resultf(w, "列 %-12s %s（可空：%t）", col.Name, col.Type.Kind, col.Type.Nullable)
	}

	for _, idx := range shop.TableProduct.Indexes() {
		show.Resultf(w, "索引 %s %v（唯一：%t，全文：%t）", idx.Name, idx.Columns, idx.Unique, idx.FullText)
	}

	return nil
}

// firstLine 返回 text 里第一行含有任一 needle 的内容，去掉首尾空白。
func firstLine(text string, needles ...string) string {
	for line := range strings.Lines(text) {
		for _, needle := range needles {
			if strings.Contains(line, needle) {
				return strings.TrimSpace(line)
			}
		}
	}

	return ""
}

func execSQL(ctx context.Context, path, statement string) error {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	_, err = db.ExecContext(ctx, statement)

	return err
}
