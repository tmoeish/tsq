// 第 1 章：从一个结构体开始。
//
// 运行：go run ./examples/01-getting-started
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite" // 注册 "sqlite" 驱动

	"github.com/tmoeish/tsq/v5"
	"github.com/tmoeish/tsq/v5/examples/01-getting-started/todo"
	"github.com/tmoeish/tsq/v5/examples/internal/show"
)

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, w io.Writer) error {
	dir, err := os.MkdirTemp("", "tsq-todo-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(dir) }()

	// ① 打开数据库。
	//
	// tsq.Open 的参数是：驱动名、DSN、这个库里有哪些表。todo.TSQTables() 是 tsq gen
	// 生成的，列出了 todo 包里的全部表。
	//
	// 选项：
	//   - SchemaPolicyCreateMissing：启动时把缺的表建出来。适合开发和演示；
	//     生产环境通常保持默认的 Manual，用迁移脚本管理表结构（第 10 章）。
	//   - WithLogger + WithSQLLogging：把执行的每条 SQL 以 debug 级别写进日志。
	//     tsq.Logger 就是 *slog.Logger 的一个子集，项目里直接传你的 slog logger；
	//     这里换成 show.SQLPrinter，只是为了把 SQL 打印得便于对照代码阅读。
	logger := &show.SQLPrinter{W: w}
	logger.On()

	rt, err := tsq.Open(ctx, "sqlite", filepath.Join(dir, "todo.db"), todo.TSQTables(),
		tsq.WithSchemaPolicy(tsq.SchemaPolicyCreateMissing),
		tsq.WithLogger(logger),
		tsq.WithSQLLogging(),
	)
	if err != nil {
		return err
	}
	defer func() { _ = rt.Close() }()

	// ② 插入一行。Insert 是生成在 *Todo 上的方法；数据库分配的自增主键会写回 ID，
	// created_at 由 TSQ 填上当前时间（因为注解里写了 managed created_at）。
	show.Step(w, "插入一行")

	first := &todo.Todo{Title: "读完第 1 章"}
	if err := first.Insert(ctx, rt); err != nil {
		return err
	}

	show.Resultf(w, "新行的 ID = %d，CreatedAt 已填：%t", first.ID, !first.CreatedAt.IsZero())

	// ③ 批量插入。TableTodo 是生成的表描述符，行级操作以外的一切都从它出发。
	show.Step(w, "批量插入")

	more := []*todo.Todo{{Title: "跑一遍 tsq gen"}, {Title: "写第一个查询"}, {Title: "买咖啡", Done: true}}
	if err := todo.TableTodo.BatchInsert(ctx, rt, more); err != nil {
		return err
	}

	show.Resultf(w, "插入了 %d 行，ID 依次是 %d、%d、%d", len(more), more[0].ID, more[1].ID, more[2].ID)

	// ④ 按主键读取。Get 找不到时返回包装了 sql.ErrNoRows 的错误；Find 找不到时返回 nil, nil。
	show.Step(w, "按主键读取")

	got, err := todo.TableTodo.Get(ctx, rt, first.ID)
	if err != nil {
		return err
	}

	show.Resultf(w, "%+v", *got)

	// ⑤ 修改后保存。Update 按主键写回整行。
	show.Step(w, "修改并保存")

	got.Done = true
	if err := got.Update(ctx, rt); err != nil {
		return err
	}

	// ⑥ 写一个查询：未完成的待办，按 ID 排序。
	//
	// 每一列都是生成的强类型字段：TableTodo.Done 是 tsq.Column[Todo, bool]，
	// 所以 EQ 只接受 bool——写成 tsq.Val("no") 是编译错误，而不是运行时的 SQL 错误。
	show.Step(w, "查询：还没做完的")

	pending, err := tsq.
		Select(todo.TableTodo.Columns()...).
		From(todo.TableTodo).
		Where(todo.TableTodo.Done.EQ(tsq.Val(false))).
		OrderBy(todo.TableTodo.ID.Asc()).
		List(ctx, rt)
	if err != nil {
		return err
	}

	for _, t := range pending {
		show.Resultf(w, "#%d %s", t.ID, t.Title)
	}

	// ⑦ 删除。表没有声明 deleted_at，所以只有 HardDelete（真删）；第 8 章讲软删除。
	show.Step(w, "删除一行")

	if err := more[2].HardDelete(ctx, rt); err != nil {
		return err
	}

	// ⑧ 计数。Count 是查询上的方法，数的就是 List 会返回的那些行。
	show.Step(w, "计数")

	n, err := tsq.Select(todo.TableTodo.Columns()...).From(todo.TableTodo).Count(ctx, rt)
	if err != nil {
		return err
	}

	show.Resultf(w, "还剩 %d 条待办", n)

	return nil
}
