# 01 从结构体开始

```bash
go run ./examples/01-getting-started
```

TSQ 的工作方式是三步：**写结构体 → `tsq gen` 生成代码 → 用生成的代码读写数据库**。

## 1. 写结构体

[`todo/todo.go`](todo/todo.go)：

```go
//tsq:table name=todos
//tsq:managed created_at
type Todo struct {
	ID        int64     `db:"id"`
	Title     string    `db:"title,size:200"`
	Done      bool      `db:"done"`
	CreatedAt time.Time `db:"created_at"`
}
```

- `//tsq:` 行是注解，写法和 `//go:` 指令一样（双斜杠后不留空格）
- `//tsq:table` 声明一张表；主键默认是 `ID` 字段，默认自增
- `//tsq:managed created_at` 让 `Insert` 自动填创建时间
- `db` 标签给出列名；`size:200` 让 DDL 写成 `VARCHAR(200)`

## 2. 生成

```bash
go install github.com/tmoeish/tsq/v5/cmd/tsq@latest
tsq gen ./examples/01-getting-started/todo
```

生成的文件：

| 文件 | 内容 |
| --- | --- |
| `todo.tsq.go` | `TableTodo`：表描述符，每一列一个强类型字段（`TableTodo.Done` 是 `tsq.Column[Todo, bool]`）；`*Todo` 上的 `Insert` / `Update` / `HardDelete` |
| `runtime.tsq.go` | `TSQTables()`：包里的全部表，交给 `tsq.Open` |
| `sqlite.sql` / `mysql.sql` / `postgres.sql` | 三种方言的建表语句 |
| `tsq.json` | 表结构的历史，下次 `tsq gen` 据此写出迁移；要提交 |

`todo.go` 里的 `//go:generate` 行让 `go generate ./...` 也能完成这一步。

## 3. 使用

[`main.go`](main.go) 依次演示：`tsq.Open` 打开数据库并建表、`Insert`、`BatchInsert`、按主键 `Get`、
`Update`、用 `Select(...).From(...).Where(...)` 查询、`HardDelete`、`Count`。

值得先记住的两点：

- 查询里的一切都有类型：`TableTodo.Done.EQ(tsq.Val("no"))` 编译不过，而不是在运行时报 SQL 错误
- 这张表没有声明 `deleted_at`，所以删除只有 `HardDelete`（真删）；软删除见第 8 章

下一章：[02 查询](../02-querying/)
