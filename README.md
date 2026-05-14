# TSQ - 类型安全的 Go SQL 查询代码生成工具

```txt
 _____  __    ____
/__   \/ _\  /___ \
  / /\/\ \  //  / /
 / /   _\ \/ \_/ /
 \/    \__/\___,_\
```

[![GitHub release (latest by date)][1]][2]
[![Build Status][3]](https://github.com/tmoeish/tsq/actions)
[![Go Report Card][4]][5]
[![License: MIT][6]][7]

[1]: https://img.shields.io/github/v/release/tmoeish/tsq
[2]: https://github.com/tmoeish/tsq/releases
[3]: https://img.shields.io/github/actions/workflow/status/tmoeish/tsq/go.yml
[4]: https://goreportcard.com/badge/github.com/tmoeish/tsq
[5]: https://goreportcard.com/report/github.com/tmoeish/tsq
[6]: https://img.shields.io/badge/License-MIT-yellow.svg
[7]: https://opensource.org/licenses/MIT

TSQ（Type-Safe Query）会把带注解的 Go 结构体生成为**表元数据、CRUD/分页助手和类型安全查询列**，让你用 Go API 组合 SQL，而不是在业务代码里手写大量字符串。

当前主线版本是 **v4 typed DSL**：`Query` / `QueryBuilder` / `Into` 都带 owner 类型，表 owner、结果 owner 和物理表语义已经拆开，联表结果与本地结果扫描会更早在编译期暴露错误。

核心关系现在可以直接记成：

- `Owner`：任何可扫描目标
- `Table`：物理表 owner，也是 mutation target，并暴露稳定的列/主键元数据
- `Result`：投影 owner，只参与查询结果映射

执行层也统一成了显式 context 语义：`SQLExecutor` / `Engine` 的执行方法都把 `ctx context.Context` 放在第一个参数。

## 先回答三个上手问题

| 问题 | 最短答案 |
| --- | --- |
| **最小要写什么？** | 一个带 `@TABLE` 注解的 Go struct。 |
| **生成后得到什么？** | 每个表生成一个 `*_tsq.go`；每个 `@RESULT` 生成一个 `*_result_tsq.go`。 |
| **怎么跑第一条查询？** | `tsq gen ./db` → 创建 `tsq.Engine` → `tsq.Select(...).From(table).Where(...).Build()` → `tsq.List(...)`。 |

## 安装

```bash
go install github.com/tmoeish/tsq/cmd/tsq@latest
```

也可以从源码构建：

```bash
git clone https://github.com/tmoeish/tsq.git
cd tsq
make build
```

## 5 分钟最小路径

### 1. 定义一个表结构

```go
package database

// @TABLE(
//   kw=["Name","Email"]
// )
type User struct {
	ID    int64  `db:"id" json:"id"`
	Name  string `db:"name" json:"name"`
	Email string `db:"email" json:"email"`
}
```

### 2. 生成代码

```bash
tsq fmt ./database
tsq gen ./database
```

`gen` 接受三种输入：

- 模块导入路径：`github.com/acme/app/internal/database`
- 相对目录：`./internal/database`
- 绝对目录：`/path/to/app/internal/database`

生成后通常会看到：

- `database/user_tsq.go`：`User` 表的列、CRUD、分页和查询助手
- `database/*_result_tsq.go`：只在你声明 `@RESULT` 时生成
- `database/sqlite.sql` / `database/mysql.sql` / `database/postgres.sql`：每种内置方言的最新全量 DDL
- `database/<dialect>.incremental.sql`：有 schema 增量时，基于 `ddl.json` 的最新增量 DDL
- `database/ddl.json`：最新 schema snapshot 与增量历史记录，用于后续 `tsq gen` 对账

如果目标文件已经存在，TSQ 只会覆盖**已有的生成文件**；遇到手写文件会拒绝覆盖并直接报错。

### 3. 跑第一条查询

```go
package main

import (
	"context"
	"database/sql"
	"log"

	_ "github.com/mattn/go-sqlite3"

	"github.com/tmoeish/tsq"
	"github.com/your/module/database"
)

func main() {
	ctx := context.Background()

	db, err := sql.Open("sqlite3", "file:app.db?cache=shared")
	if err != nil {
		log.Fatal(err)
	}

	engine := &tsq.Engine{Db: db, Dialect: tsq.SQLiteDialect{}}
	if err := tsq.Init(engine, false, true); err != nil {
		log.Fatal(err)
	}

	query, err := tsq.
		Select(database.TableUserCols...).
		From(database.TableUser).
		Where(database.User_Name.Contains("alice")).
		Build()
	if err != nil {
		log.Fatal(err)
	}

	users, err := tsq.List[database.User](ctx, engine, query)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("loaded %d users", len(users))
}
```

更完整的从零到 SQLite 示例见 [`docs/quickstart.md`](docs/quickstart.md)。

## 文档导航

| 文档 | 适合什么时候看 |
| --- | --- |
| [`docs/quickstart.md`](docs/quickstart.md) | 从空目录开始，5 分钟跑通 SQLite 示例 |
| [`docs/concepts.md`](docs/concepts.md) | 想建立 Table 注解、生成文件、QueryBuilder、Result、Runtime 的心智模型 |
| [`examples/README.md`](examples/README.md) | 想按 quickstart / cookbook / full-suite 找示例 |
| [`BEST_PRACTICES.md`](BEST_PRACTICES.md) | 想看输入校验、分页、事务、排序和生产环境建议 |
| [`MIGRATION_GUIDE.md`](MIGRATION_GUIDE.md) | 从旧 API 迁移到当前 Build-based API |

## 能力矩阵（内置 Dialect）

TSQ 当前内置的 `Dialect` 实现只有 **SQLite / MySQL / PostgreSQL**。下面的矩阵描述的是这三者在仓库当前实现下的行为，不再用“完整支持”这种泛化说法。

| 能力 | SQLite | MySQL | PostgreSQL | 说明 |
| --- | --- | --- | --- | --- |
| 生成 CRUD / 分页助手 | ✅ | ✅ | ✅ | 生成层支持一致 |
| 类型安全列与链式查询 | ✅ | ✅ | ✅ | `tsq.Select(...).From(table).Where(...).Build()` |
| `@RESULT` 结果映射 | ✅ | ✅ | ✅ | 生成 `*_result_tsq.go` |
| `InVar()` 动态 `IN (...)` | ✅ | ✅ | ✅ | 执行时展开参数 |
| `CASE` 表达式 | ✅ | ✅ | ✅ | 构建与执行都支持 |
| 非递归 CTE / `WITH` | ✅ | ❌ | ✅ | MySQL 会在执行前显式拒绝 |
| `FULL JOIN` 执行 | ❌ | ❌ | ✅ | SQL 可构建，执行能力受方言限制 |

补充说明：

- TSQ 现在只内置 **SQLite / MySQL / PostgreSQL** 三个完整闭环的 `Dialect` 实现。
- 如果你要接入自定义数据库，需要实现完整 `Dialect` 合约，而不是依赖 TSQ 在接口外推断能力、DDL 或索引行为。

## 常见边界和注意事项

### `Where(...)` / `Search(...)` 是覆盖式 setter

这两个方法都只能设置一次。

- `Where(cond1, cond2)` / `Search(col1, col2)` 的多个参数会按 `AND` 组合
- 需要 `OR` 时请显式使用 `tsq.Or(...)`

```go
query, err := tsq.
	Select(database.TableUserCols...).
	From(database.TableUser).
	Where(
		database.User_OrgID.EQ(1),
		tsq.Or(
			database.User_Name.Contains("alice"),
			database.User_Email.Contains("alice"),
		),
	).
	Build()
```

### `InVar()` 的空切片 / nil 切片语义是“显式不匹配”

`InVar()` 用于把执行时传入的切片参数展开成 `IN (...)`。  
这里有一个**刻意设计**的边界：如果执行时传入的是空切片或 `nil`，TSQ 会把它渲染成 `IN (NULL)`，从而让查询保持合法 SQL，同时返回 **0 条结果**。

这不是兜底容错，而是 API 语义的一部分：

- 适合表达“调用方当前没有任何可匹配 ID / 状态 / 分类”
- 避免业务层为了空列表额外分叉写一套“不查任何数据”的逻辑
- 让 `InVar()` 在 nil / empty / non-empty 三种输入下都保持统一调用方式

如果你的业务想表达“空列表时忽略这个筛选条件”，请在业务层显式分支，不要依赖 `InVar()` 自动跳过过滤。

### `Build()` 成功不代表所有方言都能执行

`Build()` 负责校验查询结构本身：列归属、子句顺序、结果映射、子查询形状等。  
但像 CTE、`FULL JOIN`、`INTERSECT` 这类能力是否可执行，**必须等拿到实际执行器和它对应的 dialect 之后**才能判断。

仓库里这是刻意保留的行为，因为：

- 同一个 `*tsq.Query` 可能会在不同 runtime / registry / executor 上复用
- 一个进程里可以存在多个 registry，各自绑定不同 dialect
- `Build()` 阶段并不知道最终会由哪个数据库实例执行

因此，TSQ 会在 `List/Get/Page/Count/...` 这类执行入口按实际 executor dialect 做能力校验。  
如果你要提前约束方言，请在应用层把 query 的使用范围限制到固定 executor，而不是假设 `Build()` 能替你推断运行时环境。

### Chunked helper 的事务边界由调用方控制

`ChunkedInsert`、`ChunkedUpdate`、`ChunkedDelete`、`ChunkedDeleteByIDs` 都接收 `SQLExecutor`，这是刻意设计：

- 传普通 `*sql.DB` / `*tsq.Engine`：允许按 chunk 执行，前面成功的 chunk 不会因为后面失败自动回滚
- 传 `*sql.Tx`（或包着 `*sql.Tx` 的 `Engine`）：整个 chunked 操作就运行在该事务里

换句话说，TSQ 不会在 chunked helper 里偷偷创建外层事务。  
如果你的业务需要原子性，请显式开启事务并把 `*sql.Tx` 传进去。

### 关键词搜索的“转义”不是 SQL 注入防护

TSQ 的参数绑定本身负责避免把用户输入直接拼进 SQL。  
`EscapeKeywordSearch` 的作用是**转义 LIKE 通配符**（如 `%`、`_`、`\`），避免用户输入改变搜索语义。

```go
pageReq := &tsq.PageReq{
	Page:    1,
	Size:    10,
	OrderBy: "id",
	Order:   "asc",
	Keyword: tsq.EscapeKeywordSearch(request.Keyword),
}
```

### 推荐使用当前的 Build-based API

仓库当前主路径是：

```go
query, err := tsq.
	Select(database.TableUserCols...).
	From(database.TableUser).
	Where(database.User_ID.EQ(1)).
	Build()
```

如果你在旧示例里看到 `NewQueryBuilder()`，请优先参考 `docs/quickstart.md`、`docs/concepts.md` 和 `examples/full-suite/main.go` 的写法。

### 子查询边界要显式遵守

- `Build()` 返回的是 `*tsq.Query[Owner]`，owner 类型会沿着 builder 保留下来。
- 标量子查询（如 `EQSub` / `GTSub` / `LikeSub`）和 `InSub` / `NInSub` **必须只选择一列**。
- `ExistsSub` / `NExistsSub` 只要求传入已 `Build()` 的子查询，不受返回列数限制。
- 结果投影统一使用包级 `tsq.Into(...)`，不要再写 `col.Into(...)`。

## 示例入口

- **Quickstart**：[`examples/quickstart/README.md`](examples/quickstart/README.md)
- **Advanced**：[`examples/advanced/README.md`](examples/advanced/README.md)
- **Full suite**：[`examples/full-suite/README.md`](examples/full-suite/README.md)

## 开发

```bash
make fmt
make lint
make test
make build
make examples
./bin/examples/full-suite
```

常用目标：

```bash
make help
make build
make test
make test-coverage
make fmt
make vet
make lint
make clean
make install
make update-examples
```
