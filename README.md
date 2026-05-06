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

## 先回答三个上手问题

| 问题 | 最短答案 |
| --- | --- |
| **最小要写什么？** | 一个带 `@TABLE` 注解的 Go struct。 |
| **生成后得到什么？** | 每个表生成一个 `*_tsq.go`；每个 `@RESULT` 生成一个 `*_result_tsq.go`。 |
| **怎么跑第一条查询？** | `tsq gen ./db` → 创建 `tsq.DbMap` → `tsq.Select(...).From(table).Where(...).Build()` → `tsq.List(...)`。 |

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

	dbmap := &tsq.DbMap{Db: db, Dialect: tsq.SqliteDialect{}}
	if err := tsq.Init(dbmap, false, true); err != nil {
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

	users, err := tsq.List[database.User](ctx, dbmap, query)
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

- TSQ 的**方言能力校验**还能识别 `oracle`、`sqlserver` 等名称，用于标识符长度和能力判断。
- 但仓库当前**没有内置 Oracle Dialect 实现**；如果你接入 Oracle，需要自定义 `Dialect` 并自行验证 SQL 兼容性。

## 常见边界和注意事项

### `Where(...)` / `KwSearch(...)` 是覆盖式 setter

这两个方法都会覆盖前一次设置；如果你想继续追加条件，请使用 `And(...)`。

```go
query, err := tsq.
	Select(database.TableUserCols...).
	From(database.TableUser).
	Where(database.User_OrgID.EQ(1)).
	And(database.User_Name.Contains("alice")).
	Build()
```

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

如果你在旧示例里看到 `NewQueryBuilder()`，请优先参考 `docs/quickstart.md`、`docs/concepts.md` 和 `examples/main.go` 的写法。

## 示例入口

- **Quickstart**：[`examples/quickstart/README.md`](examples/quickstart/README.md)
- **Cookbook**：[`examples/cookbook/README.md`](examples/cookbook/README.md)
- **Full suite**：[`examples/full-suite/README.md`](examples/full-suite/README.md)

## 开发

```bash
make fmt
make lint
make test
make build
make examples
./bin/examples
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
