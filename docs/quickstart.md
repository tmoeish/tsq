# Quickstart：从空目录到第一条 SQLite 查询

这份 quickstart 只做一件事：在一个全新的 Go 目录里，用 TSQ 生成表代码并跑通第一条 SQLite 查询。

## 目标

完成后你会得到：

1. 一个带 `@TABLE` 注解的表结构
2. 一个 `*_tsq.go` 生成文件
3. 一个能连 SQLite、初始化 TSQ 并查出用户列表的 `main.go`

## 1. 初始化项目

```bash
mkdir tsq-quickstart
cd tsq-quickstart

go mod init example.com/tsq-quickstart
go get github.com/tmoeish/tsq/v4@latest
go get github.com/mattn/go-sqlite3@latest
go install github.com/tmoeish/tsq/v4/cmd/tsq@latest
```

创建目录：

```bash
mkdir -p database
```

## 2. 定义表结构

新建 `database/user.go`：

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

最小起步只需要：

- 一个普通 Go struct
- `@TABLE` 注解
- 每个字段的 `db` tag

## 3. 生成 TSQ 代码

```bash
tsq fmt ./database
tsq gen ./database
```

生成后会多出：

```txt
database/
  user.go
  user_tsq.go
```

其中 `user_tsq.go` 是生成文件，不要手改。`tsq fmt` 会复用同样的包定位规则，只整理 struct 注释里的 `@TABLE` / `@RESULT` DSL，不会改写其他注释内容。

## 4. 准备一个最小 SQLite 数据库

新建 `main.go`：

```go
package main

import (
	"context"
	"database/sql"
	"log"

	_ "github.com/mattn/go-sqlite3"

	"github.com/tmoeish/tsq/v4"
	"example.com/tsq-quickstart/database"
)

func main() {
	ctx := context.Background()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(`
CREATE TABLE user (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  email TEXT NOT NULL
);
INSERT INTO user (name, email) VALUES
  ('Alice', 'alice@example.com'),
  ('Bob', 'bob@example.com');
`); err != nil {
		log.Fatal(err)
	}

	if err := tsq.Init(db, tsq.SQLiteDialect{}); err != nil {
		log.Fatal(err)
	}

	engine := tsq.CurrentEngine()

	query, err := tsq.
		Select(database.User__Cols...).
		From(database.TableUser).
		Where(database.User_Name.Contains("A")).
		Build()
	if err != nil {
		log.Fatal(err)
	}

	users, err := tsq.List[database.User](ctx, engine, query)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("matched users: %+v", users)
}
```

## 5. 运行

```bash
go run .
```

你应该能看到至少一条包含 `Alice` 的结果。

## 下一步最值得看什么

- 想理解生成了哪些符号：看 [`docs/concepts.md`](concepts.md)
- 想看分页、事务、HTTP API 场景：看 [`../examples/advanced/README.md`](../examples/advanced/README.md)
- 想看完整能力展示：看 [`../examples/full-suite/README.md`](../examples/full-suite/README.md)

## 常见问题

### `tsq gen ./database` 报找不到包

- 确认当前目录里有 `go.mod`
- 确认 `database/` 里至少有一个 `.go` 文件
- 确认命令是在模块根目录执行的

### 生成时报拒绝覆盖文件

TSQ 只会覆盖已有的生成文件。如果你手工创建了 `user_tsq.go`，生成器会拒绝覆盖它。

### 查询构建成功但执行时报方言错误

TSQ 的一部分能力（如 CTE、`FULL JOIN`）会在**执行阶段**按方言校验，而不是在 `Build()` 阶段提前拒绝。

这是刻意的：

- `Build()` 只知道查询结构，不知道最终会由哪个 executor 执行
- 同一个 `*tsq.Query` 可能会复用到不同 runtime / registry
- 不同 executor 可能绑定不同 dialect

所以 `Build()` 成功表示“查询结构合法”，不表示“任何数据库都能执行这条 SQL”。SQLite / MySQL / PostgreSQL 的边界见根目录 [`README.md`](../README.md)。

### 生成 helper 一调用就报 `initialize XxxQuery`

这说明对应的生成查询在包初始化时准备失败了，但 TSQ 不会再因为导入包直接 `panic`。  
现在的行为是：**等你调用 `Get...` / `List...` / `Page...` helper 时，把初始化错误正常返回出来**。

优先检查：

- 最近是否改过 `@TABLE` / `@RESULT` 注解但还没重新生成
- 生成代码和当前模型定义是否一致
- 查询里引用的列、索引、投影字段是否仍然合法

### `InVar()` 传空切片为什么返回空结果

这是 `InVar()` 的设计语义，不是异常。

当执行时传入空切片或 `nil` 时，TSQ 会把 `IN` 条件展开成 `IN (NULL)`，从而让 SQL 保持合法并返回 0 条结果。这样做是为了把“当前没有任何可匹配值”表达成**显式不匹配**，而不是自动忽略筛选条件。

如果你的业务含义是“空列表时不要加这个筛选”，请在业务层自己决定是否构造 `Where(...InVar())`，不要依赖 TSQ 自动跳过这个条件。

### 普通值为什么没有直接拼成 SQL 字面量

这是刻意设计。当前 TSQ 默认把普通值放进参数列表，而不是把值直接拼进 SQL 文本。

- 这样更安全，也更符合数据库驱动的常规使用方式
- 如果你要表达的是列、函数或子查询，请显式传对应表达式对象
- 如果只是普通值，就让 TSQ 负责参数绑定
