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


TSQ（Type-Safe Query）是一个强大的 Go 语言代码生成工具，专为构建类型安全的 SQL 查询而设计。通过解析 Go 结构体和注解，TSQ 能够自动生成高效、类型安全的数据库访问代码，大幅提升开发效率和代码质量。

## ✨ 核心特性

- **🔒 类型安全**：编译时检查字段类型和 SQL 语法，避免运行时错误
- **🚀 代码生成**：自动生成 CRUD 操作、复杂查询和分页功能
- **📝 注解驱动**：通过简单注解定义表结构、索引、唯一约束等
- **🔍 灵活查询**：支持联表查询、子查询、聚合函数等复杂 SQL 操作
- **📄 分页支持**：内置高效的分页查询机制
- **🗃️ 多数据库**：当前内置方言覆盖 SQLite、MySQL、PostgreSQL；不兼容的方言特性会显式报错
- **🛠️ DTO 支持**：支持复杂查询结果的数据传输对象
- **⚡ 高性能**：生成的代码经过优化，性能接近手写 SQL
- **🔍 关键词搜索**：内置关键词搜索功能，支持多字段模糊查询
- **📊 聚合查询**：支持 COUNT、SUM、AVG、MIN、MAX 等聚合函数

## 📦 安装

### 从源码构建

```bash
git clone https://github.com/tmoeish/tsq.git
cd tsq
make build
```

### 使用 go install

```bash
go install github.com/tmoeish/tsq/cmd/tsq@latest
```

## 🚀 快速开始

### 1. 定义数据结构（最新 DSL 注解示例）

```go
// @TABLE(
//   name="User",
//   ux=[{name="UxName", fields=["Name"]}],
//   kw=["Name","Email"]
// )
type User struct {
    common.ImmutableTable
    OrgID int64  `db:"org_id" json:"org_id"`
    Name  string `db:"name" json:"name"`
    Email string `db:"email" json:"email"`
}

// @TABLE(
//   name="Order",
//   idx=[{name="IdxUserItem", fields=["UserID","ItemID"]}, {name="IdxItem", fields=["ItemID"]}],
//   ct
// )
type Order struct {
    common.MutableTable
    UserID int64 `db:"user_id"`
    ItemID int64 `db:"item_id"`
    Amount int64 `db:"amount"`
    Price  int64 `db:"price"`
    Status OrderStatus `db:"status"`
}

// @TABLE(
//   ux=[{fields=["Name"]}],
//   idx=[{name="IdxCategory", fields=["CategoryID"]}],
//   kw=["Name"]
// )
type Item struct {
    common.ImmutableTable
    CategoryID int64 `db:"category_id"`
    Name       string `db:"name,size:200"`
    Price      int64  `db:"price"`
}

// @TABLE(
//   ux=[{fields=["Name"]}],
//   kw=["Name","Description"]
// )
type Category struct {
    common.ImmutableTable
    CategoryContent
}
type CategoryContent struct {
    Type        CategoryType `db:"type" json:"type"`
    Name        string       `db:"name,size:200" json:"name"`
    Description string       `db:"description,size:4096" json:"description"`
}

// @TABLE(
//   name="Org",
//   ux=[{name="UxName", fields=["Name"]}]
// )
type Org struct {
    common.ImmutableTable
    Name string `db:"name"`
}
```

### 2. 生成 TSQ 代码

```bash
tsq gen ./examples/database
```

### 3. 使用生成的代码（主流程示例）

```go
package main

import (
    "context"
    "database/sql"
    "encoding/json"
    "fmt"
    "log/slog"
    "os"
    "github.com/juju/errors"
    _ "github.com/mattn/go-sqlite3"
    "github.com/tmoeish/tsq"
    "github.com/tmoeish/tsq/examples/database"
    "gopkg.in/gorp.v2"
)

func main() {
    // 1. 连接 SQLite 内存数据库
    db, err := sql.Open("sqlite3", ":memory:")
    if err != nil {
        slog.Error("open sqlite", "error", errors.ErrorStack(err))
        os.Exit(1)
    }
    defer func() { _ = db.Close() }()

    // 2. 初始化 gorp
    dbmap := &gorp.DbMap{Db: db, Dialect: gorp.SqliteDialect{}}
    err = tsq.Init(dbmap, true, true, tsq.PrintError, tsq.PrintSQL)
    if err != nil {
        slog.Error("tsq init", "error", errors.ErrorStack(err))
        os.Exit(1)
    }

    // 初始化数据库，执行 mock.sql 文件
    mockSQL, err := os.ReadFile("examples/database/mock.sql")
    if err != nil {
        slog.Error("read mock sql", "error", errors.ErrorStack(err))
        os.Exit(1)
    }
    _, err = db.Exec(string(mockSQL))
    if err != nil {
        slog.Error("exec mock sql", "error", errors.ErrorStack(err))
        os.Exit(1)
    }

    // 3. 构造分页参数
    pageReq := &tsq.PageReq{
        Page:    1,
        Size:    10,
        Order:   "asc,desc",
        OrderBy: "user_id,order_id",
    }
    if err := pageReq.ValidateStrict(); err != nil {
        slog.Error("invalid page request", "error", err)
        os.Exit(1)
    }

    // 4. 调用 PageUserOrder，假设 user_id = 1
    ctx := context.Background()
    resp, err := database.PageUserOrder(ctx, dbmap, pageReq, 1, "图书", "视频", "杂志")
    if err != nil {
        slog.Error("page user order", "error", errors.ErrorStack(err))
        os.Exit(1)
    }

    // 5. 打印结果
    rs, _ := json.MarshalIndent(resp, "", "  ")
    fmt.Println(string(rs))
}
```

### 4. DTO 复杂查询示例

```go
// @DTO(
//   name="UserOrder"
// )
type UserOrder struct {
    UserID    int64  `json:"user_id"    tsq:"User.ID"`
    UserName  string `json:"user_name"  tsq:"User.Name"`
    UserEmail string `json:"user_email" tsq:"User.Email"`
    OrgName   string `json:"org_name"   tsq:"Org.Name"`
    OrderID     int         `json:"order_id"     tsq:"Order.ID"`
    OrderAmount float64     `json:"order_amount" tsq:"Order.Amount"`
    OrderPrice  float64     `json:"order_price"  tsq:"Order.Price"`
    OrderStatus OrderStatus `json:"order_status" tsq:"Order.Status"`
    OrderTime   time.Time   `json:"order_time"   tsq:"Order.CT"`
    ItemID    int64  `json:"item_id"    tsq:"Item.ID"`
    ItemName  string `json:"item_name"  tsq:"Item.Name"`
    ItemPrice int64  `json:"item_price" tsq:"Item.Price"`
    ItemCategory string `json:"item_category" tsq:"Category.Name"`
}

// 复杂分页查询
resp, err := database.PageUserOrder(ctx, dbmap, pageReq, 1, "图书", "视频", "杂志")
```

### 5. 查询与分页用法

```go
// 条件查询
q, err := tsq.
    Select(database.User_ID, database.User_Name, database.User_Email).
    Where(database.User_Name.Contains("admin")).
    Build()
if err != nil {
    return nil, fmt.Errorf("build query: %w", err)
}
users, err := tsq.List[database.User](ctx, dbmap, q)

// 需要保留旧式 SQL literal 行为时，可使用显式 literal helper
literalQ, err := tsq.
    Select(database.User_ID).
    Where(database.User_Name.EQLiteral("admin")).
    Build()
if err != nil {
    return nil, fmt.Errorf("build query: %w", err)
}

// 注意：Where(...) 和 KwSearch(...) 都是“覆盖式”设置；
// 如果需要继续追加过滤条件，请使用 And(...)

// 分页查询
pageReq := &tsq.PageReq{
    Page:    1,
    Size:    10,
    Order:   "asc,desc",
    OrderBy: "user_id,order_id",
}
if err := pageReq.ValidateStrict(); err != nil {
    return err
}
resp, err := database.PageUserOrder(ctx, dbmap, pageReq, 1, "图书", "视频", "杂志")
```

### 6. 表别名与自连接

```go
managerID := employeeID.As("manager")
managerName := employeeName.As("manager")

q, err := tsq.
    Select(employeeID, employeeName, managerName).
    LeftJoin(employeeManagerID, managerID).
    Build()
if err != nil {
    return nil, fmt.Errorf("build query: %w", err)
}
```

`As("manager")` 会把列重新绑定到别名表；要给函数表达式、聚合表达式或 DTO 映射列起别名，请先对基础列做 `As(...)`，再继续链式调用。

### 7. 运行时隔离与高级用法

- 默认情况下，生成代码会通过包级 `RegisterTable` / `Init` 使用全局运行时。
- 如果你在测试、多数据库场景或插件式宿主中需要隔离注册表和 tracer，可创建独立运行时：`rt := tsq.NewRuntime()`，然后使用 `rt.RegisterTable(...)`、`rt.Init(...)`、`tsq.Trace1WithRuntime(...)`。
- `PageReq.Validate()` 保持兼容语义：会把非法页码/页大小归一化为安全默认值；如果你想显式拒绝非法分页/排序输入，请使用 `PageReq.ValidateStrict()`。
- `FULL JOIN` 现在可以构建 SQL，但执行时仍受方言限制：SQLite / MySQL 会被显式拒绝，PostgreSQL 和自定义支持该能力的方言可继续执行。

### 8. 分块写入

```go
users := []*database.User{
    {OrgID: 1, Name: "张三", Email: "zhangsan@example.com"},
    {OrgID: 1, Name: "李四", Email: "lisi@example.com"},
}
err := tsq.ChunkedInsert(ctx, dbmap, users)
```

### 9. mock 数据初始化

```go
mockSQL, err := os.ReadFile("examples/database/mock.sql")
if err != nil {
    logrus.Fatal(errors.ErrorStack(err))
}
_, err = db.Exec(string(mockSQL))
if err != nil {
    logrus.Fatal(errors.ErrorStack(err))
}
```

---

### 🔍 关键词搜索

TSQ 支持多字段模糊关键词搜索，可以在查询中添加关键词过滤。

**基本用法：**

```go
// 在 users 表中搜索 name 或 email 字段包含 "john" 的用户
qb := users.NewQueryBuilder().
    Where(users.Active.EQ(true)).
    KwSearch("john", users.Name, users.Email)

q, err := qb.Build()
if err != nil {
    return err  // 处理错误而非panic
}

list, err := q.List(ctx, db)
```

**安全性注意事项：**

由于关键词搜索使用 SQL LIKE 操作符，需要对用户输入进行转义以防止 SQL 注入：

```go
import "github.com/tmoeish/tsq"

// 用户输入
keyword := request.Keyword  // 可能包含 "%" 或 "_" 等特殊字符

// 转义关键词中的 LIKE 通配符
escaped := tsq.EscapeKeywordSearch(keyword)

qb := users.NewQueryBuilder().KwSearch(escaped, users.Name, users.Email)
```

**限制和注意事项：**

- 关键词搜索使用 LIKE 操作符，可能对大表性能有影响
- 目前不支持 SQL 方言特定的全文搜索优化（如 MySQL FULLTEXT）
- 关键词只在指定的列中搜索
- LIKE 转义字符固定为 `\`（反斜杠）

**示例中的完整查询：**

更多关键词搜索和其他高级用法（聚合、子查询等），请参考 `examples/main.go` 和 `examples/database/userorder.go` 的最新写法。

---

### 🔗 联接（Join）和圆形联接限制

TSQ 支持多种联接类型（INNER JOIN, LEFT JOIN, RIGHT JOIN 等），但存在一个重要限制：**不支持圆形联接依赖**。

**什么是圆形联接？**

圆形联接是指在联接图中存在循环路径的情况。例如：
- 用户表 → 订单表 → 发票表 → 用户表（循环回到用户）

**当前限制：**

以下代码**会失败**（圆形依赖）：
```go
query := users.NewQueryBuilder().
    InnerJoin(orders, users.ID.EQ(orders.UserID)).
    InnerJoin(invoices, orders.ID.EQ(invoices.OrderID)).
    InnerJoin(users, invoices.UserID.EQ(users.ID))  // ❌ 错误：用户表已参与
```

**解决方案 - 使用表别名实现自联接：**

如果需要表示涉及同一表的复杂关系，可以使用 `AliasTable()` 创建表别名：

```go
// 创建 users 表的别名
usersAlias := tsq.AliasTable(users, "manager_users")

// 现在可以与相同表进行联接
query := users.NewQueryBuilder().
    InnerJoin(usersAlias, users.ManagerID.EQ(usersAlias.Col(users.ID)))
```

**其他替代方案：**

1. **执行多个查询** - 将圆形查询分解为多个独立查询
2. **使用子查询** - 根据目标数据库的支持使用子查询或 CTE（WITH 子句）
3. **应用逻辑处理** - 在应用层处理复杂的关系逻辑

更多联接示例，请参考 `examples/database/userorder.go`。

---

### ⚠️ 已知限制和最佳实践

**已知限制：**

| 功能 | 状态 | 说明 |
|------|------|------|
| 圆形联接 | ❌ 不支持 | 使用表别名（AliasTable）作为替代方案 |
| FULL JOIN | ⚠️ 有限 | 仅 PostgreSQL 支持；其他方言在执行时会报错 |
| 全文搜索 | ❌ 不支持 | 关键词搜索使用 LIKE；可用专门库处理全文搜索 |
| 子查询 | ⚠️ 有限 | 基本支持，但不支持所有复杂场景 |
| 事务控制 | ⚠️ 基础 | 支持基本的事务操作，不支持高级特性 |

**最佳实践：**

1. **错误处理**：总是检查 `Build()` 返回的错误，避免使用 `MustBuild()` 在生产环境

```go
// ✅ 推荐
q, err := qb.Build()
if err != nil {
    return fmt.Errorf("failed to build query: %w", err)
}

// ❌ 不推荐（仅用于初始化时）
q := qb.MustBuild()  // 会 panic
```

2. **关键词搜索安全**：始终转义用户输入

```go
keyword := tsq.EscapeKeywordSearch(userInput)
qb := qb.KwSearch(keyword, col1, col2)
```

3. **运行时隔离**：多数据库应用应使用独立的 Runtime 实例

```go
// ✅ 多DB 应用的推荐方式
runtimeMySQL := tsq.NewRuntime()
runtimePostgres := tsq.NewRuntime()

// 为每个 runtime 注册表
users.RegisterTable(...)  // 注册到 DefaultRuntime
runtimeMySQL.RegisterTable(...)  // 注册到特定 runtime
```

4. **验证分页和排序**：使用 `ValidateStrict()` 验证用户输入的排序参数

```go
// 对来自 HTTP 请求的分页参数进行严格验证
page := &PageReq{Page: userInput.Page, Size: userInput.Size}
if err := page.ValidateStrict(); err != nil {
    return errors.New("invalid pagination: " + err.Error())
}
```

5. **标识符长度检查**：对于跨数据库应用，验证标识符长度

```go
if err := ValidateIdentifierLength(tableName, "postgres"); err != nil {
    return fmt.Errorf("table name too long for postgres: %w", err)
}
```

6. **性能优化**：

   - 使用查询构建器的方法链来构建复杂查询
   - 避免在循环中重复构建相同的查询（考虑缓存或参数化）
   - 对大表使用分页而非一次性加载所有数据
   - 为常用的联接组合创建辅助方法
   - **查询缓存**：如果频繁执行相同的查询，使用应用层缓存

```go
// ✅ 为常用查询创建辅助方法
func (ub *userBuilder) WithOrders() *QueryBuilder {
    return ub.InnerJoin(orders, user.ID.EQ(orders.UserID))
}

// 使用
results, err := users.NewQueryBuilder().
    WithOrders().
    Where(users.Active.EQ(true)).
    List(ctx, db)

// ✅ 缓存频繁执行的查询
var (
    activeUsersQuery *tsq.Query
    activeUsersErr   error
)
func init() {
    activeUsersQuery, activeUsersErr = users.NewQueryBuilder().
        Where(users.Active.EQ(true)).
        Build()
}

// 使用缓存的查询
func getActiveUsers() (*tsq.Query, error) {
    if activeUsersErr != nil {
        return nil, activeUsersErr
    }
    return activeUsersQuery, nil
}

// 在应用中使用
results, err := getActiveUsers()
if err != nil {
    return nil, err
}
items, err := results.List(ctx, db)
```

**支持的数据库方言：**

- ✅ **SQLite** - 完整支持
- ✅ **MySQL** - 完整支持（5.7+）
- ✅ **PostgreSQL** - 完整支持（9.6+）
- ⚠️ **Oracle** - 基础支持（某些高级特性可能不可用）
- ❓ **其他方言** - 需要验证兼容性


## 🏗️ 构建和开发

### 开发环境要求

- Go 1.24.2+
- Make
- Git

### 构建项目

```bash
# 克隆项目
git clone https://github.com/tmoeish/tsq.git
cd tsq

# 安装依赖
make mod-tidy

# 运行测试
make test

# 构建项目
make build

# 运行所有检查和构建
make all
```

### Make 命令

```bash
make help          # 显示所有可用命令
make build         # 构建应用
make test          # 运行测试
make test-coverage # 运行覆盖率测试
make fmt           # 格式化代码
make vet           # 代码检查
make lint          # 静态分析
make clean         # 清理构建产物
make install       # 安装到 GOPATH/bin
make update-examples # 更新示例代码
```
