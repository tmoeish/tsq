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
- **🗃️ 多数据库**：支持 SQLite、MySQL、PostgreSQL 等主流数据库
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
    "os"
    "github.com/juju/errors"
    _ "github.com/mattn/go-sqlite3"
    logrus "log/slog"
    "github.com/tmoeish/tsq"
    "github.com/tmoeish/tsq/examples/database"
    "gopkg.in/gorp.v2"
)

func main() {
    logrus.SetLevel(logrus.TraceLevel)

    // 1. 连接 SQLite 内存数据库
    db, err := sql.Open("sqlite3", ":memory:")
    if err != nil {
        logrus.Fatal(errors.ErrorStack(err))
    }
    defer func() { _ = db.Close() }()

    // 2. 初始化 gorp
    dbmap := &gorp.DbMap{Db: db, Dialect: gorp.SqliteDialect{}}
    err = tsq.Init(dbmap, true, TraceDB)
    if err != nil {
        logrus.Fatal(errors.ErrorStack(err))
    }

    // 初始化数据库，执行 mock.sql 文件
    mockSQL, err := os.ReadFile("examples/database/mock.sql")
    if err != nil {
        logrus.Fatal(errors.ErrorStack(err))
    }
    _, err = db.Exec(string(mockSQL))
    if err != nil {
        logrus.Fatal(errors.ErrorStack(err))
    }

    // 3. 构造分页参数
    pageReq := &tsq.PageReq{
        Page:    1,
        Size:    10,
        Order:   "asc,desc",
        OrderBy: "user_id,order_id",
    }

    // 4. 调用 PageUserOrder，假设 user_id = 1
    ctx := context.Background()
    resp, err := database.PageUserOrder(ctx, dbmap, pageReq, 1, "图书", "视频", "杂志")
    if err != nil {
        logrus.Fatal(errors.ErrorStack(err))
    }

    // 5. 打印结果
    rs, _ := json.MarshalIndent(resp, "", "  ")
    fmt.Println(string(rs))
}

func TraceDB(next tsq.Fn) tsq.Fn {
    return func(ctx context.Context) error {
        err := next(ctx)
        return err
    }
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
    OrderTime   string      `json:"order_time"   tsq:"Order.CT"`
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
query := tsq.
    Select(database.TableUser.Cols()...).
    Where(database.User_Name.Like("%admin%")).
    OrderBy(database.User_ID.Desc()).
    MustBuild()
users, err := database.ListUserByQuery(ctx, dbmap, query)

// 分页查询
pageReq := &tsq.PageReq{
    Page:    1,
    Size:    10,
    Order:   "asc,desc",
    OrderBy: "user_id,order_id",
}
resp, err := database.PageUserOrder(ctx, dbmap, pageReq, 1, "图书", "视频", "杂志")
```

### 6. 批量操作

```go
users := []*database.User{
    {OrgID: 1, Name: "张三", Email: "zhangsan@example.com"},
    {OrgID: 1, Name: "李四", Email: "lisi@example.com"},
}
err := tsq.BatchInsert(ctx, dbmap, users)
```

### 7. mock 数据初始化

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

其它如聚合、子查询、关键词搜索等高级用法，请参考 `examples/main.go` 和 `examples/database/userorder.go` 的最新写法。


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
make update-sample # 更新示例代码
```

### 开发工作流

1. Fork 项目到你的 GitHub 账号
2. 创建功能分支：`git checkout -b feature/amazing-feature`
3. 提交代码：`git commit -m 'Add amazing feature'`
4. 推送分支：`git push origin feature/amazing-feature`
5. 创建 Pull Request

## 🤝 贡献指南

我们欢迎社区贡献！请查看 [CONTRIBUTING.md](CONTRIBUTING.md) 了解详细信息。

### 开发贡献

1. 确保代码通过所有测试：`make test`
2. 遵循 Go 代码规范：`make fmt vet lint`
3. 添加必要的测试用例
4. 更新相关文档

## 📋 开发计划

查看 [TODO.md](TODO.md) 文件了解当前的开发计划和待办事项。

## 📄 许可证

本项目采用 MIT 许可证 - 查看 [LICENSE](LICENSE) 文件了解详情。

## 🔗 相关链接

- [项目主页](https://github.com/tmoeish/tsq)
- [问题反馈](https://github.com/tmoeish/tsq/issues)
- [变更日志](CHANGELOG.md)
- [贡献指南](CONTRIBUTING.md)
