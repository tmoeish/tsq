# Concepts：TSQ 的核心心智模型

TSQ 同时包含“注解 DSL”“代码生成”“查询构建”“运行时注册”几层概念。把它们拆开看，会更容易上手。

## 一张关系图

```txt
Go struct + @TABLE / @RESULT
            |
            v
        tsq fmt
            |
            v
        tsq gen
            |
            v
  generated *_tsq.go / *_result_tsq.go
            |
            v
 generated columns + CRUD helpers + Page/List helpers
            |
            v
 tsq.Select(...).From(table).Where(...).Build()
            |
            v
 *tsq.Query[Owner] + args
            |
            v
 tsq.List/Get/Page + tsq.DbMap / Runtime
```

## 1. `@TABLE`：告诉生成器“这是一个表”

`@TABLE` 注解挂在 Go struct 上，描述这个 struct 应该被视为数据库表。

写复杂注解时，可以先直接编辑注释，再用 `tsq fmt ./database` 统一整理键顺序、缩进和列表布局。

```go
// @TABLE(
//   kw=["Name","Email"]
// )
type User struct {
	ID    int64  `db:"id"`
	Name  string `db:"name"`
	Email string `db:"email"`
}
```

它主要解决两件事：

1. 让生成器知道要为哪个 struct 产出表代码
2. 让生成器知道哪些字段参与关键词搜索、索引、唯一约束等

## 2. `*_tsq.go`：生成后的主要产物

对上面的 `User`，TSQ 会生成 `user_tsq.go`。你最常用到的是三类内容：

| 产物 | 例子 | 用途 |
| --- | --- | --- |
| 表和列元数据 | `TableUserCols`, `User_ID`, `User_Name` | 用来构建类型安全查询 |
| CRUD / 查询助手 | `GetUserByID`, `ListUser`, `PageUser` | 常见读写路径直接用 |
| 表注册逻辑 | `RegisterTable`, `Init` 所需元数据 | 让运行时知道这个表的结构 |

如果定义了 `@RESULT`，则会额外生成 `*_result_tsq.go`。

## 3. `QueryBuilder`：拼 SQL 的主路径

当前推荐的查询构建方式是 Build-based API：

```go
query, err := tsq.
	Select(database.TableUserCols...).
	From(database.TableUser).
	Where(database.User_Name.Contains("alice")).
	OrderBy(database.User_ID.Desc()).
	Build()
```

这一步的职责是：

- 收集要查哪些列
- 收集过滤、排序、分组、联接等信息
- 在 `Build()` 时生成最终 SQL 和参数

你可以把 `*tsq.Query[Owner]` 理解为“已经准备好执行的 SQL + args”，其中 `Owner` 是扫描目标。

如果把一个查询拿去当子查询用，也沿用这条规则：`Build()` 后得到的是 `*tsq.Query[Owner]`。其中：

- `EQSub` / `GTSub` / `LikeSub` 这类标量比较要求子查询只返回 1 列；
- `InSub` / `NInSub` 也要求子查询只返回 1 列；
- `ExistsSub` / `NExistsSub` 只要求子查询已经 `Build()`。

## 4. `Owner` / `Table` / `Result`：三层 owner 语义

TSQ 现在把“谁负责承接扫描结果”和“谁是物理表”拆成了三层：

| 类型 | 含义 | 典型用途 |
| --- | --- | --- |
| `Owner` | 任何可扫描目标 | 普通查询结果、手写结果 struct、`@RESULT`、表 struct |
| `Table` | 物理表 owner | `From(...)`、表列定义、`Insert/Update/Delete`、表级列/主键元数据 |
| `Result` | 投影 owner | `@RESULT` 生成结果、联表结果模型 |

所以：

- 所有 `Table` / `Result` 都是 `Owner`
- `Table` 才是 mutation target，并且会暴露稳定的 `Cols()/PrimaryKeys()` 这类表元数据
- `Result` 不是物理表，不参与底层表 mutation

这也是为什么 `tsq.Into(...)` 的目标仍然是 `Owner`：很多手写投影结果并不需要声明成 `@RESULT`，但依然是合法扫描目标。

## 5. `Result`：把多表结果映射成一个单独 struct

`@RESULT` 不是数据库表，而是“查询结果模型”。

```go
// @RESULT(name="UserOrder")
type UserOrder struct {
	UserID   int64  `tsq:"User.ID"`
	UserName string `tsq:"User.Name"`
	OrgName  string `tsq:"Org.Name"`
}
```

它适合：

- 联表列表页
- 报表 / 聚合结果
- API 返回模型和表模型不一致的场景

生成后你通常会得到像 `PageUserOrder(...)` 这样的助手。

## 6. `tsq.DbMap` / `SqlExecutor`：执行查询时的数据库上下文

TSQ 执行查询时需要一个 `DbMap`：

```go
dbmap := &tsq.DbMap{
	Db:      db,
	Dialect: tsq.SqliteDialect{},
}
```

它把两件事绑在一起：

1. 底层数据库连接
2. 当前 SQL 方言

执行时像 `tsq.List(...)`、`tsq.Get(...)`、`query.Count64(...)` 都会用到它。

底层执行接口是 `SqlExecutor`。这里最重要的约定有两点：

1. 所有执行方法都显式接收 `ctx context.Context`
2. mutation 方法只面向 `Table`，而扫描辅助方法保留对标量 / ad-hoc struct 的灵活性

也就是说，像 `Query / QueryRow / Exec / Insert` 这类方法都直接把 `ctx` 放在第一个参数，不再通过 `WithContext(...)` 返回副本 executor。

## 7. `Runtime`：表注册和隔离

默认情况下，生成代码会把表注册到全局运行时。对单数据库应用来说，这通常就够了。

如果你有这些需求，再关心 `Runtime`：

- 测试里想隔离注册状态
- 一个进程里管理多个数据库
- 插件式宿主，不希望共享全局注册表

这时可以显式创建独立运行时：

```go
rt := tsq.NewRuntime()
```

然后使用该运行时注册表、初始化 tracer 或执行需要隔离的流程。

## 8. `PageReq`、`InVar()`、`WithTable()` 分别解决什么问题

| 概念 | 解决的问题 | 什么时候需要 |
| --- | --- | --- |
| `PageReq` | 列表页分页、排序、关键词搜索 | HTTP API / 后台列表 |
| `InVar()` | 执行时传入动态切片参数 | `WHERE id IN (?)` 这类场景 |
| `WithTable()` | 把列重绑定到别名表或 CTE | 自连接、别名联表、CTE 外层引用 |

## 9. 最容易混淆的两个边界

### `Where(...)` 和 `KwSearch(...)` 不是 append

它们都会覆盖之前的设置；要继续追加条件，用 `And(...)`。

### `EscapeKeywordSearch(...)` 不是 SQL 注入防护

SQL 注入防护来自参数绑定。`EscapeKeywordSearch(...)` 只负责转义 LIKE 通配符，避免用户输入中的 `%` / `_` 改变搜索语义。

## 10. 推荐的学习顺序

1. 先跑 [`quickstart.md`](quickstart.md)
2. 再理解 `@TABLE` → `tsq gen` → `*_tsq.go`
3. 然后只学 `Select/Where/Build/List`
4. 最后再看 `Result`、分页、CTE、运行时隔离这些高级能力
