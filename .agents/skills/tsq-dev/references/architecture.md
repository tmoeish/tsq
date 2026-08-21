# 架构

TSQ 是一个被 import 的库加一个 CLI 生成器，没有服务、没有进程、没有后台循环。它的
"运行时"是别人的进程。这决定了这里的每一个设计取舍：**能在编译期挡住的错误，不留到
运行期**；能在 `Build()` 挡住的，不留到执行期。

## 三个交付物，一个版本号

| 交付物 | 位置 | 使用者怎么拿到 |
| --- | --- | --- |
| 库 | 仓库根包 `tsq` | `go get github.com/tmoeish/tsq/v4` |
| 生成器 CLI | `./cmd/tsq`（实现在 `internal/cmd`） | `go install .../cmd/tsq@vX.Y.Z` 或 GoReleaser 的二进制 |
| 示例 | `./examples` | 读源码、抄片段 |

三者共用 `internal/buildinfo` 里的版本号，见 `release.md`。

## 分层

```
cmd/tsq  ──► internal/cmd ──► internal/parser ──► internal/genmodel
                   │                                     ▲
                   └────────────► dialect ◄──────────────┘
根包 tsq ──────────────────────► dialect
```

- `internal/genmodel` 是**中立的数据模型**：`StructInfo`、`FieldInfo`、`TableMeta`、
  `IndexInfo`、`PackageInfo`。解析器往里填，模板从里读。它不 import 解析器也不 import
  命令层——这是让"解析"和"渲染"能各自被测试的原因。
- `internal/parser` 只负责 Go 源码 → `genmodel`。它做 AST 遍历、注解定位、DSL 词法/语法
  分析、字段解析和排序。
- `internal/cmd` 只负责 `genmodel` → 磁盘：模板渲染、校验、DDL 推导与渲染、文件写入。
- `dialect` 同时被库和生成器用：它既定义运行期的 SQL 方言能力，又定义生成期的 DDL 类型
  映射。这不是巧合——两边说的是同一件事（这个库支持什么），拆开必然漂移。
- 根包 `tsq` 不 import 任何 `internal/` 包。生成的代码只依赖根包和 `dialect`。

## 查询构建器：阶段式类型状态机

这是这个库最容易被误改的部分。`querybuilder.go` 定义了一串接口，
`querybuilder_stages.go` 定义了每个具体 builder 能返回什么：

```
Select(...)  ──► SelectStage  ──From──►  ┐
From(...)    ──► FromStage    ──Select─► ├──► queryBuilder
                                          │      │
                        ┌─────────────────┘      ├─Where──►  WhereStage
                        │                        ├─Search─►  SearchStage
                        │                        └─GroupBy─► GroupedStage
    WhereStage  ─Search─┐
    SearchStage ─Where──┴──► FilteredStage ─GroupBy─► GroupedStage ─Having─► HavingStage
    setops ─────────────────► CompoundStage
    ForUpdate/ForShare ─────► LockedStage
```

**`Where(...)` 和 `Search(...)` 每条链上最多各出现一次，这由 Go 类型系统在编译期强制**：
调用过 `Where` 之后拿到的是 `WhereStage`，它上面根本没有 `Where` 方法。这不是运行期校验，
不能靠加 if 来"改进"。

改这里的时候：

- 新增一个阶段就是新增一个接口加一个具体 builder 类型。返回值类型写错，编译期的约束就
  静悄悄地松了——`querybuilder_stages_test.go` 和 `compilefail_test.go` 是防线。
- `queryBuilderCore` 持有所有阶段共享的状态；具体 builder 只是它的类型化外壳。
- `builderPhase` 用来在运行期给出更好的错误信息，它**不是**约束的来源，类型才是。

## 从构建到执行

1. `Build()` → `*Query[O]`。这一步只做**结构**校验（`query_validation.go`、
   `query_plan_validate.go`）：列属不属于查询涉及的表、聚合和 GROUP BY 合不合法、
   有没有选任何列。
2. `query_plan*.go` 把构建结果变成执行计划：涉及哪些表（`query_plan_tables.go`）、
   CTE 怎么排（`query_plan_cte.go`）、SQL 怎么拼（`query_plan_sql.go`）。
3. `sql_render.go` 渲染 SQL 文本，`query_args.go` 绑定参数。
4. 执行期（`executor*.go`、`query_load.go`、`query_scalar.go`、`query_scan.go`）才校验
   **方言能力**：CTE、`FULL JOIN`、行锁在这个方言上能不能跑（`dialect_validation.go`）。

**这条分界线是有意的**：同一个 `*Query` 可以在多个方言上复用，把方言校验提前到 `Build()`
就断了这个用法。改校验时先想清楚它属于哪一边。

### Go 1.27 泛型方法的落点

Go 1.27 允许具体 receiver 的方法声明自己的类型参数，因此类型化操作归回拥有状态的对象：

- `Query.Scalar(selected)` 从 `selected` 推导结果类型，并校验它就是查询唯一的选列。
- `Query.AsSubquery(selected)` 在已构建查询上产出类型化子查询。
- `Runtime.WithTxResult(...)` 执行带一个类型化返回值的事务；多个相关值用小结果结构体承载。
- `PageRequest.Response(total, data)` 构造类型化分页响应。

没有把所有泛型函数机械地改成方法。`QueryStage`、`SQLExecutor`、`Column` / `ValueColumn` 都是
接口，而 Go 1.27 仍禁止接口方法声明类型参数；`BuildSubquery`、`MapInto`、mutation/chunked
helper 因此继续作为包级函数。为了一点调用语法把这些接口改成公开具体类型，会破坏阶段机、
事务 executor 通用性或列实现封装，收益不抵代价。

## 运行时

`Runtime`（`runtime.go`）是 `*sql.DB` 加方言加已注册表的组合，显式构造：
`NewRuntime(driverName, dsn, tables, opts...)`。没有全局 `Init()`，没有包级单例——
这是历史上被删掉的东西，不要以任何形式重新引入。

- `Runtime` 自己实现 `SQLExecutor`，所以它可以直接传给需要执行器的地方。
- `WithTx`（`tx.go`）是多操作事务的唯一入口，支持 `TxOptions.Retry`（配合
  `IsOptimisticLockError` 做乐观锁重试）。
- `runtime_schema.go` 负责 schema 对账：`Options` 上的 `TablePolicy` 和 `IndexPolicy`
  各自取 `SchemaPolicy`（`Manual` / `Validate` / `CreateMissing` / `Reconcile` /
  `Managed`），决定 `NewRuntime` 是只校验还是补齐表、列与索引。`IndexInit*` 和
  `IndexInitMode` 是弃用别名，保留是为了不破坏使用者的代码。
- `table_registry.go` 保存表元数据，`table_index.go` 保存索引元数据；生成的
  `runtime.tsq.go` 通过 `TSQTables()` 把包内所有表交给 `NewRuntime`。

## 方言

`dialect/` 下每个方言实现 `Dialect` 接口：标识符引用、占位符、DDL 类型映射、DDL 语句
渲染，以及 `SupportsCapability(Capability)`。

能力位是一份显式枚举：`CapabilityCTE`、`CapabilityExcept`、`CapabilityIntersect`、
`CapabilityFullOuterJoin`、`CapabilitySelectForUpdate`、`CapabilitySelectForShare`、
`CapabilitySelectForNoWait`、`CapabilitySelectForSkipLocked`。执行期不支持时返回
`*ErrUnsupportedCapability`，它带着能力名和方言名——错误信息里必须能看出"谁不支持什么"，
这比一句 "unsupported" 省掉一轮排查。

- `mysql.go`、`postgres.go`、`sqlite.go` 是全部实现。
- `ddl_reconcile_test.go` 覆盖运行期 schema 对账在三个方言上的行为。
- **新增能力位必须三个方言都显式表态**，否则默认值会让不支持的方言悄悄放行——
  那是运行到生产库上才会炸的一类错误。

## 追踪与错误

- `trace.go` 提供轻量的执行追踪钩子，不依赖任何外部 tracing 库。
- `sqlite_errors.go` 把 SQLite 的错误字符串映射成可判别的错误——这类映射按方言分文件放，
  不要塞进通用错误处理里。
- 乐观锁冲突是 `ErrOptimisticLockConflict`，它是**业务错误**，调用方必须处理。
