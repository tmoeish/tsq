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
`NewRuntimeContext(ctx, driverName, dsn, tables, opts...)`，`NewRuntime` 是它的
`context.Background()` 版本。ctx 约束 ping、标识符校验和 schema 策略（可能执行 DDL）。
`Close()` 关闭它自己打开的连接池。没有全局 `Init()`，没有包级单例——这是历史上被删掉的
东西，不要以任何形式重新引入。

- `Runtime` 自己实现 `SQLExecutor`，所以它可以直接传给需要执行器的地方。
- `RuntimeOptions.IdentifierValidationMode` 是类型化枚举，空值 = `Strict`，未知值被拒绝；
  `MaxPageSize` 是分页上限（`DefaultMaxPageSize` = 1000），通过 `pageSizeLimitForExecutor`
  从执行器反查运行时取到。
- 执行期日志走 `logForExecutor`（`runtime_schema.go`）：执行器属于某个运行时就用它的
  `Logger`，否则回退 `slog.Default()`。**不要在执行路径里直接调 `slog.*`。**
  这条规则曾经只在这里写着而没人执行：`logForExecutor` 引入之后只接了三个调用点，
  读路径的八处 SQL 日志和两处 rows.Close 告警一直在直调 `slog.*`。
  例外只有两处，都在拿不到执行器的地方，且都在代码里注明了理由：
  `quoteBuiltInIdentifier`（`Build()` 期，还没有 runtime）和 `appendTracers`
  （`NewRuntime` 正在组装 Runtime，`Logger` 还没落位）。
- SQL 文本与绑定参数由 `logSQLForExecutor`（`runtime_schema.go`）输出，开关是
  `RuntimeOptions.LogSQL`，级别 debug。两道短路（`logSQL` 为假、`Logger.Enabled` 为假）
  都在 `compactJSON` 之前，所以关着的时候不付序列化成本。**没有运行时的执行器
  （裸 `*sql.DB`、`WrapExecutor` 的结果）永远不打**——它没地方读这个开关。
- `WithTx`（`tx.go`）是多操作事务的唯一入口，支持 `TxOptions.Retry`（配合
  `IsOptimisticLockError` 做乐观锁重试）。commit 阶段只对明确的冲突码
  （`IsRetryableTransactionConflictError`）重试，网络类错误在 commit 阶段永不重试——
  commit 可能已经成功。
- 驱动错误分类按**接口**匹配，不 import 驱动包：`sqlite_errors.go` 认 `Code() int`，
  `postgres_errors.go` 认 `SQLState() string`（lib/pq、pgx v4、pgx v5 都实现）。MySQL 是
  唯一被 import 的驱动，因为 `MySQLError.Number` 是字段。
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
- 能力位按**当前版本基线**表态，不探测服务器版本：MySQL 8.0（CTE、INTERSECT、EXCEPT 都
  支持，FULL JOIN 不支持）、SQLite 3.39+（FULL JOIN 支持，行锁不支持）、PostgreSQL 全部
  支持。改基线是使用者可见变更。
- 每个方言还声明**单条语句的绑定参数上限**（`maxBindParams`，由 `MaxBindParams()` 查表）：
  MySQL / PostgreSQL 65535，**SQLite 32766**。分块写用它把行数换算成占位符数；方言未知时
  取最紧的那个。和能力位一样是一张表加一个遍历表的测试，不是一个写死的常数。
- `ddl_reconcile_test.go` 覆盖运行期 schema 对账在三个方言上的行为。
- **新增能力位必须三个方言都显式表态。** 这不再靠人记得：每个方言持一张
  `map[Capability]bool`（`mysqlCapabilities` / `postgresCapabilities` /
  `sqliteCapabilities`），`SupportsCapability` 只做查表（`capabilitySupport`），
  没有 `default` 分支。新增一个 `Capability` 常量就要往 `AllCapabilities()` 和三张表
  各加一行，漏掉哪张 `dialect/capability_test.go` 的
  `TestDialectsCoverAllCapabilities` 就会红。
  之前是带 `default: return false` 的 switch，漏掉一个方言不编译失败、不 lint 失败、
  不测试失败，只静默变成"不支持"。
- 能力名的规范化（`canonicalCapabilityName`）接受 `FULL JOIN` 这类别名，未登记的名字
  一律按"不支持"处理。**根包不要再复制一份规范化函数**：`dialect_validation.go` 里
  曾经有一份逐行副本（`canonicalDialectCapability`）加一个零调用的入口
  （`validateOperationForDialect`），只被自己的测试撑着。真正的执行期校验在
  `query_validation.go`，直接传类型化常量。

## 测试矩阵

- **单元测试**只用 SQLite（`modernc.org/sqlite`，无 cgo），`go test ./...` 本地零依赖。
- **集成测试**（根目录 `integration_test.go`，`package tsq_test`）在设置 `TSQ_MYSQL_DSN` /
  `TSQ_POSTGRES_DSN` 时对真实服务器跑：托管 schema 两次启动第二次零 DDL、reconcile 只改
  变化的列且收敛、乐观锁、重复键忽略、锁冲突分类（pgx v5 驱动）、能力位真实执行。
  SQLite 目标始终参与，所以套件本身每次 `go test` 都被执行。CI 的 `Integration` job
  起 MySQL 8.0 和 PostgreSQL 16 两个 service。**这是 `dialect/mysql.go` 与
  `dialect/postgres.go` 唯一的自动化覆盖**，改它们必须看这个 job 的结果。

## 追踪与错误

- `trace.go` 提供轻量的执行追踪钩子，不依赖任何外部 tracing 库。`Tracer` 是
  `RuntimeOptions.Tracers` 的元素类型，由使用者自己实现——**这里不再内置 tracer**。
  曾经有三个（`printCost` / `printError` / `printSQLTracer`），全部未导出、
  只被一个测试文件引用，使用者无从启用；SQL 日志现在归 `RuntimeOptions.LogSQL`。
- `sqlite_errors.go` 把 SQLite 的错误字符串映射成可判别的错误——这类映射按方言分文件放，
  不要塞进通用错误处理里。
- 乐观锁冲突是 `ErrOptimisticLockConflict`，它是**业务错误**，调用方必须处理。
