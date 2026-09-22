# 变更影响 — 运行时、方言与驱动

处理你匹配的每个触发器；索引与 `[门禁]` 标记的含义在 `../change-impact.md`。

## 加了 Runtime 的构造器或选项

- **先决定连接池的所有权**：`Runtime.ownsDB` 决定 `Close()` 关不关它。新构造器如果接管调用方的池，
  `ownsDB` 必须是 false，否则 `Close()` 会打断调用方在 TSQ 之外的用途。正反两侧都要测。
  `[门禁: runtime_test.go 的 NewRuntime/NewRuntimeCloses 两组]`
- **新选项写成 `With*` 函数**，值只存进 `runtimeConfig`，校验统一放在 `newRuntimeConfig` 末尾——
  非法值只从构造器报一次。
- 选项加进 `skills/tsq` 的 Runtime 小节；它是使用者唯一能看到这份清单的地方。

## 改了 schema 托管（`runtime_schema.go`、`runtime_index.go`）

- **不要重新引入任何"删掉不再声明的表或索引"的策略。** v4 的 `SchemaPolicyManaged` 靠一张全库共享
  的记账表做这件事，两个共用数据库的服务因此互删对方的表连同数据。一个 runtime 只知道自己声明了
  什么，分不清"这张表不该存在了"和"这张表是别人的"。**列不在此列**：`Reconcile` 删不再声明的列是
  有意的，`TestReconcileDropsUndeclaredColumns` 钉着。理由见 `../memory/dialect.md`。
- 索引定义只有一种比较：`sqldialect.ValidateIndex`（运行期策略和 `EnsureIndex` 共用），别在根包再写一份——
  上一份副本 `upsertIndex` 没人调用，却已经和真实路径漂移。"applied ddl" 只在语句成功之后记（重建在提交之后）。
- **SQLite 的重建**（`rebuildTable`，`AlterMode() == AlterRebuild` 时改列类型走这里）从声明的列建新表，
  所以旧表 CREATE TABLE 里声明之外的东西（UNIQUE / CHECK / 外键）、以及别处引用这张表的视图、触发器、
  外键都会丢或悬空：`InspectRebuild` 把它们列成 `Blockers`，有就拒绝重建。表自己的索引和触发器按
  `sqlite_master.sql` **原样**重建——别改回"按名字和列重建索引"，那会丢表达式、部分索引的 `WHERE` 和
  排序规则。门是 `TestReconcileRebuildKeepsIndexesAndTriggersAsCreated` 和
  `TestReconcileRefusesARebuildThatWouldLoseSomething`。它是唯一包进事务的 DDL：一连串语句必须落在同一个
  连接上。
- 自省读回来的**可空**列（SQLite 表达式索引的列名是 NULL，PG 的 `attname` 在 LEFT JOIN 下为 NULL）
  扫进 `sql.NullString`，三个方言对表达式列的表示要一致：不出现在 `Index.Fields` 里。
- **不要引入任何 TSQ 自己的记账表。** 一份全局状态被只知道局部真相的写入者覆盖，就是数据丢失。
- 加新策略档要想清楚它是不是仍然"从不删表"，并且三个方言都要在集成测试里跑。
- **不要把 DDL 包进事务**：MySQL 每条 DDL 都隐式提交，包起来只在 PG / SQLite 上成立，反而让人
  误以为它是原子的。
- 门：`runtime_schema_isolation_test.go`（SQLite）和 `internal/integration` 的
  `TestIntegrationSchemaPolicyNeverDropsUndeclaredTables`（三方言）。

## 给查询加了需要方言能力的构造

- 在渲染该构造的地方调用 `r.require(capability)`，不要在执行路径上另写检查。
- 新增 `Capability` 常量见下面"新增或改动方言能力位"。
- `render_test.go` 的 `TestDialectCapabilitiesAreCheckedWhenRendered` 同时守着"字面量里的
  关键词不算"。

## 新增或改动方言能力位

- 公开的 `dialect/dialect.go` 加 `Capability` 常量，**三个方言（mysql / postgres / sqlite）都要
  显式表态**。漏掉一个，默认值会让不支持的方言悄悄放行——那是跑到生产库上才炸的一类错。
- 执行期不支持要返回 `*dialect.UnsupportedCapabilityError`（导出 `Capability`、`Dialect` 字段）；
  `capabilityHint` 里"去哪个方言跑"的提示要跟着改。
- `internal/integration` 的 `TestIntegrationCapabilitiesExecute` 对每个方言声明支持的
  能力真跑一遍——声明了但跑不通，CI 的 `Integration` job 会红。
- 更新 `skills/tsq` 里"哪条查询能在哪个库上跑"的说明和 `README.md` 的能力矩阵。
  `[门禁: skill-check dialect]`
- 能力位按版本基线表态（见 `../architecture.md` § 方言），改基线要进 CHANGELOG 的 `### 变更`。

## 给 `Dialect` 接口加了钩子，或改了行写入（`rows.go`）

- 接口（`internal/sqldialect.Dialect`）里的钩子必须有调用方：
  `grep -rn '<钩子名>(' --include='*.go' . | grep -v internal/sqldialect/`
  必须命中根包。`ReturningClause` 曾经"有定义、有实现、零调用"六个版本，PostgreSQL 上
  `Insert` 从来没回填过主键。
- `ReturningClause(col)` 接**未加引号**的列名，方言自己加引号。
- 主键回填有两条路：`LastInsertId()` + `BatchInsertStartID`（MySQL / SQLite），和
  `INSERT ... RETURNING`（PostgreSQL）。改任何一条要看 `internal/integration` 的 CRUD 用例。

## 在执行路径上加了一个日志或诊断出口

- 必须走 `logForExecutor` / `logSQLForExecutor`（`log.go`），**不要直接调
  `slog.*`**。使用者配了 `WithLogger` 就是要所有执行期输出都进那个 Logger，
  少接一处等于那一处对他不存在。
- 加完 grep 一遍确认没漏（**只扫根包和 `internal/sqldialect/`**——`internal/parser` 是生成器，
  跑在 `tsq` CLI 里，那儿根本没有 runtime，用 `slog` 是对的）：

  ```bash
  grep -n 'slog\.\(Info\|Warn\|Error\|Debug\)' *.go internal/sqldialect/*.go | grep -v _test
  ```

  **应该一条都不命中。** 确实拿不到执行器的地方（`Build()` 期、Runtime 还没组装完）
  写成 `slog.Default().Warn(...)` 并在旁边注明理由——它不匹配上面这条 grep，所以
  "无意中直调"和"有意的例外"在形式上就分得开。当前唯一的例外是 `trace.go` 的 `appendTracers`（Runtime 还没组装完）。
- `logForExecutor` 引入时只接了三个调用点，读路径八处 SQL 日志和两处 rows.Close 告警
  一直在直调 `slog.*`，规则在 `../architecture.md` 里写了却没人执行。**"加了个统一出口"
  不等于"接完了"，接完的判据是那条 grep。**

## 加了或改了 `Capability` 常量

- `dialect/dialect_test.go` 的 `allCapabilities` 加一行，`dialect/dialect.go` 的 `capabilities` 里**三张方言表各加一行**，
  true/false 都要显式写出来。`[门禁: dialect/dialect_test.go 的 TestEnginesCoverAllCapabilities]`
- `dialect.Supports` 只做查表，**不要再引入 `default` 分支**——那正是这道门要挡的东西。
  `internal/sqldialect` 的 `SupportsCapability` 只转发给它，不另存一份表。
- `displayCapability` 和 `capabilityHint` 也要加分支，否则错误信息里
  是原始的枚举串而不是使用者认得的 SQL 语法。
- 别名（`FULL JOIN` → `FULL_OUTER_JOIN` 之类）加进 `canonicalCapability`。
  **根包不要复制这个函数**：曾经有过一份逐行副本，只被自己的测试撑着。
- 其余按上面"新增或改动方言能力位"那条走完。

## 改了驱动错误分类（`*_errors.go`、`IsRetryable*`）

- 两个 SQLite 驱动都要认：modernc 有 `Code() int` 方法，mattn 把码放在 `Error` 结构体字段里
  （`mattnSQLiteErrorCode` 按类型名和包路径反射读）。只认接口时，mattn 上的重复键和 busy 全部静默漏掉，
  `internal/integration` 里带 `cgo` 标签的 `TestSQLite3DriverIsSupported` 守着这条。

- 按接口匹配（`SQLState()` / `Code()`），**不要 `errors.AsType` 某个驱动的具体类型**：
  同一个 SQLSTATE 在 pq、pgx v4、pgx v5 里是三个 Go 类型，只认一个就静默漏掉另外两个
  （2026-08-26 之前 pgx v5 就是这样漏的）。MySQL 是唯一例外：`*mysql.MySQLError` 没有可匹配的方法，
  `mysql_errors.go` 按类型名和包路径反射读 `Number`，**根包不许 import 任何驱动**（会进使用者的
  `go.mod`）。`TestMySQLErrorsAreClassifiedWithoutImportingTheDriver` 用真实类型守着这段反射。
- **根包的测试也不许 import 驱动或 nullbio**：`go mod tidy` 会把依赖包测试的依赖记进使用者的
  `go.sum`。需要真实驱动的测试放 `internal/integration`；根包测试只允许 SQLite。
- `internal/integration` 的 `TestIntegrationLockConflictsAreRetryable` 用真实驱动验证。
