# 变更影响 — 运行时、方言与驱动

处理你匹配的每个触发器；索引与 `[门禁]` 标记的含义在 `../change-impact.md`。

## 加了 Runtime 的构造器或选项

- MySQL DSN 的参数按驱动的规则读（`mysqlDSNParam`：同名取最后一个、做 URL 解码；`parseTime` 认 `1`/`true`/`TRUE`/`True`）；
  根包不 import 驱动（`TestRootPackageImportsNoDriver`），所以自己解析查询串，别退回子串匹配。`Open` 还拒绝 `loc` 不是 `UTC` 的 DSN：
  驱动按 `loc` 写入和解读 `DATETIME`，会让 TSQ 的 UTC 时间按本地落库、把数据库填的 UTC 时间读偏。绑定出口分方言（`bindValueFor`）：
  MySQL 驱动把零值时间写成 `0000-00-00`，这里按公元 1 年绑定。

- **先决定连接池的所有权**：`Runtime.ownsDB` 决定 `Close()` 关不关它。新构造器如果接管调用方的池，
  `ownsDB` 必须是 false，否则 `Close()` 会打断调用方在 TSQ 之外的用途。正反两侧都要测。
  `[门禁: runtime_test.go 的 NewRuntime/NewRuntimeCloses 两组]`
- **新选项写成 `With*` 函数**，值只存进 `runtimeConfig`，校验统一放在 `newRuntimeConfig` 末尾——
  非法值只从构造器报一次。
- 选项加进 `skills/tsq` 的 Runtime 小节；它是使用者唯一能看到这份清单的地方。

## 改了 schema 托管（`runtime_schema.go`、`runtime_index.go`）

- **不要重新引入任何"删掉不再声明的表"的策略，也不要删别人的索引。** v4 的 `SchemaPolicyManaged` 靠一张全库共享
  的记账表做这件事，两个共用数据库的服务因此互删对方的表连同数据。一个 runtime 只知道自己声明了
  什么，分不清"这张表不该存在了"和"这张表是别人的"。**已声明的表之内不在此列**，而且只归 `Reconcile`：它删不再声明的列
  （`TestReconcileDropsUndeclaredColumns`），也删这张表上名字是 TSQ 推导形态（`derivedIndexName`：`ux_<表>_…` / `idx_<表>_…` /
  `ft_<表>_…`）却不再声明的索引（`dropUndeclaredIndexes`，门是 `TestIntegrationReconcileDropsTheIndexesItNamed`，它同时钉着
  "别的名字不碰、`CreateMissing` 不删"）。放宽这个名字判据之前先想清楚：手建索引和别的服务建的索引只靠名字和它区分。理由见 `../memory/dialect.md`。
- 全文索引按名字比，报得出列的方言（MySQL）再比列：`MATCH` 要求索引正好覆盖它点名的列，留着旧列的索引让每次检索报 1191
  （`TestIntegrationFullTextIndexFollowsItsColumns`）。PG 索引的是表达式、SQLite 没有，仍只比名字。
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
- **策略档之间的分界按"改不改已有的东西"划**：`CreateMissing` 只加（表、列、索引），已有的列不一样仍然拒绝启动，
  改列和删列是 `Reconcile` 的。`CreateMissing` 曾把"缺列"和"列不一样"一起拒绝，而三处文档都说它会加列。
- **自省比较按引擎的拼法归一**：SQLite 的名字不分大小写（`sqlite_master` 查询要 `COLLATE NOCASE`，Go 里比名字用
  `EqualFold`），默认值比较是 `sqldialect.SameDefault`，**声明侧传 `DefaultSQL` 的拼法**（剥括号；当前时间要 UTC 属性一致，旧库的本地
  `CURRENT_TIMESTAMP` 因此算差异），MySQL 读回的默认值是它自己的规范形式（按数值比；表达式默认值由 `mysqlDefault` 去掉
  字符集前缀和转义，只去字面量外的——正则版把 `'en_US'` 截成 `'en'`，修了两次，测试里要有带下划线的值），SQLite 的列类型按亲和性比（`SameColumnType` 用 `SQLiteAffinity`，生成器的迁移用同一个函数，
  主键除外）。漏一处就是"每次启动都改一次"。academy 的 `Course.Blurb`（`TEXT` 带默认值）让集成测试在 MySQL 上守着前者。
- SQLite 重建表要把 `sqlite_sequence` 的计数带过去（`renderRebuildTableStatements`），否则 AUTOINCREMENT 会重发
  已删行的主键。
- 门：`runtime_schema_isolation_test.go`（SQLite）和 `internal/integration` 的
  `TestIntegrationSchemaPolicyNeverDropsUndeclaredTables`（三方言）。
- `diffTableColumns` 在 SQLite / MySQL 上不分大小写地比列名（PostgreSQL 的带引号名字区分大小写）；按大小写比会把
  `Name` 对 `name` 读成先删后加，删列在前、数据随之丢失。
- **加列和收紧列在运行期与生成器走同一份规则**（`sqldialect.AddColumnSQL` / `AddNeedsRebuild` / `NullFill` / `ZeroLiteral`）：新的无默认值
  NOT NULL 列在 PG / MySQL 带零值默认加列再 `DROP DEFAULT`，SQLite 没有 `DROP DEFAULT`、也加不了非常量默认值，所以 `CreateMissing` 和
  `Reconcile` 都改走 `rebuildTable`；重建复制列（`rebuildCopyColumns`）跳过生成列、给新列填零值、给变成 NOT NULL 的列写 `COALESCE`。
  改其中一条路径的规则，另一条要一起改，门是 `internal/integration/schema_test.go` 的 `TestIntegrationPoliciesFollowAStructOverATableWithRows`
  （三方言、每种列类型、两档策略，表里有数据）。只在空表上测的加列证明不了什么：PG 拒绝的正是有数据的表。
- **类型比较先过 `storageType`**（PG 上无符号自增主键存成 SERIAL 系列，`uint64` 是 BIGINT），原始类型再过分方言的别名表
  （`normalizeDDLNativeTypeName(dialect, ...)`：`TIMESTAMP(3)` 对 `timestamp(3) without time zone`、MySQL 的 `INT(11)` 对 `int`）。
  改 `ColumnTypeSQL` / `AutoIncrementColumnSQL` 的拼写要回来看这两处，门是 `TestIntegrationUnsignedAutoIncrementKeysAreStable` 和
  `TestIntegrationDeclaredColumnsDoNotDrift`——后者的用例表就是"引擎用自己的拼法报回来"的清单，新支持一种拼写就加一行。
- SQLite 的探查（`ListIndexes`、`InspectRebuild`）**先读完、关掉结果集再发下一条查询**：使用者常把 SQLite 池设成一个连接，
  开着结果集再查会永远等下去（`TestSchemaPoliciesRunOnOneConnection`）。新增探查照此写。
- `Reconcile` 在 SQLite 上删列前先删覆盖它的索引（`dropIndexesOfDroppedColumns`）；索引列名比较不区分大小写（`ValidateIndex`、
  重建时的 `renderRebuildObjectStatements`）。`Validate` 的列差异是 `*SchemaMismatchError`。

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
- 常量名跟构建器方法走（`CapabilityFullJoin` 对 `FullJoin`、`CapabilityNoWait` 对 `NoWait`），**值就是错误里给使用者看的
  SQL 拼写**（`"FULL JOIN"`）。没有别名表：曾有的 `canonicalCapability` 让 `Supports("full join")` 也能查，
  等于接受字符串形态的能力名，已删除。`capabilityHint` 要加分支。
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
- **同一种情况在三个方言上要一起表态**：PG 的 55P03（`NOWAIT` 拿不到锁）被重试而 MySQL 的 3572 没有，是只看一个
  方言补码的结果。新增一个错误码时，把另两个方言的对应码一起查出来。
