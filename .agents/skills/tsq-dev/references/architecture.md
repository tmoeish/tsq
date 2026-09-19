# 架构

TSQ 是一个被 import 的库加一个 CLI 生成器，没有服务、没有进程、没有后台循环。它的
"运行时"是别人的进程。这决定了这里的每一个设计取舍：**能在编译期挡住的错误，不留到
运行期**；能在 `Build()` 挡住的，不留到执行期。

## 三个交付物，一个版本号

| 交付物 | 位置 | 使用者怎么拿到 |
| --- | --- | --- |
| 库 | 仓库根包 `tsq` | `go get github.com/tmoeish/tsq/v5` |
| 生成器 CLI | `./cmd/tsq`（实现在 `internal/cmd`） | `go install .../cmd/tsq@vX.Y.Z` 或 GoReleaser 的二进制 |
| 示例 | `./examples` | 读源码、抄片段 |

三者共用 `internal/buildinfo` 里的版本号，见 `release.md`。

## 分层

```
cmd/tsq  ──► internal/cmd ──► internal/parser ──► internal/genmodel
                   │                                     ▲
                   └──────► internal/sqldialect ──► dialect ◄──┘
根包 tsq ──────────► internal/sqldialect ──► dialect
```

- `internal/genmodel` 是**中立的数据模型**：`StructInfo`、`FieldInfo`、`TableMeta`、
  `IndexInfo`、`SchemaColumn`。解析器往里填，生成器补上需要类型信息的 `Schema`，模板从里读。
- `internal/parser` 只负责 Go 源码 → `genmodel`（`directive.go` 解析 `//tsq:` 指令）。
- `internal/cmd` 只负责 `genmodel` → 磁盘：模板渲染、校验、DDL 推导与渲染、文件写入。
- 公开的 `dialect` 只有**名字和事实**：`Name`、能力表与 `Supports` / `Check`、
  `UnsupportedCapabilityError`、生成代码声明列用的 `ColumnSpec` 一族。没有接口可实现。
- 实现在 `internal/sqldialect`（`Dialect` 接口、三个方言、DDL 渲染、schema 探查），被库和生成器
  共用：运行期的方言能力和生成期的 DDL 类型映射说的是同一件事。它用类型别名拼写公开包的类型。
- 根包唯一 import 的 internal 包是 `internal/sqldialect`；生成的代码只依赖根包和 `dialect`。

## 根包的四块

```
表描述符  table.go lookup.go   TableOf[R, K] / NewTable / Define / As / CTE；Get / Find / Fetch / FetchBy / Query
表达式    column.go expr.go   Column / Condition / Case，内部是 exprInfo
          case.go param.go    Param / ListParam / Arg
中间表示  sqlexpr.go          sqlExpr（片段）→ renderer → statement（模板）→ assemble
查询      querybuilder.go     阶段接口 + builder（builder 也实现 Subquery[O]，外层 Build 时才建）
          query_render.go     querySpec：结构校验 + 按方言渲染
          query.go            Query：缓存、绑参、执行、Page、子查询
写入      rows.go             TableOf 上的行写入与批量写
          mutation.go         UpdateTable / DeleteFrom 按条件写
执行      executor.go         封闭的 Executor、WrapExecutor
          runtime*.go tx.go trace.go schema.go log.go errors.go
```

### 表描述符（`table.go`）

表是一个值：`*TableOf[R, K]`（K 是主键类型），持有名字、列、主键、自增、托管列、搜索列、物理
schema 和索引。**行类型 R 上不需要任何方法**——v5 之前表元数据是使用者结构体上的七个方法，会和
字段重名，还逼出了 `DeclareTable` 那个初始化顺序补丁。

生成代码是一个内嵌 `*TableOf` 的结构体，**每列一个字段**，由一个构造函数建成：

```go
type CourseTable struct {
	*tsq.TableOf[Course, int64]
	ID tsq.Column[Course, int64]
}

var TableCourse = newCourseTable() // 函数里：NewTable → 各列 → Define，返回结构体
```

任何列都只能经 `TableCourse.X` 取到，所以引用列的东西都依赖 `TableCourse` 这个变量，初始化
顺序由 Go 自己保证，没有要维护的声明形状。

- 生成的结构体靠内嵌满足 `Table`（未导出方法也会被提升），所以 `From(TableCourse)` 直接可用；
  `UpdateTable` / `DeleteFrom` 收 `RowTable[R]`，R 由 Go 1.21 起的"按方法推断类型实参"从结构体上
  推出来——`TableOf[R, K]` 的 K 因此不进 `UpdateBuilder`。
- 生成的 `As(alias)` / `WithDeleted()` 返回同一个结构体，列用 `WithTable` 改绑；`NullColumn` 的
  `WithTable` 返回的动态类型仍是 `NullColumn`，生成代码断言回去。
- 列字段名不能和 `*TableOf` 的方法、`TableOf`、生成的 `As` / `WithDeleted` / `GetByX` / `FetchByX`
  重名：`internal/cmd/reserved.go` 用反射取方法集（泛型方法反射看不见，单独列出），
  `TestReservedTableNamesCoverTableOf` 扫源码核对。**给 `TableOf` 加导出方法等于让某个列名从此非法**。
- 主键查询（`lookup.go`）：`Get` / `Find` / `Fetch` / `Query()` 各自懒建一条查询，按软删除作用域
  缓存两份（`tableKeys` 被 `As` / `WithDeleted` 的副本共享）；`FetchBy` 每次现建。按字符串取时，
  Go 里对不上的值再单行问一次数据库（排序规则可能判等），遇到第一个真不存在的就停。

- `Define` 做全部定义期校验（主键、托管列、搜索列必须是本表的列，schema 与索引引用的列存在，
  不能 Define 两次），错误记在表上，由每个用到它的查询和写入报告，`Err()` 可以直接读。
- 表的身份：`Define` 的"是不是本表的列"按**指针**比；查询里的表按**引用名**（别名或表名）比，
  所以 `As` 得到的是另一个名字。别名就是带 `alias` 字段的 `TableOf` 副本；按条件写的语句拒绝别名。
- `Table` 接口是封闭的：`*TableOf[R, K]`（及内嵌它的生成结构体）和 `cteTable`。接口方法叫
  `TableName()` 而不是 `Name()`，因为列字段常叫 `Name`。

### 表达式与中间表示（`column.go`、`expr.go`、`sqlexpr.go`）

**SQL 不再是字符串。** 每个列、条件、CASE 都是一个 `exprInfo`：一段 `sqlExpr` 加上它引用的表、
聚合/去重标记和延迟报告的构建错误。`sqlExpr` 是一串片段：

| 片段 | 渲染时 |
| --- | --- |
| text | 原样输出（包括使用者 `Pred` / `Exprf` 的格式文本） |
| ident | 由方言校验长度并加引号 |
| value | 绑定值，变成占位符 |
| param | 参数，执行时按绑定的值展开 |
| query | 子查询，就地渲染自己（同一个 renderer，所以它的参数和能力需求一并收集） |
| byDialect | 方言写法不同的构造（`Year()` 等），按方言挑一份，缺席即报错 |

`renderer` 为一个方言把片段走一遍，产出 `statement`：方言相关的文本加上**尚未编号**的占位符
槽。`assemble` 在执行时按绑定的参数值填槽（列表参数按个数展开，PostgreSQL 的 `$n` 在这里编号）。

这一层替掉的是 v4 的做法：标识符编码成 base64 标记塞进字符串，执行前再扫描文本替换
`?`、判断方言能力、处理 IN 列表。那套做法的直接后果是"字符串字面量里出现 `FOR UPDATE`
会被当成行锁拒绝"这一类 bug；现在能力需求由**渲染那个构造的代码**调用 `r.require(...)`
报告，使用者的原样文本从不被扫描。

- `Condition`、`AnySubquery`、`SQLColumn` 都是封闭接口，没有导出的 `Clause()` / `SQLExpr()`
  字符串——看 SQL 用 `Query.SQL(dialect.X, args...)`（没有 `String()`：按某个方言默认渲染会误导）。
- 列的核心是不可变的 `*columnCore`，派生列（函数、聚合、`Exprf`）复制一份再改；`plain`
  标记"直接引用 table.name"，只有它能 `WithTable` / `As` 换表。
- 方言写法不同的列函数（`Length`、`Round`、`Date`、`Year/Month/Day`）用 `sqlByDialect` 分叉；
  程序给出的整数（`Substring` 边界、`Round` 精度）直接写进文本，避免 PostgreSQL 为未知类型的
  参数选错重载。SQLite 上的日期函数先取时间文本的前 19 个字符（`sqliteTimeText`），因为
  modernc 默认按 Go 的 `String()` 格式存时间。三方言的实际返回值由集成测试
  `TestIntegrationColumnFunctionsArePortable` 核对。
- `DISTINCT` 只有两种形态：查询级的 `SelectDistinct` 和聚合里的 `CountDistinct`；列上没有
  `Distinct()`，因为它放在选择列表中间是非法 SQL。
- 模式匹配（`StartsWith*` / `EndsWith*` / `Contains*`、关键词搜索）一律转义通配符并写
  `ESCAPE '~'`：SQLite 没有默认转义符，MySQL 写不出 `ESCAPE '\'`。

### 参数（`param.go`）

执行期才知道的值是**参数**：`Param[T]` 是 `Operand[T]`，`ListParam[T]` 是 `ListOperand[T]`，
`Bind` 只收 `T` 并产出密封的 `Arg`。每一列自带一对参数（`col.Param()` / `col.ListParam()`），
复制和换表都共享同一个指针，所以 `col.Bind(v)` 总能找到它。

- 绑定按**参数身份**匹配，不按位置。缺值、多余的值（语句没用到）、同一参数绑两次都是错误。
  `Page` 的计数语句和列表语句合在一起判断"用没用到"。
- 派生规格（模式参数、`NOT IN` 形式）共享根参数的值，渲染方式不同。
- 空列表：`IN` 渲染 `IN (NULL)`，`NOT IN` 渲染 `NOT IN (SELECT 1 WHERE 1 = 0)`，都不丢过滤条件。
- 内置参数：`keywordParam`（`tsq.Keyword` 绑定它；`prepare` 据此决定是否渲染搜索谓词，空关键词被丢掉）、`deletedAtParam` / `updatedAtParam`
  （按条件软删除的时间戳，执行时计算）。

v4 的 `EQVar()` 往参数列表里塞标记，值从 `List(ctx, db, args ...any)` 按位置取——个数、类型、
顺序错了要么运行期报错、要么静默查错。这是"类型安全"里最大的洞，不要以任何形式加回来。

### 查询构建器：阶段接口

约束来自**接口的返回类型**。一个具体 `builder[O]` 实现所有公共方法；只有 `Where` / `Search`
在不同阶段返回不同接口，由 `joinBuilder` / `whereBuilder` / `searchBuilder` 三个薄包装提供。

```
Select ─► SelectStage ─From──┐
From   ─► FromStage   ─Select┴► JoinStage ─Join/Correlate─► JoinStage
JoinStage ─Where─► WhereStage ─Search─► FilteredStage
JoinStage ─Search► SearchStage ─Where─► FilteredStage
(Join/Where/Search/Filtered) ─GroupBy─► GroupedStage ─Having─► HavingStage
(Join/Where/Grouped/Having/Compound) ─Union...─► CompoundStage
(大多数阶段) ─OrderBy/Limit/Offset─► OrderedStage ─ForUpdate/ForShare─► LockedStage
```

- 分组、HAVING、集合操作之后**没有**行锁（PostgreSQL 拒绝，这里在类型上就拒绝）；带搜索的
  阶段没有集合操作（关键词搜索不能跨集合）。
- `builder` 里的 `stagePhase` 序号只挡住"把接口断言回来再调"的人，不是约束来源。
- `Build()` 调 `querySpec.validate`，只做**结构**校验：FROM/JOIN 图、`Correlate`（包括
  子查询透出的外层表必须在外层查询里）、集合操作列数、`Offset` 需要 `Limit`、CTE 环、
  以及所有表达式延迟下来的错误。

### 渲染与执行（`query_render.go`、`query.go`）

- `Query` 持有校验过的 `querySpec`，按 `(方言, 计数?, 关键词?, 单行?)` 缓存渲染结果；
  `Page` 的自定义排序不缓存。
- **方言能力在渲染时检查**，所以同一个 `*Query` 能在多个方言上复用，而不支持的方言在
  第一次执行时就拿到 `*dialect.UnsupportedCapabilityError`。这条"构建只看结构、执行才看方言"
  的边界是有意的。
- CTE 在渲染时从 FROM/JOIN（含集合操作数和 CTE 自身的来源）收集、按依赖排序后提到最前。
- `Get` / `Find` / `Exists` 渲染带 `LIMIT 1` 的变体（构建器自己设了 limit 时不加），
  所以 `LIMIT` 天然在行锁子句之前。
- `Page` 吃类型化的 `Paging`，拒绝自带 `Limit` 的查询，也拒绝"构建器有 `OrderBy` 且
  `Paging.OrderBy` 非空"。排序项经 `querySpec.orderTerm` 渲染：集合操作查询按输出列名排序
  （三个方言里唯一都接受的写法），普通查询按列表达式。`PageRequest`（HTTP 字符串形态）只在
  `PageRequest.Paging(sortable...)` 里按列名或 JSON 名解析成 `Paging`，排序白名单由调用方给。
- `Page` 的计数和列表语句经 `snapshotRead` 放进一个只读事务（PG/MySQL `REPEATABLE READ`，SQLite
  默认级别）：执行器已在事务里就直接用；`*Runtime` 走 `withTxResult`（追踪里是 `page` 套 `tx`）；
  `WrapExecutor` 包的句柄能 `BeginTx` 就自己开，否则退化为两条独立语句。
- `PageKeyset` 走 `renderMode.seek`：seek 条件并进 WHERE，`paged` 模式下有 seek 就不写 OFFSET，多取一行
  判断有没有下一页。`writeBody` / `writeSimple` / `writeFromWhere` 因此接收 `renderMode`（集合操作数和 CTE
  传零值）。
- `Iter` 和 `List` 共用 `each`（逐行扫描、回调返回 false 即停），`Iter` 的追踪区间覆盖整个循环。
- 排序项由 `querySpec.orderTerm` 生成，`orderTerm.render` 按可空性决定 NULL 的位置：默认"NULL 最小"，
  PG/SQLite 写 `NULLS FIRST/LAST`，MySQL 只在需要改变默认时加 `x IS NULL` 键（集合操作上不支持）。
  不可空的值不加任何东西。
- `SelectDistinct` 是 `querySpec.Distinct`，算作分组查询：`Count` 包一层子查询数去重后的行。

### 表达式与列（`column.go`）

`exprImpl[T]` 实现 `Expression[T]`（谓词、`Asc`/`Desc`、`Pred`、`Expr`/`Exprf`）；`columnImpl[O, T]`
嵌入它再加上 `boundTo(O)`、`WithTable` / `As` / `Param` / `Bind`。**派生表达式不带扫描目标**
（`derived` 清掉 `scan` 和 `nullable`）：它的值类型和源列字段无关，所以进不了 `Select`。
`MapInto` / `MapIntoNull` 给它一个字段，`SelectValue` / `SelectNullValue` 让值本身当行
（内部就是 `Select(MapInto(expr, 恒等访问器))`）。

### 可空性（`expr.go` 的 `nullness`、`query_render.go` 的 `canBeNull` / `checkScanTargets`）

类型上分两种表列：`Column[O, T]`（NOT NULL）和 `NullColumn[O, T]`（`T` 是非 NULL 时的值类型，扫描目标是
它的可空形态）。表达式另带运行期的 `nullness`：`always`（可空列、`NULLIF`、无 ELSE 的 CASE、标量子查询）、
`emptyGroup`（SUM/AVG/MAX/MIN，没有 GROUP BY 时为 NULL）、`tables`（这些表在外连接可选侧时为 NULL）。
`merge` 按"或"合并；`COUNT`、`COALESCE`、`NULLIF`、`CASE` 自己设置；CASE 的条件不参与。

可选表由 JOIN 决定：LEFT 的右表、RIGHT 之前的所有表、FULL 两边。`columnCore.nullable` 表示扫描目标能存
NULL（`NullColumn`、`MapIntoNull`）。`Build` 时算出 `Query.scanErr`，**读行的路径**（`each` / `get`）才
返回它——子查询和 CTE 不读行，不能在 `Build` 里拒绝。`SelectValue` 的投影不可空、`SelectNullValue` 的可空，
所以同一道检查也管住了它们。
CTE 的输出列可空时，`WithTable(cte)` 重绑的列标成 `always`。

### 软删除作用域（`query_render.go` 的 `writeFromWhere`、`table.go` 的 `liveRows` / `liveSource`）

声明了 `deleted_at` 的 `TableOf` 默认 `softDeleted()`；`WithDeleted()` 返回共享同一个
`tableDef`、`includeDeleted = true` 的副本，别名委托给底层表，CTE 永远是 false。渲染时：

- 查询里有 RIGHT / FULL JOIN：每张作用域表都渲染成 `(SELECT * FROM t WHERE 活行) AS t`。
  这时 WHERE 过滤会把被保留侧的 NULL 行滤掉，ON 过滤又挡不住被保留侧自己的已删行，只有
  派生表两头都对。
- 否则 FROM 表和 INNER / CROSS JOIN 表的条件并进 WHERE，LEFT JOIN 表的条件并进它的 ON
  （放进 WHERE 会把 LEFT JOIN 变成 INNER JOIN）。
- `UpdateTable` 和软 `DeleteFrom` 在 WHERE 后追加活行条件，所以二次软删除不会重盖墓碑；
  `HardDeleteFrom` 与 `DeleteFrom(t.WithDeleted())` 都是物理删除。
- 列按 `definition()` 与名字归属表，所以 `Users` 的列可以直接用在 `Users.WithDeleted()` 上。

### 写入（`rows.go`、`mutation.go`）

- **行写入住在表描述符上**：`TableOf.Insert/Update/Delete/HardDelete` 和 `Batch*`。生成的行
  方法只是转发。字段通过列的访问器取地址再反射取值，表的列清单决定写哪些列。
- **写入路径读值不走反射**：`columnCore.get` 由 `NewColumn` 的类型化访问器构成（`value(row, col)`），
  批量写每列每行绑一个值，这是反射成本最集中的地方；零值判断和盖时间戳仍用反射（每表几列，一次）。
  基准在 `write_bench_test.go`。
- **所有绑定值经 `bindValue`**（`assemble` 和 `writeStmt.arg` 两个出口）：时间转 UTC，含 `Valuer` 产出的时间；
  托管时间戳取 `stampTime()`（UTC）。新增绑定出口必须也走它。
- **托管列在库里维护，不在模板里**：`Insert` 只在未设置时填 `created_at` / `updated_at`
  （`isUnset`：零值或 `Valuer` 返回 nil），`Update` 总是刷新 `updated_at`，软删除写墓碑。
  `applyTimestamp` / `applyTombstone` 覆盖 `time.Time`、`*time.Time`，整数墓碑，以及实现
  `sql.Scanner` 的可空包装（`sql.NullTime`、`null.Time`，根包因此不 import nullbio）。
  `timestamps_test.go` 逐个类型守着。
- 软删除 / 恢复走 `setTombstone`：只写 `deleted_at`、`updated_at`、`version`，按主键（和版本）匹配，
  并要求行当前是活的 / 已删的。`Update` 跳过主键、`version`、`created_at`、`deleted_at`，软删除表上
  追加活行条件（`WithDeleted()` 不追加）。
- 批量写按占位符数分批（`effectiveChunkSize` × `sqldialect.MaxBindParams`）：INSERT 每行约一个
  占位符每列，UPDATE 约两个（`CASE pk WHEN ? THEN ?`）。单行 UPDATE 直接 `SET c = ?`。
- `WithSkipDuplicates` 逐行插入，事务内用同一个 savepoint 包住每一行（PostgreSQL 的失败语句
  会毒化整个事务），事务外不用（PostgreSQL 拒绝事务外的 SAVEPOINT）。事务与否由执行器的
  `execScope.tx` 说明。
- Upsert（`upsert.go`）走 `writeStmt`，方言分叉写在代码里：PG/SQLite `ON CONFLICT (目标列) DO UPDATE
  SET c = excluded.c`，MySQL `AS tsq_new ON DUPLICATE KEY UPDATE c = tsq_new.c`。单行时 PG/SQLite 用
  `RETURNING pk`，MySQL 用 `pk = LAST_INSERT_ID(pk)` 让更新也报出主键，随后按主键回读
  `version` / `created_at`。
- 按条件写：`UpdateTable(table)` / `DeleteFrom(table)` / `HardDeleteFrom(table)`。
  `Set` 是泛型方法，所以 `UpdateBuilder` 是导出的具体类型；`Where` 之后切到
  `MutationStage` 接口。语句只能引用目标表本身（按 `tableDef` 指针加表名判断，别名不行，`WithDeleted()` 行）。有 `version`
  的表追加 `version = version + 1` 但不校验版本（理由见 `memory.md`）。`DeleteFrom` 在有
  `deleted_at` 的表上渲染成 UPDATE，墓碑值**执行时**才算——v4 在构建时算，包级语句会
  永远盖进程启动的时间。

### 执行器（`executor.go`）

`Executor` 是 database/sql 的三个方法加一个未导出的 `scope()`，因此是**封闭的**：
`*Runtime`、`WithTx` 的回调执行器、`WrapExecutor(handle, dialect.Name)` 的结果。裸 `*sql.DB`
编译不过——库必须知道方言才能渲染，v4 允许传裸池，结果是运行期才发现方言未知。

`execScope` 带方言、所属 runtime（日志、追踪、分页上限从这里取）和是否在事务里。

## 运行时

`Runtime`（`runtime.go`）是 `*sql.DB` 加方言加已注册表：
`Open(ctx, driverName, dsn, tables, ...RuntimeOption)` 自己开池，
`NewRuntime(ctx, db, dialect.Name, tables, ...RuntimeOption)` 接管调用方已有的池。
对外只收方言**名字**，`sqldialect.For(name)` 取实现；`Runtime.Dialect()` 也只报名字。`tables` 是
`[]Table`（生成的 `TSQTables()`），`schema.go` 的 `registerTables` 从描述符取 schema 与索引。
没有全局 `Init()`，没有包级单例——不要以任何形式重新引入。

- **连接池的所有权记在 `ownsDB` 上，`Close()` 只关自己开的那个。**
- 选项是函数式的（`runtime_options.go`），先全部应用再统一校验。
- 标识符长度校验**没有开关**，在任何 DDL 之前跑；渲染时每个标识符也按方言再校验一次。
- 执行期日志走 `logForExecutor` / `logSQLForExecutor`（`log.go`），不要在执行路径
  里直接调 `slog.*`。SQL 日志的开关是 `WithSQLLogging()`；`WrapExecutor` 的结果没有 runtime，
  不打。
- `WithTx(ctx, fn, ...TxOption)`（`tx.go`）是多操作事务的唯一入口，选项 `WithIsolation` / `WithReadOnly` /
  `WithRetry` / `WithRetryPolicy`。
  commit 阶段只对明确的冲突码（`IsTxConflictError`）重试。
- 驱动错误分类按**接口**匹配：`sqlite_errors.go` 认 `Code() int`，`postgres_errors.go` 认
  `SQLState() string`。MySQL 是唯一被 import 的驱动，因为 `MySQLError.Number` 是字段。
- `runtime_schema.go`（表与列）和 `runtime_index.go`（索引）负责 schema 对账，`TablePolicy` / `IndexPolicy` 各取一档
  （`Manual` / `Validate` / `CreateMissing` / `Reconcile`）。**四档都只增不减**，理由见 `memory.md`。

## 方言

`internal/sqldialect` 下每个方言实现 `Dialect` 接口：标识符引用、占位符、DDL 类型映射、
DDL 语句、schema 探查，以及 `SupportsCapability(Capability)`。接口只收**各方言确实不同**的方法。
它曾经是公开包，文档写着"不是扩展点"却导出了十九个方法——每次内部调整都算破坏契约，
所以挪进了 internal，公开包只留名字、能力表和列描述。

- 能力位按**当前版本基线**表态，不探测服务器版本：MySQL 8.0（FULL JOIN 不支持）、
  SQLite 3.39+（行锁不支持）、PostgreSQL 全部支持。
- 能力表住在公开的 `dialect.capabilities`（每方言一张 `map[Capability]bool`），`Supports` 只查表、
  没有 `default` 分支；新增能力位要往 `allCapabilities` 和三张表各加一行，
  `TestEnginesCoverAllCapabilities` 守着。
- 绑定参数上限（`MaxBindParams`）：MySQL / PostgreSQL 65535，**SQLite 32766**。

## 测试矩阵

- **单元测试**只用 SQLite（`modernc.org/sqlite`，无 cgo）：`render_test.go`（三方言渲染）、
  `build_test.go`（绑参与结构校验）、`exec_test.go`（SQLite 端到端）、`compilefail_test.go`
  （一次编译、每行一个"必须编译失败"的用例）。
- 在 SQLite 里耗时的单 goroutine 测试在 `-race` 下跳过（`raceEnabled`）：转译的 SQLite 在竞态
  检测下慢约四十倍，而它们没有并发可查。
- **集成测试**（`internal/integration`，只用导出 API）在设置 `TSQ_MYSQL_DSN` /
  `TSQ_POSTGRES_DSN` 时对真实服务器跑，SQLite 目标始终参与。**这是 `internal/sqldialect/mysql.go` 与
  `internal/sqldialect/postgres.go` 唯一的自动化覆盖**，改它们必须看 CI `Integration` job 的结果。

## 追踪与错误

- `trace.go`：`Tracer` 是 `func(ctx, info TraceInfo, next) error`。每个入口带上自己的 `TraceInfo`：
  表的写入用 `TableOf.traceInfo`，查询用 `Query.traceInfo`（FROM 表的物理名），按条件写的语句按种类报
  `update` / `delete`。
  渲染后的 SQL 不在这里，归 `WithSQLLogging()`。
- 错误类型以 `Error` 结尾（`OptimisticLockError`、`MissingIndexError`……），字段导出，用
  `errors.AsType` 取。乐观锁冲突是**业务错误**，调用方必须处理。
