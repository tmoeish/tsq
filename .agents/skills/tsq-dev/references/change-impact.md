# 变更影响清单

**开始改之前，逐条扫一遍触发器，命中的那条整条做完。** 这里的每一条要么已经在真实事故里
被踩过，要么是 `make harness` 会拦下但拦得比这里晚得多的东西——在这里发现比在门禁上发现
省一轮返工。

有 `[门禁]` 标记的，`make harness` 会兜底；没有标记的只有这份清单兜着。

---

## 改了根包里任何导出的符号

- `make api-snapshot` 刷新 `references/api-surface.txt`。`[门禁: api-check]`
- 更新 `skills/tsq/`——使用者照着那份技能写代码，新增的 API 要出现在里面，删掉或改签名的
  要从里面消失。`[门禁: api-check 会提示]`
- 更新 `README.md` 和 `docs/` 里出现该符号的地方。
- 在 `CHANGELOG.md` 的 `## [未发布]` 段写一条人话。破坏性变更单独放 `### 破坏性变更`——
  `script/release.py` 靠这个小节名判断要不要跨主版本。
- 破坏性变更还意味着 v5：Go 的语义化导入版本要求改 go.mod 模块路径和全部内部 import。
  见 `release.md`，不要顺手就改。

## 想给根包加一个"兼容包装"或一个不用接收者的方法

- **不要加 `Deprecated` 别名。** v5 之前攒了九个，没有任何门禁会提醒它们该走；删除它们本身就是
  大版本的理由之一。要改名就改名，把旧名写进 `CHANGELOG.md` 的破坏性变更段。
- **方法不用接收者，就说明它不该是方法。** `ExistsSub` 曾经长在每个列上却从不读那个列，逼着
  调用方随便挑一列。判据可执行：新增列方法时 grep 一下方法体里有没有出现 `c.`。
- **参数类型必须能被使用者写出名字。** 未导出的接口做参数类型时，调用能编译，但没人能声明变量或
  写 helper。要么导出成密封接口（方法保持未导出），要么换成具体类型。`[门禁: api-check 会显示它]`

## 改了查询构建器的阶段（`querybuilder.go`）

- 约束来自**阶段接口的返回类型**。改返回类型或给接口加方法，约束会静悄悄松掉，测试不看类型
  就发现不了。`compilefail_test.go` 是"这些调用必须编译失败"的清单，新增阶段或放开一个转移
  都要在那里加用例（它一次编译、按行核对错误，加用例几乎不增加耗时）。
- 只有 `Where` / `Search` 在不同阶段返回不同接口，所以只有它们住在 `joinBuilder` /
  `whereBuilder` / `searchBuilder` 上；其余方法都在 `builder` 上。给 `builder` 加方法等于让
  **每个**嵌入它的包装类型都有了这个方法——要靠接口把它藏起来。
- `stagePhase` 只挡住"把接口断言回来"的调用，不是约束来源——别把类型约束改成运行期 if。
- 新阶段要问"它能从哪些阶段进入"，以及"SQL 允许它跟在什么后面"：分组和集合操作之后没有
  行锁（PostgreSQL 拒绝），带搜索的查询没有集合操作。
- ORDER BY / LIMIT 作用于整个查询，由 `writeTail` 在查询体**之外**、行锁**之前**写；查询体
  （`writeBody`）会被复用为集合操作数和 CTE 体。

## 改了全文检索

- PostgreSQL 的索引表达式和谓词表达式必须**逐字相同**，否则索引用不上：两边都走
  `internal/sqldialect` 的 `PostgresDialect.FullTextVectorSQL`，不要在谓词里另写一份。
- 全文索引只按名字对账（`ensureFullTextIndex`）：字段比较会因为三个方言的自省差异每次启动都想重建。
- SQLite 是**按子串匹配的退化实现**，语义和另两个不同。改 `Matches` 的渲染要同时想清楚三种行为，
  `TestIntegrationFullTextSearch` 只断言三者都同意的部分。

## 改了数据库填值的列（`Fill`、`default:` / `generated:`）

- 三条路径都要一致：插入的列清单（`insertColumns`，按行分组，因为"未设置"是逐行的）、`Update` /
  `Upsert` 的 SET 清单、单行写入后的回读（`reloadColumns`）。漏一处就会写进一个数据库该自己算的列。
- **生成列不参与 schema 对账**（`diffTableColumns` 里过滤）：SQLite 的 `table_info` 根本不列它，
  MySQL/PG 报的类型和默认值也和声明不同，比较的结果是每次启动都想改一次。
- 端到端的门是 `examples/academy` 的 `runDatabaseFilledDemo` 和 `TestIntegrationDatabaseFilledColumns`
  （后者还断言第二次启动零 DDL）。

## 改了删除语义或托管列（`rows.go`、`softdelete.go`、`TableSpec`）

- **删除语义由表的类型决定，不由调用点、也不由 scope 决定**：有 `deleted_at` 的表是
  `SoftDeleteTableOf`，它的 `Delete*` 恒写墓碑；`TableOf` 上根本没有 `Delete*`，只有恒 DELETE 的 `Hard*`。
  加一个软删除入口就放在 `SoftDeleteTableOf` 上、同时在 `TableOf` 上加它的 `Hard*` 对偶，并在
  `compilefail_test.go` 加一条"普通表上编译不过"。**不要让任何删除路径读 `softDeleted()` 来选软删还是
  硬删**：它只回答"要不要活行过滤"，一旦兼任，`WithDeleted()` 就会把删除变成物理删除（v5 发版前出过，
  见 `memory.md` § 软删除）。`TestWithDeletedOnlyDropsTheLiveRowFilter` 守着 `WithDeleted()` 只改可达的行。
- 生成器按 `DeletedAtField` 在两种表类型之间选（`table.go.tmpl` 的 `$base` / `$bind`），`reserved.go`
  按同一个判据取方法集；三处的判据必须是同一个字段。
- **软删除和恢复的状态不符报 `RowStateError` 而不是 `OptimisticLockError`**：重试不能解决它，`IsOptimisticLockError` 不该为真。
- **软删除和恢复只写托管列**（`setTombstone`），自带版本校验和自增；`Update` 永远不写 `created_at` /
  `deleted_at` 且只匹配活行。`softdelete_test.go` 用一张没有 `version` 的表守着"旧副本复活已删行"。
- **托管时间戳和墓碑在库里维护**（`applyTimestamp` / `applyTombstone` / `isUnset`）。新增一种
  字段形态要同时加进这三个函数、`timestamps_test.go` 的类型表、生成器的
  `validateTimestampField` / `validateSoftDeleteField`，以及 `skills/tsq` 的"Supported field
  types"。
- `Insert` 只在字段**未设置**时盖 `created_at` / `updated_at`（导入历史数据时不能丢调用方的
  时间），`Update` **总是**刷新 `updated_at`。
- 给 `TableSpec` 加字段不是破坏性变更；给 `Table` 接口加方法也不影响使用者（它是封闭的），但
  两个实现（`TableOf`，`SoftDeleteTableOf` 经内嵌自动跟上；`cteTable`）都要跟上。`[门禁: api-check]`
- 软删除的端到端门是 `examples/academy` 的 `runSoftDeleteDemo`。
- **软删除作用域在渲染里，不在调用点**：新增一种表出现的位置（新的 JOIN 类型、`UPDATE ... FROM`、
  新的集合形态）必须在 `writeFromWhere` 里表态它的作用域放 WHERE、ON 还是派生表，并在
  `TestSoftDeleteScope` 和 `TestIntegrationSoftDeleteScopeJoins` 里各加一条。给 `Table` 接口加实现
  也要实现 `softDeleted()`。

## 改了 upsert（`upsert.go`）

- **MySQL 的 `ON DUPLICATE KEY UPDATE` 匹配所有唯一键**，`checkUpsertRows` 因此在行可能撞上别的唯一键
  时拒绝。放宽它之前先想清楚：那一行会静默地更新一条和指定键无关的行。
- 三个方言的语句形状只有 `TestIntegrationUpsert` 能证明，包括"值没变时 MySQL 仍报出主键"。本地没有
  MySQL 时，SQLite 绿不代表什么：第一版的 `version = version + 1` 在 MySQL 上和行别名 `tsq_new` 的同名列
  冲突（1052 ambiguous），**引用已有行的列一律带表名**。
- 更新时的列清单（不写键、主键、`created_at`，`version` 自增）和 `UpdateTable` 的语义保持一致；
  改一边要看另一边。

## 改了按条件写语句（`mutation.go`）

- **`version` 自增不校验是契约**。去掉自增会让并发的乐观锁静默失效，加上校验会让它退化成
  逐行更新。`exec_test.go` 的 `TestConditionalWrites` 守着"之前加载的行随后冲突"。
- 读列值用 `value(row, col)`（走 `columnCore.get`），不要回到 `field(row, col).Interface()`：那是反射路径，
  批量写会按列×行付成本。`write_bench_test.go` 是量它的地方。
- **托管时间戳是执行时绑定的内置参数**（`deletedAtParam` / `updatedAtParam`；`UpdateTable` 在调用方没 `Set` 时也刷新 `updated_at`）。改成构建时
  求值，包级语句就会永远写进程启动时间——v4 就是这么错的。
- **语句形状要在三个方言上真跑**：SET 左侧不带表限定、WHERE 带表限定，靠
  `internal/integration` 的 `TestIntegrationMutationsByCondition` 证明。
- 只能引用目标表本身：`Build` 按 `tableDef` 指针加表名比较 `allTables()`，别名会被拒，
  `WithDeleted()` 视为同一张表。放开这一点要先为
  三个方言各设计一种 `UPDATE ... FROM` 写法。
- `Set` 是泛型方法，所以 `UpdateBuilder` 必须是具体类型；`Where` 之后才是接口。
- 使用者文档三处要同步：`skills/tsq/references/REFERENCE.md` §8 与 §13、`README.md`
  "常见边界"、`BEST_PRACTICES.md` §3.8。

## 改了相关子查询的作用域传递（`Correlate`、`validateJoinGraph`）

- 子查询以 `partQuery` 片段进入外层表达式，`exprInfo.allTables()` 会把它的 `Correlate` 表透给
  外层，于是外层查询必须提供这些表。新增一种嵌套形态（新的子查询位置）时，确认它的
  `correlatedTables()` 被透出——漏掉等于放行一个外层根本没有的表。
- 集合操作数继承外层的作用域（`validate(outer)`），CTE 体传 `nil`。
- **既 `Correlate` 又 join 同一张表必须继续是构建错误**：本地表遮蔽外层表，谓词不再相关，
  而 SQL 完全合法。
- 带 `Correlate` 的 `*Query` 在 `prepare` 里被拒绝单独执行。新增执行入口要走 `prepare`。
- 断言要落在**真跑一次数据库**上（`exec_test.go` 的相关 `EXISTS`）：相关版本和被遮蔽版本渲染
  出的 SQL 都合法。

## 改了单行读取（`Get` / `Find` / `Exists`）

- 它们渲染 `renderMode{single: true}`，由 `writeTail` 加 `LIMIT 1`——位置天然在行锁之前。
  构建器自己设了 `Limit` 时不再补。
- `Exists` 与 `Find` 共用这条语句，给单行读取加的边界会同时改变 `Exists`。

## 改了 `Page()` 或构建器级分页

- `Page` 拒绝自带 `Limit` 的查询，也拒绝"构建器有 `OrderBy` 且请求也要排序"。**不要改成
  "后者覆盖前者"**：猜调用方想要哪个比说不清更糟。
- 计数语句和列表语句**合在一起**判断参数是否被用到（`prepare` 的多模式），否则只出现在列表
  语句里的参数会被误报成"未使用"。
- 排序字段按列名或 JSON 名解析；集合操作查询只能按输出列名排序。
- **计数和列表必须在同一个快照里**（`snapshotRead`）。给 `Page` 加第三条语句也要放进去；不要为了
  省一次 BEGIN 把它拆开——`TestIntegrationPageReadsOneSnapshot` 会在两条语句之间插入一行。

## 改了可空性推导或加了新的表达式构造

- 新的函数 / 表达式要想清楚它的 `nullness`：默认 `merge` 是"任一操作数可空则可空"，这对大多数 SQL 函数
  成立；**不成立的要自己设**（`COUNT` 永不为 NULL、`COALESCE` 取与、聚合在无 GROUP BY 时为 NULL）。
  漏设的后果是读行前的检查放过了一个会扫描失败的查询，或者冤枉一个正确的查询。
- 新的 JOIN 种类要在 `optionalTables` 里表态哪边会被填 NULL。
- 可空性也决定排序怎么写（`orderTerm.render`），推导错了三个方言的顺序会不一致；`TestIntegrationNullOrderingAgrees` 守着。
- 检查放在读行路径而不是 `Build`：`Build` 拒绝会把合法的子查询 / CTE 一起拒掉。
- `nullable_test.go` 覆盖拒绝与放行两张表；`TestIntegrationNullableColumns` 真跑三方言。

## 改了 `ListIn` 或列表参数

- 分块只在"结果是各块并集"时成立：参数只用一次、是 `Where` 顶层的 `col.In(param)`、查询逐行过滤。
  `exprInfo.inList` 只由 `ListParam.setOperand`（非 NOT IN）设置，并且**故意不在 `merge` 里传递**——
  被 `And` / `Or` / `Not` 包住的 IN 不能拆。给 `exprInfo` 加字段时别顺手把它加进 `merge`。
- `TestListInSplitsListsBeyondTheBindLimit` 在 `-race` 下跳过（SQLite 绑 4 万个参数太慢），MySQL/PG
  的大列表由 `TestIntegrationUpsert` 覆盖。

## 改了 SQL 渲染、中间表示或参数绑定（`sqlexpr.go`、`param.go`、`query_render.go`）

- `render_test.go` 按三方言断言完整 SQL，改渲染必然改它；改之前确认新输出是**更对**而不只是
  **不一样**。
- **能力需求必须由渲染那个构造的代码报告**（`r.require(...)`）。不要回到"渲染完再扫文本"：
  使用者的原样文本（`Pred` / `Exprf`）会被误判，子查询也会漏报。
- 新的片段类型要同时处理：`renderer.write`、`sqlExpr.correlated`（若它能包含查询）、
  `debugSQL`。
- 占位符编号在 `assemble` 里做（`Placeholder` 是**零基**）；不要在渲染时编号，列表参数的长度
  要到执行时才知道。
- 绑定规则（缺值、多余、重复都报错）在 `bindArgs`，`build_test.go` 守着。放宽其中任何一条都会
  让"查询拿到了别的值"变成静默行为。
- 渲染缓存的键是 `(方言, 计数, 关键词, 单行)`。加新的渲染模式要么进键，要么像 `Page` 的排序
  一样不缓存。
- 新增或改了一个谓词、否定形式或空列表写法：加进 `internal/integration` 的
  `TestIntegrationPredicatesMatchTheSameRows`，按**匹配到的行**断言。只核对 SQL 文本证明不了引擎接受它
  （空 `NotIn` 的 `SELECT 1 WHERE 1 = 0` 不带 `FROM`，能不能跑是引擎说了算）。

## 改了 LIKE 谓词的渲染，或改了关键字转义

- **转义值和声明转义符必须一起出现。** `escapeLikePattern` 和 `likeEscapeClause` 是同一个契约
  的两半：只转义值而不发 `ESCAPE`，在 SQLite 上查询**静默返回零行**（SQLite 没有默认转义符）。
  模式参数、模式值、关键词搜索三处都这么写。
- 转义字符**不能是反斜杠**：MySQL 拼不出 `ESCAPE '\'`。
- 断言要落在**真跑一次数据库**上：`exec_test.go` 的 `TestPageSearchesSortsAndCounts` 守 SQLite，
  `internal/integration` 的 `TestIntegrationKeywordSearchEscapesWildcards` 守另外两个方言。

## 改了批量写（`rows.go`）

- 分批的单位是**行**，数据库数的是**占位符**：
  - **上限按方言**（`sqldialect.MaxBindParams`）：MySQL / PostgreSQL 65535，**SQLite 32766**。
  - **每行占位符数按操作算**：INSERT 每列一个；UPDATE 每列两个（`CASE pk WHEN ? THEN ?`）加
    WHERE 的一到两个；DELETE 每行一到两个。改了语句形状就要回来核对 `effectiveChunkSize` 的实参。
- **`WithSkipDuplicates` 的错误处理不可移植**：事务内必须用 savepoint 括住每一行（PostgreSQL
  一条语句失败就 aborted），事务外**不能**发 savepoint（`25P01`）。事务与否读 `execScope.tx`。
  别改成 `INSERT IGNORE` / `ON CONFLICT DO NOTHING`：前者在 MySQL 上吞掉所有错误，后者让
  `RETURNING` 无法按位置回填主键。`TestIntegrationBatchInsertIgnoresDuplicatesInsideTransaction`
  只有在真实 PostgreSQL 上才有意义。
- `batch_test.go` 的宽表用例是门：它真的写一张 200 列的表。UPDATE 的求值开销约是行数² × 列数，
  所以表做宽、行做少；它在 `-race` 下跳过（转译的 SQLite 慢约四十倍，且没有并发可查），
  普通 `test` 里照跑。

## 加了 Runtime 的构造器或选项

- **先决定连接池的所有权**：`Runtime.ownsDB` 决定 `Close()` 关不关它。新构造器如果接管调用方的池，
  `ownsDB` 必须是 false，否则 `Close()` 会打断调用方在 TSQ 之外的用途。正反两侧都要测。
  `[门禁: runtime_test.go 的 NewRuntime/NewRuntimeCloses 两组]`
- **新选项写成 `With*` 函数**，值只存进 `runtimeConfig`，校验统一放在 `newRuntimeConfig` 末尾——
  非法值只从构造器报一次。
- 选项加进 `skills/tsq` 的 Runtime 小节；它是使用者唯一能看到这份清单的地方。

## 改了 schema 托管（`runtime_schema.go`、`runtime_index.go`）

- **不要重新引入任何"删掉不再声明的对象"的策略。** v4 的 `SchemaPolicyManaged` 靠一张全库共享
  的记账表做这件事，两个共用数据库的服务因此互删对方的表连同数据。一个 runtime 只知道自己声明了
  什么，分不清"这张表不该存在了"和"这张表是别人的"。理由见 `memory.md`。
- **不要引入任何 TSQ 自己的记账表。** 一份全局状态被只知道局部真相的写入者覆盖，就是数据丢失。
- 加新策略档要想清楚它是不是仍然"只增不减"，并且三个方言都要在集成测试里跑。
- **不要把 DDL 包进事务**：MySQL 每条 DDL 都隐式提交，包起来只在 PG / SQLite 上成立，反而让人
  误以为它是原子的。
- 门：`runtime_schema_isolation_test.go`（SQLite）和 `internal/integration` 的
  `TestIntegrationSchemaPolicyNeverDropsUndeclaredTables`（三方言）。

## 给查询加了需要方言能力的构造

- 在渲染该构造的地方调用 `r.require(capability)`，不要在执行路径上另写检查。
- 新增 `Capability` 常量见下面"新增或改动方言能力位"。
- `render_test.go` 的 `TestDialectCapabilitiesAreCheckedWhenRendered` 同时守着"字面量里的
  关键词不算"。

## 改了校验逻辑

先确定它属于哪一边，这条边界是有意的（见 `architecture.md`）：

- **结构**校验（FROM/JOIN 图、`Correlate`、集合操作、表达式错误）→ `querySpec.validate`，在
  `Build()` 时跑；定义期的错误 → `TableOf.Define`。
- **方言相关**校验（能力位、标识符长度、按方言分叉的构造）→ 渲染时，在第一次执行时跑。

把方言校验提前到 `Build()` 会断掉"一个 `*Query` 在多个方言上复用"这个用法。

## 新增或改动方言能力位

- 公开的 `dialect/dialect.go` 加 `Capability` 常量，**三个方言（mysql / postgres / sqlite）都要
  显式表态**。漏掉一个，默认值会让不支持的方言悄悄放行——那是跑到生产库上才炸的一类错。
- 执行期不支持要返回 `*dialect.UnsupportedCapabilityError`（导出 `Capability`、`Dialect` 字段）；
  `capabilityHint` 里"去哪个方言跑"的提示要跟着改。
- `internal/integration` 的 `TestIntegrationCapabilitiesExecute` 对每个方言声明支持的
  能力真跑一遍——声明了但跑不通，CI 的 `Integration` job 会红。
- 更新 `skills/tsq` 里"哪条查询能在哪个库上跑"的说明和 `README.md` 的能力矩阵。
  `[门禁: skill-check dialect]`
- 能力位按版本基线表态（见 `architecture.md` § 方言），改基线要进 CHANGELOG 的 `### 变更`。

## 给 `Dialect` 接口加了钩子，或改了行写入（`rows.go`）

- 接口（`internal/sqldialect.Dialect`）里的钩子必须有调用方：
  `grep -rn '<钩子名>(' --include='*.go' . | grep -v internal/sqldialect/`
  必须命中根包。`ReturningClause` 曾经"有定义、有实现、零调用"六个版本，PostgreSQL 上
  `Insert` 从来没回填过主键。
- `ReturningClause(col)` 接**未加引号**的列名，方言自己加引号。
- 主键回填有两条路：`LastInsertId()` + `BatchInsertStartID`（MySQL / SQLite），和
  `INSERT ... RETURNING`（PostgreSQL）。改任何一条要看 `internal/integration` 的 CRUD 用例。

## 给 `Operand` / `ListOperand` / `Pattern` / `Executor` 加或改了未导出方法

- 这些方法名就是使用者看到的编译错误：Go 报**按字母序第一个**缺失的方法，类型参数不符时报签名不对的那个。
  `needsTsqVal` / `needsTsqVals` / `needsRuntimeOrWrapExecutor` 是只为报错存在的标记方法，必须排在同一接口
  其他未导出方法之前（新方法别起 `a…`–`m…` 开头的名字），承载类型的那个叫 `valueOfType(T)` / `valuesOfType(T)`。
- 每个实现（列、`Param`、`Value`、阶段、`*Query`、`*Runtime`、`boundExecutor`）都要实现标记方法。
  `[门禁: compilefail_test.go 的 "a literal names tsq.Val" 等用例]`

## 退役了一个使用者写过的名字或写法

- 把旧写法加进 `script/check_docs.py` 的 `RETIRED`：它扫使用者文档、示例 README、`CONTRIBUTING.md` 和
  生成器源码（CLI 帮助文本在那里）。`api-check` 和 `tsq.*` 符号检查只认符号，认不出散文和帮助里的旧词。
  `[门禁: doc-check 的 check_retired_vocabulary]`

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
  一直在直调 `slog.*`，规则在 `architecture.md` 里写了却没人执行。**"加了个统一出口"
  不等于"接完了"，接完的判据是那条 grep。**

## 新增了一个"开关 + 若干消费点"的特性

- 开关必须从**导出的** `With*` 选项一路接到消费点。中途任何一段不可达，
  那个特性在发布出去的库里就不存在，而源码看着像它能用。
- 判据同"给 `Dialect` 接口加了钩子"那条：**grep 一遍调用方**。只被 `_test.go` 引用的
  未导出符号是这类缺陷的典型形态——`unused` linter 看不见它（测试里的引用算使用），
  所以 grep 时要显式排除 `_test.go`。
- `printSQL` context key 加它的三个未导出 tracer 就是这样活了很久：八处
  `ctx.Value(printSQL)` 在库里永远为假，唯一能设置它的 `printSQLTracer` 没导出。

## 加了或改了 `Capability` 常量

- 公开 `dialect/dialect.go` 的 `allCapabilities` 加一行，`capabilities` 里**三张方言表各加一行**，
  true/false 都要显式写出来。`[门禁: dialect/dialect_test.go 的 TestEnginesCoverAllCapabilities]`
- `dialect.Supports` 只做查表，**不要再引入 `default` 分支**——那正是这道门要挡的东西。
  `internal/sqldialect` 的 `SupportsCapability` 只转发给它，不另存一份表。
- `displayCapability` 和 `capabilityHint` 也要加分支，否则错误信息里
  是原始的枚举串而不是使用者认得的 SQL 语法。
- 别名（`FULL JOIN` → `FULL_OUTER_JOIN` 之类）加进 `canonicalCapability`。
  **根包不要复制这个函数**：曾经有过一份逐行副本，只被自己的测试撑着。
- 其余按下面"新增或改动方言能力位"那条走完。

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

## 加了或改了 `-X` ldflags（`Makefile`、`.goreleaser.yaml`）

- 目标必须是 `github.com/tmoeish/tsq/v5/internal/buildinfo.<var>`，`<var>` 必须真的在
  `internal/buildinfo/buildinfo.go` 里声明。链接器对找不到的符号**静默忽略**，二进制会
  把 build time / commit / branch 报成 `unknown` 而没有任何报错。
  `[门禁: release-check 核对两份配置里的每个 -X；CI 的 Build 运行二进制核对值]`
- 三份配置是三个副本，改变量名要一起改。

## 改了注解指令（`internal/parser/directive.go`）

- 解析器接受或拒绝什么，就是使用者能写什么。`skills/tsq` 的注解说明必须同步。
  `[门禁: skill-check dsl]`
- **不要把指令写成跨行的形态。** 一行一个关注点是这套语法唯一的好处来源：gofmt 不碰它，错误可以
  直接引用那一行，于是既不需要格式化器也不需要把偏移量映射回行号。
- 改索引名推导（`normalizeIndexNames`）会让使用者已经建好的索引对不上。这是 schema 层面
  的破坏性变更，按破坏性变更处理。
- `make examples` 重新生成，`./bin/examples/full-suite` 跑一遍。`[门禁: gen-check]`
- 改完之后确认 `examples/academy/*.tsq.go` 的 diff 为空：指令和旧 DSL 表达同一件事时，生成物应当
  逐字节相同。那是语义等价最直接的证据。

## 改了模板（`internal/cmd/*.go.tmpl`）

- 模板决定生成代码长什么样，也就决定了使用者能调用哪些方法。**改模板等于改 API。**
- `make examples` 后看一眼 `examples/academy/*.tsq.go` 的 diff——那就是使用者会看到的变化。
- `skills/tsq` 里凡是提到生成方法名的地方都要同步。`[门禁: skill-check templates]`
- 模板里新用的辅助函数要加进 `template_funcs.go` 并配测试。
- **生成代码里出现的 `tsq.X` / `tsqdialect.X` 必须是那两个包真实导出的符号。** 模板和 helper 里
  的字符串不参与本包的类型检查，写错了要到使用者自己的工程里才炸；断言"发出了这个字符串"的
  单元测试证明不了这一点。`[门禁: internal/cmd/generated_symbols_test.go]`

## 改了生成的表声明（`table.go.tmpl`、`TableOf.Define`）

- 列只能是 `TableXxx` 的字段：**不要重新生成包级列变量**，也不要把建表挪进 `init()`。初始化顺序
  正确的唯一原因是"取列必经 `TableXxx`"。
- 给 `TableOf` 加导出方法，或给模板加生成方法：同名的列字段从此非法。普通方法反射自动覆盖；
  **泛型方法要加进 `reserved.go` 的 `genericTableMethods`**，生成方法加进 `reservedTableFields`。
  `[门禁: internal/cmd/reserved_test.go 的 TestReservedTableNamesCoverTableOf]`
- 生成的方法名、参数名进 `validateGeneratedSymbolCollisions` 的清单和 `gen_test.go` 的断言；
  改了形状要 `make examples` 并看 `examples/academy/*.tsq.go` 的 diff。

## 改了 DDL 推导（`internal/cmd/ddl_render.go`）

- 三个方言的 `.sql` 输出都会变，`tsq.json` 快照也会变。看 diff 确认是预期的。
- 自定义 codec 类型（`driver.Valuer` / `sql.Scanner`）推不出列类型，使用者必须写显式的
  `db:"...,type:..."`。改推导规则前先确认新规则不会让某类类型从"必须显式"变成"猜一个"——
  猜错的列类型在建表那一刻不报错，在写入超长数据那一刻才报错。
- `internal/sqldialect/ddl_reconcile_test.go` 覆盖运行期对账，生成期变了它可能跟着变。

## 改了生成文件的命名或文件头

- `internal/parser/constants.go` 的 `TSQFileSuffix` 是唯一来源。
- 使用者的 `.gitignore`、Makefile glob 和 CI 都写死了这个后缀。这是破坏性变更，
  要写迁移指引（v4.3.0 那次的写法可以抄）。
- `script/changeset.py` 的 `GENERATED_SUFFIXES` 和 `check_release.py` 的
  `GENERATED_HEADER` 也认这个格式。

## 改了 `internal/buildinfo` 的版本号

- **必须 `make examples`**：生成文件头和 `tsq.json` 都印着版本号。
  `[门禁: gen-check、release-check]`
- 正常情况下不要手改：`make release` 会替你改，并保证四个副本一致。

## 改了生成器的构建方式（`make build` / `make build-gen`）

`bin/tsq-gen` **故意不带 `$(LDFLAGS)`**，这样生成结果只依赖源码。给它加上版本 ldflags
会让发版死锁（生成文件头要写还不存在的 tag），也会让同一份源码在脏工作区和干净检出上
生成出不同的文件。`make examples` 和 `script/check_generated.py` 必须用同一个二进制。

## 改了 examples/

- `examples/academy/mock.sql` 是手写的 schema 真相源，示例结构体改了它要跟着改。
- `make examples` 重新生成，`./bin/examples/full-suite` 必须能跑通。
- `skills/tsq` 和 `docs/` 里的代码片段是从示例抄的，示例变了片段要跟着变。
  `[门禁: skill-check examples]`
- 三个示例程序各有 `main_test.go`，别只改 `main.go`。

## 改了面向使用者的文档（README、`docs/`、`skills/tsq`）

- **实质内容只有一个归宿**：`skills/tsq/references/` 是"怎么用这个库"的唯一来源，
  `docs/` 只做索引指过去。`docs/concepts.md` 和 `docs/quickstart.md` 曾各自把同样的内容
  重写了一遍，两份必然漂移，而漂移之后更糟的是看起来还对的那份。
- 语言按**读者**划：`skills/tsq` 随发布装进别人的项目，必须英文；README、`docs/`、
  `CHANGELOG.md`、`CONTRIBUTING.md` 面向本项目读者，中文。
  `[门禁: doc-check 的 check_shipped_skill_language]`
- 这条规则在 `AGENTS.md` 里写反了好几个月（要求 README 和 `docs/` 英文，而它们一直是
  中文），没有任何东西发现过。**没有门的规则不是规则**——改语言规则就要同时改那道门。

## 改了 harness（`script/`、`Makefile`、CI）

- 门禁的顺序、跳过条件写在 `AGENTS.md` § 验证与交接，那是唯一权威处，技能只引用不复述。
- `SKILL.md` 的命令表和 `release.md` 的流程描述要同步。`[门禁: skill-check harness]`
- 新增门禁要想清楚它在发版波（只改版本号和生成物）里会不会误报——
  `check_change_log.py` 的 `RELEASE_ONLY_FILES` 就是为此存在的。
- `.github/workflows/go.yml` 和本地 `make` 目标是两条独立的真相。改了本地目标名，
  CI 里引用它的地方要一起改（v4.4.1 那次 CI 调了一个不存在的 `make update-examples`）。

## 想往 `make fmt` 里加自动改写工具

`make fmt` 里每一步都在改写源码（`go fix`、`golangci-lint fmt`、`run --fix`），末尾的
`go build ./...` 是守卫：**格式化绝不能交回一棵编不过的树。** 加任何改写工具都必须能过它，
并且要先证明它是幂等的。

排查"某个工具改坏了我的文件"之前，先确认自己是不是唯一的写入者，并在 `git archive HEAD`
出来的副本里复现——本仓有过一次把并发 agent 的编辑误判成 `go fix` bug 的教训，见
`memory.md` 2026-08-21 那条。

## 改了根包导出符号的名字，或在使用者文档里引用了 `tsq.X`

`make doc-check` 把 `README.md`、`docs/`、`skills/tsq/` 里每个 `tsq.X`（围栏块和行内
反引号都算）对照 `api-surface.txt` 的根包段落。改名先 `make api-snapshot`，再改文档，
否则门会把新名字当成不存在。`[门禁: doc-check]`

## 在非测试 Go 源码里写了中文

不行：注释、Go doc、错误文案都是使用者读的。`make doc-check` 扫 `git ls-files` 里全部
非测试、非生成、非 `examples/` 的 `.go` 文件。`[门禁: doc-check]`

## 改名或删除了一个 make 目标

`grep -rn 'make <旧名>' --include='*.md' .` 一遍。文档里的命令是给人复制粘贴的，改名之后
它们会让照做的人得到 `No rule to make target`，然后开始怀疑自己的环境。`make doc-check`
守着围栏代码块里的引用；散文里的历史提及（`memory.md`、`CHANGELOG.md` 讲事故经过时）
有意不管。`.github/workflows/` 不是 Markdown，那道门管不到，要单独 grep。

## 改了 CI 的 job 名字

`main` 的 ruleset 按**检查名**要求 `Lint`、`Coverage`、`Build`、
`GoReleaser Check`、`Integration` 全绿。改掉其中任何一个 job 的 `name:`，那个必需检查就再也不会出现在
PR 上，而"等不到的检查"等于**所有 PR 永久合不进去**，包括发版 PR。

改 job 名必须同步 ruleset：

```bash
gh api repos/tmoeish/tsq/rulesets --jq '.[] | "\(.id) \(.name)"'
gh api repos/tmoeish/tsq/rulesets/<id> --jq '.rules[] | select(.type=="required_status_checks")'
```

同理，**不要把 matrix job 加进必需检查**：`Test` 的检查名是
`Test (ubuntu-latest, 1.27.0)`，升 Go 版本就会变成另一个名字。

## 改了 CI 里安装的工具，或它的版本

- **一律钉死版本，不要 `@latest`。** CI 用 `GOTOOLCHAIN=local` 钉着 `GO_VERSION`，工具一旦发布
  要求更新 Go 的版本，`go install ...@latest` 当天就装不上，于是**每个 PR 都红**且与改动无关。
  gosec、govulncheck、golangci-lint、goreleaser 各有自己的版本变量。
- **`GoReleaser Check` 和 `Release` 必须用同一个 goreleaser**：前者跑在 PR 上，后者只在 tag
  推送**之后**跑。版本不同时，前者证明不了后者会成功，而那一步不可撤销。
  `[门禁: release-check]`
- 换版本前先在本地按 CI 的方式装一次（`GOTOOLCHAIN=local go install ...@<版本>`），确认它
  能在当前 `GO_VERSION` 下装上。

## 升级 Go 版本

`go.mod`、`.github/workflows/go.yml` 的 `GO_VERSION` 与 matrix、
`CLAUDE.md` / `AGENTS.md` 里写的版本号，全部一起改。golangci-lint 也要升到兼容版本
（`Makefile` 的 `LINT_BIN` 那行钉死了版本）。

matrix 一改，`Test` 的检查名就跟着变——所以 ruleset 的必需检查里没有它，见上一条。

## 加了新的 Go 源文件

- 根包新文件 → `feature-map.md` 要能把人带到它。`[门禁: skill-check library]`
- 有导出符号 → `make api-snapshot`。`[门禁: api-check]`
- 配套的 `_test.go` 文件名要么对应一个特性，要么对应被测文件，没有第三种。
