# 变更影响 — 查询构建、渲染与校验

处理你匹配的每个触发器；索引与 `[门禁]` 标记的含义在 `../change-impact.md`。

## 改了查询构建器的阶段（`querybuilder.go`）

- 约束来自**阶段接口的返回类型**。改返回类型或给接口加方法，约束会静悄悄松掉，测试不看类型
  就发现不了。`compilefail_test.go` 是"这些调用必须编译失败"的清单，新增阶段或放开一个转移
  都要在那里加用例（它一次编译、按行核对错误，加用例几乎不增加耗时）。
- 只有 `Where` / `Search` 在不同阶段返回不同接口，所以只有它们住在 `joinBuilder` /
  `whereBuilder` / `searchBuilder` 上；其余方法都在 `builder` 上。给 `builder` 加方法等于让
  **每个**嵌入它的包装类型都有了这个方法——要靠接口把它藏起来。
- `stagePhase` 只挡住"把接口断言回来"的调用，不是约束来源——别把类型约束改成运行期 if。
- 新阶段要问"它能从哪些阶段进入"，以及"SQL 允许它跟在什么后面"：分组和集合操作之后没有
  行锁（PostgreSQL 拒绝），带搜索的查询没有集合操作。**还要问它返回的阶段又能走到哪**：约束会沿着
  返回类型传下去，`GroupedStage` 曾嵌着返回 `OrderedStage`（带 `Lockable`）的 `Sortable`，于是
  `GroupBy().OrderBy().ForUpdate()` 能编译。`compilefail_test.go` 要为每条"绕一步"的路径各写一条，
  不只写直接拼法。
- ORDER BY / LIMIT 作用于整个查询，由 `writeTail` 在查询体**之外**、行锁**之前**写；查询体
  （`writeBody`）会被复用为集合操作数和 CTE 体。所以集合操作数自带的 ORDER BY / LIMIT / OFFSET / 锁
  **不会被写出来**，`setOp` 必须检查的是**操作数的** spec（曾经检查的是左侧自己，那条分支阶段类型
  本来就走不到，于是守卫形同虚设）。`TestBuildRejectsInvalidStructure` 的 `set operand *` 用例守着。

## 改了全文检索

- PostgreSQL 的索引表达式和谓词表达式必须**逐字相同**，否则索引用不上：两边都走
  `internal/sqldialect` 的 `PostgresDialect.FullTextVectorSQL`，不要在谓词里另写一份。
- 全文索引只按名字对账（`ensureFullTextIndex`）：字段比较会因为三个方言的自省差异每次启动都想重建。
- SQLite 是**按子串匹配的退化实现**，语义和另两个不同。改 `Matches` 的渲染要同时想清楚三种行为，
  `TestIntegrationFullTextSearch` 只断言三者都同意的部分。

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

## 改了校验逻辑

先确定它属于哪一边，这条边界是有意的（见 `../architecture.md`）：

- **结构**校验（FROM/JOIN 图、`Correlate`、集合操作、表达式错误）→ `querySpec.validate`，在
  `Build()` 时跑；定义期的错误 → `TableOf.Define`。
- **方言相关**校验（能力位、标识符长度、按方言分叉的构造）→ 渲染时，在第一次执行时跑。

把方言校验提前到 `Build()` 会断掉"一个 `*Query` 在多个方言上复用"这个用法。
