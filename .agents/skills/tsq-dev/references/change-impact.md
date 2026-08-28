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

## 改了查询构建器的阶段（`querybuilder*.go`）

- 阶段接口和具体 builder 的返回值类型必须一起改。**返回值写错，编译期约束会静悄悄松掉**，
  测试不看类型就发现不了。
- `compilefail_test.go` 是"这些调用必须编译失败"的清单，`querybuilder_stages_test.go`
  是阶段转移的清单。新增阶段两边都要加。
- `querybuilder_multi_call_test.go` 覆盖 `Where` / `Search` 只能各调一次这条约束。
- `builderPhase` 只影响错误信息，不是约束来源——别把类型约束改成运行期 if。
- 新阶段要问"它能从哪些阶段进入"：SQL 允许在过滤、分组、集合操作之后出现的子句，就得在
  **每一个**完整阶段上都可达，否则调用方得为了排序重排整条链。`PagedStage` 是这么加的。
- **渲染位置**：查询体（`buildListBodySQL`）会被复用为集合操作的操作数和 CTE 体，所以
  ORDER BY / LIMIT 这类作用于整个查询的子句必须在体**之外**拼（`buildQueryTail`），
  又必须在行锁**之前**——SQL 把锁放最后。

## 改了 `Page()` 或构建器级分页

`Page()` 是**追加**自己的 `ORDER BY` / `LIMIT` / `OFFSET`，不是替换。所以构建器级分页和
`Page()` 同时存在时会拼出两个 ORDER BY——在任何方言上都不合法。这条冲突由
`Query.hasOrderBy` / `hasLimit` 在 `buildPageSQLsWithLimit` 里显式报错挡着，
**不要改成"后者覆盖前者"**：猜调用方想要哪个比说不清更糟。
`querybuilder_paging_test.go` 的 `TestPageRefusesToFightBuilderPaging` 是那道门。

## 改了托管时间戳字段的生成代码（`tsq.go.tmpl` 的 Insert / Update）

- 生成的 `Insert` 只在字段**还是零值**时才盖 `created_at` / `updated_at`
  （`TimestampUnsetExpr`）。无条件盖会静默丢掉调用方导入历史数据时设的时间。
  `Update` 相反，**必须**无条件刷新 `updated_at`——那正是它的语义。
- `TimestampUnsetExpr` 必须覆盖 `validateTimestampField` 接受的**每一种**字段类型
  （`time.Time` / `*time.Time` / `sql.NullTime` / `null.Time`），否则生成时 panic。
  `template_helpers_test.go` 的 `TestTimestampUnsetExprCoversEveryManagedTimestampKind`
  是那道门。

## 改了 SQL 渲染或参数绑定

- `sql_render_test.go`、`query_args_test.go` 是黄金输出，改渲染必然改它们。改之前确认
  新输出是**更对**而不是**只是不一样**。
- `sql_render_bench_test.go`、`querybuilder_bench_test.go`、`query_exec_bench_test.go`
  在测热路径。渲染进热路径的字符串拼接要看一眼 bench。
- 参数顺序变了 → `condition_ordering_test.go`。

## 改了 LIKE 谓词的渲染，或改了关键字转义

- **转义值和声明转义符必须一起改。** `escapeKeywordSearch`（`query_args.go`）和
  `keywordLikeEscapeClause` 拼进谓词的那一处（`query_plan_sql.go` 的 `buildWhere`）是同一个
  契约的两半：只转义值而不发 `ESCAPE`，在 SQLite 上转义符会变成普通字符，查询**静默返回
  零行**（SQLite 没有默认 LIKE 转义符，MySQL / PostgreSQL 默认是反斜杠）。
- 转义字符**不能是反斜杠**：MySQL 拼不出 `ESCAPE '\'`（反斜杠转义掉收尾引号）。三个方言
  必须能用同一种写法，因为这个子句在 `Build()` 期就固定进 SQL 文本，那时还不知道方言。
- 断言要落在**真跑一次数据库**上，不能只比对渲染出来的字符串——这个 bug 的两个副本
  （值被转义了、子句没发）单看任何一边都是对的。`keyword_search_test.go` 守 SQLite，
  `integration_test.go` 的 `TestIntegrationKeywordSearchEscapesWildcards` 守 MySQL 和
  PostgreSQL：**一条固定进 SQL 文本的子句必须在三个方言上都能解析且语义一致**，只有真实
  服务器证明得了。此前 `integration_test.go` 对关键字搜索零覆盖。

## 改了分块（`query_chunked.go`）或批量语句的形状（`executor_mutation.go`）

- 分块的单位是**行**，数据库数的是**占位符**，两个换算因子都可能错：
  - **上限按方言**（`dialect.MaxBindParams`）。MySQL / PostgreSQL 是 65535，**SQLite 是
    32766**。曾经写死 65535 并注释成"最紧的那个"，宽表在 SQLite 上直接 `too many SQL
    variables`——而 SQLite 是单元测试唯一跑的库。
  - **每行占位符数按操作分别算**。INSERT 每列一个；**UPDATE 每列两个**
    （`col = CASE pk WHEN ? THEN ? ... END`）再加 WHERE 的 1~2 个；DELETE 每行 1~2 个。
    改了 `updateBatch` / `insertBatch` 的语句形状，就要回来核对
    `insertBindParamsPerRow` / `updateBindParamsPerRow` / `deleteBindParamsPerRow`。
- 方言未知（`WrapExecutor` 包一个裸 `*sql.DB`）时取**最紧**的上限：偏小只多几次往返，
  偏大是执行期直接失败。
- **`IgnoreErrors` 的错误处理不可移植**：`ChunkedInsert{IgnoreErrors}` 事务内必须用
  savepoint 括住每一行。PostgreSQL 一条语句失败就把事务置为 aborted，其后一律 `25P02`——
  "抓住错误继续跑"只在 SQLite / MySQL 上成立。事务外**不能**发 savepoint（PG 用 `25P01`
  拒绝事务外的 `SAVEPOINT`），判断走哪条路要穿过 `wrappedExecutor` 找 `*sql.Tx`。
  别改成 `INSERT IGNORE` / 批量 `ON CONFLICT DO NOTHING`：前者在 MySQL 上会吞掉所有错误，
  后者让 `RETURNING` 无法按位置回填主键。`TestIntegrationChunkedInsertIgnoresDuplicatesInsideTransaction`
  是那道门，且**只有真实 PostgreSQL 上才有意义**。
- 宽表端到端用例**在 `-race` 下很贵**（一条批量 UPDATE 要绑几万个占位符）。行数取"刚好越过
  错误估算下的上限"，不要为了保险随手加大——第一版用 1200 行，一个用例就占了 `test-race`
  的四分之三时间。`TestSQLiteRejectsMoreBoundParametersThanItsCeiling` 用一条简单 INSERT
  直接钉住 32766 这个数，比靠特定表形状去推便宜得多。
- `query_chunked_widetable_test.go` 是那道门——它真的插一张 40 列的表，纯粹比对算出来的
  chunk size 证明不了语句能被数据库接受。

## 改了 schema 托管（`runtime_schema.go`）或 `_tsq_managed_tables`

- **记账表是全库共享的，而每个 runtime 只知道自己那份表集。** 读、删、写都必须按
  `SchemaOwner` 划界；任何"整表覆盖"都会抹掉别的 runtime 的记账，下次启动它们就互删对方的
  表**连同数据**。
- 改记账表的形状要带**就地迁移**：老库里已经有一张旧结构的表，而 `CREATE TABLE IF NOT
  EXISTS` 不会升级它。它是 TSQ 自己的记账、不含用户数据，所以整表重建是允许的。
- **不要把 DROP 和记账更新包进一个事务**：MySQL 每条 DDL 都隐式提交，包起来只在
  PG / SQLite 上成立，反而让人误以为它是原子的。
- 门：`runtime_schema_ownership_test.go`（SQLite，含旧记账迁移）和
  `integration_test.go` 的 `TestIntegrationManagedPolicyIsScopedToItsOwner`（三方言）。

## 改了校验逻辑

先确定它属于哪一边，这条边界是有意的（见 `architecture.md`）：

- **结构**校验（列属于哪张表、聚合合不合法）→ `query_validation.go` / `query_plan_validate.go`，
  在 `Build()` 时跑。
- **方言能力**校验（CTE、FULL JOIN、行锁）→ `query_validation.go` 的
  `detectSQLCapabilities` + `dialect.ValidateCapability`，在**执行**时跑。
  （`dialect_validation.go` 只剩标识符校验。）

把方言校验提前到 `Build()` 会断掉"一个 `*Query` 在多个方言上复用"这个用法。

## 新增或改动方言能力位

- `dialect/dialect.go` 加 `Capability` 常量，**三个方言（mysql / postgres / sqlite）都要
  显式表态**。漏掉一个，默认值会让不支持的方言悄悄放行——那是跑到生产库上才炸的一类错。
- 执行期不支持要返回 `*ErrUnsupportedCapability`，带上能力名和方言名；
  `unsupportedCapabilityHint` 里"去哪个方言跑"的提示要跟着改。
- `integration_test.go` 的 `TestIntegrationCapabilitiesExecute` 对每个方言声明支持的
  能力真跑一遍——声明了但跑不通，CI 的 `Integration` job 会红。
- 更新 `skills/tsq` 里"哪条查询能在哪个库上跑"的说明和 `README.md` 的能力矩阵。
  `[门禁: skill-check dialect]`
- 能力位按版本基线表态（见 `architecture.md` § 方言），改基线要进 CHANGELOG 的 `### 变更`。

## 给 `Dialect` 接口加了钩子，或改了写路径（`executor_mutation.go`）

- 接口里的钩子必须有调用方：`grep -rn '<钩子名>(' --include='*.go' . | grep -v dialect/`
  必须命中根包。`LastInsertIdReturningSuffix` 曾经"有定义、有实现、零调用"六个版本，
  PostgreSQL 上 `Insert` 从来没回填过主键（2026-08-26 集成测试第一次跑就抓到）。
- 主键回填有两条路：`ExecContext` + `LastInsertId()` + `BatchInsertStartID`（MySQL /
  SQLite），和 `INSERT ... RETURNING` + 按顺序扫描（返回非空 `LastInsertIdReturningSuffix`
  的方言，即 PostgreSQL）。改任何一条要看 `TestEngineInsertAssignsIDsThroughReturningClause`
  和 `integration_test.go` 的 CRUD 用例。

## 在执行路径上加了一个日志或诊断出口

- 必须走 `logForExecutor` / `logSQLForExecutor`（`runtime_schema.go`），**不要直接调
  `slog.*`**。使用者配了 `RuntimeOptions.Logger` 就是要所有执行期输出都进那个 Logger，
  少接一处等于那一处对他不存在。
- 加完 grep 一遍确认没漏（**只扫根包和 `dialect/`**——`internal/parser` 是生成器，
  跑在 `tsq` CLI 里，那儿根本没有 runtime，用 `slog` 是对的）：

  ```bash
  grep -n 'slog\.\(Info\|Warn\|Error\|Debug\)' *.go dialect/*.go | grep -v _test
  ```

  **应该一条都不命中。** 确实拿不到执行器的地方（`Build()` 期、Runtime 还没组装完）
  写成 `slog.Default().Warn(...)` 并在旁边注明理由——它不匹配上面这条 grep，所以
  "无意中直调"和"有意的例外"在形式上就分得开。当前的两处例外是
  `query_validation.go` 的 `quoteBuiltInIdentifier` 和 `trace.go` 的 `appendTracers`。
- `logForExecutor` 引入时只接了三个调用点，读路径八处 SQL 日志和两处 rows.Close 告警
  一直在直调 `slog.*`，规则在 `architecture.md` 里写了却没人执行。**"加了个统一出口"
  不等于"接完了"，接完的判据是那条 grep。**

## 新增了一个"开关 + 若干消费点"的特性

- 开关必须从**导出的** `RuntimeOptions` 字段一路接到消费点。中途任何一段不可达，
  那个特性在发布出去的库里就不存在，而源码看着像它能用。
- 判据同"给 `Dialect` 接口加了钩子"那条：**grep 一遍调用方**。只被 `_test.go` 引用的
  未导出符号是这类缺陷的典型形态——`unused` linter 看不见它（测试里的引用算使用），
  所以 grep 时要显式排除 `_test.go`。
- `printSQL` context key 加它的三个未导出 tracer 就是这样活了很久：八处
  `ctx.Value(printSQL)` 在库里永远为假，唯一能设置它的 `printSQLTracer` 没导出。

## 加了或改了 `Capability` 常量

- `dialect/dialect.go` 的 `AllCapabilities()` 加一行，**三张方言表
  （`mysqlCapabilities` / `postgresCapabilities` / `sqliteCapabilities`）各加一行**，
  true/false 都要显式写出来。`[门禁: dialect/capability_test.go 的
  TestDialectsCoverAllCapabilities]`
- `SupportsCapability` 只做查表，**不要再引入 `default` 分支**——那正是这道门要挡的东西。
- `displayCapabilityName` 和 `unsupportedCapabilityHint` 也要加分支，否则错误信息里
  是原始的枚举串而不是使用者认得的 SQL 语法。
- 别名（`FULL JOIN` → `FULL_OUTER_JOIN` 之类）加进 `canonicalCapabilityName`。
  **根包不要复制这个函数**：曾经有过一份逐行副本，只被自己的测试撑着。
- 其余按下面"新增或改动方言能力位"那条走完。

## 改了驱动错误分类（`*_errors.go`、`IsRetryable*`）

- 按接口匹配（`SQLState()` / `Code()`），**不要 `errors.AsType` 某个驱动的具体类型**：
  同一个 SQLSTATE 在 pq、pgx v4、pgx v5 里是三个 Go 类型，只认一个就静默漏掉另外两个
  （2026-08-26 之前 pgx v5 就是这样漏的）。
- `integration_test.go` 的 `TestIntegrationLockConflictsAreRetryable` 用真实驱动验证。

## 加了或改了 `-X` ldflags（`Makefile`、`.goreleaser.yaml`、`Dockerfile`）

- 目标必须是 `github.com/tmoeish/tsq/v4/internal/buildinfo.<var>`，`<var>` 必须真的在
  `internal/buildinfo/buildinfo.go` 里声明。链接器对找不到的符号**静默忽略**，二进制会
  把 build time / commit / branch 报成 `unknown` 而没有任何报错。
  `[门禁: release-check 核对三份配置里的每个 -X]`
- 三份配置是三个副本，改变量名要一起改。

## 改了注解 DSL（`internal/parser/`）

- 解析器接受或拒绝什么，就是使用者能写什么。`skills/tsq` 的注解说明必须同步。
  `[门禁: skill-check dsl]`
- 改注解格式还要改 `internal/parser/format.go`（`tsq fmt` 的规范化）和
  `tableinfo.go` 的 `commentLocator`（错误行号映射）。**报错指错行比不报行号更浪费时间。**
- 改索引名推导（`normalizeIndexNames`）会让使用者已经建好的索引对不上。这是 schema 层面
  的破坏性变更，按破坏性变更处理。
- `make examples` 重新生成，`./bin/examples/full-suite` 跑一遍。`[门禁: gen-check]`

## 改了模板（`internal/cmd/*.go.tmpl`）

- 模板决定生成代码长什么样，也就决定了使用者能调用哪些方法。**改模板等于改 API。**
- `make examples` 后看一眼 `examples/academy/*.tsq.go` 的 diff——那就是使用者会看到的变化。
- `skills/tsq` 里凡是提到生成方法名的地方都要同步。`[门禁: skill-check templates]`
- 模板里新用的辅助函数要加进 `template_helpers.go` 并配测试。

## 改了 DDL 推导（`internal/cmd/ddl_render.go`）

- 三个方言的 `.sql` 输出都会变，`tsq.json` 快照也会变。看 diff 确认是预期的。
- 自定义 codec 类型（`driver.Valuer` / `sql.Scanner`）推不出列类型，使用者必须写显式的
  `db:"...,type:..."`。改推导规则前先确认新规则不会让某类类型从"必须显式"变成"猜一个"——
  猜错的列类型在建表那一刻不报错，在写入超长数据那一刻才报错。
- `dialect/ddl_reconcile_test.go` 覆盖运行期对账，生成期变了它可能跟着变。

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

`main` 的 ruleset 按**检查名**要求 `Lint`、`Coverage`、`Build`、`Docker Build`、
`GoReleaser Check` 全绿。改掉其中任何一个 job 的 `name:`，那个必需检查就再也不会出现在
PR 上，而"等不到的检查"等于**所有 PR 永久合不进去**，包括发版 PR。

改 job 名必须同步 ruleset：

```bash
gh api repos/tmoeish/tsq/rulesets --jq '.[] | "\(.id) \(.name)"'
gh api repos/tmoeish/tsq/rulesets/<id> --jq '.rules[] | select(.type=="required_status_checks")'
```

同理，**不要把 matrix job 加进必需检查**：`Test` 的检查名是
`Test (ubuntu-latest, 1.27.0)`，升 Go 版本就会变成另一个名字。

## 升级 Go 版本

`go.mod`、`.github/workflows/go.yml` 的 `GO_VERSION` 与 matrix、`Dockerfile`、
`CLAUDE.md` / `AGENTS.md` 里写的版本号，全部一起改。golangci-lint 也要升到兼容版本
（`Makefile` 的 `LINT_BIN` 那行钉死了版本）。

matrix 一改，`Test` 的检查名就跟着变——所以 ruleset 的必需检查里没有它，见上一条。

## 加了新的 Go 源文件

- 根包新文件 → `feature-map.md` 要能把人带到它。`[门禁: skill-check library]`
- 有导出符号 → `make api-snapshot`。`[门禁: api-check]`
- 配套的 `_test.go` 文件名要么对应一个特性，要么对应被测文件，没有第三种。
