# TSQ 智能体指南

用于编码智能体和 IDE 助手的规范规则集；`CLAUDE.md` 是路由到此处的入口点。

改代码之前，先读 `agents/skills/tsq-dev/SKILL.md` 以及它 `references/` 下覆盖你要碰的
领域的上下文文件。

所有权的划分，没有什么是跨越它复制的：

- **本文件包含规则** —— 必须对所有未来变更成立的约束。
- `agents/skills/tsq-dev` 包含**开发本仓的工程上下文** —— `architecture.md`、
  `feature-map.md`、`codegen.md`、`change-impact.md`、`release.md`，以及
  `references/memory.md` 里带日期的事故与决策。
- `skills/tsq`（仓库根，随发布分发）包含**给 TSQ 使用者的说明书**。它描述契约，不描述
  实现。
- `README.md`、`docs/`、`CHANGELOG.md` 是面向使用者的产品文档。

重复的语句是注定漂移的语句。交叉引用，不要复制。

## 这个仓库是什么

三个交付物共用一个版本号：

- **库**：仓库根包 `tsq`，被别的项目 import。
- **生成器**：`./cmd/tsq` CLI（实现在 `internal/cmd`），读 `@TABLE` / `@RESULT` 注解产出
  类型化代码和 DDL。
- **示例**：`./examples`，可运行的契约；`examples/academy` 的生成物被提交。

环境：Go `1.27.x`（`go.mod` 写 `go 1.27.0`，CI 用 `1.27.0`），模块 `github.com/tmoeish/tsq/v4`，
任务运行器 `make`，本地 lint 二进制 `./bin/golangci-lint`。

## 分层

- 根包 `tsq` **不 import 任何 `internal/` 包**。生成的代码只依赖根包和 `dialect`。
- `internal/genmodel` 是中立数据模型，不 import 解析器也不 import 命令层。这是"解析"和
  "渲染"能各自被测试的原因。
- `internal/parser` 只做 Go 源码 → `genmodel`；`internal/cmd` 只做 `genmodel` → 磁盘。
- `dialect` 同时被库和生成器使用：运行期的方言能力和生成期的 DDL 类型映射说的是同一件事
  （这个库支持什么），拆开必然漂移。

## 类型系统是约束的来源

查询构建器是**阶段式**的：每次方法调用返回不同的具体类型，限制接下来能调什么。

- `Where(...)` 和 `Search(...)` 每条链上**最多各出现一次**，由 Go 类型系统在编译期强制——
  调用过 `Where` 之后拿到的 `WhereStage` 上根本没有 `Where` 方法。所有过滤条件传给唯一的
  那次 `Where(...)`（多参数是 AND），OR 组用 `tsq.Or(...)`，复合子条件用 `tsq.And(...)`。
  两个子句可以共存，顺序任意。
- **不要把编译期约束改成运行期检查。** `builderPhase` 只用于改善错误信息，不是约束来源。
- 新增阶段就是新增一个接口加一个具体 builder 类型。返回值类型写错，约束会静悄悄地松掉，
  而测试不看类型发现不了——`compilefail_test.go` 和 `querybuilder_stages_test.go` 是防线，
  新增阶段两边都要加。

## 校验分两边，边界是有意的

- `Build()` 只校验**结构**：列属不属于查询涉及的表、聚合和 GROUP BY 合不合法、有没有选列。
- **方言能力**（CTE、`FULL JOIN`、行锁）在**执行**时才校验，返回 `*ErrUnsupportedCapability`，
  错误里必须带能力名和方言名。
- 把方言校验提前到 `Build()` 会断掉"一个 `*Query` 在多个方言上复用"这个用法。改校验之前
  先确定它属于哪一边。
- 新增方言能力位，`mysql` / `postgres` / `sqlite` **三个都要显式表态**。漏掉一个，默认值会
  让不支持的方言悄悄放行——那是跑到生产库上才炸的一类错。

## 生成物

- **不要手改** `*.tsq.go`、`*.result.tsq.go`、`runtime.tsq.go`、`tsq.json`、
  `examples/academy/{mysql,postgres,sqlite}.sql`、
  `agents/skills/tsq-dev/references/api-surface.txt`。改结构体、注解、模板或解析器，然后
  重新生成。唯一例外是显式调试生成输出的时候。
- `examples/academy/mock.sql` 是**手写的** schema 真相源，和示例结构体必须保持一致。
- 生成物是否同步不能用 `git diff` 判断——一波变更本来就可能合法地改动生成物。判据是
  `tsq gen --check`（`make gen-check`）：拿当前源码重新渲染一遍，看结果一不一样。
- 生成文件头印着 TSQ 版本号，所以**改了版本号也必须重新生成示例**。
- 先 `tsq fmt` 再 `tsq gen`。反过来的话 fmt 改动了注解文本，生成物立刻过期。
- 改模板等于改 API：模板决定生成代码长什么样，也就决定了使用者能调用哪些方法。

## 对外契约

- 根包和 `dialect` 的导出符号就是这个库的产品。`references/api-surface.txt` 是它的快照，
  `make api-check` 守着它，`make api-snapshot` 刷新它。
- 快照存在的意义不是禁止改 API，而是让"改了"这件事无法悄悄发生。它变了，就回头看
  `skills/tsq`、`README.md`、`docs/` 还真不真实。
- 使用 Build 式查询流程；不要重新引入已被删除的兼容包装、全局 `Init()`、engine 中间层或
  `traceManager` 层。任何形式的"方便起见加个全局默认 runtime"都是在往回走。
- 优先显式的、类型化的 API，不要字符串快捷方式。

## 命名

- `Result`，不是 `DTO`。
- `GTE` / `LTE`，不是 `GreaterOrEqual`。
- `StartsWith` / `EndsWith`。
- `Expr` / `Exprf` / `Pred` 用于自定义列表达式和谓词。
- 谓词命名的分工：RHS 用 `Op(...)`，字面量用 `OpVal(...)`，运行期占位符用 `OpVar()`，
  模式糖用 `StartsWithVal` / `StartsWithVar` 这类名字，跨列或子查询的模式匹配走 `Like(...)`。
- 表 DSL 的受管理字段名：`version`、`created_at`、`updated_at`、`deleted_at`。
- 测试文件名要么对应一个特性，要么对应一个被测文件，没有第三种。按"待办批次"命名的文件
  会从 `feature-map.md` 的清单里掉出去，因为没有哪个特性认领得了那个名字。

## 语义陷阱（改动时不要"顺手修正"）

- `InVar()` 传空或 nil 切片 = **显式不匹配**（渲染成 `IN (NULL)`）。
- `NInVar()` 传空或 nil 切片 = **显式全匹配**。
- 两者都不会静默地把过滤条件去掉。这是有意的：静默去掉过滤条件的查询会返回全表。
- `ChunkedInsert` / `ChunkedUpdate` / `ChunkedDelete` **不自动开事务**，需要全有或全无时
  由调用方用 `WithTx(...)` 包起来。
- `ForUpdate()` / `ForShare()` 只在显式事务里有意义。
- 乐观锁冲突 `ErrOptimisticLockConflict` 是**业务错误**，必须处理，不能忽略。
- 自定义 codec 字段（`driver.Valuer` / `sql.Scanner`）推不出 DDL 列类型，使用者必须写显式的
  `db:"...,type:SQL_TYPE"` 覆盖。不要把"必须显式"改成"猜一个"——猜错的列类型在建表那一刻
  不报错，在写入超长数据那一刻才报错。

## Go 代码风格

- 跟随仓库现有风格，改动保持外科手术式。
- 错误在失败调用之后**立即**处理；优先早返回；用 `errors.Is` / `errors.As` 分支。
- 所有 DB 操作第一个参数是 `context.Context`。
- 用 `%w` 包装错误。
- 注释只记录导出行为和不明显的约束。不要逐行叙述代码在做什么。
- **代码注释、Go doc、README、`docs/` 和 `skills/tsq` 用英文**——这是一个公开的库，
  这些是使用者读的东西。`CHANGELOG.md`、`AGENTS.md`、`CLAUDE.md` 和
  `agents/skills/tsq-dev` 用中文，它们的读者是维护者。

## 语言与提交

- 提交信息用**英文 Conventional Commits**：`type(scope): summary`，type 取
  `feat|fix|perf|refactor|docs|test|build|ci|chore|style|revert`。主题不超过 72 字符、
  不以句号结尾、不能是 `wip` / `update` / `fix` 这类说不清改了什么的词。
- 空一行，然后写正文：改了什么、为什么改、怎么验证的。至少 3 行、120 字符。
- 破坏性变更在 type 后加 `!` 或在正文写 `BREAKING CHANGE:`。`script/release.py` 靠这个
  推断版本递增级别。
- merge、revert、fixup、squash 提交豁免。
- `make commit-check` 校验这些，但它作为 Makefile 目标是结构性失效的（有未提交代码时它
  跳过，代码一提交 `memory-check` 又跳过）。提交信息真正被校验的唯一时机是 `commit-msg`
  钩子——**每台机器克隆后跑一次 `make hooks`**。

## 验证与交接

- 编辑期间跑窄范围的相关检查和 `make fmt`。
- 动了生成器、模板、解析器或示例：`make examples`，然后 `./bin/examples/full-suite`。
- 交接前跑 `make harness`。它的顺序是**唯一权威**，按"便宜且常失败的靠前"排：

  ```
  skill-check  memory-check  lint  vet  gen-check  api-check  release-check
  test  test-race  examples-run  commit-check
  ```

  项目技能只引用这个顺序，不复述它。
- 跨切面的运行时、查询或并发改动，`test-race` 是必需的而不是可选的：这个库没有集成环境
  兜底，测试是它唯一的安全网。
- 改了 GoReleaser 配置，跑 `goreleaser check`。
- 一波变更的定义只有一处：`script/changeset.py`（非 Markdown、非生成物）。所以改
  `Makefile`、`.golangci.yml`、`Dockerfile` 或 CI 和改 Go 代码一样，要带内存记录。

## 技能维护是变更的一部分

项目技能是下一个智能体获取上下文的地方。一个任务应该从读记录下来的上下文开始，而不是从
猜文件名再从源码重构设计开始。只有每一波都让技能比之前更真实，这才成立。

- 改代码**之前**，读覆盖你要碰的领域的上下文文件，并对每个匹配的触发器处理
  `agents/skills/tsq-dev/references/change-impact.md`。耦合的工作在同一波里做完。
- 改代码**之后**，把这波教会你的东西路由到持久的地方——一个事实，一个归宿。路由表在
  `agents/skills/tsq-dev/SKILL.md` § 维护这份技能。
- 使用者看得见的变化（新 API、改掉的语义、新的注解键、改掉的 CLI flag）必须同时进
  `skills/tsq`、`README.md`、`docs/` 和 `CHANGELOG.md` 的未发布段。
- 如果你必须读源码才能弄清楚技能本该告诉你的东西，那个空白就是缺陷的一部分。交接前补上。
- 一个被修两次的问题是一个不彻底的修复：直到 `change-impact.md` 或 `memory.md` 阻止了第三次
  发生，第二次修复才算完成。
- 就地更正，删掉变错的条目。**永远不要追加自相矛盾的内容**——过时的上下文文件比缺失的更糟，
  因为人会相信它。
- `make skill-check` 把最容易忘的几条耦合钉死了。确实判断过不需要动技能时，用
  `SKIP_SKILL_CHECK=<触发器名>` 逐条豁免，并在提交正文里写理由。无理由的总开关不提供。

## 项目内存

`agents/skills/tsq-dev/references/memory.md` 被提交，所以每台机器和 agent 共享一份记忆。
和它描述的代码在同一波里更新它。

- 只写仓库讲不出来的东西：事故及其根本原因、决定及其推理、不明显的运行时行为、值得不再
  重复的死胡同。
- 不要复述规则、包布局或使用者契约——本文件、`architecture.md` 和 `skills/tsq` 各自拥有
  它们。
- 每条标绝对日期。变错的条目就地更正或删除。
- `make memory-check` 在未提交的功能性改动没有携带内存更新时失败。发版波（只改
  `internal/buildinfo/buildinfo.go`）被精确豁免。如果一波确实没教会任何持久的东西，在提交
  正文里说明，而不是编一条。

## 发版

- 平时把人话写进 `CHANGELOG.md` 的 `## [未发布]` 段，按 `### 新增` / `### 修复` /
  `### 破坏性变更` 分节。小节名决定版本递增级别。
- `make release-dry-run` 看一眼会发成什么；`make release` 真的发：定版本号 → 写 CHANGELOG →
  重新生成示例 → `make harness` → 提交 → 打 tag → 推送。推送 tag 触发 CI 的 GoReleaser。
- **tag 推送之后不可撤销**：Go Proxy 永久缓存内容哈希，删掉重打会让全球用户 checksum 校验
  失败。发错了用 `go.mod` 的 `retract` 加一个新补丁版本，**不要删 tag 重打**。
- 跨主版本（v4 → v5）不自动做：Go 的语义化导入版本要求先改 go.mod 模块路径和全部内部
  import，那是一次真实的代码变更。`release.py` 检测到会直接拒绝。
- 完整流程、分支策略和踩过的坑见 `agents/skills/tsq-dev/references/release.md`。

## 本地文件（已 gitignore，不要提交）

`bin/`、`dist/`、`coverage.out`、根目录的 `/tsq` 二进制、`.claude/settings*.json`、
`.antigravitycli/`。`.claude/skills` 是被跟踪的符号链接，指向共享的技能目录。
