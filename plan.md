# TSQ 重构计划

## 问题概述

TSQ 当前功能可用、测试基线干净，但 runtime 查询层、表注册/追踪层、代码生成器、AST 解析器之间的职责边界不清，导致：

1. **修改成本高**：一个功能点经常横跨 `query.go`、`querybuilder.go`、`condition.go`、`sql_render.go`、`table.go` 和 `cmd/internal/parser` 多处同步修改。
2. **API 语义不一致**：有的入口返回 `error`，有的延迟记录 `buildErr`，有的直接 `panic`，使用方很难预测失败模式。
3. **隐藏状态过多**：全局表注册器、全局 tracer 注册器、构建期缓存字段和字符串片段，使行为依赖调用顺序且难以并发隔离测试。
4. **实现与概念错位**：QueryBuilder 对外像 DSL，对内却是“缓存好的 SQL 片段 + 副作用累加器”；Batch API 名义上是批量，实际大多仍是逐条执行。
5. **生成链路脆弱**：parser 先扫结构体、再回扫注释、再处理嵌入、再模板渲染，状态分散且 package 加载/AST 遍历存在重复。

## 已识别的主要设计/实现缺陷

### 1. Runtime 核心是 God file，职责耦合过重

`query.go` 同时承载了：

- Query 构建后的数据模型
- SQL 选择/分页/扫描执行
- 参数拼装
- scan destination 构造
- mutation/batch API
- executor/dialect 校验

这会让任何一个点的调整都触发大范围回归，并放大重复逻辑。

### 2. 失败模型混乱，panic/error/buildErr 并存

当前同一套 DSL 中同时存在：

- `Build()` 返回 error
- `MustBuild()` panic
- `Select()/Where()/Having()` 通过 `buildErr` 延迟失败
- `Condition`/`Function`/`Order`/`Column` 多数非法输入直接 panic
- `mergeQueryArgs()` 运行期还会因外部参数不足 panic

这让“哪些错误是用户输入错误、哪些是程序员错误、哪些可恢复”完全不清晰。

### 3. QueryBuilder 内部状态重复且容易失真

Builder 同时缓存：

- `selectCols`
- `selectColFullnames`
- `selectTables`
- `conditionClauses`
- `conditionTables`
- `conditions`
- `kwCols/kwTables`

这些字段是同一事实的多份投影，靠每个链式方法手工保持一致。后续加新能力（alias、subquery、join graph 扩展）时极易出现“一个投影更新了，另一个没更新”的错误。

### 4. SQL AST/IR 缺失，导致渲染、校验、方言适配散落各处

当前系统基本直接拼字符串：

- builder 负责 SQL 结构拼装
- `condition.go` 内部同时负责表达式和参数语义
- `sql_render.go` 负责 marker 渲染与 bindvar 改写
- `table.go` 又单独做方言分支

缺少统一的中间表示，导致 dialect、identifier quoting、bindvar rewriting、count wrapping、keyword search、order whitelist 都以“局部规则”存在，不易复用也不易扩展。

### 5. 全局可变注册表破坏可组合性

`table.go` 和 `trace.go` 使用进程级全局状态：

- 全局表注册器
- 全局 tracer 列表

这会带来：

- 测试顺序依赖
- 包 import 副作用
- 多数据库实例难以隔离
- 初始化行为不显式

`Init()` 还会顺便修改全局 tracer，进一步混淆“初始化数据库”与“配置 runtime”。

### 6. Batch API 名不副实，且错误识别脆弱

`BatchInsert/Update/Delete` 主要仍按单条调用 gorp：

- 性能预期与 API 名称不匹配
- 错误处理粒度与事务边界不清楚
- `isDuplicateKeyError()` 通过匹配错误字符串做方言判断，维护性差

这部分既误导用户，也让后续真正实现高性能批量 SQL 时难以平滑演进。

### 7. Parser/Generator 流程重复遍历，状态机分散

`internal/parser` 当前流程是：

1. 递归加载 package
2. 扫描 struct 声明
3. 解析嵌入字段
4. 再次遍历文件注释提取 DSL
5. 过滤目标包结果
6. 解析 import 依赖
7. 渲染模板

问题在于：

- package load/AST parse 多阶段重复
- `ParseState`、`StructInfo` 扩展状态、DSL parser、table metadata parser 分散
- comment 关联逻辑复杂且脆弱
- generator 依赖模板 helper 中的字符串规则，而不是稳定的生成模型

### 8. 生成器存在“模型层缺失”的问题

`cmd/gen.go` 直接把 parser 输出喂给模板，模板 helper 再二次推导命名、导入、软删除表达式、时间字段行为。  
这意味着领域规则同时分布在：

- parser
- template helper
- template 文本

规则没有单一归属，难以测试和迁移。

### 9. 文档承诺与实现语义有偏差

README 强调“类型安全、高性能、复杂查询、批量操作、多数据库”，但实际 runtime 里：

- 大量非法调用会 panic
- 批量操作主要是逐条执行
- 多数据库能力分散在若干局部兼容逻辑中

这会造成 API 预期与实现现实不一致。

## 重构目标

1. 建立清晰的 runtime 分层：**查询规格 -> 校验 -> SQL 计划 -> 执行**。
2. 统一错误语义：默认返回 error，仅保留显式 `Must*` panic 入口。
3. 移除大部分全局隐式状态，使注册、追踪、方言能力显式注入。
4. 把 parser/generator 改造成单向数据流：**加载 -> 语义模型 -> 生成模型 -> 模板渲染**。
5. 保持外部 API 尽量兼容；不兼容项通过适配层与迁移文档管理。

## 总体策略

采用 **“先内部解耦，再收敛 API”** 的渐进式重构，避免一次性重写：

1. 先提炼内部模块边界，不急着改所有公开 API。
2. 引入兼容适配层和 characterization tests，保证现有生成代码仍可工作。
3. 待内部模型稳定后，再逐步收缩不合理 API（尤其是 panic-heavy 部分和全局注册部分）。

## 分阶段执行计划

### 阶段 1：建立 runtime 新边界，不改对外行为

**目标**：把 `query.go` 中混杂职责拆开，但保留现有公开 API。

**重构动作**

- 新建 runtime 内部分层（可先放在同 package，稳定后再细分目录）：
  - `query_spec`: Query/Builder 的结构化规格
  - `query_plan`: count/list/page SQL 计划
  - `query_exec`: list/get/load/page/count/exists 执行器
  - `query_scan`: scan holder 与 field pointer 调度
  - `mutation_exec`: insert/update/delete/batch
- 从 `query.go` 抽离重复逻辑：
  - executor 校验
  - SQL 渲染
  - args merge
  - scan dest 构建
  - page SQL 构造
- 给每类模块建立窄接口，避免继续直接操作 `Query` 内部字段。

**完成标准**

- `query.go` 不再同时承载查询、分页、批量、扫描四大类逻辑。
- 现有公开 API 行为与测试结果保持一致。

### 阶段 2：把 QueryBuilder 从“副作用累加器”改成结构化规格构建器

**目标**：消除重复缓存字段，建立单一事实来源。

**重构动作**

- 定义内部 `QuerySpec`：
  - select expressions
  - source tables / join graph
  - filters
  - group by
  - having
  - keyword search
  - ordering / paging capability metadata
- QueryBuilder 只写入 `QuerySpec`，不再缓存 `conditionClauses` / `selectColFullnames` 这类派生字段。
- 所有字符串 SQL 统一在 planner/render 阶段生成。
- 将 `validateJoinGraph()`、`requiresWrappedCount()` 等规则迁移到 spec/planner 层。

**完成标准**

- Builder 内部不再保存多份可失真的派生状态。
- 任意 SQL 文本都可以从 `QuerySpec` 单向推导生成。

### 阶段 3：统一错误模型，收敛 panic 面

**目标**：明确 API 失败语义，降低运行期意外 panic。

**重构动作**

- 定义错误分类：
  - programmer error（仅 `Must*` / 显式 panic helper）
  - invalid DSL/spec error
  - execution error
  - unsupported dialect/capability error
- 将以下 panic-heavy 入口逐步改为 error-returning internal API，再保留兼容包装：
  - `Condition` 组合和 predicate 构造
  - `Fn/FnExpr/Fn0`
  - `OrderBy.Expr` / `ReverseOrder`
  - `NewCol` / `Into`
  - `mergeQueryArgs`
- 对外保留 `MustBuild`、必要的 `MustXxx` 包装，但把 panic 边界收缩到少数显式入口。

**完成标准**

- 普通链式查询构建路径不再因常见输入错误触发隐式 panic。
- panic 只发生在显式 `Must*` 或明确标注的 programmer error 边界。

### 阶段 4：去全局状态化

**目标**：让注册表、tracer、dialect 能按实例装配。

**重构动作**

- 引入显式 `Registry` / `Runtime` / `Config` 概念：
  - 表注册器
  - tracer 集合
  - dialect 能力
- `Init()` 改造成基于实例配置执行；全局函数只作为默认单例兼容层。
- generated code 优先注册到显式 registry；保留旧 `RegisterTable` 兼容入口。
- 将 tracer 组装从全局切换为 per-runtime 或 per-executor。

**完成标准**

- 单测可在不污染进程全局状态的前提下并行构造多个 runtime。
- `Init()` 不再隐式修改 unrelated global state。

### 阶段 5：重写 batch/mutation 能力的抽象边界

**目标**：让 API 名称、性能语义、错误语义一致。

**重构动作**

- 区分两层能力：
  - `Chunked*`：按批分块但逐条执行
  - `Bulk*`：真正生成批量 SQL（按 dialect 能力开启）
- 为 duplicate key/constraint error 引入方言能力接口，替代字符串匹配。
- 明确事务策略：由调用者提供事务，库内不隐式开启大事务。
- 将 mutation helper 从 `query.go` 中独立出去，避免和查询 API 耦合。

**完成标准**

- “批量”术语与真实行为一致。
- 每种方言下支持/不支持的批量语义显式可见。

### 阶段 6：重构 parser 为单向流水线

**目标**：减少重复遍历和隐式状态，使解析过程可观察、可缓存、可测试。

**重构动作**

- 设计统一 pipeline：
  - package loader
  - AST collector
  - struct semantic analyzer
  - embedded resolver
  - annotation/DSL resolver
  - generation model builder
- 合并多轮文件遍历；尽量一次 AST 遍历收集结构体与注释锚点。
- 给 `ParseState` 建立更清晰的数据职责，消除“半成品 StructInfo + 后续回填”风格。
- 统一 package load cache，避免重复 `packages.Load`.

**完成标准**

- parser 主流程可按阶段单测。
- package/file 解析次数明显减少，状态迁移更直接。

### 阶段 7：引入生成模型层，瘦身模板 helper

**目标**：把代码生成规则从模板字符串中抽离出来。

**重构动作**

- 在 `cmd` 与 `parser` 之间加入 `GenerationModel`：
  - 文件名
  - import 列表
  - 列/字段映射
  - 软删除/时间字段策略
  - DTO 引用展开结果
  - 需导出的符号名
- 模板 helper 只做轻量格式化，不再承载领域规则。
- 合并 `gen` / `genDTO` 的重复流程。
- 为 generated file collision、symbol collision、index collision 提供统一验证入口。

**完成标准**

- 模板可视为纯展示层。
- 生成规则的绝大部分可以在不渲染模板的情况下单测。

### 阶段 8：兼容层、文档与迁移收尾

**目标**：安全完成对外迁移。

**重构动作**

- 增加 deprecated 兼容层：
  - 全局注册 API
  - panic-heavy legacy helper
  - 旧 batch API 命名
- README 与 examples 更新为新推荐用法。
- 增加迁移说明：
  - 哪些 API 保持兼容
  - 哪些 API 仅 deprecated
  - 哪些行为（如 panic -> error）属于有意修正

**完成标准**

- README 承诺与实现语义一致。
- coding agent 能据迁移说明逐阶段提交而不引入大面积行为漂移。

## 建议的落地顺序（给 coding agent）

1. **先拆文件与内部模块，不改公开签名。**
2. **再引入 QuerySpec/Plan，并让旧 API 走新内核。**
3. **随后统一错误模型，保留兼容包装。**
4. **再做 registry/runtime 去全局化。**
5. **最后处理 parser/generator 流水线与模板模型层。**

## 每阶段必须补的测试

1. **Characterization tests**：锁定现有公开 API、SQL 输出、生成文件命名、DSL 解析结果。
2. **Error contract tests**：明确哪些入口返回 error，哪些入口故意 panic。
3. **Integration tests**：至少覆盖 SQLite runtime 查询、分页、批量删除/插入、examples 生成。
4. **Golden tests**：对 generator 输出使用 golden file，而不是只断言局部字符串。
5. **State isolation tests**：验证 registry/tracer 去全局化后可并发隔离。

## 约束与注意事项

- 不建议一次性 rewrite；必须做分阶段、可回滚、可兼容的内核替换。
- 优先消除内部耦合和失败模型混乱，再追求 API 美观。
- generator/parser 重构前，要先补 characterization tests，否则极易“看起来更干净，实际上生成结果悄悄变了”。
- 对 README 中“高性能批量操作”“类型安全”这类表述，若短期无法完全兑现，应在文档中降级为更准确的承诺。

## 当前基线

- 当前 `go test ./...` 通过。
- README、示例和现有单元测试表明：项目当前主要风险不在“功能失效”，而在“架构边界不清导致未来改动成本持续升高”。
