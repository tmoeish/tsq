# 变更日志

本文档记录了 TSQ 项目的所有重要变更。

**注意：** 本项目曾误发过一系列 `v1.0.x` 版本。为了纠正版本混乱，我们已撤回 `v1.0.20` 及其之前的所有版本，并正式从 `v1.1.0` 开始新的迭代。

格式基于 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.0.0/)，
项目遵循 [语义化版本控制](https://semver.org/lang/zh-CN/)。

## [Unreleased]

### 变更（Breaking Changes）
- 查询现在必须显式调用 `From(table)`，不再从 `Select(...)` 或 Join 链隐式推导主表
- `Join` / `LeftJoin` / `RightJoin` / `FullJoin` 改为直接接收可变 `Condition`，移除旧的 `.Join(...).On(left, right)` 两步 API
- 非 `CROSS JOIN` 必须提供 ON 条件；Join 条件必须同时引用已引入表和当前连接表，提前拒绝缺失连接关系或引用未来表的查询

### 改进
- 生成的表 DSL 增加 `Cols()`，让 Build 阶段可以校验列确实属于 `From` / Join 图中的表
- `Alias(...)` 表会同步重绑定生成列集合，别名查询也能参与列归属校验
- 示例、文档和生成模板统一迁移到显式 `From` 与新 Join API

## [3.7.1] - 2026-04-30

### 改进
- `tsq gen` 的 DSL 报错现在会稳定带出具体源码文件和行号，便于直接定位 `@TABLE` / `@RESULT` 中的错误位置
- 统一梳理 DSL 解析与校验错误文案，未知 key、值类型不匹配、缺失括号或花括号、`pk` 格式错误、索引与字段引用错误都会给出更接近 DSL 语义的提示
- 语法错误位置映射改为基于真实 DSL 内容偏移，避免报错落到注释块开头而不是实际出错行

## [3.7.0] - 2026-04-30

### 新增
- **深度集成 juju/errors**: 全项目（除 examples 源码外）切到 `github.com/juju/errors` 进行错误处理，实现全链路 Error Trace 堆栈追踪
- **结构化错误审计**: 核心查询与执行逻辑增加语义化的 `Annotate` 上下文描述，告别原始错误透传

### 改进
- **高信号错误信息**: 优化生成模板，在报错时通过 `tsq.CompactJSON` 输出紧凑的对象快照，防止 `ErrorStack` 刷屏，同时让调试现场数据一目了然
- **SQL 执行报错降噪**: 移除直接在 Error 消息中注入完整长 SQL 的暴力做法，改为更简洁的语义描述（如 "failed to execute count query"），提升日志整洁度
- **代码生成模板升级**: `tsq gen` 模板同步更新，使生成的代码默认具备 `errors.Trace` 和带语义上下文的 `Annotate`
- **测试兼容性提升**: 更新测试套件中的错误断言，全面兼容 Go 1.13+ 的 `errors.Is` 和 `errors.As` 模式

## [3.6.0] - 2026-04-29

### 新增
- `tsq gen` 现在会同时生成并维护 `sqlite.sql` / `mysql.sql` / `postgres.sql`、按需生成的 `*.incremental.sql`，以及与生成代码同目录跟踪的 `ddl.json`

### 改进
- DDL 增量基于 `ddl.json` 中的 schema snapshot 和按表分组的变更记录计算，记录使用 `time.DateTime` 时间戳并以格式化 JSON 输出
- `tsq gen -v` 现在会输出按表分组的 DDL 变更摘要；终端输出时会按 table / create / add / alter / drop 使用不同高亮，非终端输出保持纯文本
- SQLite 遇到列类型变更时，现在会生成可执行的重建表增量 DDL，而不是仅给出手工处理提示
- CLI 错误输出改为运行时错误默认静默 usage，终端中以彩色 `Error:` 前缀呈现；DSL 字段不存在时的提示也更明确，会说明应使用 Go struct 字段名而不是 db 列名
- `examples/database` 同步纳入生成的 DDL SQL/JSON 工件，并修正 `Item` DSL 中 `IdxSPU` 对 `SPUID` 字段的引用

## [3.5.1] - 2026-04-29

### 改进
- `tsq fmt` 现在会收紧 struct 上 `@TABLE` / `@RESULT` 注释块周围的空白布局，并产出与 Go 1.26 注释格式化稳定兼容的结果
- `tsq fmt --help` 与相关测试同步更新，明确说明注解周围空白也会被规范化

## [3.5.0] - 2026-04-29

### 新增
- 增加 `tsq fmt` 子命令：按包扫描 Go struct 注释中的 `@TABLE` / `@RESULT`，统一格式化键顺序、缩进、逗号和字符串引号，并只回写注解片段

## [3.4.0] - 2026-04-29

### 新增
- `tsq gen` 增加生成计划校验能力：`--dry-run` 会显示 `CREATE / UPDATE / UNCHANGED / STALE`，`--check` 会把陈旧生成文件也纳入失败条件
- 增加 `docs/quickstart.md` 与 `docs/concepts.md`，补齐从空目录上手到理解生成模型的文档链路

### 改进
- 重写 README 首屏与 examples 导航，拆分 quickstart / cookbook / full-suite，明确最小使用路径、能力边界和方言矩阵
- `tsq gen --help` 现在明确说明 package 参数格式、生成文件命名、覆盖规则和常见排查方式
- `InitWithOptions` 现在会把索引初始化模式和 schema 事件处理器持久绑定到 `DbMap`，并由 `WithContext` 继承，避免初始化后语义漂移
- SQL 能力校验现在会把 Oracle `MINUS` 视为 `EXCEPT` 能力的一种写法，执行前即可给出一致的方言提示
- `QueryBuilder` 增加显式覆盖式 setter：`SetWhere` 与 `SetKwSearch`
- 生成的索引查询 helper 现在会保留源 DSL 索引名并复用缓存查询，减少排查和重复构建成本

## [3.3.0] - 2026-04-28

### 新增
- 增加公开 searched `CASE` API：`Case[T]().When(...).Else(...).End()`

### 改进
- expression columns 现在会跟踪额外引用表，使 `CASE` 与 `FnExpr(...)` 这类多表表达式可以正确参与 query planning
- 更新 README、`examples/main.go` 与 `examples/README.md`，补充可运行 CASE 示例

## [3.2.0] - 2026-04-28

### 新增
- 增加非递归 `WITH` / CTE 支持：可用 `CTE(name, query)` 创建 CTE table handle，并通过现有 `WithTable` / `RebindColumn` 复用列定义

### 改进
- query planning 现在会递归收集 CTE 依赖，并在列表、计数、关键词分页与 compound query 下统一生成 `WITH ... AS (...)`
- 对不支持 CTE 的方言增加执行前能力校验，当前能力表下会显式拒绝 MySQL 上的 CTE 查询
- 更新 README、`examples/main.go` 与 `examples/README.md`，补充可运行 CTE 示例

## [3.1.0] - 2026-04-28

### 新增
- 增加标准 SQL 集合查询 API：`Union`、`UnionAll`、`Intersect`、`IntersectAll`、`Except`、`ExceptAll`

### 改进
- 复合查询的 `COUNT` 现在会自动包裹子查询，保证分页统计与聚合统计语义正确
- 复合查询分页排序改为基于结果列名生成 `ORDER BY`，避免在 compound query 上错误引用原表限定名
- 更新 README、`examples/main.go` 与 `examples/README.md`，补充集合查询的可运行示例

## [3.0.2] - 2026-04-28

### 改进
- 重新整理 `cmd/tsq.go.tmpl` 与 `cmd/tsq_result.go.tmpl` 的布局、缩进和区块顺序，提升模板可读性而不改变生成语义
- 扩展仓库级 `AGENT.md`，补充主流 Go 开发与设计最佳实践，便于 coding agent 与 IDE 助手保持一致的实现风格

## [3.0.1] - 2026-04-28

### 新增
- 新增仓库级 `AGENT.md`，统一 coding agent / IDE assistant 的项目规则、验证顺序和 Go 开发约束

### 改进
- 用软链统一常见 agent / IDE 入口文件，避免多份规则副本长期漂移

### 修复
- 停止跟踪本地构建与本地工具产物：移除根目录 `tsq`、`coverage.out` 与 `.claude/settings.local.json`
- 更新 `.gitignore`，避免上述本地文件再次被误提交

## [3.0.0] - 2026-04-28

### 变更（Breaking Changes）
- 全面清理误导性命名：查询结果结构统一从 DTO 语义迁移为 Result，生成符号同步改为 `Result<Type>`
- DSL 托管字段统一改为显式命名：`version`、`created_at`、`updated_at`、`deleted_at`
- 条件 API 统一命名：`GETVar/LETVar/GESub/LESub` 更名为 `GTEVar/LTEVar/GTESub/LTESub`
- 字符串匹配 API 统一为行业常用复数形式：`StartsWith*` / `EndsWith*`
- 原始列表达式 API `Fn0` 更名为 `FnRaw`

### 改进
- 示例数据库 schema、示例结构体和生成代码统一切换到新托管字段命名
- `examples/main.go` 输出摘要中的 `dto` 节点更名为 `result`，与公开 API 保持一致
- 生成模板、解析器、测试和文档统一到 `@RESULT` 注解与新字段命名

## [2.2.0] - 2026-04-28

### 新增
- 增加 `EscapeKeywordSearch()` 函数用于安全的关键字搜索参数化，防止 LIKE 注入
- 增加 `ValidateIdentifierLength()` 函数进行跨方言标识符长度验证（MySQL 64, PostgreSQL 63, Oracle 30, SQLite 无限制）
- 增加 `MaxTracers` 常量限制追踪器列表最大大小为 100 以防止内存泄漏
- 增加 `RegistrationError` 类型和 `RegistrationErrorType` 枚举用于结构化注册错误处理
- 增加 `Runtime` 类型用于实现隔离的表册和跟踪管理器
- 增加 `AliasTable()` 和 `RebindColumn()` 支持表别名和自联接
- 增加 `Col[T].As()` 和 `Col[T].WithTable()` 方法用于列重绑定
- 增加 `PageReq.ValidateStrict()` 用于严格分页/排序验证
- 增加 `Order` 作为 `Direction` 的别名以统一排序合约
- 增加 `DbMap.Insert/Update/Delete` 的真实批量写入能力，支持多行 `VALUES`、批量 `CASE ... WHEN` 更新和 `IN (...)` 删除
- 增加 `make fmt` 的 golangci-lint v2 格式化与自动修复流程，统一执行 `gofumpt`、`gci`、`modernize`、`tagalign` 和 `wsl_v5`

### 改进
- **错误处理统一化**：
  - `Registry.Register()` 现返回 `RegistrationError` 而非 panic，支持 nil 表、nil 添加函数等的结构化错误处理
  - `Runtime.RegisterTable()` 现返回错误以与 `Registry.Register()` 保持一致性
  - 所有 defer 块现统一遵循"检查并记录"模式，确保资源总被清理
  
- **代码质量改进**：
  - 提取 `prepareQueryExecution()` 辅助方法消除 `queryInt()`, `queryFloat()`, `queryStr()` 间的代码重复（减少 ~80 行代码）
  - 改进 `MustBuild()` 文档，明确标记其用于初始化时（可能导致 panic），不推荐在生产环境使用
  - 添加详细的资源清理验证测试确保数据库连接和行集正确关闭
  
- **SQL 安全加固**：
  - 新增 `EscapeKeywordSearch()` 帮助函数防止 LIKE 注入（正确的转义顺序：先转义反斜杠）
  - 添加 README 部分说明 LIKE 注入风险和使用 `EscapeKeywordSearch()` 的最佳实践
  - 实现方言感知的标识符长度验证，长标识符（>50 字符）通过 `slog.Warn()` 进行记录
  
- **追踪管理并发安全**：
  - 增强 `TraceManager.AddTracer()` 执行 `MaxTracers` 限制以防止无限增长
  - 改进 `appendUniqueTracers()` 进行严格的去重，使用映射跟踪已见追踪器
  - 为 `restore()` 操作添加 `restoreMu` 互斥锁，原子化追踪器快照→清空→恢复序列
  - 添加并发压力测试 `TestConcurrentTracerAddDuringRestore` 验证竞态条件修复
  
- **文档完善**：
  - README 新增"已知限制和最佳实践"章节，列表展示不支持的功能和解决方案
  - 新增查询缓存指南，推荐应用层缓存、驱动程序缓存、连接池缓存等策略
  - 详细文档说明圆形联接限制和 `AliasTable()` 自联接解决方案
- 在 `query.go` 添加资源清理模式文档，说明统一的 defer 块错误处理约定
- 改进 `validateJoinGraph()` 代码注释，解释为何不支持圆形依赖及推荐的多查询解决方案
- `ChunkedInsert/Update/Delete` 现按 chunk 走批量调用，避免在批处理入口退化为逐条执行
- `.golangci.yml` 调整为更贴近仓库实际的高信号配置：补充官方 v2 formatter 设置，移除高噪声或不匹配项目的规则
- SQL 渲染缓存键生成改为基于 `strings.Builder` 和 `md5.Sum` 的无异常路径实现，移除不可达的 panic 分支

### 修复
- 修复 `SafeOperation()` 和 `SafeOperationWithContext()` 之前 recover 后未返回 `PanicRecoveryError` 的问题
- 修复 `SafeFieldPointerCall()` 之前 recover 后仍返回空错误的问题，现在会稳定返回 `ErrFieldPointerPanic`

## [2.1.0] - 2026-04-28

### 新增
- 增加 `Col[T].InVar()`，支持在执行阶段把切片/数组参数展开为动态 `IN (...)` 占位符
- 恢复 `examples/database/userorder.go` Result 示例，并重新生成 Result 查询构建器
- 增加 `make examples` 目标，用于统一刷新生成代码并构建示例程序

### 改进
- 重写 `examples/main.go`，示例程序现在一次覆盖 CRUD、别名/重绑定、聚合、关键词搜索、分页、Result、`InVar` 与分块写操作
- 更新 `examples/main_test.go`，为示例程序和 Result 分页查询补充冒烟测试
- 更新 README 与 `examples/README.md`，使文档示例与当前 Build-based API 和可运行示例保持一致

### 移除
- 删除 `examples/database/helpers.go` 中已无必要的 `mustBuild()` 兼容包装

## [2.0.1] - 2026-04-27

### 移除（Breaking Changes）
- **❌ 删除了公开 `MustBuild()` 方法** - 这是一个基于 panic 的反模式，不安全
  - 之前：`query := qb.MustBuild()` - 可能在生产环境 panic
  - 现在：`query, err := qb.Build(); if err != nil { return err }` - 安全的错误处理
  - 影响：所有使用 `MustBuild()` 的代码需要迁移到 `Build()` 并进行显式错误处理
  - 迁移指南见 MIGRATION_GUIDE.md

### 改进
- 所有测试已迁移到使用 `Build()` 和显式错误处理（保留包私有的 `mustBuild()` 仅供测试和生成代码初始化使用）
- 更新 README.md 移除所有 `MustBuild()` 示例，推荐显式错误处理最佳实践
- 重新生成所有示例代码（6 个 `*_tsq.go` 文件）确保一致性

### 验证
- ✅ 700+ 核心测试通过
- ✅ 竞态检测无问题 (`go test -race`)
- ✅ 代码质量无问题 (`go vet`)
- ✅ 公开 API 中完全移除 MustBuild

## [2.0.0] - 2026-04-23

- 运行时状态隔离：包级别的表册和跟踪管理器现已移至 `Runtime` 实例中，保留全局包装器以维持向后兼容性
- 条件错误模型：将 `Predicate()` 和相关表达式构造函数从基于 panic 改为基于错误的模型，无效输入返回错误而非崩溃
- 关键词搜索硬化：使用显式标记而非后期追加来处理关键词参数，确保占位符和参数计数对齐
- SQL 渲染器整合：提取共享的 SQL 扫描器逻辑，消除状态机重复
- 分页和排序合约：统一 `Direction`/`Order` 使用，区分 `Validate()`（正常化）和 `ValidateStrict()`（严格检查）
- README 示例同步：文档和代码示例现已与当前 API 对齐
- 追踪管理器并发安全：增强 `TraceManager` 并发安全性以防止追踪器恢复期间的竞态条件

### 修复
- 修复 `Registry.Register()` 和 `Runtime.RegisterTable()` 进行显式 nil 检查，返回结构化错误而非隐式 panic
- 修复 `PageReq.Validate()` 恢复向后兼容的正常化语义（之前过于严格）
- 修复 `Col[T]` 变换检测通过显式 `transformed` 标志而非名称比较
- 修复 `TraceManager.AddTracer()` 和 `AddUnique()` 强制执行追踪器列表上限以防止无限增长
- 修复 `TraceManager.restore()` 中的竞态条件：添加 `restoreMu` 互斥锁以原子化追踪器恢复与并发添加操作

### 向后兼容性
- `PageReq.Validate()` 继续正常化无效值（原有行为）；使用 `ValidateStrict()` 进行严格检查
- 所有公共 API 保持兼容；注册错误现通过返回值而非 panic
- 旧的全局初始化模式继续有效；新代码应使用 `Runtime` 实例
- 所有现有测试（700+）继续通过，验证无回归

### 性能优化
- 提取查询执行前置步骤到 `prepareQueryExecution()` 减少代码重复和分支预测成本
- 添加查询计划缓存指南和最佳实践文档，推荐应用层缓存模式

## [1.1.0] - 2026-04-23

### 新增
- 增加索引自动校验和创建功能，支持 MySQL, SQLite, PostgreSQL
- 增加 `WrapExecutor` 用于跨 DB/TX 的方言透传
- 增加 `MatchByInputOrder` 辅助函数用于结果重排序
- 支持 `InitWithOptions` 进行更灵活的初始化配置
- 增加了 Docker 构建支持
- 增加了 GoReleaser 自动化发布配置
- 完善项目文档结构
- 添加贡献指南和许可证
- 增加项目标准化配置

### 改进
- 增强 SQL 渲染器，支持更复杂的标识符引号和注释保留
- 优化 Tracing 机制，支持全局 Tracer 的快照和恢复
- 改进 CI 工作流，增加冒烟测试和示例自动更新校验
- 优化了 Makefile 的跨平台兼容性
- 增强了 Result 生成器的类型安全
- 优化 README 文档
- 更新项目介绍和使用指南

### 修复
- 修复了某些方言下索引重复创建的问题
- 修复了 tracing 中 reflect 使用的一些潜在问题
- 修复文档链接和格式问题

## [1.0.20] - 2024-XX-XX

### 新增
- 基础的 TSQ 代码生成功能
- 支持 @TABLE、@RESULT、@UX、@KW、@IDX 注解
- 自动生成类型安全的 CRUD 操作
- 分页查询功能
- 复杂查询和子查询支持
- 联表查询功能

### 技术特性
- 支持 SQLite、MySQL、PostgreSQL 数据库
- 编译时类型检查
- 高性能查询生成
- 灵活的查询构建器

### 工具链
- 命令行工具 `tsq gen`
- Go 模板系统
- 代码生成和格式化
- 基础测试框架

## [1.0.0] - 2024-XX-XX

### 新增
- 项目初始版本
- 核心代码生成引擎
- 基本的数据库支持

---

## 版本说明

### 版本类型
- **Major (主版本)**: 不兼容的 API 变更
- **Minor (次版本)**: 向下兼容的功能性新增
- **Patch (修订版本)**: 向下兼容的问题修正

### 变更类型
- **新增**: 新功能
- **改进**: 对现有功能的改进
- **修复**: 问题修复
- **移除**: 移除的功能
- **安全**: 安全相关的修复
- **废弃**: 即将移除的功能

### 贡献指南
如需了解如何贡献变更日志，请参阅 [CONTRIBUTING.md](CONTRIBUTING.md)。 
