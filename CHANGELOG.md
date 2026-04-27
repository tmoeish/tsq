# 变更日志

本文档记录了 TSQ 项目的所有重要变更。

**注意：** 本项目曾误发过一系列 `v1.0.x` 版本。为了纠正版本混乱，我们已撤回 `v1.0.20` 及其之前的所有版本，并正式从 `v1.1.0` 开始新的迭代。

格式基于 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.0.0/)，
项目遵循 [语义化版本控制](https://semver.org/lang/zh-CN/)。

## [Unreleased]

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
- 增强了 DTO 生成器的类型安全
- 优化 README 文档
- 更新项目介绍和使用指南

### 修复
- 修复了某些方言下索引重复创建的问题
- 修复了 tracing 中 reflect 使用的一些潜在问题
- 修复文档链接和格式问题

## [1.0.20] - 2024-XX-XX

### 新增
- 基础的 TSQ 代码生成功能
- 支持 @TABLE、@DTO、@UX、@KW、@IDX 注解
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