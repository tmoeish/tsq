# 代码地图

"X 在哪实现的？" 先查这里，再去 grep。

## 根包：查询构建

| 关注点 | 文件 |
| --- | --- |
| 阶段接口、具体 builder 类型、`Select` / `From` 入口 | `querybuilder.go` |
| 每个阶段能返回什么（阶段转移表） | `querybuilder_stages.go` |
| 共享状态、join、条件累积 | `querybuilder_core.go` |
| `Build()` 的各阶段实现 | `querybuilder_stages.go`（末尾） |
| 集合运算 UNION / INTERSECT / EXCEPT | `querybuilder_setops.go` |
| `ForUpdate` / `ForShare` / NOWAIT / SKIP LOCKED | `querybuilder_lock.go` |
| CTE 声明 | `cte.go`、`query_plan_cte.go` |
| 执行入口（`Load` / `List` / `Page` 的 builder 侧） | `querybuilder_exec.go` |

## 根包：列、条件、表达式

| 关注点 | 文件 |
| --- | --- |
| 列接口与绑定列 | `column.go`、`column_impl.go` |
| 投影列（`Expr` / `Exprf` / 别名） | `column_projection.go` |
| 条件与 `And` / `Or` | `condition.go` |
| 列上的谓词（`EQ` / `GTE` / `StartsWith` / `InVar` …） | `predicate_column.go` |
| 子查询谓词与 `Subquery[T]` | `predicate_subquery.go`、`subquery.go` |
| RHS 抽象（列 vs 字面量 vs 占位符 vs 子查询） | `rhs.go` |
| SQL 函数、聚合、`CASE` | `function.go` |
| 表达式与类型化表达式 | `expression.go` |
| `ORDER BY` | `order.go` |
| 分页 `PageRequest` / `Validate` / `Offset` | `paging.go` |

## 根包：计划、渲染、执行

| 关注点 | 文件 |
| --- | --- |
| 执行计划结构 | `query_plan.go` |
| 计划涉及哪些表 | `query_plan_tables.go` |
| CTE 排序与去重 | `query_plan_cte.go` |
| SQL 文本拼装 | `query_plan_sql.go`、`sql_render.go` |
| 参数绑定 | `query_args.go` |
| 结构校验（`Build()` 时） | `query_validation.go`、`query_plan_validate.go`、`validation.go` |
| 方言能力校验（执行时） | `query_validation.go` 的 `detectSQLCapabilities`，经 `dialect.ValidateCapability` |
| 标识符校验（长度与字符集） | `dialect_validation.go`（这个文件**只剩**标识符校验，能力校验不在这儿） |
| 查询对象与执行（含泛型 `Query.Scalar`） | `query.go`、`query_load.go`、`query_scalar.go`、`query_scan.go` |
| 执行器接口与包装 | `executor.go`、`executor_wrap.go`、`sql_executor.go` |
| 写操作（Insert / Update / Delete / Upsert） | `executor_mutation.go`、`executor_mutation_meta.go` |
| 分批写（`ChunkedInsert` / `ChunkedUpdate` / `ChunkedDelete`） | `query_chunked.go`（chunk 大小按方言的绑定参数上限换算，见 `change-impact.md`） |
| 按条件写（`UpdateTable` / `DeleteFrom`、`MutationStage`、`Mutation.Exec`） | `mutation.go`（`mutation_test.go` 守语句形状、参数顺序与 `version` 自增；`integration_test.go` 的 `TestIntegrationMutationsByCondition` 在三方言上真跑） |

## 根包：运行时

| 关注点 | 文件 |
| --- | --- |
| `NewRuntime`、`Options`、`SQLExecutor` 实现 | `runtime.go` |
| schema 对账（`TablePolicy` / `IndexPolicy`） | `runtime_schema.go` |
| 执行期日志与 SQL 日志 | `runtime_schema.go` 的 `logForExecutor` / `logSQLForExecutor` / `compactJSON` |
| 事务与重试（`WithTx`、`WithTxResult`、`TxOptions`、`TxRetryConfig`） | `tx.go` |
| 表注册与元数据 | `table.go`、`table_registry.go` |
| 索引元数据 | `table_index.go` |
| 表别名 | `table_alias.go` |
| `Owner` 约束 | `owner.go` |
| 追踪钩子 | `trace.go` |
| SQLite 错误映射 | `sqlite_errors.go` |
| PostgreSQL 错误映射（`SQLState()` 接口，覆盖 pq / pgx v4 / pgx v5） | `postgres_errors.go` |
| 真实 MySQL / PostgreSQL 集成测试 | `integration_test.go`（`package tsq_test`，env DSN 驱动） |
| 命名转换（snake / camel） | `case.go` |

## 方言

| 关注点 | 文件 |
| --- | --- |
| `Dialect` 接口、`Capability` 枚举、`ErrUnsupportedCapability` | `dialect/dialect.go` |
| MySQL | `dialect/mysql.go` |
| PostgreSQL | `dialect/postgres.go` |
| SQLite | `dialect/sqlite.go` |

## 生成器

| 关注点 | 文件 |
| --- | --- |
| CLI 入口、子命令注册 | `cmd/tsq/main.go` |
| `tsq version`（默认表格 / `--short` / `--json`） | `internal/cmd/version.go` |
| `tsq fmt` | `internal/cmd/fmt.go` |
| `tsq gen`（flag、校验、渲染、写盘） | `internal/cmd/gen.go` |
| 模板 | `internal/cmd/tsq.go.tmpl`、`tsq_result.go.tmpl`、`tsq_runtime.go.tmpl` |
| 模板辅助函数 | `internal/cmd/template_helpers.go` |
| 渲染用的数据结构 | `internal/cmd/generation_model.go` |
| DDL 类型推导与渲染 | `internal/cmd/ddl_render.go` |
| DDL 快照（`tsq.json`） | `internal/cmd/ddl_state.go` |
| 版本号 | `internal/buildinfo/buildinfo.go` |

## 解析器

| 关注点 | 文件 |
| --- | --- |
| 包级遍历、`ParseResult` | `internal/parser/package.go` |
| 结构体解析、import 别名消歧 | `internal/parser/struct.go` |
| 字段解析、tag 解析 | `internal/parser/field.go` |
| 注解定位、DSL 提取、错误行号映射 | `internal/parser/tableinfo.go` |
| DSL 词法与语法分析 | `internal/parser/dsl.go` |
| 注解排版规范化（`tsq fmt` 的核心） | `internal/parser/format.go` |
| 常量、默认字段名 | `internal/parser/constants.go` |
| 解析错误类型 | `internal/parser/errors.go` |
| 中立数据模型 | `internal/genmodel/model.go` |

## 示例

| 关注点 | 文件 |
| --- | --- |
| 示例 schema 真相源（手写） | `examples/academy/mock.sql` |
| 表结构体与注解 | `examples/academy/{course,track,learner,instructor,enrollment}.go` |
| `@RESULT` 投影 | `examples/academy/learningjourney.go` |
| 嵌入基表（`ImmutableTable` 等） | `examples/academy/base.go` |
| 运行时装配 | `examples/academy/bootstrap.go` |
| 可复用场景 | `examples/academy/scenarios.go` |
| 三个可运行程序 | `examples/{quickstart,advanced,full-suite}/main.go` |

## harness

| 关注点 | 文件 |
| --- | --- |
| 本波变更范围的唯一定义 | `script/changeset.py` |
| 内存与提交信息门禁 | `script/check_change_log.py` |
| 技能同步触发表 | `script/check_skills.py` |
| 生成物同步 | `script/check_generated.py` |
| 对外 Go 契约快照 | `script/check_api_surface.py` |
| 版本号一致性 | `script/check_release.py` |
| 版本号读写 | `script/version.py` |
| 发版 | `script/release.py` |
| git 钩子安装 | `script/install_hooks.py` |
