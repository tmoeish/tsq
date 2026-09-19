# 代码地图

"X 在哪实现的？" 先查这里，再去 grep。

## 根包：查询与表达式

| 关注点 | 文件 |
| --- | --- |
| 阶段接口、`builder`、`Select` / `From`、`Build`、builder 上的执行入口 | `querybuilder.go`（类型约束由 `compilefail_test.go` 守） |
| `querySpec`：结构校验、FROM/JOIN 图、`Correlate`、集合操作、CTE 收集与排序、按方言渲染 | `query_render.go` |
| `Query`：渲染缓存、绑参、`List` / `ListIn`（`checkSplittable`、`exprInfo.inList`）/ `Iter` / `Get` / `Find` / `Exists` / `Count` / `Scalar` / `Page`（`snapshotRead`）、`SQL()`、子查询 | `query.go`（`exec_test.go` 的 `TestIterStreamsRowsAndStops`、`TestPageInsideATransactionUsesIt`；快照一致性三方言真跑在 `TestIntegrationPageReadsOneSnapshot`） |
| 中间表示：片段、`renderer`、`statement`、`assemble`、按方言分叉的片段 | `sqlexpr.go`（`render_test.go` 按三方言断言输出） |
| 参数：`Param` / `ListParam` / `Arg` / `Keyword`、绑定校验、空列表渲染、LIKE 转义 | `param.go`（`build_test.go` 守绑定规则） |
| `Expression` / `Column` / `NullColumn` 的接口与实现、谓词、`Expr` / `Pred`、`NewNullColumn`、`MapInto` / `MapIntoNull`、可空形态识别 | `column.go`（`nullable_test.go`、`compilefail_test.go`） |
| 单值查询 `SelectValue` / `SelectNullValue` | `querybuilder.go`（`exec_test.go` 的 `TestSelectValueReadsOneExpression`） |
| 可空性推导（`exprInfo.null` / `nullness`）、外连接可选表、读行前检查（`checkScanTargets`） | `expr.go`、`query_render.go`、`query.go`（`nullable_test.go`；三方言真跑在 `TestIntegrationNullableColumns`） |
| 固定值 `Val` / `Vals`（从不为 NULL，NULL 用 `SetNull` 写；`Value` 和 `Param` 共同实现模式函数的 `Pattern[S]`） | `values.go`、`param.go`（`values_test.go`；模式转义三方言真跑在 `TestIntegrationKeywordSearchEscapesWildcards`） |
| 包级类型约束函数（`Text` / `Number`、聚合、字符串、数值、日期、`Coalesce` / `NullIf`、`StartsWith` 等模式函数、`Searchable`） | `functions.go`（`compilefail_test.go` 守约束；`internal/integration` 的 `TestIntegrationColumnFunctionsArePortable` 三方言真跑） |
| `Condition`、`And` / `Or` / `Not`、`Exists` / `NotExists`、`exprInfo` | `expr.go` |
| `CASE` | `case.go` |
| `ORDER BY` 方向解析、NULL 排序位置（`NullsFirst` / `NullsLast`、`orderTerm.render`） | `order.go`、`query_render.go`（`order_test.go`；三方言真跑在 `TestIntegrationNullOrderingAgrees`） |
| 游标分页 `Keyset` / `KeysetPage` / `PageKeyset`、游标编解码与指纹、seek 条件 | `keyset.go`（`keyset_test.go`；时间值游标三方言真跑在 `TestIntegrationPageKeysetOverTimestamps`） |
| 分页 `Paging` / `Page`、HTTP 形态 `PageRequest`（`Paging(sortable...)` / `Keyset` 同时校验）、排序字段错误类型 | `paging.go`（`exec_test.go` 的 `TestPageSearchesSortsAndCounts`） |
| 软删除作用域（`WithDeleted`、`liveRows` / `liveSource`、JOIN 位置规则） | `table.go` + `query_render.go` 的 `writeFromWhere`（`exec_test.go` 的 `TestSoftDeleteScope`；`internal/integration` 的 `TestIntegrationSoftDeleteScopeJoins`） |

## 根包：表与写入

| 关注点 | 文件 |
| --- | --- |
| `TableOf` / `NewTable` / `Define`、`Table` 接口、别名、CTE、`debugSQL` | `table.go` |
| 按主键 / 唯一列读取（`Get` / `Find` / `Fetch` / `FetchBy` / `Query()`、排序规则兜底） | `lookup.go`（`lookup_test.go`；`RowTable` 推断也在那里测） |
| 写入路径基准（批量 INSERT / UPDATE 的语句构建） | `write_bench_test.go` |
| 行写入与批量写、托管时间戳、数据库填值列（`Fill`、`insertColumns`、`reloadColumns`）、软删除与 `Restore`（`setTombstone`）、`TableOf.BatchDeleteByPK`、`WithSkipDuplicates` | `rows.go`（`exec_test.go` 端到端；`batch_test.go` 宽表分批；`timestamps_test.go` 托管字段类型；`softdelete_test.go` 行级软删除与恢复） |
| Upsert（`TableOf.Upsert` / `BatchUpsert`、键解析、MySQL 多唯一键拒绝、主键回读） | `upsert.go`（`exec_test.go` 的 `TestUpsertMatchesLiveRowsOfASoftDeletedUniqueIndex`；`internal/integration` 的 `TestIntegrationUpsert` 三方言真跑） |
| 按条件写（`UpdateTable` / `DeleteFrom` / `HardDeleteFrom`、`Mutation`） | `mutation.go`（`exec_test.go`；`internal/integration` 的 `TestIntegrationMutationsByCondition` 三方言真跑） |
| 错误类型 `OptimisticLockError` | `errors.go` |
| 关联装配 `AttachMany` / `AttachOne`（父键收集、按键分组、走 `ListIn`） | `attach.go`（`attach_test.go`；三方言真跑在 `TestIntegrationAttachLoadsChildrenInOneQuery`） |
| 全文检索（`//tsq:fulltext`、`TableOf.FullText`、`Matches`、三方言渲染） | `fulltext.go` + `internal/sqldialect/*.go` 的 `FullTextIndexSQL` / `FullTextVectorSQL`（`fulltext_test.go`；三方言真跑在 `TestIntegrationFullTextSearch`） |
| 表注册、`SchemaPolicy`、`MissingTableError` / `MissingIndexError`、`Logger` | `schema.go` |
| 索引策略执行 | `table_index.go` |

## 根包：运行时与执行器

| 关注点 | 文件 |
| --- | --- |
| 封闭的 `Executor`、`execScope`、`WrapExecutor`、`DBTX` | `executor.go` |
| `Open` / `NewRuntime`、连接池所有权、标识符校验 | `runtime.go`（选项在 `runtime_options.go`） |
| schema 对账、执行期日志与 SQL 日志 | `runtime_schema.go` |
| 事务与重试（`WithTx`、`WithTxResult`、`TxOption` 与 `WithRetry` 等、`RetryPolicy`、错误谓词） | `tx.go`（`tx_test.go`） |
| 追踪钩子 | `trace.go` |
| SQLite / PostgreSQL / MySQL 错误映射 | `sqlite_errors.go`、`postgres_errors.go`、`mysql_errors.go`（反射读 `*mysql.MySQLError`，不 import 驱动） |
| 杂项（`isNilValue`、标识符校验、重复键判断、谓词值校验） | `util.go` |
| 真实 MySQL / PostgreSQL 集成测试、MySQL 错误分类 | `internal/integration/integration_test.go`（只用导出 API，env DSN 驱动；放在根包外是为了不把驱动和 nullbio 带进使用者的 `go.sum`） |
| 测试夹具（`Users` / `Orders` 表、`newSQLite`、`wideTable`） | `fixtures_test.go` |

## 方言

| 关注点 | 文件 |
| --- | --- |
| 公开的方言名、`Capability` 表、`Supports` / `Check`、`UnsupportedCapabilityError` | `dialect/dialect.go` |
| 生成代码用的列描述（`ColumnSpec`、`ColumnType`、`Kind*`、`Fill*`） | `dialect/schema.go` |
| `Dialect` 接口、`For(name)`、`Index`、`MaxBindParams`、DDL 渲染 | `internal/sqldialect/dialect.go` |
| MySQL | `internal/sqldialect/mysql.go` |
| PostgreSQL | `internal/sqldialect/postgres.go` |
| SQLite | `internal/sqldialect/sqlite.go` |

## 生成器

| 关注点 | 文件 |
| --- | --- |
| CLI 入口、子命令注册 | `cmd/tsq/main.go` |
| `tsq version`（默认表格 / `--short` / `--json`） | `internal/cmd/version.go` |
| `tsq gen`（flag、校验、渲染、写盘） | `internal/cmd/gen.go` |
| 模板 | `internal/cmd/table.go.tmpl`、`result.go.tmpl`、`runtime.go.tmpl` |
| 生成结构体的保留字段名（与 `TableOf` 方法撞名的检查） | `internal/cmd/reserved.go`（`reserved_test.go`） |
| 模板辅助函数 | `internal/cmd/template_funcs.go` |
| 生成文件清单与增量写盘计划 | `internal/cmd/generation_plan.go` |
| DDL 类型推导与渲染 | `internal/cmd/ddl_render.go` |
| DDL 快照（`tsq.json`） | `internal/cmd/ddl_state.go` |
| 版本号 | `internal/buildinfo/buildinfo.go` |

## 解析器

| 关注点 | 文件 |
| --- | --- |
| 包级遍历、`ParseResult` | `internal/parser/package.go` |
| 结构体解析、import 别名消歧 | `internal/parser/struct.go` |
| 字段解析、tag 解析 | `internal/parser/field.go` |
| `//tsq:` 指令解析、索引命名、查询派生 | `internal/parser/directive.go` |
| 常量、默认字段名 | `internal/parser/constants.go` |
| 解析错误类型 | `internal/parser/errors.go` |
| 中立数据模型 | `internal/genmodel/model.go` |

## 示例

| 关注点 | 文件 |
| --- | --- |
| 示例 schema 真相源（手写） | `examples/academy/mock.sql` |
| 表结构体与注解 | `examples/academy/{course,track,learner,instructor,enrollment}.go` |
| `//tsq:result` 投影 | `examples/academy/learningjourney.go` |
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
