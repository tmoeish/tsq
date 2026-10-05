# 项目内存 — 数据库与方言行为

判据与索引在 `../memory.md`。

## 决定：TSQ 的 schema 管理从不删表 (2026-08-28 事故，2026-09-09 v5 定案)

`SchemaPolicyManaged` 靠全库共享的 `_tsq_managed_tables` 记住"我托管过哪些表"，不在当前声明里的
就 DROP，而每个 runtime 用**自己那份表集整个覆盖**它。两个服务共用一个库时来回摧毁对方的表和数据。

**判据**：一份**全局**状态被一个只知道**局部**真相的写入者整个覆盖，就是数据丢失。v5 删掉整档（`SchemaOwner` 补丁也不要），
删表交给迁移，门是 `runtime_schema_isolation_test.go` 加同名集成用例。**列不同**：`Reconcile` 删不再声明的列是有意的（维护者
2026-09-22 确认），别照 2026-09-20 审计"只增不减"的措辞去掉它；门是 `TestReconcileDropsUndeclaredColumns`。

## "抓住错误继续跑"在 PostgreSQL 的事务里不成立 (2026-08-28)

PG 事务里任一语句失败，事务即 aborted，其后语句都报 `25P02`。**凡是"捕获错误后继续用同一个连接"的代码，都要问
PG 上还能不能用**；`WithSkipDuplicates` 的修法见 `../impact/write.md` § 改了批量写。

## 转义值和声明转义符是同一件事的两半，只做前一半是静默错误 (2026-08-28)

转义了 `%` / `_` 却渲染裸 `LIKE ?`：SQLite 没有默认转义符，搜 `a_b` 返回零行，另两个方言靠默认反斜杠侥幸正确。
**任何"对值做了预处理"的功能都要问"数据库怎么知道"**；约束和门在 `../impact/query.md` § 改了 LIKE 谓词的渲染。

## 同一个 SQLSTATE 在三个驱动里是三个 Go 类型 (2026-08-26)

曾匹配 pgx **v4** 的 `*pgconn.PgError`；v5 的是另一个类型，重试和 `WithSkipDuplicates` 静默失效，而
单测 fixture 恰好也是 v4。修法是匹配接口 `interface{ SQLState() string }`（pq / pgx v4 / pgx v5 都实现）。
**驱动错误分类永远按接口，不按具体类型**（MySQL 例外见 `../impact/runtime.md` § 改了驱动错误分类）；集成测试用真实 pgx v5 守着。

## 决定：方言能力位按版本基线表态，否决"版本可配置" (2026-08-26)

能力位曾按 2018 年前的引擎写死。**否决**给方言加 `ServerVersion`：`Build()` 之前不知道会连哪个库，版本只能执行时探测，
方言就得带状态。改成按基线表态（MySQL 8.0、SQLite ≥3.39），更老的引擎拿到数据库报错而不是 `UnsupportedCapabilityError`。

## 决定：commit 阶段只对明确冲突码重试 (2026-08-26)

曾一刀切不重试 commit 失败（有歧义），但 PG 的 `40001` **经常在 COMMIT 时才抛**，而这些码（40001 / 40P01 / 55P03、
MySQL 1205 / 1213 / 3572）保证事务已回滚。现在 commit 阶段只放行明确冲突码，网络类错误仍不重试。

## 能力位的 `default` 分支是那道门自己的漏洞 (2026-08-26)

`default: return false` 把"忘了写"和"决定不支持"变成同一件事；**要穷尽就用表加遍历表的测试**（`dialect.capabilities`）。

## 决定：v5 设计收尾——数据库与方言行为 (2026-09-17)

- **`Dialect` 不是扩展点**（定案）：按方言分叉的拼写（日期、`ROUND`、NULL 排序、全文检索）按方言名写在
  库里，第四种方言会在这些构造上报错。否决把它们搬进接口：那等于把没有测试的代码放进公开契约。
- **PG 索引自省曾看不见表达式索引**（列号 0，内连接 `pg_attribute` 丢行，GIN 每次启动报 42P07）；现在 `LEFT JOIN`。
- **全文检索三个方言不是一回事**：MySQL `MATCH ... AGAINST`、PG `to_tsvector @@ plainto_tsquery`、SQLite
  退化成子串匹配（FTS5 要影子表和触发器）。排序和操作符不可移植，只有 `Capability` 说得清拿到哪一种。
  `TableIndex` 加字段记得 `cloneTableIndex`：曾逐字段复制，`FullText` 标记就在那里丢过。
- **生成列不参与 schema 对账**：SQLite 的 `table_info` 不列它，每次启动都会再 ADD（duplicate column）。
- **MySQL 的 `Index.Constraint` 指"外键需要的索引"**（删它报 1553）：别改成读 `TABLE_CONSTRAINTS`，那里把每个唯一索引都列成
  UNIQUE 约束，TSQ 自己建的也在内，Reconcile 就再也不能重建任何唯一索引。
- **NULL 排序默认最小值**：MySQL/SQLite 本来如此只需改 PG；换默认就得给 MySQL 每个可空排序加 `IS NULL` 键。
- **时间在绑定出口统一转 UTC，不只是托管时间戳**：SQLite 按文本存时间，本地时间和 UTC 行按文本比较会错。

## 决定：SQLite 的 `INTEGER PRIMARY KEY` 不写 `AUTOINCREMENT` 也算自增 (2026-09-28)

它就是 rowid，库会分配键；`AUTOINCREMENT` 只多保证"已删行的键不再发"。把手写表当漂移会让 `Validate` 起不来、
`Reconcile` 为一个使用者自己的选择重建整张表。TSQ 自己建的表仍写 `AUTOINCREMENT`，重建时也保留它的计数。
