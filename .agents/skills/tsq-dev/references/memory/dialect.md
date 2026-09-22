# 项目内存 — 数据库与方言行为

判据与索引在 `../memory.md`。

## 决定：TSQ 的 schema 管理从不删表 (2026-08-28 事故，2026-09-09 v5 定案)

`SchemaPolicyManaged` 靠全库共享的 `_tsq_managed_tables` 记住"我托管过哪些表"，不在当前声明里的
就 DROP，而每个 runtime 用**自己那份表集整个覆盖**它。两个服务共用一个库时来回摧毁对方的表和数据。

**判据**：一份**全局**状态被一个只知道**局部**真相的写入者整个覆盖时，覆盖就是数据丢失。问题不在
DROP 那段逻辑（它按自己的记账是对的），在记账的**范围**和写入者的范围不一致。

v4 的补丁是给记账加 owner 维度（`SchemaOwner`）。**v5 把整档删掉**：runtime 分不清"这张表不该存在了"
和"这张表是别人的"，删表交给迁移脚本；门是 `runtime_schema_isolation_test.go` 加同名集成用例。**列不同**：
`Reconcile` 删不再声明的列是有意的（原型期测试库跟着代码走，生产用 `Manual`，维护者 2026-09-22 确认）。
2026-09-20 的审计按"只增不减"的旧措辞把它判成了缺陷，**别照那个措辞去掉它**；门是
`TestReconcileDropsUndeclaredColumns`。

## "抓住错误继续跑"在 PostgreSQL 的事务里不成立 (2026-08-28)

`WithSkipDuplicates` 抓到重复键就 `continue`，在 **PostgreSQL 事务里必然失败**：任何语句失败后事务
即 aborted，其后语句都报 `25P02`，调用方拿到的错误还不是重复键错误。

**判据**：错误处理策略的可移植性取决于"失败之后连接还剩下什么状态"，三个数据库答案不一样。
凡是"捕获错误后继续用同一个连接"的代码，都要问一句 PG 上还能不能用。

修法（事务内每行 savepoint、事务外不发、为什么不用 `INSERT IGNORE` / `DO NOTHING`）见
`../impact/write.md` § 改了批量写。

## 转义值和声明转义符是同一件事的两半，只做前一半是静默错误 (2026-08-28)

`escapeKeywordSearch` 一直在转义 `%` / `_`，但渲染出来的谓词是裸 `LIKE ?`。**SQLite 没有
默认的 LIKE 转义字符**，转义前缀于是变成普通字符，搜 `a_b` 在 SQLite 上返回零行；
MySQL / PostgreSQL 默认转义字符恰好是反斜杠才侥幸正确——而单元测试只跑 SQLite，两侧的
端到端关键字用例都是零，所以谁都没发现。**固定进 SQL 文本的子句要三个方言都验。**

转义字符选 `~` 不选反斜杠的硬约束见 `../impact/query.md` § 改了 LIKE 谓词的渲染。

**引申**：任何"我们对值做了预处理"的功能都要问一句"数据库怎么知道"——值被改了而契约没被
声明时，行为由各方言的默认值决定，而默认值本来就不一样。

## 同一个 SQLSTATE 在三个驱动里是三个 Go 类型 (2026-08-26)

曾匹配 pgx **v4** 的 `*pgconn.PgError`；v5 的是另一个类型，重试和 `WithSkipDuplicates` 静默失效，而
单测 fixture 恰好也是 v4。修法是匹配接口 `interface{ SQLState() string }`（pq / pgx v4 / pgx v5 都实现）。
**驱动错误分类永远按接口，不按具体类型**（MySQL 例外见 `../impact/runtime.md` § 改了驱动错误分类）；集成测试用真实 pgx v5 守着。

## 决定：方言能力位按版本基线表态，否决"版本可配置" (2026-08-26)

能力位曾按 2018 年前的引擎写死，README 忠实复述了这些错误。考虑过给 `MySQLDialect` 加
`ServerVersion` 字段，**否决**：`Build()` 之前根本不知道会连哪个库，版本只能执行时探测，
那就得每个 `Dialect` 值带状态，和"方言是无状态值类型"冲突。改成按基线表态：MySQL 8.0、
SQLite ≥3.39。代价是更老的引擎拿到数据库报错而不是 `UnsupportedCapabilityError`。

## 决定：commit 阶段只对明确冲突码重试 (2026-08-26)

原来一刀切 `stage != commit`，理由是 commit 失败有歧义。但 PG 的 `40001` **经常在 COMMIT 时
才抛**，且这些码（40001 / 40P01 / 55P03、MySQL 1205 / 1213）保证事务已回滚——PG 最典型的
重试场景被一刀切排除了。现在 commit 阶段只放行明确冲突码，网络类错误仍不重试。

## 能力位的 `default` 分支是那道门自己的漏洞 (2026-08-26)

规则写着"新增能力位三个方言都要显式表态"，但三个 `SupportsCapability` 都是 `switch` 加
`default: return false`——漏掉一个方言不编译失败、不 lint 失败、不测试失败，只静默变成
"不支持"。现在是公开 `dialect` 包里每方言一张表加一个遍历表的测试。

**引申，对所有"必须穷尽"的 switch 都成立**：`default` 分支把"忘了写"和"决定不支持"变成
同一件事，而这两件事需要不同的处理。要穷尽性就别给它兜底分支——用表加一个遍历表的测试。

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
