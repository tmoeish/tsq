# 项目内存 — 数据库与方言行为

判据与索引在 `../memory.md`。

## 决定：TSQ 的 schema 管理从不删表 (2026-08-28 事故，2026-09-09 v5 定案)

`SchemaPolicyManaged` 靠全库共享的 `_tsq_managed_tables` 记住"我托管过哪些表"，不在当前声明里的
就 DROP，而每个 runtime 用**自己那份表集整个覆盖**它。两个服务共用一个库时来回摧毁对方的表和数据。

**判据**：一份**全局**状态被一个只知道**局部**真相的写入者整个覆盖，就是数据丢失。v5 删掉整档（`SchemaOwner` 补丁也不要），
删表交给迁移，门是 `runtime_schema_isolation_test.go` 加同名集成用例。**已声明的表之内不同**：`Reconcile` 删不再声明的列（维护者 2026-09-22 确认），
也删这张表上**按 TSQ 的推导名**（`ux_<表>_…` / `idx_` / `ft_`）却不再声明的索引（维护者 2026-10-05 定案：改宽唯一键后旧索引还在拦合法的行）；别的名字的索引谁的都可能是，不碰。

## 决定：改 schema 的策略在锁里跑，一个库同时只有一个实例在改 (2026-10-06)

几个实例一起启动（滚动发布、多副本）时各自发现同一张表或同一列缺失，除了第一个都死在"already exists"上（PG 还会撞自己目录表的唯一键）。`lockSchema`：PG 用会话级 advisory lock、MySQL 用 `GET_LOCK`，
**策略的每条语句都在持锁的那个连接上跑**（`schemaDB`）——只有一个连接的池拿不出第二个；SQLite 没有跨语句的锁，同进程用互斥量、从第一次碰库（Ping）就拿，跨进程不保证。`Validate` 不改东西，不加锁。

**MySQL 的持锁连接强制严格模式，池本身不在严格模式只警告不拒绝**（同日）：非严格模式下 `ALTER ... MODIFY` 把放不进新类型的值截掉只留 warning，
"放不进去就拒绝"在那里是假的；改列类型是 TSQ 自己发起的，所以由 TSQ 保证。普通写入被截断是部署选的 `sql_mode` 的语义（老 schema 依赖它），拒绝启动会把它们全挡在外面。
**会话文本不是 UTF-8 就拒绝启动**（同日，和 `loc` 同一类：悄悄存错）：pgx 不设 `client_encoding`，LATIN1 库上 `é` 存成 `Ã©` 还数成两个字符；`SQL_ASCII` 库不转换、不检查。非严格模式只警告是因为那是部署选的语义，这个不是任何人想要的。
**决定（维护者 2026-10-06）：列类型比字段宽的地方加 `CHECK` 守住字段范围**（`sqldialect.RangeCheck`，PG 的无符号、SQLite 除 int64 外的全部整数；MySQL 原生）：`Set(qty, Sub(qty, n))` 曾把负数写进 `uint32` 的列、整行读不出。约束叫 `ck_<列>`，三处要一起认它——列定义（`ColumnDefinitionSQL`）、检查（PG `pg_constraint`、SQLite 解析 `sqlite_master.sql`，进 `Column.Check`）、比较（`SameRangeCheck` 按数值比，PG 会改写表达式）；SQLite 重建的阻断项要先把 `ck_` 剥掉（`sqliteWithoutRangeChecks`）。生成器的迁移假定上一份声明建的表带着它，更早的表靠 `Reconcile`。PG 改类型要**先删它**（第十轮随机迁移找到：`qty >= 0` 按 VARCHAR 解析就报 operator does not exist）。

## "抓住错误继续跑"在 PostgreSQL 的事务里不成立 (2026-08-28)

PG 事务里任一语句失败即 aborted，其后都报 `25P02`：**凡是"捕获错误后继续用同一个连接"的代码，都要问 PG 上还能不能用**（`WithSkipDuplicates` 的修法见 `../impact/write.md` § 改了批量写）。

## 转义值和声明转义符是同一件事的两半 (2026-08-28)

转义了 `%` / `_` 却渲染裸 `LIKE ?`，SQLite 没有默认转义符、搜 `a_b` 零行。**任何"对值做了预处理"的功能都要问"数据库怎么知道"**；门在 `../impact/query.md` § 改了 LIKE 谓词的渲染。

## 同一个 SQLSTATE 在三个驱动里是三个 Go 类型 (2026-08-26)

曾匹配 pgx **v4** 的具体类型，v5 是另一个类型，重试静默失效。**驱动错误分类按接口（`SQLState()`）**（MySQL 例外见 `../impact/runtime.md` § 改了驱动错误分类），集成测试用真实 pgx v5 守着。

## 决定：方言能力位按版本基线表态，否决"版本可配置" (2026-08-26)

能力位曾按 2018 年前的引擎写死。**否决**给方言加 `ServerVersion`：`Build()` 之前不知道会连哪个库，版本只能执行时探测，
方言就得带状态。改成按基线表态（MySQL 8.0、SQLite ≥3.39），更老的引擎拿到数据库报错而不是 `UnsupportedCapabilityError`。
能力表不留 `default` 分支：它把"忘了写"和"决定不支持"变成同一件事，穷尽靠表加遍历表的测试（`dialect.capabilities`）。

## 决定：commit 阶段只对明确冲突码重试 (2026-08-26)

曾一刀切不重试 commit 失败（有歧义），但 PG 的 `40001` **经常在 COMMIT 时才抛**，而这些码（40001 / 40P01 / 55P03、
MySQL 1205 / 1213 / 3572）保证事务已回滚。现在 commit 阶段只放行明确冲突码，网络类错误仍不重试。

## 决定：v5 设计收尾——数据库与方言行为 (2026-09-17)

- **`Dialect` 不是扩展点**（定案）：按方言分叉的拼写（日期、`ROUND`、NULL 排序、全文检索）按方言名写在
  库里，第四种方言会在这些构造上报错。否决把它们搬进接口：那等于把没有测试的代码放进公开契约。
- **PG 索引自省曾看不见表达式索引**（列号 0，内连接 `pg_attribute` 丢行，GIN 每次启动报 42P07）；现在 `LEFT JOIN`。
- **全文检索三个方言不是一回事**：MySQL `MATCH ... AGAINST`、PG `to_tsvector @@ plainto_tsquery`、SQLite
  退化成子串匹配（FTS5 要影子表和触发器）。排序和操作符不可移植，只有 `Capability` 说得清拿到哪一种。
  `TableIndex` 加字段记得 `cloneTableIndex`：曾逐字段复制，`FullText` 标记就在那里丢过。**MySQL 自然语言模式下 `*` 没有含义却仍是记号**
  （2026-10-08 随机搜索词差分）：单独、空白后、短语后的 `*` 是 1064 语法错，其余运算符字符是普通文本；`matchAgainst` 包 `REPLACE(?, '*', '')`，参数仍是常量、索引照用。
- **存在的生成列不参与 schema 对账，缺失的算缺列**（2026-10-06）：SQLite 的 `table_info` 不列生成列，曾每次启动都再 ADD（duplicate column），于是整个排除在比较之外，结果缺了声明的生成列的表 `Validate` 也放行、读它才报 no such column。现在自省读 `table_xinfo`，只比"在不在"；表达式三个引擎各报各的，仍不比。
- **MySQL 的 `Index.Constraint` 指"外键需要的索引"**（删它报 1553）：别改成读 `TABLE_CONSTRAINTS`，那里把每个唯一索引都列成
  UNIQUE 约束，TSQ 自己建的也在内，Reconcile 就再也不能重建任何唯一索引。
- **NULL 排序默认最小值**：MySQL/SQLite 本来如此只需改 PG；换默认就得给 MySQL 每个可空排序加 `IS NULL` 键。
- **时间在绑定出口统一转 UTC 并截到微秒（`boundTime`），不只是托管时间戳**：SQLite 按文本存时间，本地时间和 UTC 行按文本比较会错；带纳秒的值 SQLite 原样存、MySQL 四舍五入、PG 截断，同一个谓词三个答案（2026-10-05）。
- **SQLite 的 `INTEGER PRIMARY KEY` 不写 `AUTOINCREMENT` 也算自增**（2026-09-28）：它就是 rowid；当成漂移会让 `Validate` 起不来、
  `Reconcile` 为使用者自己的选择重建整张表。TSQ 自己建的表仍写 `AUTOINCREMENT`，重建时保留它的计数。

## 只有真实引擎说得出的 schema 行为 (2026-10-05，第五、六轮审计)

前四轮只能推理，第一次真跑就找出一串推理看不见的事；**改 schema 路径要真跑"类型 × 引擎 × 策略"的矩阵**（`internal/integration/schema_test.go`）。

- **PG 改类型的 `USING` 要按"源 × 目标"逐对想**：`c::VARCHAR(5)` 静默截短（第四轮只修同类、第五轮改成 `c::TEXT`），而 `bytea::TEXT` 是十六进制拼法（`abc` → `\x616263`）、
  `text::BYTEA` 按转义串读（`\101` → `A`）——第五轮的修法落在 bytea 源上又是一次静默改写（第六轮 P1）。字节与文本走 `convert_from` / `convert_to`。
- **MySQL 的时间字面量只在 TIMESTAMP 范围内才能带时区**：`'0001-01-01 00:00:00+00:00'` 在默认的 `time_zone=SYSTEM` 下**静默存成 `0000-00-00`**，此后每条复制表的 ALTER 都失败；零值字面量因此分方言（`ZeroLiteral`）。
- **决定（2026-10-05，维护者）：类型和默认值先按文本比，文本说不一样再问引擎**（`ProbeColumn` → `AdoptSpelling`）。别名表补了三轮仍漏（`DECIMAL(10)`、`INT[]`、`(1+1)`、带反斜杠的字面量）；
  按声明在**临时表**里建这一列读回拼法，与库里那一列一致即同一个东西。**否掉永久探测表**（`Validate` 下也要跑、进 binlog、崩了留表）。**比较的两边要出自同一个读法**：MySQL 的临时表不走数据字典，拿它和
  `information_schema` 比，表达式默认值多一层括号、二进制字面量和 4 字节字符各是各的拼法，逐个还原是又一张别名表；改成库里那一列也按 `SHOW CREATE TABLE` 的写法建进临时表（2026-10-06）。
- **决定（2026-10-05，维护者）：改类型时三个方言给同一个结果——能转的转，转不了的拒绝**（`sqldialect.SQLiteRetype*`）。SQLite 什么值都存，原样复制后 `Validate` 通过而整张表读不出来。小数取整、数值转布尔写进复制表达式；文本转数值 / 布尔 / 时间
  运行期在**提交前**按 `typeof` 检查并回滚，生成器写成手工注释（脚本的执行者不会停，见 `codegen.md`）。**否掉"先改名旧表 + `INSERT OR ROLLBACK` 守卫"的重排**：要 `legacy_alter_table`，动的是出过 P0 的重建顺序。 **随机表结构差分**（2026-10-06，第八轮：随机列 / 默认值 / 索引建表、随机改动后 `Reconcile`、再启动零 DDL，400 × 3 引擎）只剩三件事：PG 的布尔默认值 `1` / `0`（`DefaultSQL` 改拼法）、PG 带默认值的列跨类型改动要先 `DROP DEFAULT`、MySQL 带索引的列改 BLOB 要先删不再声明的索引（`dropIndexesInTheWay`）。PG 拒绝数字 / 布尔 / 时间 ↔ bytea / 时间之间的改动是对的，不补 `USING`。
- **决定（维护者 2026-10-05）**：运行期策略给有数据的表加 NOT NULL 列也补零值，与生成器共用 `AddColumnSQL`（带零值默认加列再去掉，SQLite 没有 `DROP DEFAULT` 所以重建）。PG 的无符号自增主键是加宽类型的 SERIAL，`uint64` 是 `BIGSERIAL`。
- **决定（维护者 2026-10-05）：`Open` 拒绝 `loc` 不是 UTC 的 MySQL DSN**，否掉"只写文档"：`loc=Local` 是教程里的标准写法，而它让数据库填的 UTC 时间读回来差一个时区、不报错。`NewRuntime` 看不到 DSN，只能靠文档。
