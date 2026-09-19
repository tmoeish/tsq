# 项目内存

这个文件被提交，所以每台机器、每个 agent 共享同一份记忆。**和它描述的代码在同一波里更新它。**

只写仓库本身讲不出来的东西：事故及其根本原因、决定及其推理、不明显的运行时行为、值得不再重复的死胡同。不要复述规则（`AGENTS.md` 拥有）、包布局（`architecture.md` 拥有）或使用者契约（`skills/tsq` 拥有）——重复的陈述注定漂移。按主题归入已有小节，不要按日期另起一条。**就地纠正错误的条目、删除陈旧的条目，永远不要追加自相矛盾的内容**：过时的内存比没有内存更糟，人会相信它。

## 一条记录什么时候该走

这个文件被每一个 agent 加载进上下文，长度是**每次会话都要付的成本**；但"处理完就删"也是错的，
有些记录的价值恰恰在问题消失之后才开始——它解释了代码为什么长这样。

**判据只有一条：删掉它之后，有人会不会重犯、或者重新调查一遍？**

| 类型 | 事情了结之后 |
| --- | --- |
| **搁置项**（`已知未处理：` 开头） | **删掉**，问题没了记录就没有读者 |
| **事故 + 根因** | **留**，有门禁挡着了就压成一行指向那道门 |
| **决定 + 理由**、**死胡同** | **永久留**，删了下一个人会改回去或重试一遍 |

## 数据库与方言行为

### 决定：TSQ 的 schema 管理只增不减 (2026-08-28 事故，2026-09-09 v5 定案)

`SchemaPolicyManaged` 靠全库共享的 `_tsq_managed_tables` 记住"我托管过哪些表"，不在当前声明里的
就 DROP，而每个 runtime 用**自己那份表集整个覆盖**它。两个服务共用一个库时来回摧毁对方的表和数据。

**判据**：一份**全局**状态被一个只知道**局部**真相的写入者整个覆盖时，覆盖就是数据丢失。问题不在
DROP 那段逻辑（它按自己的记账是对的），在记账的**范围**和写入者的范围不一致。

v4 的补丁是给记账加 owner 维度（`SchemaOwner`）。**v5 把整档删掉**：一个 runtime 分不清"这张表
不该存在了"和"这张表是别人的"，所以删表根本不该是启动期的决定。保留 `Manual` / `Validate` /
`CreateMissing` / `Reconcile`——`Reconcile` 已经覆盖"开发期改了结构重启就跟上"，少的只有"删掉不再
声明的表"，那件事交给迁移脚本。门是 `runtime_schema_isolation_test.go`（第二个 runtime 什么都不
声明，两张表都必须还在）加同名的集成用例。

### "抓住错误继续跑"在 PostgreSQL 的事务里不成立 (2026-08-28)

`WithSkipDuplicates` 抓到重复键就 `continue`，在 **PostgreSQL 事务里必然失败**：任何语句失败后事务
即 aborted，其后语句都报 `25P02`，调用方拿到的错误还不是重复键错误。

**判据**：错误处理策略的可移植性取决于"失败之后连接还剩下什么状态"，三个数据库答案不一样。
凡是"捕获错误后继续用同一个连接"的代码，都要问一句 PG 上还能不能用。

修法（事务内每行 savepoint、事务外不发、为什么不用 `INSERT IGNORE` / `DO NOTHING`）见
`change-impact.md` § 改了分块或批量语句的形状。

### 转义值和声明转义符是同一件事的两半，只做前一半是静默错误 (2026-08-28)

`escapeKeywordSearch` 一直在转义 `%` / `_`，但渲染出来的谓词是裸 `LIKE ?`。**SQLite 没有
默认的 LIKE 转义字符**，转义前缀于是变成普通字符，搜 `a_b` 在 SQLite 上返回零行；
MySQL / PostgreSQL 默认转义字符恰好是反斜杠才侥幸正确——而单元测试只跑 SQLite，两侧的
端到端关键字用例都是零，所以谁都没发现。**固定进 SQL 文本的子句要三个方言都验。**

转义字符选 `~` 不选反斜杠的硬约束见 `change-impact.md` § 改了 LIKE 谓词的渲染。

**引申**：任何"我们对值做了预处理"的功能都要问一句"数据库怎么知道"——值被改了而契约没被
声明时，行为由各方言的默认值决定，而默认值本来就不一样。

### 同一个 SQLSTATE 在三个驱动里是三个 Go 类型 (2026-08-26)

曾匹配 pgx **v4** 的 `*pgconn.PgError`；v5 的是另一个类型，重试和 `WithSkipDuplicates` 静默失效，而
单测 fixture 恰好也是 v4。修法是匹配接口 `interface{ SQLState() string }`（pq / pgx v4 / pgx v5 都实现）。
**驱动错误分类永远按接口，不按具体类型**（MySQL 例外见 `change-impact.md`）；集成测试用真实 pgx v5 守着。

### 决定：方言能力位按版本基线表态，否决"版本可配置" (2026-08-26)

能力位曾按 2018 年前的引擎写死，README 忠实复述了这些错误。考虑过给 `MySQLDialect` 加
`ServerVersion` 字段，**否决**：`Build()` 之前根本不知道会连哪个库，版本只能执行时探测，
那就得每个 `Dialect` 值带状态，和"方言是无状态值类型"冲突。改成按基线表态：MySQL 8.0、
SQLite ≥3.39。代价是更老的引擎拿到数据库报错而不是 `UnsupportedCapabilityError`。

### 决定：commit 阶段只对明确冲突码重试 (2026-08-26)

原来一刀切 `stage != commit`，理由是 commit 失败有歧义。但 PG 的 `40001` **经常在 COMMIT 时
才抛**，且这些码（40001 / 40P01 / 55P03、MySQL 1205 / 1213）保证事务已回滚——PG 最典型的
重试场景被一刀切排除了。现在 commit 阶段只放行明确冲突码，网络类错误仍不重试。

## API 契约与接口设计

### 全局 `Init()` 和 engine 中间层是被删掉的，不要重新引入 (2026-08-21)

历史上有过包级全局 `Init()`、`engine` 中间层和 `traceManager` 层，都被删了，换成显式的
`NewRuntime(...)`。全局单例让"这个查询用的是哪个库"不可回答，测试也没法并行；中间层是纯
转发，只让调用栈多一层。**任何"方便起见加个全局默认 runtime"都是在往回走。**

### 决定：公开的 `dialect` 只有名字和事实，实现在 `internal/sqldialect` (2026-09-19，v5)

`Dialect` 曾导出十九个方法（含 schema 探查和 DDL 渲染）却"不是扩展点"：tag 之后每次内部调整都算破坏
契约。对外只收 `dialect.Name`；代价是根包 import 这**唯一一个** internal 包，集成测试仍只用导出 API。

### 文档描述了一个不存在的阶段；"最紧的上限"是断言要去量 (2026-08-28)

`api-check` 和 `doc-check` 都只看符号：**"文档提到的符号都存在"不等于"描述的用法都成立"**。上限写死
65535 注释"最紧的"，SQLite 其实是 32766——**修一类 bug 要把这一类的实例都数一遍**。同类（2026-08-26）：
**stringly-typed 的开关，空值 `""` 永远是那个没人写的分支**，违规被静默丢弃。用类型化枚举或不留开关。

### 接口里"有定义、有实现、零调用"的钩子 (2026-08-26)

`Dialect.ReturningClause` 零调用，PG 上 `Insert` 从没回填过主键，只跑 SQLite 的测试一直绿；现在由
集成测试挡着。`Integration` 红着的 PR #61 被 auto-merge 合入（**auto-merge 只等必需检查**），此后它
成了必需检查。**说"某检查是不是必需"之前先查 ruleset**（`gh api repos/tmoeish/tsq/rulesets/<id>`）。

### 集成测试为什么长这样，以及暂时不做的几件事 (2026-08-26)

核心断言"托管 schema 第二次启动零 DDL"（v4.2.0 的 Critical 事故都表现为它）。用 env DSN 而不是
build tag，SQLite 目标因此每次 `go test` 都跑。

- **Docker 镜像不推送 registry**。`Docker Build` 是必需检查，但产物没人消费；推送要配
  ghcr 权限和 tag 策略，等有真实使用者再说。
- **不替换 `gopkg.in/nullbio/null.v6` 和 `serenize/snaker`**。前者出现在生成代码里
  （`examples/academy/*.tsq.go` import 它），是使用者契约；后者只在生成器里做
  CamelToSnake，换实现等于改所有使用者的表名推导。

### 决定：相关子查询靠 `Correlate(...)` 显式声明，不靠推断 (2026-09-03)

`validateJoinGraph` 要求每张被提到的表都在 FROM/JOIN 图里，相关引用天生不满足。旧报错建议
`use CrossJoin`，**照做会静默改变语义**：join 进来的表遮蔽外层同名表，谓词不再相关，而且不报错。

**否掉"自动放行未知表"**：那等于把打错的表名一起放行。既 `Correlate` 又 join 同一张表是构建错误；带
`Correlate` 的查询不能单独执行。`api-check` 看不见方法调用，语义由
`TestCorrelatedSubqueryCarriesItsParameters` 守着。

### 决定：按条件写语句不校验 `version` 但自增它；`Set*` 是泛型方法 (2026-09-03)

`UpdateTable[T]()` / `DeleteFrom[T]()` 是给"调用方手里没有行对象"的场景的，校验版本没有
意义；但**自增不能省**：不自增，批量改动之前加载的对象随后 `Update(...)` 时版本号仍然对得上，
会静默覆盖掉批量改动——那正是使用者声明 `version` 想防的事。显式赋值版本列是构建错误。

否掉的两个替代：给 `Update(item)` 加"跳过版本校验"开关，解决不了"手里没有对象"的真正
场景，还把乐观锁变成可选项；让 `*Query[O]` 长出 `.Update()`，`Query` 背着 SELECT 列和分页
语义，两边校验都说不清。

`Set*` 做成 Go 1.27 泛型方法是为了把"列和值类型一致"从运行期校验升成编译错误，代价是它们
只能住在 `Where` 之前的具体类型上（接口方法不能带类型参数）。`Where` 必需由类型强制，全表
操作要写显式 `And()`——"静默去掉过滤条件"在写路径同样不允许。`UpdateTable` 至今不碰任何托管
字段；`DeletedAt` 是例外，理由见下条。

### 决定：v5 核心重写——表达式树、命名参数、表描述符、封闭执行器 (2026-09-17)

发版前的设计审计发现四个根上的问题，改实现只是在上面雕花，于是重写了根包：
- **SQL 是带 base64 标记的字符串**，执行前扫文本替换 `?`、判断方言能力。能力判断因此两次出错：
  字面量里的 `FOR UPDATE` 被当成行锁（2026-09-16）；而"从 builder 结构判断"又会漏掉以文本
  进入外层的子查询。现在是片段树按方言渲染，子查询是结构化片段，**能力由渲染那个构造的代码
  报告**，使用者的原样文本从不被扫描——两种错都不再可能。
- **`EQVar()` 的值按位置从 `args ...any` 取**：个数、类型、顺序错了只在运行期发现或静默查错，
  这是"类型安全"里最大的洞。现在是 `Param[T]` / 列自带参数，按身份绑定、`Bind` 只收 `T`。
  否决了"生成带类型签名的查询函数"（v4 就是那样，函数爆炸）和"按列身份隐式绑定"（同一列两个
  值时无解）。
- **表元数据是使用者结构体上的七个方法**：和字段重名，还逼出了 `DeclareTable` 补丁。现在是
  `TableOf[R, K]` 描述符，行类型上没有接口；列是生成结构体的字段（见"列是表结构体的字段"）。
- **`Executor` 就是 database/sql 的三个方法**：裸 `*sql.DB` 能编译，方言到运行期才发现未知。
  现在是封闭接口。
列函数的可移植性要**在三个方言上真跑**才知道（`TestIntegrationColumnFunctionsArePortable`）：
MySQL 的 `LENGTH` 数字节；PostgreSQL 没有 `round(double, int)`；modernc 默认按 Go 的 `String()`
格式存时间，SQLite 的日期函数读不了（取前 19 个字符再算）；SQLite 的 `UPPER` 只认 ASCII（只能写进
文档）。列上的 `Distinct()` 放在选择列表中间是非法 SQL，所以换成 `CountDistinct` 和 `SelectDistinct`。
顺带修掉：`DeleteFrom` 的墓碑时间在构建时求值（包级语句永远写进程启动时间）；`Year()` 返回列
自身类型且得到文本；`StartsWithVal` 不转义通配符；分组后仍能 `ForUpdate`。

### 决定：v5 设计收尾——函数是包级泛型、分页类型化、少生成查询 (2026-09-17)

- **列函数是包级泛型函数**（`tsq.Upper(col)`），用 `Text` / `Number` 约束（方法不能约束类型参数）；搜索列
  只能是 `string`。
- **固定值是 `tsq.Val(v)` 右值，`*Val` 方法全删**（一度否决，后由维护者拍板）：类型只由值推断，`int64` 列上
  写 `tsq.Val(int64(90))`，别因为要写转换改回去。
- **`Page` 吃 `Paging`**，字符串形态的 `PageRequest` 只在 HTTP 边界；`Paging(sortable...)` 要求列出可排序
  列，v4 按选出来的列放行，未建索引的列也能被客户端拿来排序。
- **只为主键和唯一索引生成查询**：普通索引和前缀的查询要排序、限量，生成器猜不到，照抄就是全表读取。
- **Upsert 在 MySQL 上遇到别的唯一键也可能冲突就拒绝**：`ON DUPLICATE KEY UPDATE` 没有冲突目标，会静默更新
  无关的行。批量同键两行报错（PG 不许一条语句改一行两次），批量不回读。
- **可空性：类型区分表列，表达式在运行期推导**（`exprInfo.null`）。外连接和无 GROUP BY 的聚合只在查询
  上下文里可知，检查因此在读行前而不在 `Build`（会拒掉合法子查询）。否决值类型包成 `Null[T]`：一个值不能
  同时是两种 `RHS`。
- **`Dialect` 不是扩展点**（定案）：按方言分叉的拼写（日期、`ROUND`、NULL 排序、全文检索）按方言名写在
  库里，第四种方言会在这些构造上报错。否决把它们搬进接口：那等于把没有测试的代码放进公开契约。
- **PG 索引自省曾看不见表达式索引**：表达式的列号是 0，内连接 `pg_attribute` 丢掉整行，GIN 索引每次启动都被
  当成缺失（42P07）；现在 `LEFT JOIN`。
- **写入热路径的反射成本在每列每行一个值上**：改用列自带的类型化取值函数后，100 行批量 INSERT 快约 19%、
  UPDATE 约 28%（`write_bench_test.go`）；零值判断和盖时间戳仍用反射。
- **关联装配不引入关系 DSL**：`AttachMany` 只做收键、一次查询、按键分组，子查询仍由调用方给出。
- **全文检索三个方言不是一回事**：MySQL `MATCH ... AGAINST`、PG `to_tsvector @@ plainto_tsquery`、SQLite
  退化成子串匹配（FTS5 要影子表和触发器）。排序和操作符不可移植，只有 `Capability` 说得清拿到哪一种。
  `TableIndex` 加字段记得 `cloneTableIndex`：曾逐字段复制，`FullText` 标记就在那里丢过。
- **生成列不参与 schema 对账**：SQLite 的 `table_info` 不列它，每次启动都会再 ADD（duplicate column）。
- **没匹配到行分两种错误**：版本不符 `OptimisticLockError`（可重试），状态不符 `RowStateError`（重试无用）。
- **派生表达式不是列**：`derived` 不留扫描目标，`Select(tsq.Date(时间列))` 在编译期就写不出来（以前运行期
  扫描失败）；单值查询走 `SelectValue`。
- **Go 1.27 允许组合字面量用提升字段作键**（`outer{c: 1}`）：拆结构体时旧字面量照样编译，别当成改完了。
- **NULL 排序默认最小值**：MySQL/SQLite 本来如此，只需改 PG；MySQL 没有 `NULLS` 子句，换默认就得给每个
  可空排序加 `IS NULL` 键。
- **时间在绑定出口统一转 UTC，不只是托管时间戳**：SQLite 按文本存时间，本地时间和 UTC 行按文本比较会错。
- **超长列表参数用显式 `ListIn`，否决自动分块**：`OR`、`NOT IN`、排序、聚合、LIMIT 分块后语义都变。
- **游标分页展开成 `a < ? OR (a = ? AND b > ?)`，不用行值比较**：后者只在所有列同向时成立。最后一列必须
  是主键（否则同值行会被跳过或重复）；游标带排序指纹。
- **关键词是执行参数 `tsq.Keyword`**：放在 `Paging` 里时搜索结果只能分页读，没法 `Iter` 或单独 `Count`；
  空关键词在 `prepare` 里丢掉，否则报参数未使用。
- **`Page` 的一致性靠只读快照事务，不靠 `COUNT(*) OVER()`**：窗口函数在 `DISTINCT` 前求值、PG 不能和
  `FOR UPDATE` 同用、越界页没有行带回总数。代价是一对 BEGIN/COMMIT（单语句的 `ListIn` 因此不开事务）。
- `BatchDeleteByPK` 挪到 `TableOf` 上，吃主键的 `BindList`：包级版本要再校验"列是不是主键"。
- **v5 明确不支持复合主键**（维护者定案）：`pk=A,B` 报错并指向单列代理键 + `//tsq:unique A,B`；要支持
  就是 v6（`TableOf` 的 K、`Get` / `Fetch`、`BatchDeleteByPK`、乐观锁 WHERE 全要变形状）。

### 两个测试各自编码了相反的意图，代码同时满足它们 (2026-09-16)

一条测试断言 `WithMaxPageSize(5000)` 能放行 3000，另一条断言"没有 runtime 能抬高绝对上限"，两条都绿：
`Validate` 把上限夹到 1000 而 `Normalize` 不夹。**一对矛盾的断言可以同时为真，只要实现里有两条路径各
满足一条**；同一个概念的两个入口要放在一个用例里比。后来只留一条路径：`PageRequest.Paging` 只拒绝没有
意义的输入，大小由 `Page` 按 runtime 上限封顶（2026-09-19），`Validate` / `Normalize` / `MaxPageSize()` 删除。

定案取名字：`DefaultMaxPageSize` 是**默认**，`WithMaxPageSize(n)` 是这个 runtime 的上限，双向生效。
把常量当硬顶会让 `WithMaxPageSize(5000)` 变成一句空话——库不该用一个编译期常量去否决调用方明确的选择。

### 决定：Runtime 用函数式选项，并且不关别人的连接池 (2026-09-16，v5)

`options ...*RuntimeOptions` 让"没传选项"和"传了一个选项值"是同一个签名，字段零值又兼任"没设置"。
构造器是 `Open(ctx, driver, dsn, ...)`（自己开池）和 `NewRuntime(ctx, db, dialect.Name, ...)`（用别人的池），
照 `sql.Open` 的分工命名。**关键约束是所有权**：`Close()` 只关自己开的池（`ownsDB`），关掉调用方的池
会打断它在 TSQ 之外的用途；正反各有一个测试。

标识符长度校验去掉了三档模式：超长的名字到不了服务端，`warn` / `skip` 只是把失败推后；生成期也校验
（派生的索引名最容易超限）。

### 决定：v5 的命名规则，改回去之前先读这里 (2026-09-16，v5)

v5 不背兼容，一次把名字改到"最合理"。定下的几条规则，每条都是有意的：
- 错误**类型**以 `Error` 结尾（`OptimisticLockError`），`Err` 前缀只留给哨兵变量——Go 标准库的惯例。
- 右值接口叫 `Operand` / `ListOperand`，不叫 `RHS` / `SetRHS`：它们出现在最常见的编译错误里，而 Set 已经是
  UPDATE 赋值的意思。索引上装列名的字段叫 `Columns`，`Fields` 只指 Go 字段（指令里的用法）。
- 否定一律 `Not*`（`NotIn`、`NotLike`）。v4 的 `NIn` 和 `NotExists` 并存，同一个意思两种拼法。`NE` 保留，它是比较运算符。
- 可选参数用函数式选项（`RuntimeOption`、`BatchOption`），不用 `...*XxxOptions`。只对插入有意义的
  `WithSkipDuplicates` 传给别的 `Batch*` 会**报错**而不是被忽略：被静默忽略的选项就是 v4 的零值歧义。
- 事务选项跟在回调后面（`WithTx(ctx, fn, tsq.WithRetry(...))`），和 `RuntimeOption` 一个形状；
  `TxOptions` 结构体让九成调用在中间传 `nil`。`WithTablePolicy` / `WithIndexPolicy` **保留**：示例的表来自
  `mock.sql`、只让 TSQ 管索引，合并成一个选项就表达不了。追踪给 `TraceInfo{Op, Table}`，没有表名的
  span 说不清在做什么。
- 表是描述符，方法在 `TableOf` 上，不在使用者的结构体上。`Table` 的方法叫 `TableName()` 不叫
  `Name()`：生成结构体的列字段常叫 `Name`，同名会遮住接口方法。
- 没有 `BuildSubquery` / `AsSubquery`：阶段和 `*Query` 自己实现 `Subquery[O]`，`SelectValue` 的阶段就是
  值的子查询，错误推迟到外层 `Build`（旧形状要把选出的列再写一遍）；把 `O` 当值类型是安全的，只有
  `SelectValue` 的 `O` 会和某列的 `T` 相同。`MapInto` 的 JSON 名默认取源列，要改才 `.Named`。
- 按主键读在库里且有类型（`TableXxx.Get` / `Fetch`），唯一索引生成 `GetByX` / `FetchByX`，缺行时包装
  `sql.ErrNoRows`；单行写入的错误**只带主键**（`users id=5`），不序列化整行（列值会进日志）。
- `dialect` 包里不加 `DDL` 前缀（包名已经说明语境）；`Dialect` 接口只收**各方言确实不同**的方法，
  三家返回同一常量的方法内联掉。模板不许拼接常量名（`Kind{{ .Kind }}`）：符号门禁只认完整的
  `tsqdialect.X`，拼出来的名字改名后照样"通过"，所以由 `columnKindRef` 显式列出。

### 决定：读单行只留两个入口，语义写在名字里 (2026-09-09，v5)

`Get`（无行报 `sql.ErrNoRows`）和 `Find`（`nil, nil`）；删掉的 `Load(holder)` 没法不比较错误就表达
"没查到"。`Count` 只留 `int64`（截断是静默的）。单行读取加 `LIMIT 1`，**必须在行锁之前**——这道门
在 v5 核心重写时随旧测试文件一起丢过，现在是 `TestSingleRowReadsLimitBeforeTheLock`。`Exists` 不用
`COUNT`：它要访问每个匹配行，去回答第一行就能定的问题。

### 决定：v5 不留兼容别名，且"不用接收者的方法"要变成函数 (2026-09-09)

v4 攒下九个 `Deprecated` 符号，没有任何门禁会提醒它们该走——**兼容包装只会积累**，删掉它们本身就是
大版本存在的理由。同一波删掉 `Unique` / `NUnique` / `Concat`（只会返回构建错误）和 `Column.Now()`。

**方法体里不出现 `c.`，就说明它不该是方法**：`User_Name.Now()` 和 `User_ID.Now()` 完全一样，
`ExistsSub` 逼调用方随便挑一列。后者的参数类型还未导出——**调用能编译，但使用者写不出类型名**，
也就写不了 helper；现在是导出的密封接口 `AnySubquery`。

### 决定：软删除是默认的删除语义，物理删除要显式说 (2026-09-09，v5)

判据是真实用法：软删除的行在业务上就是删掉了，只有审计才回头看。声明 `deleted_at` 的表上
`Delete` 打墓碑、`HardDelete` 物理删，没声明的表两者同义。

**已删行不可见是表的默认作用域，不是生成查询里的过滤条件**（2026-09-17）。模板加 `deleted_at = 0` 时，
手写查询和 JOIN 里的软删除表全都漏掉。作用域放 WHERE 会把 LEFT JOIN 变成 INNER JOIN，放 ON 挡不住
RIGHT JOIN 被保留侧的已删行，所以有 RIGHT / FULL JOIN 时整张表改成活行派生表；`WithDeleted()` 是唯一出口。

- **软删除不再复用 update 路径**：复用时 `Update` 要写 `deleted_at`，没有 `version` 的表上旧副本一次 `Update`
  就把行复活，手工构造的行还会清零 `created_at`。现在 `Update` 不碰这两列，`Delete` / `Restore` 只写托管列。
- **墓碑值靠 `applyTombstone` 按字段形态分派**，最后一环 `sql.Scanner.Scan(now)` 同时吃下
  `sql.NullTime` 和 `null.Time`，**根包因此不必 import nullbio**。

此前端到端零覆盖（和 `*time.Time` 那个 bug 同一盲区），门是 `runSoftDeleteDemo`。

## 构建与代码生成

### 决定：注解是 `//tsq:` 指令行，不是写在注释里的 DSL (2026-09-16，v5)

旧的 `@TABLE(...)` 是写在 doc comment 里的括号 DSL，**gofmt 会重排它**，于是生成器长出一个
`tsq fmt` 命令把注解排回解析器要的样子，外加"先 fmt 再 gen"的规则、670 行格式化器、以及一套把
字节偏移映射回行号的定位器。**没有一件是在解决使用者的问题**，它们都在解决"我们把结构化数据放进
了 gofmt 管辖的地方"这个自找的问题。指令行 gofmt 不碰，这些就全没了。**判断一个辅助工具是不是
必要，先问它在解决谁的问题。**

**v5 是全新版本，不做后向兼容也不提供迁移路径**（维护者 2026-09-16 定的）：旧 DSL 解析器、`tsq migrate`、
`ddl.json` 旧状态文件、`--tpl` 自定义模板、`MIGRATION_GUIDE.md` 一律删除。**不要为了"方便升级"把
任何一样加回来**——兼容层在这个仓库里只会积累，v4 就是这么攒出九个 `Deprecated` 的。

语义等价的验证办法是**生成物逐字节相同**：示例全部换成指令后重新生成 diff 为空，这比逐条比对解析结果更强,它覆盖了全部下游推导（索引名、查询名、DDL）。



### 决定：列是表结构体的字段，不是包级变量 (2026-09-19，v5)

`Course_ID` / `Course__Cols` 每表往包里撒十几个带下划线的名字，别名要逐列 `WithTable`，还靠"句柄 → 列
→ Define"三步声明加一个按文件名排序的示例门禁（`academyqueries.go`，已删）守初始化顺序。现在是内嵌
`*TableOf` 的 `CourseTable`：取列必经 `TableCourse`，初始化顺序由 Go 保证；`As` 一次改绑整套列。
**代价**：列字段不能和 `TableOf` 的方法重名，`tsq gen` 报错（`reserved.go`），所以 `Table.Name()` 改成了
`TableName()`，**以后给 `TableOf` 加导出方法都会让某个列名非法**，加之前想清楚。

### 按字符串批量取数要以数据库的判等为准 (2026-09-19)

生成的 `FetchXxxByTitle` 逐字节比对返回的行，MySQL 默认 `_ci` 排序规则下 `'go'` 匹配到 `'Go'`，却报
`sql.ErrNoRows`。现在 Go 里对不上的**字符串**再单行问一次，遇到第一个真不存在的就停——**别对整数也这么做**：
第一版对 70000 个缺失整数键发了 70000 条查询。

### 版本号有四个副本，生成物那份最容易忘 (2026-08-21)

版本号传导进生成文件头和 `tsq.json`，**改版本号必须重新生成示例**，`release.py` 依赖这一点。生成文件
后缀（`TSQFileSuffix`）还被 `changeset.py` 和 `check_release.py` 认着，改它要一起改。

## 验证与门禁系统

### 又一次：只被自己的测试撑着的代码，在库里是不存在的 (2026-08-26，2026-09-09)

一次审计同时抓到三处同一形状的东西：未导出的 `printSQL` key 加三个 tracer（读路径八处
`ctx.Value` 在发布出去的库里**永远为假**）；`canonicalCapabilityName` 的逐行副本加一个零调用入口；
以及 `_test.go` 里自己定义 `AGENTS.md` 明令禁止的包级 `Runtime` 单例——守着它的 `api-check` 只看
快照，而 `_test.go` 的导出符号不进快照。**规则的门在哪，绕过它的路就在哪。**

两条教训比"删掉了"值钱：**`unused` linter 看不见这类东西**（`_test.go` 里的引用算使用，判据只能是
排除 `_test.go` 之后 grep 调用方）；**一个只被自己的测试引用的符号，测试证明的是它自洽，不是它可达**
——绿色的测试在这里是伪装。`change-impact.md` 为此加了两条带 grep 的触发器。

**2026-09-09 又一次，这次在代码生成侧**：模板 helper 发出 `tsq.TimePtr(...)`，根包没有这个符号，
声明 `*time.Time` 托管字段的使用者拿到的是**自己工程里**的编译错误，而 `skills/tsq` 一直把它列为
受支持；守着它的单元测试断言的正是那个字符串。**模板和 helper 里的字符串不参与本包的类型检查**，
`api-check` 又只看根包快照，缝正好在"生成代码引用的符号存不存在"。门是
`internal/cmd/generated_symbols_test.go`，它装上后立刻抓到第二个（`PageRespType` 渲染的
`tsq.PageResp` 其实叫 `PageResponse`）。

### 写在 AGENTS.md 里但没有门的规则，几个月都是假的 (2026-08-26)

`AGENTS.md` 要求 "README、`docs/`、`skills/tsq` 用英文"，而实测只有 `skills/tsq` 是对的——
这条规则从写下那天起就没成立过，`doc-check` 当时扫不到 Markdown 的语言。

选择是改规则而不是翻译九百行：分界线按**读者**划才站得住，然后给英文那一侧加了门。
**留下的是判据：写规则的时候就问"谁来发现它被违反了"。** 答不出来的规则不要写进
`AGENTS.md`，写进去只会让下一个读到它的人相信一件假事。

### 合并前后各跑一次的检查要看同一个输入；文档与 CI 的 make 目标是两条真相 (2026-08-21)

squash 会改写提交信息（追加 ` (#59)`）、SHA 和历史形状；`check_change_log.py` 量长度前剥掉 ` (#\d+)`。
`doc-check` 守着文档里的 `make X`，但**管不到 `.github/workflows/`**（CI 调过不存在的目标），改名时手动 grep。

### cobra 的互斥标志组按 `Changed` 位判定，测试里必须手动清 (2026-08-21)

`VersionCmd` 是包级单例，状态跨 `Execute()` 存活：`Flags().Lookup(name).Changed = false`；`-shuffle=on` 为此而开。

### 发版波必须从内存门禁里豁免 (2026-08-21)

豁免是 `check_change_log.py` 的 `RELEASE_ONLY_FILES`，**精确白名单而不是开关**：多碰一个别的文件门就活过来。

## 流程与工具

### 生成的 `.sql` 文件头停在旧版本是**有意的**，别去"修"它 (2026-08-28)

`examples/academy/*.sql` 的头写着 `tsq-v4.1.19` 不是忘了重新生成：`tsq.json` 保存首次建 schema 时的
原始 `.sql`，后续变更以带日期的迁移段追加，**文件头记的是 schema 的出身**。"修"成当前版本会让每次
发版都重写三个 DDL 文件头，把真正的 schema 变更淹掉。

### 能力位的 `default` 分支是那道门自己的漏洞 (2026-08-26)

规则写着"新增能力位三个方言都要显式表态"，但三个 `SupportsCapability` 都是 `switch` 加
`default: return false`——漏掉一个方言不编译失败、不 lint 失败、不测试失败，只静默变成
"不支持"。现在是公开 `dialect` 包里每方言一张表加一个遍历表的测试。

**引申，对所有"必须穷尽"的 switch 都成立**：`default` 分支把"忘了写"和"决定不支持"变成
同一件事，而这两件事需要不同的处理。要穷尽性就别给它兜底分支——用表加一个遍历表的测试。

### `release-check` 两次装反：**先数清楚合法状态有几个** (2026-08-21，2026-09-16)

**一道门要先问"合法状态有几个"，只有一个时才用等号。** 这里错了两次：

- 版本号 vs 最新 tag：第一版要求"严格大于"，但合法状态有两个（发版之间 buildinfo 等于最新 tag；
  `release.py` 跑 harness 时 buildinfo 领先于 tag），两个都被拦。真正的错误状态只有一个：
  buildinfo **低于**最新 tag。
- 版本号 vs 模块主版本：曾要求相等。跨主版本必须分两步（先一波正常变更把 `/vN` 和全部 import 改完，
  再发首个 vN），两步之间模块路径已是 v5 而 buildinfo 还是 4.x——**严格相等把这个合法过渡态拦死，
  于是迁移根本没法作为独立的一波合入**。现在只查 `module_major() < code.major`。

### 两份技能必须各住各的目录，别为了少一个符号链接把它们并在一起 (2026-08-21)

**布局是文档的一部分**：软链在一起就是在说"它俩是一伙的"。改路径用 `git mv` 并 `grep -rn` 一遍。

### squash 的粒度是 PR，所以 PR 的粒度就是你能保留的历史粒度 (2026-08-21)

第一次真跑 PR 发版流程，同一天被这一件事绊了三次：`pull --ff-only` 报分叉、卷进发版 PR 的三条
提交信息从 `git log` 消失、新分支叠在未合并分支上冲突。三个动作现在都是 `AGENTS.md` § 发版
里的规则，`release.py` 还会拒绝 `origin/main..main` 不为空的发版。

### 把并发写入者的改动误判成了工具的 bug (2026-08-21)

曾断定 `make fmt` 里的 `go fix` 会把树改坏并删掉它——**错的，已改回**：另一个 claude 进程在同一
工作区边跑边写，`go fix` 报的编译错误是它遇到的，不是它造成的。

- **"我改了 A，然后 B 坏了"在有并发写入者时什么都不能证明。** 先确认自己是不是唯一写入者
  （`ps aux | grep claude` 加 `lsof -p <pid> -a -d cwd`），再在 `git archive HEAD` 的副本里
  复现。当时几次 `git checkout -- '*.go'` 丢掉了对方未提交的工作。

### 给 main 和 tag 加了 ruleset，发版随之改成 PR 流程 (2026-08-21)

`main` 禁直推、必须走 PR 且六个必需检查全绿；`refs/tags/v*` 禁删除/移动/强推。两条都对仓库
所有者生效。**tag 那条更重要**：删掉或移动已发布的 tag 是唯一不可恢复的操作（Go Proxy 永久缓存）。

- **必需检查不能放 matrix job**（名字带 Go 版本，升版本就永远等不到）；理由和当前选的六个
  检查见 `change-impact.md` § 改了 CI 的 job 名字。
- **用 `gh pr merge --auto`，不要"等 CI 再合"**：PR 刚建出来的头几秒没有任何 check 注册，
  `gh pr checks --watch` 那一刻会以 "no checks reported" 直接退出。
- **验证服务端规则不能用 `git push --dry-run`**——它不联服务端，看起来永远成功。要真推一次；
  测 tag 规则用不合法 semver 的探针 tag（`v-ruleset-probe`），受 `v*` 规则管但 Go Proxy 忽略。

### 版本号是给使用者的，不是给每一次提交的 (2026-08-21)

v4.4.2 使用者拿到的和 v4.4.1 一模一样。判据是"使用者拿到的东西变了没有"，`release.py` 的
`user_visible_changes` 自己算，表格在 `AGENTS.md` § 发版。

### CI 里用 `@latest` 装的工具，会在它发新版本的那天让每个 PR 变红 (2026-09-09)

goreleaser v2.18.1 一发布就要求 Go >= 1.27.1，CI 用 `GOTOOLCHAIN=local` 钉着 1.27.0，`@latest`
随即装不上，**每个 PR 的 GoReleaser Check 都红**且与改动无关。更贵的是 release job 的
`version: latest`：它只在**tag 推送之后**才跑，那一步不可撤销；两处版本不同时 check job 也证明不了 release 会成功。

### `-X` 打错包路径是**静默**失败的 (2026-08-21)

链接器对找不到的 `-X` 符号直接忽略，三份配置各犯过一次。`release-check` 核对路径，CI 的 `Docker Build` 跑镜像核对值——**静态检查证明路径对，跑产物证明值到了，缺一不可。**

### 生成器不能带 `git describe` 的版本号，否则发版是死锁 (2026-08-21)

用带 `-X version=$(git describe)` 的 `bin/tsq` 生成，文件头记的是 git 描述的版本：想写对头部得先打
tag，想打 tag 得先过 `release-check`。所以 `make build-gen` **故意不带 `$(LDFLAGS)`** 编 `bin/tsq-gen`
（报告 `internal/buildinfo` 字面量）；`bin/tsq` 是给人用的 CLI，两个二进制的分工不要合并。

### 决定：两份技能按所有权拆开，不按篇幅 (2026-08-21)

理由不是篇幅是所有权。同一份文件同时服务两拨读者时，写给使用者的部分会因为开发者觉得
"这个细节太内部"而被删掉，反过来也一样。`skill-check` 的每条触发器都是从"哪类改动会让哪份
文档变假"倒推出来的，`hint` 里写着理由。

### 生成物是否同步不能用 `git diff` 判断 (2026-08-21)

一波变更本来就可能合法地改动生成物，`git diff` 会对每一波正当改动都失败。判据只能是
"重新渲染一遍看结果一不一样"，即 `make gen-check`。

### `make commit-check` 单独存在时是失效的 (2026-08-21)

未提交时它跳过，提交后 `memory-check` 又跳过，写提交信息那一刻活着的永远是另一道门。
提交信息真正被校验的唯一时机是 `commit-msg` 钩子，`make hooks` 每台机器必须跑一次。

## 搁置项与决定不做的事

决定：**CLI 不拆子模块，改为收紧根包自己的依赖**（2026-09-17 重测）。只 import 根包的模块 tidy 后，
`x/tools` / `cobra` 早已不在其 `go.sum`，拆分不改变任何东西；真正进去的是根包自己的非测试 import（MySQL 驱动，连 `go.mod` 都进）和**根包测试**
的 import（pgx、nullbio）——tidy 会记录依赖包测试的依赖。修法：MySQL 错误改反射读取、集成测试挪进
`internal/integration`、时间戳测试用本地同形类型，门是 `TestRootPackageImportsNoDriver`。剩下只有
SQLite 驱动（根包单测离不开它）。复测：临时模块 `replace` 到本仓，tidy 后看 `go.sum`。
