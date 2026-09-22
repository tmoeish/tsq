# 项目内存 — API 契约、命名与依赖面

判据与索引在 `../memory.md`。

## 全局 `Init()` 和 engine 中间层是被删掉的，不要重新引入 (2026-08-21)

历史上有过包级全局 `Init()`、`engine` 中间层和 `traceManager` 层，都被删了，换成显式的
`NewRuntime(...)`。全局单例让"这个查询用的是哪个库"不可回答，测试也没法并行；中间层是纯
转发，只让调用栈多一层。**任何"方便起见加个全局默认 runtime"都是在往回走。**

## 决定：公开的 `dialect` 只有名字和事实，实现在 `internal/sqldialect` (2026-09-19，v5)

`Dialect` 曾导出十九个方法（含 schema 探查和 DDL 渲染）却"不是扩展点"：tag 之后每次内部调整都算破坏
契约。对外只收 `dialect.Name`；代价是根包 import 这**唯一一个** internal 包，集成测试仍只用导出 API。

## 文档描述了一个不存在的阶段；"最紧的上限"是断言要去量 (2026-08-28)

`api-check` 和 `doc-check` 都只看符号：**"文档提到的符号都存在"不等于"描述的用法都成立"**。上限写死
65535 注释"最紧的"，SQLite 其实是 32766——**修一类 bug 要把这一类的实例都数一遍**。同类（2026-08-26）：
**stringly-typed 的开关，空值 `""` 永远是那个没人写的分支**，违规被静默丢弃。用类型化枚举或不留开关。

## 接口里"有定义、有实现、零调用"的钩子 (2026-08-26)

`Dialect.ReturningClause` 零调用，PG 上 `Insert` 从没回填过主键，只跑 SQLite 的测试一直绿；现在由
集成测试挡着。`Integration` 红着的 PR #61 被 auto-merge 合入（**auto-merge 只等必需检查**），此后它
成了必需检查。**说"某检查是不是必需"之前先查 ruleset**（`gh api repos/tmoeish/tsq/rulesets/<id>`）。

## 决定：v5 核心重写——表达式树、命名参数、表描述符、封闭执行器 (2026-09-17)

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
  `TableOf[R, K]` 描述符，行类型上没有接口；列是生成结构体的字段（见 `codegen.md` 的"列是表结构体的字段"）。
- **`Executor` 就是 database/sql 的三个方法**：裸 `*sql.DB` 能编译，方言到运行期才发现未知。
  现在是封闭接口。
列函数的可移植性要**在三个方言上真跑**才知道（`TestIntegrationColumnFunctionsArePortable`）：
MySQL 的 `LENGTH` 数字节；PostgreSQL 没有 `round(double, int)`；modernc 默认按 Go 的 `String()`
格式存时间，SQLite 的日期函数读不了（取前 19 个字符再算）；SQLite 的 `UPPER` 只认 ASCII（只能写进
文档）。列上的 `Distinct()` 放在选择列表中间是非法 SQL，所以换成 `CountDistinct` 和 `SelectDistinct`。
顺带修掉：`DeleteFrom` 的墓碑时间在构建时求值（包级语句永远写进程启动时间）；`Year()` 返回列
自身类型且得到文本；`StartsWithVal` 不转义通配符；分组后仍能 `ForUpdate`。

## 决定：Runtime 用函数式选项，并且不关别人的连接池 (2026-09-16，v5)

`options ...*RuntimeOptions` 让"没传选项"和"传了一个选项值"是同一个签名，字段零值又兼任"没设置"。
构造器是 `Open(ctx, driver, dsn, ...)`（自己开池）和 `NewRuntime(ctx, db, dialect.Name, ...)`（用别人的池），
照 `sql.Open` 的分工命名。**关键约束是所有权**：`Close()` 只关自己开的池（`ownsDB`），关掉调用方的池
会打断它在 TSQ 之外的用途；正反各有一个测试。

标识符长度校验去掉了三档模式：超长的名字到不了服务端，`warn` / `skip` 只是把失败推后；生成期也校验
（派生的索引名最容易超限）。

## 决定：v5 的命名规则，改回去之前先读这里 (2026-09-16，v5)

v5 不背兼容，一次把名字改到"最合理"。定下的几条规则，每条都是有意的：
- 错误**类型**以 `Error` 结尾（`OptimisticLockError`），`Err` 前缀只留给哨兵变量——Go 标准库的惯例。
- `tsq gen --help` 曾在 v5 里印着 `@TABLE`（门只看符号不看文字）；编译错误里的方法名同样是给人读的文案，见 `../impact/api.md`。
- 导出面只留使用者用得到的（2026-09-19 逐个查示例和文档的引用）：只供库内部读的取值方法一律不导出。
- 右值接口叫 `Operand` / `ListOperand`（不叫 `RHS` / `SetRHS`，Set 已是 UPDATE 赋值）；装列名的字段叫 `Columns`。
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
- 方言类型不加 `DDL` 前缀。模板不许拼接常量名（`Kind{{ .Kind }}`）：符号门禁只认完整的 `tsqdialect.X`，
  拼出来的名字改名后照样"通过"，所以由 `columnKindRef` 显式列出。

## 决定：v5 不留兼容别名，且"不用接收者的方法"要变成函数 (2026-09-09)

v4 攒下九个 `Deprecated` 符号，没有任何门禁会提醒它们该走——**兼容包装只会积累**，删掉它们本身就是
大版本存在的理由。同一波删掉 `Unique` / `NUnique` / `Concat`（只会返回构建错误）和 `Column.Now()`。

**方法体里不出现 `c.`，就说明它不该是方法**：`User_Name.Now()` 和 `User_ID.Now()` 完全一样，
`ExistsSub` 逼调用方随便挑一列。后者的参数类型还未导出——**调用能编译，但使用者写不出类型名**，
也就写不了 helper；现在是泛型 `Exists[T](Subquery[T])`，任何阶段都能传。

## 决定：CLI 不拆子模块，收紧根包自己的依赖 (2026-09-17 重测)

只 import 根包的模块 tidy 后，生成器的依赖本就
不在其 `go.sum`；进去的是根包的非测试 import 和**根包测试**的 import（tidy 记录依赖包测试的依赖）。修法：MySQL
错误改反射读取、集成测试挪进 `internal/integration`，门是 `TestRootPackageImportsNoDriver`，只剩 SQLite 驱动（根包
单测离不开它）。复测：临时模块 `replace` 到本仓，tidy 后看 `go.sum`。

## 决定：v5 不支持复合主键 (2026-09-17)

维护者定案：`pk=A,B` 报错并指向单列代理键 + `//tsq:unique A,B`；要支持就是 v6（`TableOf` 的 K、`Get` / `Fetch`、`BatchDeleteByPK`、乐观锁 WHERE 全要变形状）。
