# 项目内存 — API 契约、命名与依赖面

判据与索引在 `../memory.md`。

## 全局 `Init()` 和 engine 中间层是被删掉的，不要重新引入 (2026-08-21)

全局单例让"这个查询用的是哪个库"不可回答、测试没法并行；中间层是纯转发。换成显式的 `Open` / `NewRuntime`。
**任何"方便起见加个全局默认 runtime"都是在往回走。**

## 决定：公开的 `dialect` 只有名字和事实，实现在 `internal/sqldialect` (2026-09-19，v5)

`Dialect` 曾导出十九个方法（含 schema 探查和 DDL 渲染）却"不是扩展点"：tag 之后每次内部调整都算破坏
契约。对外只收 `dialect.Name`；代价是根包 import 这**唯一一个** internal 包，集成测试仍只用导出 API。

## 文档描述了一个不存在的阶段；"最紧的上限"是断言要去量 (2026-08-28)

`api-check` 和 `doc-check` 只看符号：**"文档提到的符号都存在"不等于"描述的用法都成立"**（三轮审计各抓到几处：`SelectValue(Sum)` 会被拒、Attach 承诺的顺序被 `ListIn` 拒绝、`ScalarNull`……），
改示例先跑一遍；**复写的副本最先过时**（`docs/skill.md` 复述规则那一节因此换成链接）。上限曾写死 65535 注释"最紧的"，SQLite 其实是 32766——**修一类 bug 要把这一类的实例都数一遍**。
同类：**stringly-typed 的开关，空值 `""` 永远是那个没人写的分支**，用类型化枚举或不留开关。接口里"有定义、有实现、零调用"的钩子
（2026-08-26：`Dialect.ReturningClause` 零调用，PG 上 `Insert` 从没回填主键）规则在 `../impact/runtime.md` § 给 `Dialect` 接口加了钩子。

## 决定：v5 核心重写——表达式树、命名参数、表描述符、封闭执行器 (2026-09-17)

发版前的设计审计发现四个根上的问题，改实现只是在上面雕花，于是重写了根包：
- **SQL 曾是带 base64 标记的字符串**，执行前扫文本判断方言能力：字面量里的 `FOR UPDATE` 被当成行锁，以文本进入
  外层的子查询又被漏报。现在是片段树按方言渲染，**能力由渲染那个构造的代码报告**，使用者的原样文本从不被扫描。
- **`EQVar()` 的值曾按位置从 `args ...any` 取**，是"类型安全"里最大的洞。现在 `Param[T]` 按身份绑定。否决了
  "生成带类型签名的查询函数"（v4 那样，函数爆炸）和"按列身份隐式绑定"（同一列两个值时无解）。
- **表元数据曾是使用者结构体上的七个方法**（和字段重名）：现在是 `TableOf[R, K]` 描述符。
- **`Executor` 曾就是 database/sql 的三个方法**，裸 `*sql.DB` 能编译：现在是封闭接口。

列函数的可移植性只有真跑三方言才知道（MySQL `LENGTH` 数字节、PG 没有 `round(double, int)`、SQLite `UPPER` 只认 ASCII），门是 `TestIntegrationColumnFunctionsArePortable`。

## 决定：Runtime 用函数式选项，并且不关别人的连接池 (2026-09-16，v5)

`options ...*RuntimeOptions` 让"没传选项"和"传了一个选项值"是同一个签名，字段零值又兼任"没设置"。`Open`（自己开池）/
`NewRuntime`（用别人的池）照 `sql.Open` 分工；**`Close()` 只关自己开的池**（`ownsDB`），正反各有一个测试。
标识符长度校验去掉了三档模式（超长名字到不了服务端，`warn`/`skip` 只是推后失败）；生成期也校验（派生索引名最容易超限）。

## 决定：v5 的命名规则，改回去之前先读这里 (2026-09-16，v5)

v5 不背兼容，一次把名字改到"最合理"。定下的几条规则，每条都是有意的：
- 错误**类型**以 `Error` 结尾（`OptimisticLockError`），`Err` 前缀只留给哨兵变量——Go 标准库的惯例。`tsq gen --help` 曾在 v5 里印着 `@TABLE`（门只看符号不看文字）；编译错误里的方法名同样是给人读的文案，见 `../impact/api.md`。
- 导出面只留使用者用得到的（2026-09-19 逐个查示例和文档的引用）：只供库内部读的取值方法一律不导出。
- 右值接口叫 `Operand` / `ListOperand`（不叫 `RHS` / `SetRHS`，Set 已是 UPDATE 赋值）；装列名的字段叫 `Columns`。
- 否定一律 `Not*`（`NotIn`、`tsq.NotLike`）；同一个意思只留一种拼法（`NIn` 与 `NotExists` 并存过，`Join` 与 `InnerJoin` 也是，删前者）。
- 必填参数写成 `(first, more...)`（2026-09-28 维护者定案，编译期挡住空 `Where()` / 无 `ON` 的 join）。**`Select` 例外**：`Select(R.Columns()...)`
  是主用法，拆开会逼每个调用写 `cols[0], cols[1:]...`。只对部分类型有意义的操作一律包级泛型函数（`tsq.Like[S Text]`），不留列方法。
- 可选参数用函数式选项（`RuntimeOption`、`BatchOption`），不用 `...*XxxOptions`。只对插入有意义的
  `WithSkipDuplicates` 传给别的 `Batch*` 会**报错**而不是被忽略：被静默忽略的选项就是 v4 的零值歧义。
- 事务选项跟在回调后面（`WithTx(ctx, fn, tsq.WithRetry(...))`），和 `RuntimeOption` 一个形状；
  `TxOptions` 结构体让九成调用在中间传 `nil`。`WithTablePolicy` / `WithIndexPolicy` **保留**：表来自迁移、
  只让 TSQ 管索引的项目，合并成一个选项就表达不了。追踪给 `TraceInfo{Op, Table}`，没有表名的
  span 说不清在做什么。
- 表是描述符，方法在 `TableOf` 上，不在使用者的结构体上。`Table` 的方法叫 `TableName()` 不叫
  `Name()`：生成结构体的列字段常叫 `Name`，同名会遮住接口方法。
- 没有 `BuildSubquery` / `AsSubquery`：阶段和 `*Query` 自己实现 `Subquery[O]`，`SelectValue` 的阶段就是
  值的子查询，错误推迟到外层 `Build`（旧形状要把选出的列再写一遍）；把 `O` 当值类型是安全的，只有
  `SelectValue` 的 `O` 会和某列的 `T` 相同。`MapInto` 的 JSON 名默认取源列，要改才 `.Named`。
- 按主键读在库里且有类型（`TableXxx.Get` / `Fetch`），唯一索引生成 `GetByX` / `FetchByX`，缺行时包装
  `sql.ErrNoRows`；单行写入的错误**只带主键**（`users id=5`），不序列化整行（列值会进日志）。
- 客户端分页错误统一为 `PageRequestError`（2026-09-29，原 `SortError` 只管排序）；`Executor` 不加 `Dialect()` 方法而用 `DialectOf`：
  导出方法排在 `needsRuntimeOrWrapExecutor` 前面，传 `*sql.DB` 时编译器就不再报那个指路的方法名。
- 2026-09-28 第二轮：`UpdateTable` → `UpdateStage`（只有 `Set`）→ `SetStage` → `MutationStage`，不赋值的 UPDATE 编译不过；能力常量跟构建器方法
  命名、值即 SQL 拼写（删掉别名表）；`WrapExecutor` 返回 error。**`ColumnSpecs()` / `Indexes()` 保持导出**：审计曾想收起，但集成测试和工具靠它改 schema。
- Upsert 的冲突描述是值 `tsq.OnConflict(键...).Update(列...)`（2026-09-29 维护者定案），键和列按 R 定型。否决了做成
  `BatchOption`（R 被擦掉，别的表的列只能运行期报错）和另开 `UpsertOnly`（同一件事两种写法）。
- 方言类型不加 `DDL` 前缀。模板不许拼接常量名（`Kind{{ .Kind }}`）：符号门禁只认完整的 `tsqdialect.X`，
  拼出来的名字改名后照样"通过"，所以由 `columnKindRef` 显式列出。

## 决定：tracer 的契约由库执行，不只写在文档里 (2026-10-06)

"必须调用 `next` 并返回它的错误"曾只是一句话：不调用就是"成功但没执行"（`Insert` 没插入、`Get` 返回 nil 行和 nil 错误），调两次就执行两次，传 nil context 让 database/sql 带着锁 panic、`Close` 永不返回。`Runtime.traced` 现在把三种都变成错误；tracer 仍可以用自己的错误拒绝一次操作。同类（2026-10-09）：`Err()` 早就会说"零值 TableOf"，但写入方法先过 `traceInfo` 解引用 `t.def`、`Query()` / `As()` / `ColumnSpecs()` 直接解引用，照样 panic——防线必须放在方法**最先走**的那条路上，不是放在后面某处。

## 决定：v5 不留兼容别名，且"不用接收者的方法"要变成函数 (2026-09-09)

v4 的九个 `Deprecated` 符号没有门禁提醒它们该走——**兼容包装只会积累**，删掉它们是大版本存在的理由。**方法体里不出现 `c.`，就说明它不该是方法**
（`User_Name.Now()` 与 `User_ID.Now()` 相同）；`ExistsSub` 的参数类型未导出——**调用能编译，但使用者写不出类型名**，写不了 helper；现在是泛型 `Exists[T](Subquery[T])`。

## 决定：CLI 不拆子模块，收紧根包自己的依赖 (2026-09-17 重测)

生成器的依赖本就不进使用者的 `go.sum`；进去的是根包及**根包测试**的 import。MySQL 错误改反射读取、集成测试挪进
`internal/integration`，门是 `TestRootPackageImportsNoDriver`。复测：临时模块 `replace` 到本仓，tidy 后看 `go.sum`。

## 决定：v5 不支持复合主键 (2026-09-17)

维护者定案：`pk=A,B` 报错并指向单列代理键 + `//tsq:unique A,B`；要支持就是 v6（`TableOf` 的 K、`Get` / `Fetch`、`BatchDeleteByPK`、乐观锁 WHERE 全要变形状）。
