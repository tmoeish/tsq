# 变更日志

本文档记录了 TSQ 项目的所有重要变更。

**注意：** 本项目曾误发过一系列 `v1.0.x` 版本。为了纠正版本混乱，我们已撤回 `v1.0.20` 及其之前的所有版本，并正式从 `v1.1.0` 开始新的迭代。

格式基于 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.0.0/)，
项目遵循 [语义化版本控制](https://semver.org/lang/zh-CN/)。

## [未发布]

v5 是一个重新设计过的版本，不提供对 v4 的兼容层：没有别名、没有迁移命令、没有旧注解语法的读取器。
模块路径是 `github.com/tmoeish/tsq/v5`，CLI 用 `go install github.com/tmoeish/tsq/v5/cmd/tsq@latest` 安装。

### 新增

- **TSQ 自己的拼法变了，`tsq gen` 也写一段迁移**：`tsq.json` 现在记下每个方言上每一列的渲染（类型、默认值、范围约束）。新版本的 TSQ 把一列拼成了别的样子而模型没变——比如这几波的 UTC 默认值、范围约束、带精度的类型——此前 `.sql` 文件里什么都不会多，用它建出的库在运行时启动就是一处不匹配。现在这和模型变化一样是一段带日期的迁移（历史里记作 `respell column ...`）：MySQL / PostgreSQL 写 `ALTER`，SQLite 在默认值或约束变了时重建、只是类型拼法在同一亲和性内变了就写"nothing to run"。升级 TSQ 后 `tsq gen --check` 会先红一次，跑一遍 `tsq gen` 写下这段即可。在记录渲染之前生成的文件没有可比的对象：第一次运行只记录，更早的拼法变化不补段（运行时 `Validate` 会指出、`Reconcile` 会补）。
- **SQLite 上也拒绝 MySQL / PostgreSQL 会拒绝的值**：SQLite 不检查 `VARCHAR(n)` 的长度、也没有 JSON 类型，于是本地 SQLite 上写得进去的超长字符串和非法 JSON 到了 MySQL / PostgreSQL 才报错。现在写进列的值（`Insert` / `Update` / `Upsert` / `Batch*`，以及 `Set(col, tsq.Val(v))` / `Set(col, 参数)`）在语句执行前按列的长度（按字符数）检查，SQLite 上超长就拒绝并说明"SQLite 会收下、MySQL 和 PostgreSQL 会拒绝"；不是 JSON 的 `json.RawMessage` 在三个引擎上都被拒绝。比较不受影响（超长的值只是匹配不到），`type:` 列和落到 TEXT 一族的长度也不检查。
- **整数列在 PostgreSQL 和 SQLite 上也守住字段的范围**：PostgreSQL 没有无符号类型（`uint32` 落到 `BIGINT`），SQLite 的 `INTEGER` 不论字段多宽都是 64 位，于是 `Set(t.Stock, tsq.Sub(t.Stock, qty))` 减到负数、乘法越过字段宽度时，值写进去了、字段读不回来，这一行从此读不出；MySQL 的 `UNSIGNED` 和宽度会直接拒绝写入。现在列的类型比字段宽时，TSQ 随列写一条 `CONSTRAINT ck_<列> CHECK (...)`（PostgreSQL 上的无符号字段；SQLite 上除 `int64` 以外的所有整数字段），越界写入三个引擎一致被拒绝。约束是声明的一部分：此前建的表在 `Validate` 下是一处不匹配，`Reconcile` 会补上（已有越界行时拒绝）；`tsq gen` 的 `CREATE TABLE` 带着它；SQLite 的重建不再被 TSQ 自己的 `CHECK` 阻断，别人写的 `CHECK` 仍然阻断。原始 `type:`、主键和生成列不加。
- 每个查询阶段都有 `SQL(dialect, args...)`：不必先 `Build()` 就能看渲染出的 SQL，文档原本就是这么写的。
- 算术表达式 `tsq.Add` / `tsq.Sub` / `tsq.Mul` / `tsq.Div`，按 `Number` 约束类型：扣库存写 `Set(t.Stock, tsq.Sub(t.Stock, qty))`，不再需要 `Exprf` 和丢了类型的参数。整数除法在三种方言上都取整（MySQL 的 `/` 返回小数，那里写成 `DIV`）；除以零在 PostgreSQL 上报错、在另两个方言上是 NULL，所以除数不是非零 `tsq.Val` 时 `Div` 的结果按可能为 NULL 处理。
- 新增游标分页 `Query.PageKeyset(ctx, db, tsq.Keyset{Size, OrderBy, After}, args...)`：按上一页最后一行的排序值翻页，深页不再越翻越慢、中途插入的行也不会让页面错位。排序列必须被查询选出，且包含查询里每张表（FROM 和每个 JOIN）的主键；`Next` 是不透明字符串，换了排序会被拒绝。`PageRequest` 新增 `After` 字段和 `Keyset(sortable...)`。
- 关键词搜索改为执行参数 `tsq.Keyword(term)`，所有读取方法都能用（此前只有 `Page` 通过 `Paging.Keyword` 支持，搜索结果没法 `Iter` 导出或单独 `Count`）；`Paging.Keyword` 删除。空关键词不搜索，对没有 `Search` 的查询传非空关键词报错。
- 新增 `Query.ListIn(ctx, db, listParam, values, args...)`：列表参数超过方言绑定上限时按上限分块、在同一快照里读完再拼接，只接受分块不改变结果的查询。`TableXxx.Fetch` / `FetchBy` 用它，任意数量的键都能取（此前超过 SQLite 的 32766 个就报错）。
- 新增 `TableXxx.Restore` / `BatchRestore` 和生成的 `row.Restore(ctx, db)`：恢复软删除的行，是清除 `deleted_at` 的唯一入口。
- 根包不再 import 任何数据库驱动（MySQL 错误改为反射识别），根包测试也不再 import 驱动和 nullbio：只用库的项目 `go mod tidy` 之后 `go.mod` 不会多出间接依赖，`go.sum` 里只剩 SQLite 驱动（根包单测需要）。
- `TableXxx.GetBy(ctx, db, col, value, conds...)` / `FindBy`（没有时 `nil, nil`）：按唯一列读一行（列加上 `conds` 里用 `EQ` 固定的列，必须覆盖主键或某个唯一索引，否则报错，而不是返回任意一行），与 `FetchBy` 成对；生成的 `GetByX` / `FindByX` 调它，没有额外条件时查询只构建一次（此前每次调用都重新构建和渲染）。
- `TableXxx.Upsert(ctx, db, &row, tsq.OnConflict(key...))` 和 `BatchUpsert(ctx, db, rows, conflict, options...)`：按主键（不写 `OnConflict`，批量版传零值 `tsq.Conflict[R]{}`）或某个唯一索引插入或更新，PostgreSQL / SQLite 渲染成 `ON CONFLICT ... DO UPDATE`，MySQL 渲染成 `ON DUPLICATE KEY UPDATE`。`tsq.OnConflict(key...).Update(cols...)` 让冲突的行只改这几列（外加 `updated_at`、`version`），否则写整行——nil 字段会把库里的值写成 NULL。键和列按表的行类型定型，别的表的列编译不过。更新时 `version` 自增不校验、`updated_at` 刷新、`created_at` 保留；单行版本回读主键、`version`、`created_at` 和数据库填的列，PostgreSQL / SQLite 上用同一条语句的 `RETURNING`。MySQL 会匹配所有唯一键，因此行可能撞上别的唯一键时直接拒绝。追踪操作名为 `upsert`。
- `Query.Iter(ctx, db, args...)` 返回 `iter.Seq2[*O, error]`，逐行扫描，大结果集不必整体读进内存；`break` 会结束查询。追踪操作名为 `iter`。
- `tsq.DialectOf(db)`：任何执行器（包括 `WithTx` 回调里的）的方言，事务里也能用 `dialect.Supports` 选查询形状；此前只有 `*Runtime.Dialect()`。
- 阶段上直接有 `Iter` 和 `PageKeyset`（此前只有 `Page`，其余要先 `MustBuild()`）。
- `tsq.RebindNull(col, table)`：`NullColumn` 换表后仍是 `NullColumn`（`WithTable` 返回 `Column`，生成代码此前要做类型断言）。
- 追踪里硬删除是 `hard_delete`、恢复是 `restore`，和软删除 `delete` 分开（此前软硬删除同名）。

### 破坏性变更

**注解**

- 注解是 `//tsq:` 指令行（`//go:` 的形态），一行一个关注点，gofmt 不会改动它们，所以没有 `tsq fmt`：

  ```go
  //tsq:table name=course pk=ID
  //tsq:managed created_at
  //tsq:unique Title
  //tsq:index TrackID
  //tsq:search Title,Summary
  ```

  `pk=` 默认自增，`assigned` 表示由调用方给值；索引没写 `name=` 时按 `ux_` / `idx_` 加表名和字段推导。指令写错时，报错带文件名和行号。

**表描述符**

- 生成的表是一个结构体 `XxxTable`：内嵌 `*tsq.TableOf[Xxx, K]`（K 是主键类型），**每列一个字段**——`TableCourse.Title` 而不是包级变量 `Course_Title`，`TableCourse.Columns()` 而不是 `Course__Cols`。表名、列、主键、自增、托管列、搜索列、物理 schema 与索引都在这一个值上。**行结构体不再实现任何接口**，`Owner` / `Result` 标记接口删除。
- 一个构造函数依次建表、建列、`Define`，引用 `TableXxx` 的东西天然在表完成之后初始化，没有声明顺序要记，`DeclareTable` 删除。手写表同样用 `tsq.NewTable[R, K]` / `tsq.NewColumn` / `Define`（有 `deleted_at` 的表用 `tsq.NewSoftDeleteTable[R, K]`，列绑到它内嵌的 `TableOf`，`Define(spec, deletedAt)`），定义错误由 `Err()` 和每个用到它的查询报告。
- 按主键读写都有类型：`TableXxx.Get(ctx, db, id)`（没有时包装 `sql.ErrNoRows`）、`Find`（没有时 `nil, nil`）、`Fetch(ctx, db, ids...)`（按给定顺序、任意数量），`BatchDeleteByPK` / `BatchHardDeleteByPK` 收 `[]K`——传错类型编译不过（此前收任意 `Arg`，运行时才检查）。`TableXxx.FetchBy(ctx, db, col, values, conds...)` 按其他唯一列取，`TableXxx.Query()` 是读全表（带声明的搜索列）的查询。
- 别名是表的方法：`pre := TableCourse.As("pre")` 返回的表上每列都已绑到别名（`pre.ID`）；`Column.As` 和 `tsq.AliasTable` 删除，单列改绑用 `col.WithTable(source)`。`Table` 接口的 `Name()` 改名为 `TableName()`，列字段因此可以叫 `Name`。
- `tsq.UpdateTable` / `HardDeleteFrom` 收 `tsq.RowTable[R]`，`tsq.DeleteFrom` 只收 `tsq.SoftDeleteTable[R]`；生成的表结构体、`*tsq.TableOf` 和 `*tsq.SoftDeleteTableOf` 按各自的形状满足它们。对别名执行会被拒绝。
- `TSQTables()` 返回 `[]tsq.Table`，`TableRegistration` 删除；schema 与索引从描述符读取，`ColumnSpecs()` / `Indexes()` 可供工具使用。手写 `TableSpec` 时 `Define` 要求 `ColumnSpecs` 覆盖每一列、主键和自增与 `PrimaryKey` / `AutoIncrement` 一致。
- `Paging.Offset()` 删除：offset 是 `Page` 的实现细节，使用者手算 offset 正是它要避免的事。`DefaultMaxPageSize` 挪到分页文件并改为约束 `Paging.Size`。

**参数**

- 执行期的值是**参数**，不再按位置传：`TableCourse.ID.EQ(TableCourse.ID.Param())` 写进查询，执行时传 `TableCourse.ID.Bind(5)`；列表用 `In(col.ListParam())` 与 `col.BindList(ids...)`；一列需要两个值时用 `tsq.NewParam[T]("name")`。执行方法的变参类型是密封的 `tsq.Arg`，按参数身份匹配：缺值、多余的值、重复绑定都会报错，值的类型在编译期检查。
- 所有 `*Var()` 谓词、`SetVar`、`Bind` / `BindSlice` / `Expression` 删除。
- 模式匹配是包级函数：`tsq.StartsWith(col, pattern)` / `EndsWith` / `Contains` 及 `Not` 形式，`pattern` 是 `tsq.Val("x")` 或参数（`tsq.Pattern[S]`），不再分值和参数两套函数；只接受字符串类的列（`tsq.Text`），都会转义通配符并声明 `ESCAPE`。按原样使用模式的 `Like` / `NotLike` 同样改为包级函数 `tsq.Like(col, pattern)`，列方法删除（此前整数列上也能调用）。
- 固定值统一写成 `tsq.Val(v)`，列表写成 `tsq.Vals(vs...)`，放在任何接受同类型列的位置：`EQ` / `Between` / `tsq.Like` / `Set` / `Case` / `When` / `Coalesce`……`EQVal` / `InVal` / `BetweenVal` 等全部 `*Val` 方法，以及 `SetVal`、`WhenVal` / `ElseVal`、`CoalesceVal` / `NullIfVal` 删除。值的类型只由值本身推断，无类型数字常量是 `int`：`int64` 列上写 `tsq.Val(int64(90))`，写错时编译报 `does not implement tsq.Operand[int64]`。比较里的 `NULL` 报错，`Set` 里 nil 指针写入 `NULL`。

**查询**

- **类型系统区分可空列**：可为 NULL 的字段（指针、`sql.NullX`、`sql.Null[T]`、nullbio 类型）生成为 `tsq.NullColumn[X, T]`（`tsq.NewNullColumn[T]`），按值类型 `T` 比较（`Nickname.EQ(tsq.Val("x"))`），`UpdateTable(...).SetNull(col)` 只接受它。查询在读行之前检查：可空列、外连接可选侧的表、无 `GROUP BY` 的 `SUM`/`AVG`/`MAX`/`MIN`、`NullIf`、无 `Else` 的 `CASE`、标量子查询，读进不能存 NULL 的字段一律报错（此前要等数据里真有 NULL 才在扫描时失败）；`tsq.MapIntoNull` 映射进可空字段，`Coalesce` 消除可空性，`tsq.SelectNullValue` 把单个值读成 `sql.Null[T]`（`Scalar` 此前把 NULL 静默读成零值）。`Set` 往 NOT NULL 列赋可能为 NULL 的值时报错。`tsq.Text` / `tsq.Number` 不再包含 `sql.NullX`。`NewColumn` 用在可空字段类型上是定义错误。
- SQL 在执行时按方言从表达式树渲染并按方言缓存，`Condition` / `SQLColumn` 不再暴露 `Clause()` / `SQLExpr()` 字符串；要看 SQL 用 `Query.SQL(dialect.X, args...)`（`Query.String()` 删除：它一律按 SQLite 渲染，会误导），`ListSQL` / `CountSQL` 等删除。方言能力（`FULL JOIN`、行锁、CTE、`INTERSECT` / `EXCEPT`）由渲染该构造的代码检查，不再扫描 SQL 文本。
- 阶段接口去掉了 SQL 不允许的转移：分组、`HAVING`、集合操作之后不能加行锁，带搜索的查询不能做集合操作。构建器的具体类型不再出现在签名里，`Select(...).From(...)` 返回 `JoinStage`。
- **查询阶段本身就是子查询**：`tsq.SelectValue(col).From(t).Where(...)` 直接放在比较、`In`、`Set` 的右边，不用先 `Build`，错误由外层 `Build` 报告；任何阶段都能传给 `Exists`。`tsq.BuildSubquery` 和 `Query.AsSubquery` 删除（它们要把选出的列再写一遍，每个子查询多一段错误处理）。
- `tsq.MapInto(source, field)` / `MapIntoNull(source, field)` 不再要求 JSON 名，默认取源列的；需要时 `.Named("x")`。
- 查询只有一个入口 `tsq.Select(...).From(...)`，`tsq.From[O](t).Select(...)` 删除。
- `TableXxx.Update(ctx, db, &row, cols...)` 和生成的 `row.Update(ctx, db, cols...)` 可以只写指定的列（`updated_at`、`version` 照常维护）：部分 `Select` 读出的行用它保存。**对这样的行直接 `Update` / `BatchUpdate` / `Upsert` 会报错**并列出它读过的列，不再悄悄把没读的列写成零值（库用弱引用记住这些行，行被回收后记录随之消失）。
- `CASE` 的结果有类型，且第一个分支是必填参数：`tsq.Case(cond, rhs).When(cond, rhs).Else(rhs).End()`，类型由第一个分支推断；没有分支的 `Case[T]()` 删除。
- 列函数从列方法改为**包级泛型函数**，并按列类型约束：`tsq.Upper(col)` / `Lower` / `Trim` / `Length` / `Substring` 只接受字符串类的列（`tsq.Text`），`tsq.Sum` / `Avg` / `Round` / `Ceil` / `Floor` / `Abs` 只接受数值列（`tsq.Number`），`tsq.Count` / `CountDistinct` / `Max` / `Min` / `Date` / `Year` / `Month` / `Day` / `Coalesce` / `NullIf` 接受任意列。套在类型不合的列上编译不过。
- 列函数在三个方言上返回相同的值：`Year` / `Month` / `Day` 返回 `int64`（此前返回列自身类型且得到文本），`Date` / `Year` / `Month` / `Day` 只接受 `time.Time` 列（可空的也行）；`Date` 返回 `'YYYY-MM-DD'` 文本；`Length` 数字符（MySQL 上是 `CHAR_LENGTH`，此前数字节）；`Round` 在 PostgreSQL 的浮点列上也能用；`Substring` 的边界直接写进 SQL，避免 PostgreSQL 选错重载。SQLite 上的日期函数同时认 modernc 驱动默认的 Go 时间文本格式（此前返回 NULL）。
- 列方法 `Distinct()` 删除（放在选择列表中间会生成非法 SQL），改为 `tsq.CountDistinct(col)` 和查询级的 `tsq.SelectDistinct(...)`。
- 搜索列由 `tsq.Searchable(col)` 声明，只接受字符串类的列；`//tsq:search` 和 `//tsq:fulltext` 接受 `string` 以及底层类型是 `string` 的具名类型（此前只认字面的 `string`）。
- 阶段接口由 `tsq.Sortable` / `Lockable` / `Combinable` / `Groupable` 组合而成，helper 可以只接受其中一种能力。
- 集合操作查询的 `OrderBy` 按输出列名渲染，三个方言都能执行。
- `tsq gen` 在生成时拒绝超过任一方言长度上限的表名、列名和索引名，并给出修改方法（通常是给索引写 `name=`）；此前要到运行时启动才报错。
- 相关子查询的外层表会传给外层查询校验：外层没有提供该表时构建失败。
- 新增 `tsq.Not(cond)`；`GroupBy` 只能调用一次。
- **必填参数在签名上**：`Where(cond, more...)`、`Search(col, more...)`、`GroupBy(col, more...)`、`Having(cond, more...)`、`OrderBy(term, more...)`、`Correlate(t, more...)`，带条件的 join 写成 `InnerJoin(t, on, more...)` / `LeftJoin` / `RightJoin` / `FullJoin`：空调用和不带 `ON` 的 join 编译不过（此前构建时才报错，或渲染出 MySQL 拒绝的 `JOIN t`）。运行期拼出来的条件列表写成 `Where(tsq.And(conds...))`。`Join` 删除，内连接统一写 `InnerJoin`；没有条件的是 `CrossJoin`。
- 排序、截取和行锁的子句按 SQL 的顺序、各一次出现在类型上：`OrderBy` → `Limit` → `Offset`（`Offset` 只能跟在 `Limit` 之后），行锁后最多一个 `NoWait` / `SkipLocked`，`Case(...).Else(...)` 之后只有 `End()`。重复调用、`Offset` 不带 `Limit`、`Else` 之后再 `When` 此前能编译、构建时才报错（后者静默把分支挪到 `ELSE` 前面）。新增阶段 `LimitedStage`、`OffsetStage`、`LimitedResultStage`、`CaseElseStage`。
- 已构建的 `*Query` 和阶段一样能做集合操作的操作数（`Union(q)`）和 CTE 的查询体（`tsq.CTE("x", q)`）；此前只收阶段，构建一次的查询没法复用。
- 阶段接口和能力接口（`Sortable`、`Lockable`、`Combinable`、`Groupable`、`SelectStage`、`CaseStage` 等）是封闭的：只有本包的构建器实现它们。

**写入**

- 行写入在表描述符上：`TableXxx.Insert/Update/HardDelete(ctx, db, &row)` 与 `BatchInsert/BatchUpdate/BatchHardDelete(ctx, db, rows, options...)`，软删除表另有 `Delete` / `BatchDelete`；生成的行方法转发给它们。包级的 `tsq.Insert` / `tsq.Update` / `tsq.Delete` / `tsq.Batch*` 删除，按主键删除是 `TableXxx.BatchHardDeleteByPK(ctx, db, ids, options...)`，软删除表另有 `BatchDeleteByPK`。
- 托管列由库维护，不再由生成代码维护：`Insert` 只在未设置时填 `created_at` / `updated_at`，`Update` 总是刷新 `updated_at`。单行写入的错误带主键（`users id=5`），乐观锁冲突以 `*OptimisticLockError`（字段导出）包装返回。
- 按条件写：`tsq.UpdateTable(TableXxx)` / `tsq.DeleteFrom(TableXxx)` / `tsq.HardDeleteFrom(TableXxx)`，`UpdateTable` 返回 `*UpdateStage[R]`，上面只有 `Set` / `SetNull`，第一次赋值之后是 `*SetStage[R]`（`Set` 是泛型方法，只能是具体类型），所以不赋值的 UPDATE 编译不过；删除返回封闭接口 `DeleteStage[R]`，`Where(cond, more...)` 之后是封闭的 `MutationStage[R]`；`Set` 接受列、参数、`tsq.Val` 或子查询，`tsq.Val` 包 nil 指针可写 `NULL`。`Mutation.SQL()` 改为 `SQL(dialect, args...)`。

**执行器与运行时**

- `tsq.Executor` 是封闭接口：`*Runtime`、`WithTx` 回调里的执行器、`tsq.WrapExecutor(handle, dialect.MySQL)` 的结果（`handle` 是任何 `tsq.DBTX`：`*sql.DB`、`*sql.Tx`、`*sql.Conn`）。**裸 `*sql.DB` 不再能传入**——库必须知道方言才能渲染。
- `tsq.Open(ctx, driver, dsn, tables, ...)` 自己开连接池；`tsq.NewRuntime(ctx, db, dialect.Postgres, tables, ...)` 用调用方已有的池，`Close()` 只关闭自己开的池。选项是函数式的：`WithSchemaPolicy` / `WithTablePolicy` / `WithIndexPolicy` / `WithLogger` / `WithSQLLogging` / `WithTracers` / `WithMaxPageSize`。
- Schema 策略四档：`Manual`（默认，生产用）、`Validate`、`CreateMissing`、`Reconcile`（开发和测试用，改了结构重启就跟上）。**TSQ 从不删表**：不删表、不删未声明的索引，也不建任何记账表；`Reconcile` 会删掉表里不再声明的列。
- 标识符长度校验恒为严格，没有关闭开关。
- `Tracer` 的签名是 `func(ctx, info tsq.TraceInfo, next) error`：`info.Op` 是操作，`info.Table` 是写入的表或查询的 FROM 表（span 名终于能说清是哪张表）。`UpdateTable` / `DeleteFrom` 报 `update` / `delete` 而不是 `exec`；`TraceOpScalar` / `TraceOpExec` 删除。
- `Runtime.Dialect()` 返回方言名 `dialect.Name`。

**读写语义**

- **软删除是一种表类型**：声明了 `deleted_at` 的表生成为内嵌 `*tsq.SoftDeleteTableOf[R, K]` 的结构体，只有它有 `Delete` / `BatchDelete` / `BatchDeleteByPK` / `Restore` / `BatchRestore` / `WithDeleted()`，`tsq.DeleteFrom` 也只收它。没有 `deleted_at` 的表上写这些是**编译错误**（`tsq.DeleteFrom` 报 `missing method needsDeletedAtOrHardDeleteFrom`），删除只有 `HardDelete` / `BatchHardDelete` / `BatchHardDeleteByPK` / `tsq.HardDeleteFrom`。于是 `Delete` 永远是软删，会真删数据的调用永远带 `Hard`：`grep HardDelete` 就是工程里全部物理删除点。
- **已删行是表的默认作用域**：引用这张表的每个查询（包括手写查询、JOIN 里的表、子查询和 CTE 里的表）以及 `UpdateTable` / 软 `DeleteFrom` 都看不到已删行。LEFT JOIN 的条件并进 `ON`；有 RIGHT / FULL JOIN 时表按活行派生表读取。`TableXxx.WithDeleted()` 只去掉活行过滤、不改变语句做什么：经由它的 `Delete` / `BatchDeleteByPK` / `DeleteFrom` 仍然是软删除，已删行被重新盖一次墓碑；不经由它时，重复的软删除不会重写墓碑时间。`HardDeleteFrom` 作用于所有行。软删除走 UPDATE，乐观锁校验、`version` 自增和 `updated_at` 刷新照常生效。`DeleteFrom` 的软删除时间戳**在执行时**计算（此前在构建时计算，包级语句会一直写入进程启动的时间）。
- `tsq.Open` 接受 `sqlite3`（github.com/mattn/go-sqlite3）这个驱动名，并且能识别它的错误类型——它把 SQLite 结果码放在结构体字段里而不是方法上，此前重复键和 busy 重试在这个驱动上会静默失效。
- 新增 `tsq.IsDuplicateKeyError`：判断主键或唯一索引冲突，不用自己去匹配各驱动的错误类型。
- 按方言分叉的 SQL 片段可以延迟到渲染时构造（`tsq.Matches` 的 PostgreSQL 分支因此用当前方言来引号和拼表达式，而不是由根包自己挑一个方言实例）。
- **只支持 MySQL / PostgreSQL / SQLite**，公开 API 只收方言名，没有可以实现的方言接口。
- 行写入绑定值不再走反射（列上带一个由生成的访问器构成的取值函数）：100 行的批量 INSERT 约快 19%，批量 UPDATE 约快 28%（`write_bench_test.go`）。
- 新增 `tsq.AttachMany` / `tsq.AttachOne`：给一批父行一次性装配子行（内部走 `ListIn`，父键去重分块），不再需要每行一次查询。子查询由调用方给出，它的过滤、排序和软删除作用域决定哪些子行算在内。
- **全文检索**：`//tsq:fulltext Title,Summary` 声明全文索引，`tsq.Matches(TableXxx.FullTextTitleAndSummary(), tsq.Val(term))` 搜索它：每个全文索引生成一个按字段命名的方法（别名表上同样可用），手写表用 `TableOf.FullText("索引名")`（此前是 `FullText(name ...string)`，没有或有多个索引时运行期才报错）。MySQL 渲染 `MATCH ... AGAINST`（并创建 `FULLTEXT` 索引），PostgreSQL 渲染 `to_tsvector('simple', ...) @@ plainto_tsquery` 并建 GIN 表达式索引，SQLite 没有 TSQ 能管理的全文索引，同一个谓词退化为按子串匹配（`dialect.CapabilityFullTextSearch` 报告是哪一种）。全文索引只按名字对账。
- **数据库填值的列**：`db:"col,default:SQL"` 让列有 DDL 默认值，并且字段为 NULL（nil 指针、无效的 `sql.Null`）时插入语句直接不写这一列（由数据库填）；字段必须能存 NULL，`tsq gen` 和 `Define` 拒绝不能存 NULL 的字段，因为零值（`false`、`0`）也是要写入的值，单行 `Insert` 之后把值读回；`db:"col,generated:SQL"` 声明生成列（`GENERATED ALWAYS AS (SQL) STORED`），`Insert` / `Update` / `Upsert` 永不写它，单行插入后读回。托管列和主键不允许这样标注，`tsq gen` 会拒绝。生成列由建表语句创建，之后 schema 策略不再比较它（三个方言的自省结果不一致）。
- 列定义的 DDL 渲染库和生成器共用一份实现，不再各写一份。
- 新增 `*tsq.RowStateError`（用 `errors.AsType` 判断，它不是可重试的错误，所以没有 `Is*` 函数）：删除一个已删除的行、恢复一个未删除的行，报的是行的状态不对，而不是乐观锁冲突（那种重试没用），没有 `version` 列的表也会报。
- `Query.ListIn` 在列表一条语句装得下时不再开事务。
- **派生表达式不再能直接 `Select`**：列（`Column` / `NullColumn`）知道自己扫描进哪个字段，函数、`CASE`、`Expr` / `Exprf` 产出的是 `tsq.Expression[T]`，没有行归属。此前 `Select(tsq.Date(时间列))` 能编译、执行时才报扫描错误。现在用 `tsq.MapInto` 指定字段，或用新增的 `tsq.SelectValue` / `tsq.SelectNullValue` 让值本身成为行（`Query.Scalar` / `ScalarNull` 因此删除）。`WithTable` / `Param` / `Bind` 只在列上。
- **可空值的排序在三个方言上一致**：NULL 一律当作最小值（升序在前、降序在后），PostgreSQL 显式写 `NULLS FIRST/LAST`；`OrderBy.NullsFirst()` / `NullsLast()` 可改，MySQL 用 `IS NULL` 排序键模拟（集合操作上拒绝）。此前 PostgreSQL 与另两个方言的顺序相反。
- **时间统一用 UTC**：托管时间戳以 UTC 写入，绑定到 SQL 的所有 `time.Time`（含 `*time.Time`、`sql.NullTime`、`null.Time`）也先转成 UTC。SQLite 按文本存时间，不同时区写入的行此前按文本比较和排序会出错。`tsq.UpdateTable` 在执行时自动刷新 `updated_at`（显式 `Set` 的值优先），与 `Update`、软删除、`Upsert` 一致。
- **行级写入不越权改托管列**：`Update` 不再写 `created_at` 和 `deleted_at`，并且在软删除表上只匹配未删除的行——手工构造的行不会把 `created_at` 清零，删除之前读出的旧副本也不会把行复活。软删除只写 `deleted_at` / `updated_at` / `version`，不顺带保存行上其他改动；删除已删除的行报 `RowStateError`（经由 `WithDeleted()` 时重新盖墓碑）。恢复用 `Restore`。`Upsert` 写入的行总是未删除状态。
- 读单行只有两个入口：`Get` 在没有行时返回包装 `sql.ErrNoRows` 的错误，`Find` 返回 `nil, nil`。`Get` / `Find` / `Exists` / `Scalar` 最多读一行，`Exists` 不再走 `COUNT`。`Count` 返回 `int64`。
- 批量写的选项是 `WithBatchSize(n)` 和只对插入有效的 `WithSkipDuplicates()`（传给其他入口会报错）。
- 事务：`runtime.WithTx(ctx, fn, options...)` / `WithTxResult(ctx, fn, options...)`，选项是 `tsq.WithIsolation(level)`、`WithReadOnly()`、`WithRetry(predicate)`、`WithRetryPolicy(policy)`；`TxOptions` 删除（此前九成调用要在中间传一个 `nil`），`DefaultRetryPolicy()` 返回值而不是指针。重试谓词：`IsRetryableTxError`、`IsOptimisticLockError`、`IsRetryableNetworkError`、`IsTxConflictError`。
- 分页：`Query.Page(ctx, db, tsq.Paging{Page, Size, OrderBy}, args...)`，排序项是 `[]tsq.OrderBy`，写错列名编译不过。HTTP 形态的 `tsq.PageRequest` 用 `req.Paging(可排序列...)` / `req.Keyset(...)` 转换，并把请求里的 `keyword` 带给 `Page` / `PageKeyset`（查询有 `Search` 时生效，没有时忽略；另传一个不同的 `tsq.Keyword` 报错——此前忘了传关键词就悄悄返回不带搜索的结果），同时校验（负数、越界页号、非法 order 报错），排序白名单由端点给出；超过 runtime 上限的大小由 `Page` 封顶而不报错。没有单独的 `Validate` / `Normalize`，也没有 `Runtime.MaxPageSize()`。`Page` 的计数和数据在同一个只读事务里读取（MySQL / PostgreSQL 用 `REPEATABLE READ`），并发写入不会让 `Total` 和 `Data` 对不上；传入事务执行器时直接使用该事务。结果类型是 `tsq.Page[T]`（与游标分页的 `KeysetPage[T]` 成对），有 `Page` / `Size` / `Total` / `TotalPages` / `Data`（从不为 nil），`PageRequest.Offset()` / `Response()` 改为 `Paging.Offset()` 与库内部构造。HTTP 参数解析交给调用方的 binder。

**查询 API 命名**

- 否定谓词统一写作 `Not*`：`NotIn`、`tsq.NotLike`、`NotBetween`、`tsq.NotStartsWith`……
- `tsq.Exists(sq)` / `tsq.NotExists(sq)` 是包级泛型函数，任何查询阶段或 `*Query` 都能传，不论选了几列。
- 没有 `Unique` / `NUnique` / `Concat` / `Now()` 这类不读接收者或只会失败的列方法，需要时用 `Expr` / `Exprf`。
- **编译错误自己说出改法**：把字面值直接传给比较，报错是 `missing method needsTsqVal`；传切片给 `In` 是 `needsTsqVals`；类型不对是 `have valueOfType(int) want valueOfType(int64)`；传裸 `*sql.DB` 是 `needsRuntimeOrWrapExecutor`。这些是未导出的方法名，只出现在报错里。
- 右值接口叫 `tsq.Operand[T]`，IN 的列表右值叫 `tsq.ListOperand[T]`：`RHS` 是行话却出现在最常见的编译错误里，`SetRHS` 的 Set 又和 `UpdateStage.Set` 的赋值撞词。
- `OrderBy` / `Limit` / `Offset` 之后的阶段叫 `OrderedStage`（原 `PagedStage`，名字暗示"已分页"，而 `Page` 恰恰拒绝带 `Limit` / `Offset` 的查询）。
- `IndexSpec`（原 `TableIndex`，与 `dialect.ColumnSpec` 对称，`MissingIndexError` 内嵌它，因此也带 `FullText`），`IndexSpec.Columns` 与 `MissingIndexError.Columns`（原 `Fields`，装的是列名，指令里的 field 指 Go 字段）；`TableSpec.ColumnSpecs` 与 `TableOf.ColumnSpecs()`（原 `Schema`，只含列定义，不含索引；`Schema` 也因此不再是保留的列字段名）。
- `Set` 只收表的列 `Column[R, T]`：此前收 `TypedColumn`，`MapInto` 的结果列能编译、运行时才报错。
- 全文检索的检索词类型叫 `tsq.MatchTerm`（和 `tsq.Matches` 配对），避免和关键词搜索那套 `Search` 名字混淆。
- 错误类型以 `Error` 结尾且字段导出：`OptimisticLockError`、`RowStateError`、`PageRequestError`（客户端的分页请求错了都是它，`Field` + `Reason`：页码或页大小为负、页码超过上限、排序字段未知或有歧义、方向不是 asc/desc、两个列表长度不一致、游标无效或属于另一种排序；此前只有排序问题有类型，叫 `SortError`）、`SchemaMismatchError`（`Validate` 下列不一致，`Changes` 列出每一列；此前是纯文本）、`MissingIndexError`、`MissingTableError`（表名字段叫 `Table`，与其他错误一致），以及 `dialect.UnsupportedCapabilityError`；`RowStateError.Op` 是 `tsq.TraceOp`（新增 `TraceOpRestore`，恢复也按它追踪），`Need` 是 `tsq.RowState`（`RowExists` / `RowLive` / `RowDeleted`），此前都是自由文本；`OptimisticLockError` / `RowStateError` 的 `Expected` 和 `Actual` 都是 `int64`。`RegistrationError` 删除，注册错误由 `Define` 报告。
- 其余命名：`NewColumn`、`Runtime.WithTxResult[T]`。
- 只留使用者用得到的导出面：`OrderBy` 只有 `NullsFirst()` / `NullsLast()`（排序方向类型 `Order`、`ASC` / `DESC`、`Reverse` 和两个取值方法是内部实现）；`SQLColumn` 只有 `Name()`；`Param` / `ListParam` 没有 `Name()`；`Page` 只有 `HasNext()`（上一页就是 `Page > 1`）；`SQLColumns`、`TableOf.SearchColumns()` 不导出；`dialect.ColumnSpec` 没有只在读回数据库结构时才有意义的 `NativeType`。

**生成代码**

- 表文件：`XxxTable` 结构体与 `TableXxx` 值，外加 `As(alias)`（返回同样的结构体，列一起改绑），软删除表另有 `WithDeleted()`，返回 `XxxTableWithDeleted`：列、`As` 和全文索引都在，但没有 `GetByX` / `FindByX` / `FetchByX`——软删除表的唯一索引包含 `deleted_at`，一个值只在活行里唯一，在已删行上按它查找写了就编译不过，以及每个唯一索引的 `GetByEmail(ctx, db, email)`、`FindByEmail(ctx, db, email)` 和 `FetchByEmail(ctx, db, emails...)`（复合索引 `A,B` 是 `GetByAAndB(ctx, db, a, b)` / `FindByAAndB` / `FetchByAAndB(ctx, db, a, bs...)`）。列字段按结构体里的声明顺序排列（嵌入结构体的字段在嵌入处），`Columns()` 也按这个顺序选列；此前按字段名排序。主键查询在 `TableOf` 上，不再生成 `QueryXxx*` / `FetchXxxByID` 变量和函数。**普通索引和唯一索引前缀不生成查询**：这类查询需要排序和限量，用构建器写。
- 列字段与表的方法重名（`Update`、`Query`、`Columns`、`As`……）时 `tsq gen` 报错并指出字段，改 Go 字段名即可（`db` tag 保留列名）。
- 生成的参数名按缩写词整体小写（`ids`、`uid`），不再出现 `iDs`。
- 行方法：`Insert` / `Update` / `HardDelete`，软删除表另有 `Delete()` / `Restore()` / `IsDeleted()`（报告加载时这一行是否带墓碑；名字说的是它检查什么，`Active` 容易被读成"业务上启用"）。
- Result：`XxxResult` 结构体（每个结果字段一个 `ResultColumn`）与 `ResultXxx` 值，用法是 `tsq.Select(ResultXxx.Columns()...)`。
- `runtime.tsq.go` 只剩 `TSQTables()`；生成文件、`tsq.json` 和各方言 `.sql` 由 `tsq gen` 维护，不再有 `--tpl` / `--resulttpl`。

**`dialect` 包**

- `dialect` 只剩名字和事实：方言名 `dialect.MySQL` / `Postgres` / `SQLite`（类型 `dialect.Name`）；能力常量与 `dialect.Supports(name, capability)`、`dialect.Check(name, capability)`；`*dialect.UnsupportedCapabilityError`（导出 `Capability`、`Dialect` 字段）；生成代码声明列用的 `ColumnSpec`、`ColumnType`、`ColumnKind`（`KindBool` … `KindTime`）、`Fill`。
- `Dialect` 接口、`MySQLDialect` / `PostgresDialect` / `SQLiteDialect`、schema 探查、DDL 渲染和绑定上限都是内部实现，不再导出：它们从来不是扩展点，导出只会让每次内部调整都变成破坏性变更。`tsq.NewRuntime`、`tsq.WrapExecutor`、`Query.SQL`、`Mutation.SQL` 收 `dialect.Name`。`WrapExecutor` 返回 `(Executor, error)`，句柄为 nil 或方言未知时报错（此前返回 nil，错误在第一条语句才出现）。
- 能力常量按构建器方法命名，值就是错误里显示的 SQL：`CapabilityFullJoin`（原 `CapabilityFullOuterJoin`）、`CapabilityForUpdate` / `CapabilityForShare` / `CapabilityNoWait` / `CapabilitySkipLocked`（原 `CapabilitySelectFor*`）；`Supports` / `Check` 不再接受 `"full join"` 这类字符串拼写。

**生成的 schema**

- 没写 `name=` 的索引按**列名**推导名字（`ux_<表>_<列>...`），不再把 Go 字段名转成蛇形：字段 `SKU`（列 `sku`）的唯一索引从 `ux_products_s_k_u` 变成 `ux_products_sku`，列名和字段名不一致的字段也终于出现在索引名里。字段名的蛇形和列名一致的（绝大多数）不受影响；受影响的表下次 `tsq gen` 会在迁移里删掉旧索引、建新索引。要保留旧名字，在指令上写 `name=`。

### 修复

- **PostgreSQL 上先写入带主键的行（fixture、导入、按外部 id 的 upsert），之后由数据库生成主键的第一条插入就撞主键**：MySQL / SQLite 的计数器会自动跳过写入的键，PG 的序列不会。现在 `Insert` / `BatchInsert` / upsert 写入了自增主键的值之后，在同一个执行器上把序列推到不小于写入的最大键（`setval`）；会话没有序列的 `UPDATE` 权限时行照样写入、记一条警告（语句本身先检查权限，所以不会把事务弄成 aborted）。
- **PostgreSQL 上 `Reconcile` 遇到别的工具建的、主键没有生成器的表，拒绝并只说 "manual change required"**，而 MySQL 会加 `AUTO_INCREMENT`、SQLite 会重建。现在 PG 给主键加 `GENERATED BY DEFAULT AS IDENTITY`，并把序列推到现有行的最大键之后；需要先加宽的键先加宽（没有序列可加宽，身份列跟列的类型）。反向（声明改成 `assigned`）PG 去掉身份列或 `SERIAL` 默认值，与 MySQL 去掉 `AUTO_INCREMENT` 一致。
- **SQLite 上 TSQ 自己建的 `assigned` 整数主键表，第二次启动 `Validate` 就报 "auto-increment true, declared false"**：`INTEGER PRIMARY KEY` 就是 rowid，不写 `AUTOINCREMENT` 数据库也会生成，检查时一律当成自增。现在 rowid 别名列同时匹配两种声明（它既生成也接受给定的键），`Reconcile` 不再为此重建。
- **连上 MariaDB 或 8.0.19 之前的 MySQL，启动不报错、第一次 upsert 才报语法错**（`INSERT ... AS alias` 是 8.0.19 才有的，MariaDB 根本没有，MariaDB 的 `JSON` 也只是 `LONGTEXT` 的别名）。现在 `Open` 读 `VERSION()`，MariaDB 和老版本直接拒绝并说明；读不到版本照旧放行。文档明确：MySQL 方言只支持 MySQL 8.0.19+，不支持 MariaDB。
- `tsq gen` 对 MySQL 行宽的警告只算 65535 字节的行格式上限，没算 InnoDB 每页 8126 字节的行内上限：三百个 `VARCHAR(20)` 过了前者、`CREATE TABLE` 照样报 1118（每个超过 40 字节的字符串列在页内按 40 字节算）。现在两条都算。
- **回调里再调 `WithTx` 会另开一个事务**：在另一条连接上跑，看不见外层未提交的写入，连接池只有一条连接时两边互等到超时（没有超时就永远等）。现在回调里的 `WithTx` 加入外层事务，内层回调在自己的 savepoint 下跑：写入属于外层事务、读得到外层的写入、出错只回滚到 savepoint 并把错误交给外层决定。选项以外层为准（内层的 `WithRetry` / `WithIsolation` 不起作用）；事务通过回调拿到的 `ctx` 传递，要往下传。回调里拿 runtime 调的 `Page`（自带只读快照事务）同样加入外层事务，看得见回调写的行。`Iter` 正在遍历外层事务时，内层 `WithTx` 直接拒绝（连接上正传着行，再跑语句会把行和事务一起弄坏），提示先遍历完或改用 `List`。回调之外留下来的 `ctx` 带的是已结束的事务，按普通 `ctx` 处理。
- `Reconcile` 遇到"删一列、加一列且形状相同"（改了字段名）时，除了原有的"删列丢数据"警告，再明确说一句：改名的字段只有迁移里的 `RENAME COLUMN` 能保住值，这里什么都不复制。
- 改了 `//tsq:table name=`，生成器看到的是"删一张表、建一张表"：新表是空的、`DROP TABLE` 注释掉等人确认，行就这么丢了。现在两张表列相同时，段首先写出注释掉的 `ALTER TABLE ... RENAME TO ...` 和索引改名（索引名随表名推导；PG `ALTER INDEX ... RENAME`、MySQL `RENAME INDEX`、SQLite 删了重建），改名时跑它而不是两张表的段。
- 改了字段名或 `db` 标签，生成器看到的是"删一列、加一列"：迁移段加新列（空的）、`DROP` 注释掉等人确认，数据就这么丢了。现在删加的两列形状相同时，段首先写出保住数据的 `ALTER TABLE ... RENAME COLUMN ... TO ...`（注释掉），改名时跑它而不是后面的语句。
- 生成的迁移里，同名重建成唯一索引（普通索引变唯一、换列）的那一段先 `DROP` 再 `CREATE UNIQUE`：有重复行时 `CREATE` 失败而旧索引已经没了。文件现在在 `DROP` 之前写一行说明，点名要先查重复的列。
- **旧版 `tsq gen` 遇到新版写的 `tsq.json` 会悄悄改写成自己的形态**（丢掉新版记录的渲染和它不认识的字段），两个版本的 CLI 来回改写、各自为对方的拼法写一段迁移。现在版本号比自己新的状态文件一律拒绝，并给出要安装的版本；开发构建（没有发布版本号）不比较。
- **运行期策略改列时抹掉了 DBA 加在列上的注释和排序规则**：MySQL 的 `MODIFY COLUMN` 和 PostgreSQL 的 `ALTER COLUMN TYPE` 都按声明重写整列，列自己的 `COLLATE`（悄悄改变比较和唯一索引的语义）和 `COMMENT` 随之丢失。SQLite 的重建同样丢掉列上的 `COLLATE`。现在自省读出列自己的排序规则（和 MySQL 的注释），改列和重建时原样带上；它们不是 TSQ 声明的东西，`Validate` / `Reconcile` 不比较它们。生成的迁移文件不知道库里的样子，`MODIFY COLUMN` 里要自己重写这些属性（文档已说明）。
- keyset 的排序项是对已选列的表达式（`tsq.Upper(t.Title).Asc()`）时，错误此前说"column title must be selected by the query"而 title 明明选了；现在说明它是表达式、keyset 只按已选的列定位。
- **`CreateMissing` / `Reconcile` 要建的唯一索引撞上重复行时，表已经改完、索引建不成，之后每次启动都重复这一幕**：现在在任何 DDL 之前先查那些行（缺失的唯一索引，以及 `Reconcile` 会按声明重建的同名索引），有重复就以 `*tsq.DuplicateRowsError` 拒绝启动（点名重复的值和行数），表原样不动。索引里的新列对每一行都是同一个值（零值或默认值），不参与区分；可空且无默认值的新列到处是 NULL、永不冲突，这种索引仍交给引擎。改类型被引擎拒绝而留下的半截改动不在此列：MySQL 的 DDL 隐式提交，TSQ 有意不把策略包进事务。
- **空字符串默认值 `''` 和"没有默认值"被当成一回事**：声明去掉 `default:''` 后，`tsq gen` 的迁移和运行期 `Reconcile` 都不写 `DROP DEFAULT`，库里的 `''` 留下；之后这一列改类型时 PostgreSQL 报 `default for column cannot be cast automatically`（第十四轮四代迁移回放找到）。现在 `SameDefault` 把两者分开——三个引擎本来就报得出区别（NULL 对 `''`）。
- `tsq.WrapExecutor` 包住的事务（任何带 `Commit` / `Rollback` 的句柄）现在和 `WithTx` 的执行器一样记住引擎自行回滚事务的错误（MySQL 死锁），之后的语句一律拒绝，不再各自自动提交；`Commit` 仍是调用方的。包住的句柄返回 nil 行时不再触碰它。
- **`SchemaMismatchError` 只说 `alter column n`，不说哪里不一样**：现在每一列都列出差异（引擎报告的类型对声明的拼法、NULL 对 NOT NULL、默认值、范围约束），比如 `alter column js (type jsonb, declared JSON)`。MySQL 上没有 `CREATE TEMPORARY TABLES` 权限的用户（只有 DML 权限的应用账号是常态）让"问引擎两种拼法是不是一回事"的探测跑不了，此前只在日志里警告、错误里仍是一句不匹配；现在那一行带上"数据库无法被问及，按文本比较"和引擎给的原因。
- **pgx `simple_protocol` 模式下 `client_encoding` 不是 UTF8 时 `Open` 照常成功、之后每条查询都失败**（`simple protocol queries must be run with client_encoding=UTF8`）：启动时读会话编码的探测把"读不到"当作"不检查"，而这正是驱动拒绝一切查询的那种会话。现在驱动因编码拒绝会话时 `Open` 就拒绝启动并带上驱动的原因；读不到设置（兼容实现没有它）仍然放行。
- **MySQL 上 `WithTx` 回调吞掉死锁后继续，前面的写入丢了、后面的写入留下、没有任何错误**：InnoDB 检测到死锁（1213）或锁表满（1206）时回滚**整个**事务并让会话退出事务（`@@in_transaction=0`），之后回调里的每条语句都各自自动提交，最后的 `COMMIT` 什么也不提交。现在事务执行器记住这类错误：之后的语句一律拒绝（说明事务已被数据库回滚、此时执行会自行提交），`WithTx` 拒绝提交并返回那个死锁错误（`IsTxConflictError` 为真，配 `WithRetry` 就整体重跑）。PostgreSQL 本来就拒绝失败后的语句且驱动报告"commit 变成了 rollback"，SQLite 的失败不结束事务，两者不变；MySQL 的重复键等语句级错误也不变——事务照常继续。
- **`[N]byte` 字段（UUID、哈希的常见形态）生成器收下、运行时却写不进也读不出**（`sql: converting argument $4 type: unsupported type [16]uint8, a array`）：database/sql 只绑定和扫描字节切片，不碰数组。现在 `[N]byte`、具名数组类型（`type UUID [16]byte`）、它们的 `*T` 和 `sql.Null[T]` 形态在三个引擎上都按字节写入、读回数组；库里存着别的长度是读取错误（说明值几字节、字段几字节），不是截断的键。自带 `Value` / `Scan` 的类型（`uuid.UUID`）照旧用自己的。
- **`size:` 写在数字、布尔、时间字段上此前被静默忽略**（`int64` 配 `size:10` 生成的是 `BIGINT`，不是十位的列）：现在 `tsq gen` 拒绝并说明 `size:` 只用于字符串和 `[]byte`。
- **MySQL 上全文检索词里有 `*` 就报语法错**（`Error 1064: syntax error, unexpected $end, expecting FTS_TERM or FTS_NUMB or '*'`）：自然语言模式下 `*` 没有任何含义（不做前缀匹配），但 InnoDB 的解析器照样把它当记号，单独一个 `*`、空白后的 `*`、短语后的 `"a b"*` 都是语法错，而 PostgreSQL 和 SQLite 对同一个词都正常返回——搜索框里的内容原样传进 `tsq.Matches` 就可能 500。现在 MySQL 上渲染成 `MATCH(...) AGAINST (REPLACE(?, '*', '') IN NATURAL LANGUAGE MODE)`，`AGAINST` 的参数仍是常量，全文索引照用。其余运算符字符（`+ - " ( ) ~ < > @`）在自然语言模式下本来就是普通文本。
- **`PageRequest.Keyword` 里的 NUL 字节现在是请求错误**：PostgreSQL 拒绝任何文本参数里的 `0x00`（搜索在 PostgreSQL 上是 500、在另两个引擎上是零行），而没有人会往搜索框里敲 NUL。`Paging()` / `Keyset()` 现在返回 `*PageRequestError{Field: "keyword"}`，三个引擎上都是 400。
- **PostgreSQL 上带范围约束的列改成文本类型被拒绝**（`operator does not exist: character varying >= integer`）：`ALTER COLUMN TYPE` 会按新类型重新解析列上的 `CHECK`，`qty >= 0` 对 `VARCHAR` 无法解析。现在改类型前先删掉范围约束，新类型仍需要的再加回。随机生成的迁移在三引擎上执行找到的。
- 原始 `type:` 列从可空改成 NOT NULL 而表里有 NULL 时，生成的迁移此前只有一条会被引擎拒绝的语句；现在上面多一行说明：这一列没有 TSQ 知道的零值，先把 NULL 填上，否则改动被拒绝。
- **`bool` 字段写 `default:1` / `default:0`，PostgreSQL 建表失败**（`default expression is of type integer`）：MySQL 和 SQLite 两种写法都收，PostgreSQL 只收 `TRUE` / `FALSE`。现在布尔默认值按各引擎的写法写出，同一份模型三个引擎都能建；漂移比较本来就把两种写法当一回事。
- **PostgreSQL 上带默认值的列换类型被拒绝**（`default for column cannot be cast automatically to type boolean`）：`ALTER COLUMN TYPE ... USING` 只转换存储的值，默认值由服务器自己转换，转不了就整条拒绝。现在跨类型的改动先 `DROP DEFAULT`、再改类型、再 `SET DEFAULT` 新值。
- **MySQL 上带索引的列改成 `BLOB` / `TEXT` 时 `Reconcile` 启动失败**（1170，`used in key specification without a key length`）：列先改、索引后删，而索引还在时 MySQL 不许改。现在不再声明的、按 `tsq gen` 命名的索引在它覆盖的列被修改**之前**删除；仍在声明的索引不动，服务器的拒绝就是答案。
- PostgreSQL 上 `bool` 字段改成浮点字段：`BOOLEAN` 没有到浮点的转换，现在经 `INTEGER` 转。
- **MySQL 上 `Coalesce(Sum(整数列), tsq.Val(n))` 读不回整数字段**："总数，没有就 0"这个最常见的写法在 MySQL 上报 `converting "12.000000000000000000000000000000" to a int64: invalid syntax`：`SUM(整数)` 是 DECIMAL，旁边的参数在预编译时没有类型，服务器就给结果 30 位小数；`Add` / `Sub` / `Mul` / `Div` 里的参数同样。现在算术和 `Coalesce` / `NullIf` 里的整数绑定值在 MySQL 上写成 `CAST(? AS SIGNED)`（无符号是 `UNSIGNED`），结果和另两个引擎一样是整数。
- **批量更新遇到过期行时，含 JSON 列的行被误判为没写成**：`BatchUpdate` 在匹配行数不够时回读这一批、按值判断哪些行写成了，JSON 列按文本比——MySQL（以及 PostgreSQL 的 `JSONB`）存的是规范化后的文档（键排序、冒号后加空格），于是写成的行也被当成过期的：错误里多报了它，它在内存里的 `version` 没有前移，下一次更新它就撞上一个并不存在的乐观锁冲突。现在 JSON 按文档比较。
- **会话的文本编码不是 UTF-8 时，文本被悄悄存成别的字符**：PostgreSQL 的数据库编码是 `LATIN1` 这类时，pgx 不会自己要求编码，会话就跟着数据库走；MySQL 的 DSN 写了 `charset=latin1` 同理。Go 字符串的 UTF-8 字节被当成 latin1 字符读：`é` 存成了 `Ã©`，`Length` 数成两个字符，`Substring` 从一个字符中间切开，而同一个会话读回来又是完整的，所以全程不报错，直到别的客户端来读这张表。现在运行时在启动时拒绝这样的会话并说明改法：PostgreSQL 在 DSN 里加 `client_encoding=UTF8`，MySQL 去掉 `charset` / `collation`（驱动默认 `utf8mb4`）。`SQL_ASCII` 的 PostgreSQL 库什么都不转换，不检查。
- **MySQL 上 `BatchUpdate` 对非连接字符集的表直接报错**：表（或被写的某一列、主键）是 `latin1`、`gbk`、`ascii` 这类字符集，而连接是 `utf8mb4`——早于 utf8mb4 的老库都是这样——多行更新报 `Illegal mix of collations (latin1_swedish_ci,IMPLICIT) and (utf8mb4_general_ci,COERCIBLE) for operation 'UNION'`，一行也写不进去。服务器在比较和赋值时会把参数转成列的字符集，但给 `UNION` 定类型时不会。现在遇到这个错误（语句在写任何行之前就被拒绝）就把这一批改成逐行更新，结果相同，速度是单行更新的速度。
- **MySQL 非严格模式下，`Reconcile` 改列类型会截断数据**：`sql_mode` 里没有 `STRICT_TRANS_TABLES` 时，`ALTER TABLE ... MODIFY` 把放不进新类型的值直接截掉（`'abcdefghij'` 进 `VARCHAR(5)` 成了 `'abcde'`），只留一条没人读的 warning；严格模式下同一条语句被拒绝。现在 schema 策略在它自己的连接上始终按严格模式执行，结束后把会话的 `sql_mode` 还原；放不进去的改动在任何模式下都被拒绝。
- **MySQL 的 `ANSI_QUOTES` / `ANSI` 模式下，TSQ 自己建的表每次启动都被判为漂移**：这个模式下 `SHOW CREATE TABLE` 用双引号写列名，向引擎求证拼写的那一步在里面找不到列，于是 `type:REAL`、`INTEGER UNSIGNED`、表达式默认值等引擎有自己写法的列，`Validate` 启动失败、`Reconcile` 每次启动都重跑同一条 `ALTER`。`NO_BACKSLASH_ESCAPES` 下 `BINARY` 列的字面量默认值同样误报（服务器读不回它自己写出的 `'ab\0\0'`）。两种模式现在都和默认模式一样：第二次启动零 DDL。
- MySQL 连接池不在严格模式时，启动时记一条 warning：放不进列的值会被截断或夹到范围边界而不是被拒绝，写入"成功"但存下的是另一个值。只提醒不拒绝——模式是部署自己的选择。
- **`tsq.Ceil` / `tsq.Floor` / `tsq.Round` 的结果类型随引擎变**：SQLite 上浮点数的 `Ceil` / `Floor` 交回的是整数，`Div(Ceil(a), Floor(b))` 于是成了整数除法（9 除以 8 得 1 而不是 1.125），超出 64 位整数范围的值（`1e19`、`1e300`）被截成 `9.22e18`；整数上三个函数各引擎各答各的——PostgreSQL 的 `CEIL` / `FLOOR` 交回 `double precision`，一百万以上的值读不回整数字段、2^53 以上丢末位、再 `Div` 直接报 `function div(double precision, ...) does not exist`，`Round(整数, 2)` 在 PostgreSQL 上得 `7.00` 读不回来，在 SQLite 上得浮点数、再 `Div` 得 `3.5`。现在浮点数的 `Ceil` / `Floor` 在 SQLite 上仍是浮点数、任意大小都对；整数没有可舍入的东西，三个函数原样交回它，不调引擎函数。
- 文档写明 `Round` 的一处引擎差异：只在十进制写法上是平局的值（`1.005` 实际存成 `1.00499999999999989`），PostgreSQL 和 MySQL 按写法进位得 `1.01`，SQLite 按存储的值得 `1.0`。需要确定舍入方向的金额用 `DECIMAL` 列或最小单位的整数。
- **多个实例同时启动时，除了第一个都起不来**：滚动发布或多副本在 `CreateMissing` / `Reconcile` 下一起启动，各自发现同一张表、同一列或同一个索引缺失，各自去建，后到的死在"already exists"上（PostgreSQL 上两条同名的 `CREATE TABLE IF NOT EXISTS` 并发时还会撞系统目录的唯一键，SQLite 是 `database is locked`）。现在改 schema 的策略在锁里跑，一个库同时只有一个实例在改，等锁的实例拿到锁时 schema 已经就绪：PostgreSQL 用会话级 advisory lock，MySQL 用 `GET_LOCK`，策略的每条语句都在持锁的那个连接上执行（只有一个连接的池也够用）；SQLite 没有可跨语句持有的锁，同一进程内的运行时轮流来，跨进程不协调。`Validate` 不改任何东西，不加锁。
- **tracer 不守约时，操作"成功"但没有执行，或者执行两次**：tracer 返回 nil 却没调用 `next`，`Insert` 什么都没插入也不报错、`Get` 返回 nil 行和 nil 错误、`WithTx` 的回调被跳过；调用两次 `next`，语句就执行两次（`Set(x, x+1)` 加了两次）；给 `next` 传 nil context，database/sql 带着锁 panic，此后 `Close` 永不返回。现在这三种都变成明确的错误，语句最多执行一次；tracer 仍然可以用自己的错误拒绝一次操作。
- **PostgreSQL 的 `TIMESTAMPTZ` 列读回来不是 UTC**：pgx 按会话的本地时区交回带时区的时间，这是唯一一处读回的时间不在 UTC 的地方。现在读行时统一转成 UTC。
- **没设置过的 `json.RawMessage` 在把参数写进语句的驱动模式下仍被拒绝**：上一条修复把它绑成了普通字节切片，MySQL 的 `interpolateParams=true` 和 pgx 的 simple protocol 会把字节切片拼成二进制字面量，JSON 列不收。现在绑定值保持 `json.RawMessage` 类型。
- **`tsq gen` 的两处报错说的不是问题本身**：`tsq.json` 里留着合并冲突或被截断时，报的是"non-generated DDL state file"；传了 `./...` 或一个文件时，报的是"package directory does not exist"。现在分别说明是状态文件损坏（并提示从版本库恢复）、是通配路径、是文件而不是目录。
- **分组表达式在 `HAVING`、嵌套表达式和 `ORDER BY` 里再用一次，MySQL 和 PostgreSQL 报错**：`GroupBy(tsq.Upper(note))` 之后写 `Having(tsq.Upper(note).NE(...))` 或选出 `tsq.Lower(tsq.Upper(note))`，`Build` 放行、SQLite 能跑，MySQL 报 1054 / 1055（它只认整个出现在选择列表或 `ORDER BY` 里的分组表达式）；分组表达式带绑定值时（`tsq.Add(qty, tsq.Val(10))`）PostgreSQL 也报错，因为每写一次就是一个新参数，它把两处当成两个表达式。现在这两个引擎上，这样的出现被写成 `MAX(表达式)`——同一组里它只有一个值，聚合可以出现在任何位置。只改写原本必然报错的写法：聚合里面的出现（`Count(Upper(note))`）、`Expr` 手写的函数里面的出现、子查询里面的出现、分组的列、本身就是分组表达式的选择项都保持原样。
- **`tsq.Max` / `tsq.Min` 作用于布尔列在 PostgreSQL 上报错**（没有 `MAX(boolean)`）：那里改写成 `BOOL_OR` / `BOOL_AND`，三个引擎答案一致。
- **`tsq.Ceil` / `tsq.Floor` 在 mattn/go-sqlite3 上报 `no such function`**：这个驱动默认不编入 SQLite 的数学函数。SQLite 上改用不依赖数学函数的写法，两个驱动都能用。
- **没设置过的 `json.RawMessage` 写不进 JSON 列**：nil 的字节切片按空字节绑定，而空字节不是 JSON，MySQL 和 PostgreSQL 拒绝这一行。现在按 JSON 的 `null` 写入，和 `encoding/json` 对 nil `RawMessage` 的写法一致。
- **`NewRuntime` 检查不了 MySQL 连接池的 `parseTime` 和 `loc`**：`Open` 从 DSN 里读这两项，交给 `NewRuntime` 的连接池没有 DSN 可读，`loc=Local` 的池子照样能建起运行时，数据库填的 UTC 时间读回来差一个时区。现在 `NewRuntime` 发一条查询问驱动怎么读时间，两项不对都拒绝。
- **MySQL 上行宽超限要到建表才知道**：每个 `VARCHAR` 按最长值计入 65535 字节的行宽，`size:16383` 一列就占满，几个长字符串加起来也会超（建表报 1118）。`tsq gen` 现在像索引键长那样给出警告，并在 `mysql.sql` 里那条 `CREATE TABLE` 上方写明。
- **手写在生成的 `XxxTable` 上的方法和生成的方法重名**（`GetByX`、`FindByX`、`FullTextX`、`As`、`WithDeleted`）：以前只能等编译器报"method redeclared"，报在它后读到的那个文件上，可能是生成的文件。现在 `tsq gen` 直接指出手写的那一行。
- **PostgreSQL 上 `[]byte` 与 `string` 互改类型会静默改写数据**：生成的迁移和 `Reconcile` 把 `[]byte` → `string` 写成 `USING c::TEXT`，`abc` 变成 8 个字符的文本 `\x616263`；`string` → `[]byte` 写成 `USING c::BYTEA`，文本被当成转义串解析（`tab\101x` 变成 `tabAx`）。MySQL 和 SQLite 保留原字节，改完 `Validate` 照样通过。现在写 `convert_from(c, 'UTF8')` / `convert_to(c, 'UTF8')`，不是文本的字节会让迁移失败而不是被改写。
- **SQLite 改列类型后整张表读不出来**：SQLite 在任何声明类型下都原样存值，重建表时把 `1.5` 抄进整数列、`2` 抄进布尔列、`'Hello'` 抄进整数或时间列都"成功"，`Validate` 通过，随后每次读这张表都报 Scan 错误；MySQL 的数值 → `BOOLEAN` 同样留着 `2`。现在三个方言对同一个改动给出同一个结果：小数四舍五入成整数，数值变布尔时非零即真（PostgreSQL 的 `USING c <> 0`、MySQL 在 `MODIFY` 前的一条 `UPDATE`、SQLite 重建时的复制表达式）；文本变成数值、布尔或时间时，转不过去的值在 MySQL / PostgreSQL 上让改动失败，SQLite 上 `Reconcile` 在提交前检查、报出列名、行数和一个例子并保持原表不动，生成的迁移则把这个改动写成注释交给人（脚本拦不住一个转不过去的值）。
- **声明的原始类型和默认值每次启动都被当成"变了"**：`type:DECIMAL(10)`、`INTEGER UNSIGNED`、`CHAR`、`BIT`、`NVARCHAR(10)`、`YEAR(4)`、`FLOAT(24)`（MySQL），`INT[]`、`VARCHAR(10)[]`、`NUMERIC(10)`、`FLOAT(53)`（PostgreSQL），以及 `(1+1)`、`(CURRENT_DATE)`、带反斜杠的字面量这类默认值，数据库报回来的拼法和声明的不一样，`Validate` 在 TSQ 自己建的表上起不来，`Reconcile` 每次启动都重发同一条 `ALTER`。别名表补了三轮仍有漏网，现在文本比较说"不一样"时改问引擎：在本会话的临时表里按声明建这一列，读回引擎自己的拼法，和库里那一列的拼法一致就是同一个东西。只有文本上不同的列才探测；没有建临时表的权限时退回文本比较并在日志里告警。MySQL 的临时表和永久表报默认值的拼法不同（表达式多一层括号、二进制字面量一边是文本一边是十六进制、4 字节字符在数据字典里是 `?`），所以那里把库里那一列也按 `SHOW CREATE TABLE` 的写法建进临时表，两边都从临时表读；PostgreSQL 上非主键列的 `type:SERIAL` / `BIGSERIAL` 按它自带的序列认出来。
- **`Validate` 放过缺了生成列的表**：生成列整个不参与 schema 比较（三个引擎报它的方式都不一样，SQLite 的 `table_info` 干脆不列它），于是表建好之后才声明的 `generated:` 列，`Validate` 通过、`CreateMissing` 和 `Reconcile` 也不加，第一次读就报 `no such column`。现在已存在的生成列照旧不比较，声明了却缺失的算缺列：`Validate` 报出来，`CreateMissing` / `Reconcile` 加上并为已有的行算好值（SQLite 只能以 `VIRTUAL` 形式加生成列，所以重建表）；没写表达式的 `generated` 列 TSQ 加不了，缺了就启动失败。
- **`BatchUpdate` 的耗时随批大小平方增长**：多行更新给每一列写一个按主键分支的 `CASE`，数据库对每一行逐个分支求值，默认一批 1000 行就是一千乘一千。4000 行 × 20 列实测 SQLite 6.5 秒（每批 50 行只要 0.55 秒），MySQL 431 毫秒，PostgreSQL 335 毫秒。现在把这批行的值写成一个行列表，和表按主键、版本号连接后赋值：同样的数据 SQLite 0.53 秒、MySQL 151 毫秒、PostgreSQL 62 毫秒。赋值的检查和单行 `Update` 一样，超出列宽的字符串被拒绝而不是截断。
- **时间绑定时不截到微秒**：`time.Now()` 带着列存不下的纳秒，SQLite 按文本原样存、MySQL 四舍五入、PostgreSQL 截断，同一个带纳秒的谓词在三个引擎上匹配三组不同的行。现在每个绑定的时间（行字段、`Val`、参数）都截到微秒，和托管时间戳一致。
- **`sql.NullInt32` / `sql.NullInt16` / `sql.NullByte` 不写 `type:` 就被 `tsq gen` 拒绝**，而 `sql.NullInt64` / `NullString` 等同门类型能自动推导列类型。现在它们按各自的值类型推导。
- **绑定值超过方言上限时的报错说不清该怎么办**：`col.In(tsq.Vals(...))` 放进七万个值，三个驱动各报各的（`too many SQL variables`、`Prepared statement contains too many placeholders`、`extended protocol limited to 65535 parameters`）。现在语句在发出前被拒绝，报错写明绑定了多少、上限多少，并指向 `ListIn` 和 `Batch*`。
- **事务重试的两次尝试之间等待时间固定**：两个互相死锁的事务同时失败、同时重试，下一次还会撞上。现在每次等待在退避时长的一半到全长之间随机取。
- 生成的迁移注释和文档对"带 `type:` 的 NOT NULL 新列"给的建议是"声明 `default:`"，而 `tsq gen` 不许不能存 NULL 的字段写 `default:`；两处都改成了能照做的说法。文档新增一段：多个 goroutine 共用 SQLite 时 DSN 要设 `busy_timeout` 和 WAL，否则并发读写直接报 `database is locked`。
- **`Reconcile` 留着不再声明的索引**：去掉一个 `//tsq:unique`，或给它加一列（推导出的名字随之改变），旧的唯一索引仍在库里，继续拒绝声明已经允许的行，而同一档策略对不再声明的列是会删的。现在 `Reconcile` 删掉已声明的表上按 TSQ 推导名（`ux_<表>_…` / `idx_<表>_…` / `ft_<表>_…`）命名却不再声明的索引；别的名字的索引、别的档位、没声明的表都不碰。
- **同名全文索引换了列检测不到**：只按名字比，索引留在旧的列上；MySQL 的 `MATCH` 要求索引正好覆盖它点名的列，每次检索报 1191。现在 MySQL 上也比列，`Reconcile` 重建，其他档位报错。
- **`tsq gen` 的几处**：`//tsq:unique A,B` 和 `//tsq:unique AAndB` 都生成 `GetByAAndB`，生成的代码编译不过，现在生成时报错并点名两条指令；类型别名（`type Stamp = time.Time`）做 `created_at` / `updated_at` 被当成不认识的类型拒绝，现在按它代表的类型处理；`//tsq:search` 写在指针字段上的报错说的是"keyword fields"，map 字段的报错是 `*ast.MapType`，都改成点名指令和源码里的类型。
- **`ListIn` 列表长到被切分后会丢行**：切分后的结果按主键去重，把 join 本来就重复的行也去掉了，同一个查询短列表返回 8 行、长列表返回 2 行。现在只有读单张表的查询才去重（那里一行只可能出现一次），带 join 的查询保留各段返回的每一行。
- **MySQL DSN 带 `loc=Local` 时数据库填的时间读回来差一个时区**：驱动按 `loc` 写入和解读 `DATETIME`，TSQ 的时间于是按本地时间落库（文档说一律 UTC），而数据库自己填的 UTC 时间（`default:CURRENT_TIMESTAMP`）读回来偏了时区那么多小时，不报错。`tsq.Open` 现在拒绝 `loc` 不是 `UTC` 的 DSN；显示时用 `time.Time.In` 转换。
- **自定义 bool 类型的字段在 MySQL 和 SQLite 上读不出来**：`type Flag bool`（含 `*Flag`、`sql.Null[Flag]`）能生成、能写入，每次读都报 `unsupported Scan`——这两个库把布尔报成整数，database/sql 只替内建的 `bool` 转换。现在 TSQ 自己读进命名类型。
- **SQLite 上对时间列做 `Max` / `Min` / `Coalesce` 读不出来**：SQLite 把时间存成文本，驱动只对声明成时间的列返回 `time.Time`，表达式的结果是字符串，扫描失败（两个 SQLite 驱动都是）。现在时间字段按文本也读得进。
- **`tsq.Round` 和 `tsq.Avg` 在 MySQL 上答案不同**：MySQL 对 `DOUBLE` 用银行家舍入（`-6.5` 得 `-6`、`8.5` 得 `8`），对整数列求平均只留四位小数（`1.6667`）。现在浮点数在 MySQL 上经精确小数舍入、整数列按 `DOUBLE` 求平均，三个方言一致：平局远离零。
- **MySQL 上 `SelectDistinct` 配 `.NullsLast()` 排序报 3065**：NULL 位置靠 `expr IS NULL` 排序键实现，它不在选择列表里。现在对选出的表达式用它的别名判断。
- **`Coalesce(可空列, 非空列)` 仍被当成可能为 NULL**：读进不能存 NULL 的字段时被拒绝，报错还建议"用 Coalesce"。它只在两边都为 NULL 时为 NULL。
- **MySQL 上没赋值的 NOT NULL 时间字段插不进去**：驱动把 Go 的零值时间写成 `0000-00-00`，MySQL 默认模式拒收；PostgreSQL 和 SQLite 存成公元 1 年。现在 MySQL 上也按公元 1 年绑定，读回仍是零值。
- **按主键批量删除在 PostgreSQL / SQLite 上漏报不存在的键**：同一批里有只差大小写的两个键（`"ABC"` 和 `"abc"`）而库里只有一个时，另一个被当成删掉了。大小写折叠只该用于 MySQL 的不区分大小写比较。
- **PostgreSQL 改列类型仍会静默截断**：跨类型、或任一侧写了 `type:` 的改动（`type:TEXT` 改成 `size:5`、`VARCHAR` 改成 `type:CHAR(3)`、整数或时间改成短字符串）写成 `USING 列::新类型`，显式转换把放不下的值直接截短，运行期 `Reconcile` 和生成的迁移段都是。现在字符类型的目标写 `USING 列::TEXT`，长度交给赋值检查，放不下就失败、数据不动；`BOOLEAN` 和整数互转也能执行了（此前转 `BIGINT` 报"无法转换"）。
- **运行期策略没法给有数据的表加 NOT NULL 列**：`CreateMissing` / `Reconcile` 在 PostgreSQL 和 SQLite 上对任何类型都失败、在 MySQL 上对时间列失败，"改了结构直接重启就跟上"只在空表上成立。现在和生成的迁移一样给已有行填类型的零值（带零值默认加列再去掉默认）；SQLite 没有 `DROP DEFAULT`，这类列和默认值不是常量的列（`CURRENT_TIMESTAMP`）通过重建表添加。SQLite 上 `Reconcile` 把可空列改成 NOT NULL 时也先填掉 NULL（此前有一行是 NULL 就失败）。
- **PostgreSQL 上无符号自增主键第二次启动就对不上**：`uint` / `uint16` / `uint32` / `uint64` 的自增主键建成和 Go 类型同宽的 `SERIAL`，校验却按加宽一档的类型比：TSQ 自己建的表过不了 `Validate`，`Reconcile` 把 `uint64` 主键改成 `NUMERIC(20)`。现在主键按加宽后的宽度建（`uint16` 是 `SERIAL`，`uint` / `uint32` / `uint64` 是 `BIGSERIAL`）。已经按旧规则建的表，`Reconcile` 会加宽列和序列；用迁移文件的项目自己 `ALTER ... TYPE BIGINT`。
- **MySQL 上迁移写入的零值时间是坏的**：`'0001-01-01 00:00:00+00:00'` 在默认的 `time_zone=SYSTEM` 下被静默存成 `0000-00-00`，在显式会话时区下报 1292，之后这张表上每条复制整表的 `ALTER` 都失败。生成的迁移（新增 NOT NULL 时间列、可空改非空）和 `Reconcile` 都写它；现在 MySQL 上不带时区。新增 NOT NULL 的大字符串或 `[]byte` 列写成 `DEFAULT ''` / `DEFAULT X''` 被 MySQL 拒绝（1101），现在写成表达式默认值。
- **TSQ 自己建的列每次启动都被判成漂移**：`Validate` 报不匹配，`Reconcile` 每次启动重跑同一条 `ALTER`（PostgreSQL 上是整表重写）。MySQL：字面量默认值是 `'(none)'` 这样带括号的、含 `::` 的、首尾有空格的，或时间字面量；`type:` 写成 `REAL`、`DOUBLE PRECISION`、`BOOL`、`INT(11)`、不带精度的 `DECIMAL` / `NUMERIC`、带 `COLLATE` 的。PostgreSQL：`type:` 写成 `FLOAT`、`TIME`、`TIMESTAMP(3)`、`INT4`、`INT8`、`FLOAT8`。
- **PostgreSQL 上 `size:` 超过 10485760 的字符串建表失败**：那是 `VARCHAR` 的上限。现在写成 `TEXT`（MySQL 本来就转 `LONGTEXT`）。
- **`HardDelete` / `BatchHardDelete` 删一个已经不存在的行时报告成功**（表没有 `version` 列时），而 `BatchHardDeleteByPK` 会点名报错。现在它们一致：不存在的行返回 `*RowStateError`，`Keys` 点名；版本变了的行仍是 `OptimisticLockError`。
- **`BatchUpsert` 写入一部分行之后才报错**：某组行没有可写的列时，前面的组已经写进库。现在写之前检查每一组；只有自增主键要写的行（不可能冲突）按插入处理，和 `Insert` 一致；只合并相邻的同形状行，按切片顺序写。
- **`BatchUpsert` 之后行在内存里看起来是最新的，其实不是**：时间戳被盖上了，版本号却没跟上数据库，接着 `Update` 必然冲突。现在托管列保持传入时的值，文档写明要 `Update` 先重新加载。
- **`OnConflict(...).Update(col)` 点名的 `default:` 列为 nil 时不写**：和行级 `Update(col)` 不一致，冲突时没法把它清成 NULL。现在点名了就写 NULL。
- **MySQL 上按不分大小写的字符串主键 `BatchDeleteByPK`，删到的行被报成"没删到"**：数据库读回的键拼写不同。现在删到的行数够了就全算删到，否则按不分大小写、忽略尾部空格再比一次。
- **切分执行的 `ListIn` 可能把同一行返回两次**：两个数据库认为相等、Go 认为不等的键落在不同的段里。现在按主键去重（结果是表的行时）。
- **`HAVING` 里用分组表达式被拒**：`GroupBy(tsq.Upper(note))` 配 `Having(tsq.Upper(note).NE(...))` 在 SQL 标准、PostgreSQL 和 SQLite 上合法，曾被当成没分组的 `note` 拒绝。MySQL 只认选择列表或 `ORDER BY` 里整个重复的分组表达式，放进 `HAVING` 或套一层函数执行时报 1054 / 1055——那里先在 CTE 里分组再过滤。
- **`tsq.Open` 拒绝 `parseTime=1`**：驱动接受它，检查却只认 `parseTime=true` 这个子串。现在按驱动的规则读这个参数。
- **结构体内部写的 `//tsq:` 行被静默忽略**：只有类型上方的注释会被读，写在字段上的 `//tsq:index` 什么也不建。现在报错并给出位置。
- **生成器的几处小问题**：两个字段 `URL`、`Url` 组成的唯一索引生成的参数同名、编译不过；报错里打印生成器内部的导入别名（`tsqtime.Time`）；某个方言渲染出同一个类型的声明变化（`[]byte` 加 `size:`）仍写成迁移，MySQL 上白复制一遍表；MySQL 索引警告对 `[]byte` 列建议改 `size:`，那没有用（现在建议 `type:VARBINARY(n)`）。

- **`Pred` / `Exprf` 里带 `OR` 时查出已软删除的行**：自定义 SQL 原样拼进 `WHERE`、不加括号，软删除过滤和其他 `Where` 条件只套住最后一个 `OR` 分支（`AND` 优先级更高）；算术的操作数同样缺括号，`Mul(x.Exprf("%s + 1"), 2)` 算成 `x + 2`。现在自定义 SQL 整体加括号。
- **只差一个绑定值的两个表达式被当成同一个**：`GROUP BY` / `ORDER BY` 按选择列表里"长得一样"的表达式写成列序号，`CASE amount > 100` 和 `CASE amount > 1000` 被认成一个，查询按错误的列排序、分组检查放过了没分组的列，游标分页也可能取错列的值。现在比较时带上绑定值和参数身份。
- **PostgreSQL 迁移和 `Reconcile` 把 `VARCHAR` 改短时静默截断数据**：类型变更一律写 `USING col::T`，显式转换把超长的值截到新长度而不报错。现在只在没有赋值转换的类型之间（`BOOLEAN` 改 `INTEGER`）才写 `USING`，放不下的值让迁移失败。
- **把可空列改成 NOT NULL 时，PostgreSQL / MySQL 的迁移不处理已有的 NULL**：PostgreSQL 直接失败，MySQL 严格模式失败、否则静默变零值。现在三个方言都先把 NULL 填成默认值或零值（SQLite 重建时本来就这样），迁移里注明填了什么；新加的无默认值 NOT NULL 列同样给已有行零值。
- **SQLite 上给有数据的表加 `CURRENT_TIMESTAMP` 默认值的列（例如给已有表加 `created_at`）或无默认值的 NOT NULL 列，迁移必然失败**：`ADD COLUMN` 拒绝这两种。现在改用重建表。
- **PostgreSQL 上 `Reconcile` 把默认值改成本地时间**：`SET DEFAULT` 写的是原样的 `CURRENT_TIMESTAMP` 而不是建表用的 UTC 表达式；默认值比较也认不出本地时间和 UTC 的区别，旧库里的本地时间默认值永远不会被纠正。现在两处都按 UTC 写、按 UTC 比。
- **PostgreSQL 上整数 `Div` 返回小数**：`SUM` 的结果和 `uint64` 列是 `NUMERIC`，`/` 保留小数，读进 `int64` 失败。现在写成 `DIV()`。
- **MySQL 上 `NullsLast` / `NullsFirst` 对带绑定值的表达式失效**：排序项写成列序号后，`IS NULL` 键变成了常量 `2 IS NULL`。
- **`Exists` 拒绝只在选择列表或 `ORDER BY` 里用到的参数**（`SELECT 1` 去掉了它们）。
- **MySQL 上带下划线的 `TEXT` 默认值（`'en_US'`）仍被当成差异**：去掉字符集前缀时没分清字面量内外，把 `'en_US'` 截成了 `'en'`。
- **唯一索引建在自定义切片类型（`json.RawMessage`、`type Hash []byte`）上时，生成的代码编译不过**：查找方法要求可比较的类型。现在这类字段走和 `[]byte` 一样的查询。

- **`BatchDeleteByPK` / `BatchHardDeleteByPK` 对没删到的主键报告成功**：不存在的键（软删除时还有已删除的行）现在让调用返回 `*RowStateError`，`Keys` 列出它们；存在的键照样删除，重复的键只算一次。PostgreSQL / SQLite 用 `RETURNING` 拿到删到的键，MySQL 软删按本次的墓碑回查、硬删在删除前查。要"删掉匹配到的，不管有没有"用 `DeleteFrom` / `HardDeleteFrom`。
- **MySQL 上 `TEXT` 列的默认值每次启动都被当成差异**：这种默认值只能写成表达式，`information_schema` 读回来是 `_utf8mb4\'x\'`，`Validate` 起不来、`Reconcile` 每次都改一遍。现在按声明的写法还原后再比较。
- **运行时 `Reconcile` 在 SQLite 上为 `VARCHAR` 长度变化重建整张表**：SQLite 只按类型亲和性存值，重建丢掉触发器和手建索引却什么也没改。现在同一亲和性的类型视为相同（主键除外），和生成器的迁移一致。

- **`BatchInsert` 分配的主键不按切片顺序**：行里把 `default:` 列留给数据库的和没留的写成不同的语句，此前先插完一种形状的所有行，自增主键因此跳着分配。现在只合并相邻的同形状行。
- **新行的 `version` 从 0 开始，DDL 默认值却是 1**：TSQ 插入的行和手写 SQL 插入的行版本号不一致。`Insert` / `Upsert` 现在把为零的版本从 1 开始，调用方设了的版本（导入）保留。
- **单行 `Insert` / `Upsert` 为回读数据库填的列多发一条查询**：PostgreSQL 和 SQLite 上现在在写入语句里用 `RETURNING` 取回主键和这些列；MySQL 没有 `RETURNING`，照旧按主键回读。
- **SQL 日志把硬删除记成 `delete`**：和软删除同名，也和追踪里的 `hard_delete` 不一致。现在日志和错误信息里都是 `hard_delete`。
- `Exists` 选出全部列再 `LIMIT 1`，现在是 `SELECT 1`（分组、`DISTINCT`、自带 `Limit` 的查询保持原形状，它们的行由选择列表或顺序决定）；`PageKeyset` 的第一页不再带 `OFFSET 0`；选择列表里重名的列不再改名成 `tsq_c5`，而是 `categories_name`（别的表的列）或 `price_cents_2`；`RowState` 打印成 `live` / `deleted` / `existing`，错误信息写成 `needs live rows`。
- **`AttachMany` / `AttachOne` 的子查询不能排序**：文档说子行保持子查询的顺序、`AttachOne` 取子查询顺序里的第一条，但它们调用的 `Query.ListIn` 拒绝任何带 `ORDER BY` 的查询。切分键列表只会改变跨语句的整体顺序，从不改变取到哪些行，所以 `ListIn` 现在接受 `ORDER BY`：放得进一条语句时保持它，切分后每段各自有序；`LIMIT`、分组、聚合、`DISTINCT`、集合运算仍然拒绝。

- **SQLite 的重建式迁移可能删光整张表**：迁移先把旧表改名、建新表、复制行，复制一失败（新增 NOT NULL 列、表上有生成列），`sqlite3` 命令行并不停下，接着删掉旧表并提交。现在按 SQLite 文档的步骤先建新表、复制、再删旧表改名，并关掉外键；复制不会失败：新增的 NOT NULL 列用零值填、生成列交给新表计算，做不到的情况整段写成需要人工处理的注释。
- **迁移里的 `DROP TABLE` / `DROP COLUMN` 没有任何提示就能被执行**：改表名、改 `db` 标签，甚至把 `//tsq:table` 写成 `// tsq:table`，都会生成删表删列。现在这类语句（包括丢列的重建）以注释形式写在 `-- DESTRUCTIVE` 下，`tsq gen` 给出警告，像指令却不是指令的注释也会报出来。
- **删掉带索引的字段生成的迁移在三个方言上都失败**：先删列再删索引。现在索引先删。**SQLite 上新增生成列写成 `ADD COLUMN … STORED`**，SQLite 不接受：现在走重建。示例的迁移历史因此重新生成。
- **SQLite 上 `Reconcile` 按大小写比较列名**：库里的 `Name` 和声明的 `name` 被当成"删一列、加一列"，先删列把数据删了，随后加列失败、启动不了。SQLite 和 MySQL 的列名现在不分大小写地比较。运行期的重建也不再复制生成列，并给新增的 NOT NULL 列填零值。
- **`[]byte` 字段没赋值时 `Insert` 失败**：它的列是 NOT NULL，而 nil 按 NULL 绑定。现在按空字节写入。
- **`BatchDelete` / `BatchRestore` / `BatchHardDelete` 里有一行过期时，写成的行在内存里停在旧状态**，错误也说不出是哪一行，重试永远失败。现在和 `BatchUpdate` 一样回读：写成的行带上新状态和版本，`Keys` 列出没写成的行，后面的语句照常执行。
- **`BatchInsert` 带 `WithSkipDuplicates()` 时，后面的行失败会让已经入库的行在内存里丢掉主键和时间戳**，被跳过的重复行又留着库里没存过的时间戳。
- **带 JOIN 的 `PageKeyset` 静默跳行**：只要求最后一列是某张表的主键，一对多时父表主键重复，按它翻页跳过了其余子行。现在排序必须包含查询里每张表的主键。**从 CTE 读的 `PageKeyset` 会 panic**，现在报错。
- **经 CTE 或 `MapInto` 读回表的行类型，再 `Update` 会把没读到的列写成零值**：只有"同一张表的普通列"才会被当成部分读取。现在按行里实际被填的字段判断。
- **集合运算两边选列顺序不同时，字段被静默对调**：`Select(ID, Name, Email).UnionAll(Select(ID, Email, Name))` 把邮箱读进了姓名。现在构建时报错。

- **嵌入指针结构体（`*Base`）的表生成的代码一读就 panic**：生成的访问器解引用那个指针，而 `new(R)` 里它是 nil。现在 `tsq gen` 报错，要求按值嵌入。
- **`pk=Code` 用在 string 字段上又没写 `assigned` 时 `tsq gen` 直接 panic**：现在报错并提示加 `assigned`。
- **在模块外用绝对路径跑 `tsq gen /path/to/pkg` 找不到包**：包按导入路径从当前目录重新加载；加载失败时也不再吞掉 `go/packages` 给出的原因。
- **结构体叫 `Runtime` 时，它的生成文件被 `runtime.tsq.go` 覆盖**：现在报文件名冲突。
- **软删除表上叫 `IsDeleted`（或 `Insert`、`Update`、`HardDelete`、`Delete`、`Restore`）的字段让生成代码编译不过**：这些是生成在行类型上的方法，现在 `tsq gen` 报错。
- **结构体名缩写成 `db` / `ctx` / `tsq`（如 `DeviceBinding`）时，接收者遮住了行方法的参数**，生成代码编译不过。现在换成 `row`。
- **`[N]byte` 字段加 `type:` 后生成 `*[]byte` 访问器**，编译不过。
- **结果上的 `//tsq:search` 被接受然后静默丢掉**：结果没有生成查询可放搜索，现在报错（文档曾说它在结果上也有效，已更正）。
- **SQLite 重建式迁移里新增 NOT NULL 又没默认值的列时，缺少非重建路径那条手工处理的提示**，在有数据的表上执行到一半失败；迁移现在也保留 `AUTOINCREMENT` 的计数。
- **`db:"col,generated"`（不带表达式）被写成普通的 NOT NULL 列**，`CreateMissing` 建出的表上每次 `Insert` 都失败。这种列属于拥有这张表的迁移：生成的 DDL 用注释留出它，运行期策略拒绝替它建表。
- **包里与 TSQ 无关的结构体会让 `tsq gen` 中止**：带 `db` 标签的 map 字段、嵌入另一个包的接口（`io.Reader`）都会报一条既不说结构体也不说字段的错误。现在只有被表或结果用到的结构体才会报错，并点出结构体和字段。
- **包里有 `import "C"` 时 `tsq gen` 报 `cannot import package C`**。
- 泛型结构体上的 `//tsq:table`、引用另一个结果的结果，以前生成编译不过的代码，现在报错；`default:'a, b'` 不再在引号里的逗号处被截断；`//tsq:unique A, B`（逗号后有空格）不再报"字段名为空"；写在非结构体类型上的指令不再被静默忽略。
- **MySQL 上超过 3072 字节键长的索引（如 `VARCHAR(2000)` 上的唯一索引）和 `TEXT` 列上的索引，建表时才被数据库拒绝**：现在 `tsq gen` 打出警告，并在 `mysql.sql` 那条语句上方注明原因。不报错：PostgreSQL / SQLite 接受这种索引，只跑在它们上面的 schema 不该因此生成失败。
- **`SchemaPolicyCreateMissing` 从不补缺失的列**：文档、Go doc 和 README 都说它会，代码却把"缺一列"和"列不一样"一起当成不匹配而拒绝启动。现在缺的列会加上；列不一样或多出未声明的列仍然拒绝启动。
- **SQLite 上 `Reconcile` 重建表后，被硬删除的最大主键会被重新分配**：重建丢掉了 `sqlite_sequence` 里的计数，新表从复制过来的最大主键接着数。现在计数随表保留。
- **SQLite 按表名、索引名自省时区分大小写**：表建成 `"Users"`、声明成 `users` 时，`Validate` 报表不存在，`CreateMissing` 把一条什么也没做的 `CREATE TABLE IF NOT EXISTS` 记成已执行。
- **MySQL 上布尔和 decimal 默认值每次启动都被当成漂移**：MySQL 把 `true` 读回成 `1`、decimal 的 `0` 读回成 `0.00`。现在数值按值比较。
- **SQLite 上手写的 `id INTEGER PRIMARY KEY`（没写 `AUTOINCREMENT`）被当成与自增主键不一致**。
- **MySQL 的 `NOWAIT` 加锁失败（错误 3572）不被 `IsTxConflictError` 识别**，而 PostgreSQL 的同一种情况（55P03）会被重试。
- **平铺的集合运算链在不同方言上返回不同的行**：`a.Union(b).Intersect(c)` 原样渲染，SQLite 从左到右算出 `(a ∪ b) ∩ c`，MySQL 和 PostgreSQL 让 `INTERSECT` 优先算出 `a ∪ (b ∩ c)`。现在链一律从左到右求值：`INTERSECT` 前面有 `UNION` / `EXCEPT` 时，前面的部分包成派生表。要 `a ∪ (b ∩ c)` 就把组合好的操作数传进去：`a.Union(b.Intersect(c))`。
- **`IntersectAll` / `ExceptAll` 在 SQLite 上报语法错误**，而不是 `UnsupportedCapabilityError`：它们借用了 `INTERSECT` / `EXCEPT` 的能力位，而 SQLite 只有不带 `ALL` 的那种。新增能力位 `dialect.CapabilityIntersectAll` / `CapabilityExceptAll`。
- **`Page` 对集合运算查询的排序不做检查**：`Paging.OrderBy` 绕过了构建器 `OrderBy` 的那道校验，`Upper(col)` 被静默当成按 `col` 排序，别的表的列按名字绑上。现在两条路径用同一个检查。
- **`PageRequest.Order` 只给一个方向时报错**：文档一直写"每个字段一个，或者一个管全部"，代码只接受前者。现在一个方向用于所有排序字段。
- **空的 `NotIn` 在 PostgreSQL 的非整数列上报错**：它渲染成 `NOT IN (SELECT 1 WHERE 1 = 0)`，PostgreSQL 拿整数和列比较，`varchar = integer` 失败。现在 `NotIn(tsq.Vals())` 在构建时就是 `1 = 1`，空的列表参数渲染成 `(col NOT IN (NULL) OR 1 = 1)`，语义不变（空 `NotIn` 显式全匹配）。
- **嵌套集合运算的操作数逃过了读行前的可空性检查**：`a.Union(b.Union(c))` 里 `c` 可能为 NULL 的列只在扫描时报 `converting NULL`。
- **分组或 `DISTINCT` 查询选了两个同名列时，MySQL 上 `Count` / `Page` 失败**（错误 1060）：计数把查询包成派生表，派生表不许重名。现在同名列从第二次出现起换成生成的名字，读行按位置，调用方无感知。
- **子查询里的 `Search` 被静默丢掉**：关键词是执行参数，子查询收不到，搜索谓词就没了。现在构建时报错。
- **`Exists` 会因为选中列可能为 NULL 而报错**，而它根本不读那一行。
- **`NullColumn.WithTable(cte)` 在 CTE 已经 `COALESCE` 过时仍被当成可为 NULL**：现在可空性按 CTE 体推导。
- **`In(带 Limit 的子查询)` 在 MySQL 上被拒绝**（错误 1235）：现在写成派生表。
- **CTE 选了两个同名输出列**（`SUM(amount)` 与 `MAX(amount)`）**时引用它们有歧义**，以前要到数据库执行时才报错，现在构建时报错。
- **有生成列的表，读出所有可写列的行被当成"部分列读取"拒绝 `Update`**：完整性按全部列算，而生成列从来不写。
- **`BatchUpdate` 里有一行过期时，其余行在库里已经写成，内存里却被退回旧的 `updated_at` 和 `version`**，错误也说不出是哪一行，拿同一批行重试永远失败。现在写成的行带上新版本，`OptimisticLockError.Keys` 列出过期行的主键；一行过期也不再打断后面的语句。`RowStateError` 同样新增 `Keys`。
- **没有 `version` 列的表 `Update` 一个不存在（或已软删除）的行时报告成功**：现在返回 `*RowStateError`。MySQL 写入原值时报告零行变化，那不算错。
- **`Insert` / `Upsert` 失败后行上留着库里从未存过的时间戳、主键和清空的墓碑**：现在只有写成的行保留它们。
- **`BatchDelete` / `BatchRestore` 让 `*time.Time` 字段的所有行指向同一个时间**：改一行的时间会改掉全部。
- **`BatchUpdate` 里两行主键相同时，后一行被静默丢掉**：现在报错。
- **指向零值时间的非 nil `*time.Time` 被当成调用方设置过的 `created_at`**，于是存进公元 1 年。
- **MySQL 批量插入回填主键时假定 `auto_increment_increment = 1`**：多主部署下除第一行外全错。现在按该变量的步长回填。
- **驱动报不出 `LastInsertId` 时 `Insert` 静默留下零主键**：现在返回错误。
- `Restore` 和 `RowStateError` 的 Go doc 与实际错误类型不符，已更正。

- **字段类型写成 `sql.Null[T]`（或任何实例化的泛型类型）时 `tsq gen` 直接报 `unsupported field type: *ast.IndexExpr`**，而文档一直说可空字段可以用 `sql.Null[T]`。现在解析器接受泛型类型，生成器按 `go/types` 写出完整类型，DDL 推导把 `sql.Null[T]` 当作可为 NULL 的 `T`，托管时间列也接受 `sql.Null[time.Time]`。示例改用 `sql.Null[time.Time]` 后，模块不再依赖 `gopkg.in/nullbio/null.v6`（生成器仍按类型路径识别 nullbio 类型）。

- **按唯一字符串键批量读取，在不区分大小写的排序规则下会误报"不存在"**：MySQL 默认的 `utf8mb4_0900_ai_ci` 让 `'intro to go'` 匹配到 `'Intro to Go'`，旧的生成代码却逐字节比对返回的行，于是报 `sql.ErrNoRows`。现在对 Go 里对不上的字符串逐个再问一次数据库，以数据库的判断为准，遇到第一个确实不存在的键就停止。

- **字段类型来自 `database/sql`（如 `sql.NullString`）时，生成代码编译不过**：模板写出 `tsqsql.NullString`，却从未导入 `tsqsql`。现在有一个真正 `go build` 生成物的测试守着。
- **声明 `*time.Time` 托管时间戳字段时，生成代码引用了不存在的 `tsq.TimePtr`**：托管时间戳现在由库维护，生成代码不再涉及。
- **字符串字面量里出现 `FOR UPDATE` 之类的词，会让正常查询被当成使用了不支持的能力而拒绝执行**：能力检测不再扫描 SQL 文本，而是由渲染对应构造的代码报告。
- **v4 的 `ContainsVal` 等模式方法不转义通配符**：`ContainsVal("50%")` 会匹配 `50` 开头的任何内容。现在与关键词搜索一样转义并声明 `ESCAPE`。
- **相关子查询的外层表没有被外层查询校验**：外层没 join 那张表时构建成功、执行时才由数据库报错。
- **有生成列的表 `Upsert` 必然失败，`default:` 列被写成 Go 零值**：`Upsert` / `BatchUpsert` 此前把所有列写进 INSERT，生成列在三个方言上都被数据库拒绝（示例里的 `Course` 就是这样），未设置的默认值列绑的是零值而不是让数据库填。现在和 `Insert` 用同一条规则：生成列从不写，默认值列只在行里设了值时才写（更新时未设置的保留原值），单行 `Upsert` 把数据库填的列读回来。
- **有 `version` 列的表批量写 1000 行在 SQLite 上失败**：按主键和版本匹配曾渲染成每行一个 `OR`，而 SQLite 的表达式深度上限 1000 恰好等于默认批量大小，不传任何选项的 `BatchUpdate` / `BatchDelete` / `BatchHardDelete` 从 998 行起报 `Expression tree is too large`。现在渲染成 `pk IN (...) AND CASE pk WHEN ? THEN version = ? ... END`。
- **`ListIn` 会把 `Not(col.In(list))` 按块拆开**：每块 `NOT IN` 都匹配其他块排除的行，结果被重复拼接且不报错。现在和 `NotIn` 一样拒绝。
- **`driver.Valuer` 值的 NULL 检查从未生效**：`Pred` / `Expr` 里的 `sql.NullString` 这类结构体 Valuer 被当成不可比较而拒绝，底层不是结构体的 Valuer 即使是 NULL 也被放行，渲染成 `col = NULL` 静默零行。游标分页对 NULL 的排序值同样漏检。
- **集合操作的操作数自带的 `OrderBy` / `Limit` / `Offset` / 行锁被静默丢弃**：守卫检查的是左侧而不是操作数，`Union(q.OrderBy(...).Limit(3))` 渲染时前三条的限制消失。现在构建时报错。
- **失败的 `Update` 也改掉了调用方行上的 `updated_at`**：时间戳在任何校验之前写进行里，版本冲突或部分列行被拒绝之后，行对象带着一个数据库里不存在的时间，`version` 却没动。现在执行失败时恢复原值。
- **`WithMaxPageSize(0)` 被当成"没设置"**：静默落回默认的 1000。现在小于 1 的值报错。
- **DDL 失败了日志仍记 "applied ddl"**：日志写在执行之前，SQLite 重建表在事务回滚后也照样宣称每条语句都已应用。现在只在成功（重建是提交）之后记录。
- **schema 对账对列默认值的比较**：在第一个 `::` 处截断，PostgreSQL 读回的 `'a::b'::text` 永远对不上声明的 `'a::b'`，每次启动都重设默认值；同时一律转小写，`'Active'` 改成 `'active'` 时看不出差别。现在只截掉字面量之外的类型转换，两个带引号的字面量按原样比较。
- **MySQL 上 `Upsert` 读不到主键时静默返回成功**：文档承诺回读 `version` / `created_at`，这时却什么也没读。现在返回错误。
- **JSON 标签里带引号时生成失败**：标签原样拼进字符串字面量。生成的表名、列名、索引名和 JSON 名现在都用 Go 的引号转义写出。
- **迁移记录里新增 `NOT NULL` 且没有默认值的列**：`ADD COLUMN` 在有数据的表上三个方言都会失败，迁移段里却没有任何提示。现在语句前带一行注释，说明失败条件和两种修法（声明 `default:` 或先回填）。
- `AttachMany` / `AttachOne` 在发出查询之前检查 `assign`，而不是查完再报错。
- **软删除和恢复的版本冲突被报成 `RowStateError`**：陈旧副本的 `Delete` / `Restore` 报"需要一个活行"，`WithRetry(tsq.IsOptimisticLockError)` 不会重试本该重试的冲突，而 `Restore` 的文档写的正是 `OptimisticLockError`。现在匹配不上时回读版本号：版本变了报 `OptimisticLockError`，只是状态不对才报 `RowStateError`。
- **MySQL 上 `TEXT` / `TINYTEXT` 列被当成 `MEDIUMTEXT`**：一个只能存 255 字节的 `TINYTEXT` 被判定与声明为 100000 字符的字符串一致，`Reconcile` 从不修正，写入长数据时才报错。现在两者按原始类型读回，只和显式的 `type:TEXT` / `type:TINYTEXT` 一致。
- **MySQL 上 `Reconcile` 会去删外键正在用的索引**：MySQL 的索引列表从不标记约束，"约束支撑的索引不许重建"这条保护在 MySQL 上从不生效，使用者拿到驱动的 1553 错误。现在外键需要的索引被标出，重建会被拒绝并说明原因。
- **分组、HAVING、集合操作之后绕一次 `OrderBy` 就能加行锁**：`GroupBy(...).OrderBy(...).ForUpdate()` 能编译，PostgreSQL 拒绝执行。现在这些阶段的 `OrderBy` / `Limit` / `Offset` 返回新的 `OrderedResultStage`（经 `ResultSortable`），上面没有 `ForUpdate` / `ForShare`。
- **聚合、`CASE` 等派生选择项没有列名，CTE 和集合操作的 `ORDER BY` 按名字找不到它们**：`CTE` 里的 `SUM(fee_cents)` 在外层用 `FeeCents.WithTable(cte)` 引用时报 `no such column`，集合操作按派生项排序时 SQLite 报 `does not match any column`。现在不是裸列的选择项写成 `AS <列名>`；集合操作的排序项必须是输出列（`ResultColumn` 新增 `Asc()` / `Desc()` 用来按选中的投影排序），`Upper(col)` 这类没选中的表达式在构建时报错，而不是被静默换成它包着的列。
- **嵌套的集合操作在 SQLite 上是语法错误**：`a.Union(b.Union(c))` 渲染成带括号的复合 SELECT，SQLite 不认。现在嵌套的操作数写成派生表，三个方言都能执行。
- **`UpdateTable(nil)` 或 `Set(nil, ...)` 直接 panic**：现在和构建器其他地方一样是构建错误。
- **实现了 `driver.Valuer` / `sql.Scanner` 的自定义类型被按底层类型猜列类型**：文档一直要求这类字段写显式 `type:`，生成器却没有检查，`type Status string` 的 `Value()` 返回整数时建出 `VARCHAR` 列，写入时才报错。现在 `tsq gen` 拒绝并给出修法；只是按底层类型存储的具名类型（`type Level int`）不需要 `Value()`，删掉它即可推导。显式 `type:` 的可空 codec 类型（带 `Valid bool` 的结构体）此前在 Go 侧是 `NullColumn`、DDL 里却是 `NOT NULL`，现在两边一致。
- **几种字段形状让生成代码编译不过**：result 字段的类型来自别的包时 result 文件不写 import；两个同名包的类型都按包名拼写（导入的是 `pkg` / `pkg1`）；本包泛型类型用别的包的类型实例化时漏掉那个 import；唯一索引字段叫 `Ctx` / `Db` / `T` / `Tsq` 时生成的 `GetByX` 参数和 `ctx`、`db`、接收者、`tsq` 包重名。
- **软删除表上的全文索引带上了 `deleted_at`**：MySQL 拒绝把整数列放进 FULLTEXT，PostgreSQL 的 `coalesce` 类型不匹配，两边的 DDL 都执行不了。唯一索引和普通索引仍以 `deleted_at` 打头。
- **两个字段映射到同一列时生成非法的 `CREATE TABLE`**：重复的 `db` 标签（大小写不同也算）或 `A, B string` 共用一个标签，现在 `tsq gen` 报错。
- **删掉一个结构体后 `tsq gen` 不删它的生成文件**：残留文件引用已不存在的类型，包编译不过，之后每次 `gen --check` 都失败。现在和过期的 DDL 文件一样删除。
- **文档写着 `//tsq:result [name=X]`**：result 不收任何选项（名字对投影没有含义），文档已更正。
- **SQLite 上有表达式索引时 runtime 启动失败**：`PRAGMA index_info` 对表达式列报 NULL 列名，读索引列表时 `converting NULL to string`——只要库里有一个 `CREATE INDEX ... ON t(lower(x))`，任何索引策略都起不来。现在和 PostgreSQL 一样，表达式列不计入索引的列。
- **SQLite 上 `Reconcile` 改列类型时静默丢掉约束、改写索引**：重建表时新表只按声明的列建，UNIQUE / CHECK / 外键约束随旧表消失；索引按名字和普通列重建，表达式索引被丢掉、`(a, lower(b))` 变成 `(a)`、部分索引丢掉 `WHERE`，触发器全丢；先 RENAME 旧表还会让别处的视图、触发器和外键指向随后被删掉的临时表。现在表自己的索引和触发器按原始语句重建，其余无法保留的情况拒绝重建并说明原因，交给迁移。
- **两个不同的 CTE 同名时静默合并**：`WITH` 按名字去重，第二个 CTE 的查询和参数消失，引用它的分支读到第一个 CTE 的结果。现在构建时报错；同一个 CTE 在多个分支里引用仍只写一次。
- **`BatchUpsert` 的同键检查能被绕过**：检查发生在清除 `deleted_at` 之前，一行带墓碑、一行活着的同邮箱被当成不同的键，SQLite 上静默只剩一行；可空键按指针地址比较也会漏检。现在按实际写入的值比较。
- **`Build` 不检查分组**：选了既不在 `GROUP BY` 里、也不在聚合里的列，SQLite 返回组里任意一行的值，PostgreSQL 执行时报错。现在构建时报错并点名那一列；分组了表的主键时，同表其他列照常可选。
- **`Count` 忽略 `Limit`**：`Limit(2)` 的查询 `Count` 出全部匹配行数；排序里用了参数的查询 `Count` 时反而报"多余的参数"。现在带 `Limit` / `Offset` 的查询包一层再数，`Count` 接受和 `List` 相同的参数。
- **能构建、执行时才失败的几种查询**：`DISTINCT` / 聚合之后加行锁、CTE 里用 `Correlate`（CTE 看不到外层）、顶层集合操作数用 `Correlate`、带搜索的查询 `GroupBy` 之后做集合操作、对 nil 的 `*Query` 调 `Get`（panic）。现在都在构建或调用时报错。
- **`NotIn(可空子查询)` 静默返回零行**：子查询结果里有一个 NULL，`NOT IN` 就对每一行都是 UNKNOWN。现在构建时拒绝，提示改用 `NotExists` 或在子查询里用 `Coalesce` 滤掉 NULL。
- **`AttachMany` 的子键没被选出或可空时读错**：没选出子键时所有子行挂不上，可空键按指针比较永不相等。现在没选出直接报错，可空键按值比较、NULL 键跳过；文档写明挂接按 Go 值精确匹配，不跟数据库排序规则走。
- **部分列读出的行能被 `Insert`**：和整行 `Update` 一样会把没读的列写成零值。现在同样拒绝。
- **MySQL 上按条件写的两种形状和别的方言结果不同**：`SET a = b, b = a` 在 MySQL 上从左到右求值（后一个赋值读到新值），`UPDATE` / `DELETE` 的子查询读同一张表报 1093。现在构建时拒绝前者，MySQL 上渲染时拒绝后者并给出改法。
- **重建索引时先删后建**：新索引建不出来（比如新加的唯一约束和现有数据冲突）时旧索引已经没了。现在先按临时名建新索引，成功后才删旧的、改回原名。
- **MySQL 的 `DATETIME` 只存到秒**：托管时间戳写进去被四舍五入，内存里的行和读回的行不一致。时间列改为 `DATETIME(6)`（`DEFAULT CURRENT_TIMESTAMP` 相应写成 `CURRENT_TIMESTAMP(6)`），库写入的时间戳截断到微秒（MySQL 和 PostgreSQL 的精度）；`Reconcile` 会把已有的 `DATETIME` 列加宽，用生成的迁移文件的项目需要自己执行 `ALTER TABLE ... MODIFY ... DATETIME(6)`（生成器的迁移历史不会为方言拼写的变化补一条迁移）。
- **只有自增主键的表不能 `Insert`**：渲染出 `INSERT INTO t () VALUES ()`，只有 MySQL 接受。现在把主键列交给数据库（SQLite 写 `NULL`，其他写 `DEFAULT`），单行和批量都能写。
- **`IsRetryableNetworkError` 把裸的 `io.EOF` 当成断线**：回调里读文件读到末尾就会让整个事务重跑。现在只认 `io.ErrUnexpectedEOF`。追踪器数量上限（超出时往 `slog.Default` 打警告）一并删除。
- **result 不能投影 LEFT JOIN 可空一侧的列**：字段类型必须和源列完全相同，所以 NOT NULL 列没法读进 `sql.Null[T]` / `*T`。现在接受源列值类型的任何可空形式，并用 `MapIntoNull`。
- **生成的名字和包里已有的名字冲突时生成出编译不过的包**：包里手写了 `TableX` / `XTable` / `TSQTables`，或一张表叫另一张表生成出的类型名（`Row` 与 `RowTable`）。现在 `tsq gen` 报错并指出声明的位置。
- **同一字段担任两个角色、两个结构体映射同一张表时不报错**：主键同时是 `version` 或时间戳，`created_at` 和 `updated_at` 是同一个字段，两个结构体的 `name=` 只差大小写。生成器和 `Define` / `Open` 现在都拒绝。
- **生成器的校验错误不带位置**：现在以 `文件:行:列: 结构体名:` 开头。
- **生成代码的几处瑕疵**：`HardDelete` 的文档注释跟在上一个函数的 `}` 后面；result 文件的"Code generated"注释紧贴 `package`，成了包文档；列表参数名简单加 `s`（`statuss`、`categorys`）。
- **`tsq gen --check` 发现过期和其他错误用同一个退出码**：现在过期退出 2，其他错误退出 1，CI 能区分"忘了跑 gen"和"包坏了"；帮助里 `-v` 显示为 `-v, --verbose`，并列出 `runtime.tsq.go`。
- **LIKE 的大小写敏感性随方言不同，文档没说**：SQLite 忽略 ASCII 大小写，MySQL 按排序规则，PostgreSQL 区分。模式函数和关键词搜索的文档现在写明，并给出三方言一致的写法。
- **两张表共用一个行类型时，按窄 `Select` 读出的行 `Update` 会把没读的列写成零值**：部分读取的判断按行类型查表，先定义的表胜出，读的是另一张表时判断落空。现在 `Define` 拒绝第二张（不同名的）表使用同一个行类型，归档表、分片表用自己的类型（`type OrderArchive Order`）。
- **`Not(col.In(空列表))` 一行都不返回**：空 `In` 渲染成 `IN (NULL)`，结果是 UNKNOWN 而不是 FALSE，取反仍是 UNKNOWN。现在空 `In` 是明确的 FALSE、空 `NotIn` 是明确的 TRUE，取反后都对；非空列表的 SQL 不变。
- **版本冲突只差在时间列上时，下一次 `Update` 会覆盖别人的修改**：冲突后回读判断"是不是自己写成的"时跳过了时间列，别人只改了时间（或写了同样的值）就被当成自己写的，内存里版本号前移。现在时间列也比较（精度到微秒），冲突行留在 `OptimisticLockError.Keys` 里、版本号不动。
- **生成的 SQLite 迁移把可空列改成 NOT NULL 时会清空整张表**：重建时原样复制这一列，存着 NULL 的行让复制失败，`sqlite3` 命令行继续执行并删掉旧表。现在这些行填入类型的零值，复制不会失败。
- **按非主键的唯一键 `Upsert`、主键由调用方指定时**：冲突行更新了库里的行，内存里却还是自己提议的主键，随后回读失败（此时更新已经发生）；`BatchUpsert` 静默留下错的主键。现在这些行拿到库里那一行的主键。
- **`GetBy` / `FindBy` / `FetchBy` 接受不唯一的列**：多行匹配时返回任意一行、或把其余的报成缺失。现在要求唯一（见"新增"）。
- **`default:` 列写不进零值**：零值被当成"未设置"，`false` / `0` / `""` 永远写不进去，库里存的是默认值（批量插入时内存与库还不一致）。现在只有 NULL 交给默认值（见上）；示例的 `Course.Currency` 因此改为 `*string`。
- **`json.RawMessage` 这类具名字节切片为 nil 时被写成 NULL**，插入 NOT NULL 列失败。现在和 `[]byte` 一样写成空字节。
- **`DEFAULT CURRENT_TIMESTAMP` 在 PostgreSQL 和 MySQL 上存的是会话时区的本地时间**，而 TSQ 写入的时间都是 UTC。现在渲染成 `(CURRENT_TIMESTAMP AT TIME ZONE 'UTC')` / `(UTC_TIMESTAMP(6))`；已有的表结构不会自动改，用迁移文件的项目要自己改默认值。
- **`FetchBy` / 生成的 `FetchByX` 按时间取行时，时区不同的值被报成缺失**：取回的行按 Go 的 `==` 对应，时区和单调时钟都参与比较。现在按瞬间对应。
- **`tsq.json` 丢了之后，后续的结构变化从迁移记录里消失**：生成器把已有的 `.sql` 当成历史起点。现在 `.sql` 在而 `tsq.json` 不在时拒绝运行，提示从版本库恢复或删掉 `.sql` 重新开始。
- **索引换表、或表改名保留原索引名时，迁移先建后删而失败**（PostgreSQL 和 SQLite 的索引名全库唯一）。现在所有删除索引的语句最先执行，被删除（注释掉）的表让出新索引要用的名字。
- **MySQL 迁移用 `MODIFY COLUMN` 改生成列时丢掉了 `GENERATED` 子句**，列变成 TSQ 从不写的普通 NOT NULL 列。现在生成表达式的变化留给手写迁移，并注释说明。
- **PostgreSQL 忽略无符号**：`uint16` 建成 `SMALLINT`、`uint32` 建成 `INTEGER`、`uint64` 建成 `BIGINT`，都装不下 Go 类型的上半段。现在取下一档更宽的类型（`uint64` 为 `NUMERIC(20)`），读回时 `NUMERIC(20)` 对应 `uint64`；其他 `NUMERIC` 和 `DATE` 不再被当成浮点和时间列。已有的列 `Reconcile` 会加宽，用迁移文件的项目需自己改。
- **集合运算按一个没选中的列排序时，可能按同名的派生项排序**：派生项以源列名作别名（`LENGTH(name) AS name`），按 `name` 排序实际按长度排。现在这种排序在构建时报错。
- **文档写 `*[]byte` 是可空字节字段**，解析器却拒绝它。文档改为 `sql.Null[[]byte]`。
- **`Page` 的 `Paging.OrderBy` 跳过了构建器 `OrderBy` 的检查**：分组查询按未分组的列分页在 SQLite 上返回任意一行的值、PostgreSQL 上报错；出错的表达式渲染成 `ORDER BY  ASC`；引用查询外的表要到数据库才报错。现在它和构建器的排序过同一套校验。
- **能构建、每个方言执行时都失败的几种查询**：聚合出现在 `WHERE`、连接条件或 `GROUP BY` 里，聚合套聚合，没有分组却按聚合排序（`Count` 返回数字而 `List` 失败）。现在构建时报错。
- **`SelectDistinct` 按没选中的列排序**只有 SQLite 能执行（按每组任意一行排），PostgreSQL 和 MySQL 拒绝；**行锁加在 `LEFT` / `RIGHT` / `FULL JOIN` 上**PostgreSQL 拒绝。现在都在构建时报错。
- **集合运算里第二个及以后的操作数用了 `Correlate`，外层却没有那张表**，照样能构建、到数据库才报错。现在每个操作数的外层表都会检查。
- **分组或排序的表达式里有绑定值时 PostgreSQL 拒绝执行**（例如按 `CASE` 分桶）：PostgreSQL 给每个占位符重新编号，选择列表里的 `$1` 和 `GROUP BY` 里的 `$4` 被当成不同的表达式。现在这种表达式在 `GROUP BY` / `ORDER BY` 里写成它在选择列表中的位置。
- **结果全是绑定值的 `CASE` 在 PostgreSQL 上被当成文本**：数字按字符串排序（`10` 排在 `9` 前），pgx 也无法把 `int64` 绑成文本。现在 PostgreSQL 上这些结果显式转换成结果类型。
- **按主键分组时，RIGHT / FULL JOIN 里的软删除表的其他列被放行**，而这时它被读成活行派生表，PostgreSQL 不会从派生表的主键推出函数依赖。现在这些列必须分组。
- **相关子查询的 `JOIN ... ON` 不能引用外层查询的表**，被误报成"未连接"。现在外层表在 `ON` 里也可用。
- **`AttachMany` / `AttachOne` 不认嵌入了 `database/sql` 可空类型的键**（guregu `null.Int` 的形状），`NewNullColumn` 却接受它们。现在按相同的规则找值字段。
- **`PageKeyset` 不接受结果类型里对主键的投影**（`MapInto(TableX.ID, ...)`）作为唯一排序列，结果类型因此无法游标分页。现在接受。
- **零值的 `Query`、`Mutation`、`TableOf`、`UpdateStage` 和 `Searchable(nil)` 直接 panic**：现在和构建器其他地方一样返回错误。
- **`Like` 的文档说"没有转义字符"**：实际由数据库决定（MySQL / PostgreSQL 用反斜杠，SQLite 没有）。文档已更正，字面匹配请用 `StartsWith` / `Contains`。
- **SQLite 连接池只有一个连接时，只要有索引，启动就永远卡住**：读索引列表时没关结果集就去查每个索引的列，需要第二个连接；表重建的检查也一样。现在先读完再查。
- **`Reconcile` 在 SQLite 上删不掉带索引的列**（SQLite 拒绝，而 TSQ 不删未声明的索引），启动失败。现在先删掉覆盖这一列的索引。
- **没有 `version` 列的表 `Delete` / `Restore` 一行已经不存在的行时报成乐观锁冲突**，重试帮手会去重试；同一行的 `Update` 报的是 `RowStateError`。现在一致报 `RowStateError`。
- **SQLite / MySQL 上索引列名按大小写比较**：库里 `"Code"` 上的索引被 `Validate` 拒绝、被 `Reconcile` 每次重建，表重建时还会被当成删掉的列上的索引而丢掉。现在不区分大小写（TSQ 本来就拒绝只差大小写的两列）。
- **MySQL 上 `MEDIUMINT` 被当成 `INT`、`DECIMAL` 被当成 `DOUBLE`**，手工迁移成更窄或会舍入的类型能通过 `Validate`。现在它们按原始类型比较。
- **单行 `BatchInsert(..., WithSkipDuplicates())` 被跳过的行读回了撞上的那一行的值**。现在跳过的行不读回。
- **`Define` 接受 TSQ 写不了的托管列**：`bool` 的 `created_at`、字符串的 `version`、`time.Time` 的墓碑（每一行都被当成已删除）、`int16` 的墓碑（`UnixNano` 被截断，有时截成 0）、既唯一又全文的索引，都要到第一次写入才报错。现在 `Define` 拒绝。
- **MySQL 的 TEXT / BLOB / JSON 列不接受字面量默认值**（错误 1101）：现在渲染成表达式 `DEFAULT ('...')`。
- **MySQL DSN 没有 `parseTime=true` 时所有托管时间戳都读不回来**，报错离原因很远。现在 `Open` 直接拒绝这样的 DSN，文档写明 `NewRuntime` 的池也需要它。
- **空结果有时是 `nil`（JSON 里是 `null`）、有时是 `[]`**：`List`、`AttachMany` 给没有孩子的父行，现在和 `Fetch`、`Page.Data` 一样是空切片。
- **nil 选项的处理不一致**：nil 的 `BatchOption` 和 tracer 被静默跳过，`WithLogger(nil)` 被当成默认值，而 nil 的 `RuntimeOption` / `TxOption` 报错。现在一律报错。
- **Go doc 与行为不符**：`WrapExecutor` 说没有页大小上限（实际按 `DefaultMaxPageSize`）；`PageRequest.Keyset` 说最后一个排序字段必须是主键（实际要包含每张表的主键）；`CapabilityFullTextSearch` 被说成执行时检查（实际只报告 `Matches` 怎么匹配）；`Mutation.Exec` 没说 MySQL 默认只数值真正变化的行；`MaxPageNumber` 的溢出说明不对。已更正。
- **使用者文档里编译不过的例子**：`BEST_PRACTICES.md` 的 `ID.EQ(1)` 这类裸值、`Set(UpdatedAt, tsq.Val(null.TimeFrom(...)))`，REFERENCE 里不是合法 Go 的可空列声明；追踪操作名的列表缺项。已更正。
- **名为 `Table` 的表结构体（或名为 `Result` 的结果）生成编译不过的代码**（`TableTable` 重复声明）。行类型上手写了与生成方法同名的方法（如 `Update`）同样编译不过。现在 `tsq gen` 报错并指出位置。
- **不带 `-v` 时看不到"删除语句被注释掉"的警告**：改表名、改 `db` 标签在生成器眼里和删除一样，警告是使用者得知这件事的唯一途径。现在每次都打印。
- **`type ( ... )` 分组上方的 `//tsq:table` 作用到组里每个结构体**：现在报错，要求写在具体的类型上。
- **`db` 标签里不认识或写错的选项被静默忽略**（`sise:10`、`size:ten`、`defualt:5`）：现在报错；`default:` 与 `generated:` 同时出现也报错。
- **同一组字段上的全文索引和唯一索引被当成重复**：两者回答不同的查询，现在允许；重复的 `//tsq:search` 字段现在报错而不是生成两遍。
- **只改了大小写的列名（`name` → `Name`）生成了 `ADD COLUMN`**，MySQL 和 SQLite 把两个拼写当成同一列而失败。现在是列改名：PostgreSQL 上 `RENAME COLUMN`，另两个方言写一条说明不需要执行的注释；SQLite 重建表时按大小写不敏感复制。
- **SQLite 为它并不检查的类型变化重建整张表**（`VARCHAR` 长度、`INT` 到 `BIGINT`），重建会丢掉触发器和手建的索引。现在同一类型亲和性内的变化只写注释。
- **PostgreSQL 的 `ALTER COLUMN ... TYPE` 没有 `USING`**，没有隐式转换的变化（布尔到整数、文本到整数）直接失败；加宽自增主键时序列仍停在旧类型的上限。现在带 `USING`，并同时加宽序列。
- **MySQL 上 `[]byte` 的 `size:` 被忽略**，一律建成 64 KiB 的 `BLOB`。现在按大小选 `BLOB` / `MEDIUMBLOB` / `LONGBLOB`。
- **包里没有带指令的结构体时生成四个空的 schema 文件**并提示去执行它们；没有任何列字段的结果静默地什么都不生成。现在都报错。
- **每次 `tsq gen` 都重写所有生成文件**，即使内容没变，触发构建缓存和文件监视；`-v` 的输出绕过了命令的输出流。现在内容不变的文件不写。
- **`//tsq:unique Tags,Tag` 生成了两个同名参数**，`FetchByTagsAndTag` 编译不过。现在列表参数避开同名。
- **几条指错方向的报错**：字符非法的标识符被说成"太长"；带字段但缺 `db` 标签被说成"没有这个字段"；嵌入其他包类型里的未导出字段报"找不到字段"；不存在的包目录套了三层"failed to parse"。现在各自说清原因和改法。

### 其他

- **文档对齐实际行为**（第四轮审计）：`skills/tsq` 不再让人用不存在的 `ScalarNull`；README 不再说 `*sql.DB` 可以直接当执行器、不再提不存在的 `PageRequest.Validate` 和"自定义方言合约"；`docs/skill.md` 复述规则（含 `tsq gen` 会拒绝的旧注解写法）的一节换成指向 REFERENCE 的链接；REFERENCE 写明 `Search` 要 `tsq.Searchable` 包住列、可空时间墓碑配唯一索引会被拒绝、`.sql` 和 `tsq.json` 总会生成、`Upsert` 的冲突参数；`BEST_PRACTICES.md` 去掉"生成 helper 初始化静态查询"的过时说法；示例里几处不准的说明和打印改正。

- **示例整个重写**：`examples/` 现在是 11 章由浅入深的教程（从结构体和 `tsq gen` 到方言与追踪），每章一个可运行的程序，打印每一步、TSQ 实际发出的 SQL 和结果，并带一个断言输出的测试；第 2 到 11 章共用一个网店模型 `examples/shop`。原来的 `examples/academy` 挪到 `internal/integration/academy`，只作集成测试的夹具；`quickstart` / `advanced` / `full-suite` 三个程序删除。

- CLI 改用标准库 `flag`，不再依赖 cobra、pflag、`golang.org/x/term`。flag 仍可写在参数之后（`tsq gen ./pkg --check`），`tsq --version` / `tsq help <命令>` 照旧可用；报错着色认 `NO_COLOR`。
- 删掉了从未发布到任何镜像仓库的 Docker 镜像构建；CI 的 `Build` 改为运行构建出的二进制，核对注入的版本和 commit。

- 模板与模板 helper 里出现的每个 `tsq.X` / `tsqdialect.X` 都对照真实包的导出符号校验（`internal/cmd/generated_symbols_test.go`）。

## [4.10.0] - 2026-09-03

### 新增

- **相关子查询（`Correlate(...)`）**: 子查询现在可以引用外层查询的列，只要先用 `Correlate(TableOuter)` 声明那张外层表——它和 join 方法处在同一个阶段上，写在 `Where(...)` 之前。此前这类查询在 `Build()` 就被 join 图校验拒掉，只能改写成 `NIn(子查询)`（而那个改写在子查询列可空时**不等价**：`NOT IN` 碰到 `NULL` 返回零行）。同一张表既 `Correlate` 又 join 是构建错误——join 进来的表会遮蔽外层的同名表，谓词随即不再相关；带 `Correlate` 的查询也不能单独执行，只能当子查询用。相关 `NOT EXISTS` 的语义由真跑 SQLite 的用例守着。
- **`tsq.TableWithCols(table, cols)`**: 原样返回 `table`，第二个参数从不被读取。它存在的唯一目的是让包级表变量对列切片留下一次**可见的引用**。生成代码现在把表变量声明成 `var TableXxx tsq.Table = tsq.TableWithCols(Xxx{}, Xxx__Cols)`。手写 `tsq.Table` 实现的使用者应照同样的方式声明。

### 修复

- **只选部分列的包级查询变量会在包初始化时 panic，报"列不属于本表"**: 现象是 `column deleted_at does not belong to table task` 指着一列明明存在的列。成因是 Go 的包级初始化顺序只认初始化表达式里出现的引用，而 `Cols()` 是通过接口方法在运行期去取 `Xxx__Cols` 的，依赖分析看不见——只选部分列的**投影**查询从头到尾不会提到那个切片，于是可以排在它前面初始化。此时切片不是空的（空会被当成"没有列信息"而放行），而是**长度已满、元素全是 `nil`**：切片头是编译期静态数据，元素赋值发生在 init 里。炸不炸取决于包内文件名顺序，所以同一种写法在一个文件里正常、换个文件就 panic，而报错内容完全不指向成因。修法是让依赖显式可见（见上面的 `TableWithCols`）。**使用者需要重新生成代码**：此前生成的表变量里没有这个锚点。用 `Select(Xxx__Cols...)` 的常规查询从来不受影响。
- **列校验对"列切片尚未初始化"给出可诊断的报错**: 表报告了 N 列但每一列都是 `nil` 时，不再说"列不属于本表"，而是直接说明列切片正在被包初始化填好之前读取，并指向 `tsq.TableWithCols`。这条路径覆盖手写的 `tsq.Table` 实现和尚未重新生成的旧代码。
- **join 图报错不再建议用 `CrossJoin`**: 此前的报错是 `table X is referenced outside the join graph; use CrossJoin to include it explicitly`。照做能编译、能构建、也能跑，但子查询里 join 进来的那张表会**遮蔽**外层同名表，谓词随即不再相关——对每一行外层数据取值都相同，`NOT EXISTS` 写法的典型表现是返回全部行或一行不返回，且没有任何报错。现在的报错说明这张表不在本查询的 `FROM`/`JOIN` 图里，并在它可能是外层引用时指向 `Correlate(...)`。

## [4.9.0] - 2026-09-03

### 新增

- **按条件的批量 `UPDATE` / `DELETE`（`tsq.UpdateTable[T]()` / `tsq.DeleteFrom[T]()`）**: 此前“把满足条件的行都改掉”在 TSQ 里没有入口，只能先 `List` 再逐行 `Update`（每行都走乐观锁校验）或退回裸 SQL。现在是一个阶段式构建器：`Set` / `SetVal` / `SetVar` 是 Go 1.27 的泛型方法，列和值的类型在编译期对上；`Where(...)` 必需且只能一次，由类型系统强制；`Build()` 产出可复用的 `*tsq.Mutation[T]`，`Exec` 返回影响行数。这类语句**不校验** `version`，但有 `version` 的表会自动 `version = version + 1`，让批量改动之前加载的对象在自己的 `Update(...)` 时拿到 `ErrOptimisticLockConflict`；显式赋值版本列是构建错误。`updated_at` / `deleted_at` 不自动处理。只能引用目标表本身，不支持 JOIN、别名、`LIMIT`、`RETURNING`；子查询条件可以用。三个方言都经集成测试真跑验证。

## [4.8.0] - 2026-08-28

### 新增

- **`dialect.MaxBindParams(d)`**: 报告一个方言单条语句能绑多少参数，供使用者自己给批量操作定尺寸。
- **查询构建器补上 `OrderBy` / `Limit` / `Offset`**: 此前构建器**根本没有**这三个阶段——排序的唯一入口是 `Page()` 里基于字符串的 `PageRequest.OrderBy`，`List()` / `Get()` 无法排序也无法限量，而导出的 `OrderBy` 类型和 `Column.Asc()` / `Desc()` 是一组**零消费方的死 API**。随发布的 `skills/tsq` 却一直把这三个阶段写在文档里，照着抄的使用者编译不过。现在它们从任何一个完整阶段都可达，只有 `ForUpdate()` / `ForShare()` 能跟在后面（与 SQL 的子句顺序一致）。`Offset` 必须配 `Limit`（裸 OFFSET 在 MySQL 和 SQLite 上是语法错误，`Build()` 直接拒绝）；`Count()` 忽略这三个子句；构建器级分页与 `query.Page(...)` 冲突时返回错误而不是拼出两个 ORDER BY。
- **`RuntimeOptions.SchemaOwner`**: 给 `SchemaPolicyManaged` 的表托管记账划定归属范围，空值等价于 `"default"`。**同一个数据库上有多个 runtime 托管表时必须设置。**

### 变更

- **生成的 `Insert` 不再覆盖调用方已设置的 `created_at` / `updated_at`**: 此前无条件盖成 `now()`，导入历史数据或回填时调用方设的时间会被静默丢弃——一个调用方控制不了的 `created_at` 算不上 `created_at`。现在只在字段是零值（`IsZero()` / `== nil` / `!Valid`）时才盖。`Update` 仍然无条件刷新 `updated_at`，那正是它的语义。
- **schema 策略为 `Manual` 时改用 info 级日志**: `Manual` 是默认值、也是推荐的生产用法（schema 交给迁移工具），此前每次启动都为此打两条 WARN。

### 修复

- **关键字搜索的通配符转义在 SQLite 上完全失效**: `PageRequest.Keyword` 一直会被转义，但渲染出来的谓词是裸 `LIKE ?`，而 SQLite 没有默认的 LIKE 转义字符——于是转义字符变成普通字符，搜 `a_b` 在 SQLite 上返回**零行**（MySQL / PostgreSQL 因为默认转义字符是反斜杠而侥幸正确）。现在谓词带显式 `ESCAPE '~'`，转义字符从反斜杠改成 `~`（MySQL 根本拼不出 `ESCAPE '\'`，反斜杠会转义掉字符串字面量的收尾引号）。副作用：关键字里的反斜杠现在在三个方言上都是普通字符，此前在 MySQL / PostgreSQL 上会被当成转义前缀。
- **分块用的参数上限对 SQLite 是错的，UPDATE 的估算还少了一半**: 上限此前是写死的 65535，注释称它是"支持的数据库里最紧的"——不是：**SQLite 是 32766**，于是 33 列以上的表按默认 `ChunkSize` 批量插入会被 SQLite 直接拒绝（`too many SQL variables`），而 SQLite 恰好是单元测试唯一跑的数据库。另外每行参数数按"每列一个"估算，只对 INSERT 成立：批量 UPDATE 渲染成 `col = CASE pk WHEN ? THEN ? ... END`，**每列每行绑两个**，宽表的 UPDATE 即使在正确的上限下也会超。现在上限按方言查表（`dialect.MaxBindParams`，方言未知时取最紧的那个），每行参数数按操作分别估算。
- **`SchemaPolicyManaged` 会删掉别的 runtime 的表和数据**: TSQ 把自己托管的表记在 `_tsq_managed_tables` 里，而这张记账表是**全库共享**的，每个 runtime 启动时用自己那份表集**整个覆盖**它。于是两个服务共用一个库时：A 启动记下 `{a1,a2}`；B 启动看到 `{a1,a2}` 不在自己的声明里，**把它们连数据一起 DROP**，再把记账改成 `{b1,b2}`；A 重启又反过来删掉 B 的。两边来回摧毁对方的表。现在记账按 owner 分区：只删自己 owner 记下、且自己不再声明的表，覆盖也只覆盖自己那一段。旧版本写下的无 owner 记账会在启动时就地迁移，原有的行归到 `default` owner（单 runtime 部署的行为因此完全不变）。删表前会先打一条 WARN 说明要删哪张表。
- **`ChunkedInsert{IgnoreErrors: true}` 在 PostgreSQL 事务里必然失败**: 实现是"逐条插入、抓到重复键就跳过"，但 **PostgreSQL 在任何语句失败的那一刻就把整个事务置为 aborted**，其后所有语句一律以 `25P02` 被拒，直到事务结束。于是在 `WithTx(...)` 里用 `IgnoreErrors` 时，第一条被忽略的重复键会毒掉整批，调用最终返回的还是一个**不是重复键错误**的错误。SQLite 和 MySQL 事务不会因此失效，所以只跑这两者的测试看不见。现在事务内的每一行都用 savepoint 括起来（三个方言接受同样的 `SAVEPOINT` / `RELEASE SAVEPOINT` / `ROLLBACK TO SAVEPOINT`）；事务外不发 savepoint，因为每条插入本来就是自己的隐式事务，而 PostgreSQL 会用 `25P01` 拒绝事务外的 `SAVEPOINT`。
- **`wrapExecutor` 里一段不可达的分支让 runtime 附加失效**: 同一个条件被写了两遍，第二遍在方言匹配时直接返回未包装的执行器，于是"给没有 runtime 的执行器附上 runtime"那条路永远走不到。当前调用方都不受影响（`WithTx` 传的是 `*sql.Tx`，它不是 `dialectProvider`），但代码在骗读者。

## [4.7.0] - 2026-08-26

### 新增

- **`RuntimeOptions.LogSQL`**: 打开后，每条渲染出来的 SQL 及其绑定参数会以 debug 级进 `RuntimeOptions.Logger`。此前读路径里的 SQL 日志挂在一个未导出的 context key 上，唯一能设置它的 tracer 也未导出——那些日志语句在发布出去的库里**永远不会执行**，源码里却看着像个能用的特性。参数是原样打的，含敏感数据时不要开。
- **`tsq.MaxPageNumber`**: `PageRequest.Page` 的上限（1000000）。
- **`PageRequest.NormalizeWithLimit` / `ValidateWithLimit`**: 带显式上限的分页校验，让 HTTP handler 能和 `RuntimeOptions.MaxPageSize` 量同一把尺子。无参版本仍按绝对上限 `DefaultMaxPageSize` 判断。
- **`dialect.AllCapabilities()`**: 返回全部能力位，供使用者在构建查询前探测方言支持情况。

### 修复

- **执行期日志绕过 `RuntimeOptions.Logger`**: `query_load.go` / `query_scalar.go` / `query_validation.go` 的诊断日志直接调 `slog.*`，配了 `Logger` 的使用者只收得到批量插入的两条告警。现在统一走 `logForExecutor`。
- **`Runtime.QueryRowContext` 在错误路径泄漏 `*sql.DB` 和 goroutine**: runtime 为 nil 或未初始化时，每次调用都 `sql.OpenDB` 一个新池且从不 `Close`，而 `sql.OpenDB` 会起一个只有 `Close` 能停的 goroutine。现在改用一个进程内共享的错误池。
- **`PageRequest.Offset()` 对越界页码静默返回第一页**: `Page` 超过内部上限时返回 0，也就是**第一页的数据**，而 `Validate()` 不检查 `Page` 上界。现在 `Validate()` 拒绝越界页码，`Offset()` 夹到最后一页而不是回到第一页。
- **`ChunkedInsert` / `ChunkedUpdate` / `ChunkedDelete` 按行数切块会超出参数上限**: 一条批量语句大约每行每列绑定一个占位符，而 PostgreSQL 每条语句最多 65535 个参数，宽表的 1000 行一批会被数据库直接拒绝。现在 `ChunkSize` 是上界，宽表自动切得更小（永不低于每条一行）。
- **无符号主键在驱动返回负数 id 时回绕**: `LastInsertId()` 是 `int64`，负值转成 `uint64` 会变成一个巨大的主键。现在负值不回填，字段留在零值表示"未知"。

### 变更

- **删除未导出且不可达的 tracer**: `printCost` / `printError` / `printSQLTracer` 和 `printSQL` context key 全部删除，SQL 日志改由 `RuntimeOptions.LogSQL` 提供。这些符号不在对外 API 面上，使用者代码不受影响。
- **方言能力位改成显式声明表**: 三个方言各持一张 `map[Capability]bool`，每个能力都要写明 true/false。此前是带 `default: return false` 的 switch，新增能力位漏掉某个方言时既不编译失败也不测试失败，只会静默变成"不支持"。新增的 `TestDialectsCoverAllCapabilities` 现在会拦住这种情况。
- **`unused` linter 开启**: 随之删除九处死代码（`OrderBy.err` 字段、`querySpec` 的六个计数方法、`columnImpl.rawCondition`、一个测试 helper）。
- **`gosec` 成为真正的门禁**: CI 里的 `gosec` 一直带着 `-no-fail`，对任何输入都报成功。现在 SARIF 上传那一趟保留 `-no-fail`，另加一趟会失败的检查（排除 G304 和 G201，理由写在 workflow 里）。`gosec` 与 `govulncheck` 的版本从 `@latest` 固定到具体 tag。
- **`docs/` 瘦成索引**: `docs/concepts.md` 和 `docs/quickstart.md` 曾把 `skills/tsq/references/` 里的内容用中文重写了一遍，两份讲同一件事的文档必然漂移。现在它们只做索引，实质内容以 `skills/tsq/` 为唯一来源。
- **文档语言规则按读者重划**: `AGENTS.md` 此前要求 README 和 `docs/` 用英文，而它们一直是中文，没有任何门禁发现过。现在规则是"Go 注释 / Go doc / `skills/tsq` 用英文，其余面向本项目读者的文档用中文"，并由 `make doc-check` 守着英文那一侧。

## [4.6.0] - 2026-08-26

### 新增

- **`NewRuntimeContext`**: `NewRuntime` 的带 `context.Context` 版本。启动阶段会 ping 数据库并按 `TablePolicy` / `IndexPolicy` 执行 DDL（含 SQLite 整表重建），此前跑在 `context.Background()` 上无法超时或取消。`NewRuntime` 等价于传 `context.Background()`。
- **`Runtime.Close()`**: 关闭 `NewRuntime` 打开的连接池；nil 安全。此前只能 `rt.DB().Close()`，且文档没有提。
- **`RuntimeOptions.MaxPageSize`**: 每个运行时可配置的分页上限，默认 `DefaultMaxPageSize`（1000，与此前硬编码值一致）。不带运行时的 `PageRequest.Normalize()` 仍用默认值。
- **`IdentifierValidationMode` 类型化**: `RuntimeOptions.IdentifierValidationMode` 从 `string` 改为 `tsq.IdentifierValidationMode`，常量 `IdentifierValidationStrict` / `Warn` / `Skip`。现有的 `"skip"` 字面量仍能编译；未知值现在被 `NewRuntime` 拒绝。
- **MySQL / PostgreSQL 集成测试**: `integration_test.go` 在设置 `TSQ_MYSQL_DSN` / `TSQ_POSTGRES_DSN` 时对真实服务器运行 schema 托管幂等性、reconcile 收敛、乐观锁、重复键忽略、锁冲突分类和能力位执行；CI 新增 `Integration` job（MySQL 8.0 + PostgreSQL 16）。此前 `dialect/mysql.go` 与 `dialect/postgres.go` 没有任何自动化覆盖。

### 变更

- **方言能力位按当前版本基线表态**: SQLite 现在声明支持 `FULL OUTER JOIN`（SQLite ≥ 3.39，内置的 modernc 驱动满足）；MySQL 现在声明支持 CTE、`INTERSECT`、`EXCEPT`（基线 MySQL 8.0，`INTERSECT`/`EXCEPT` 需 8.0.31+）。TSQ 不探测服务器版本：仍在使用已 EOL 的 MySQL 5.7 的项目，这些查询会在执行时收到数据库报错，而不再是 `ErrUnsupportedCapability`。
- **根包不再硬依赖 `lib/pq` 和 `jackc/pgconn`**: PostgreSQL 错误改用驱动共有的 `SQLState()` 接口识别。使用者的 `go.sum` 会少掉这两个模块及其间接依赖。
- **代码注释与生成器文案统一为英文**: 根包和 `internal/` 的中文注释（含 `ChunkedOptions` 等导出类型的 Go doc）全部改为英文；`make doc-check` 现在守着这条。
- **`tsq version` 不再打印无信息量的 `branch`**: 值为 `HEAD`（tag 触发的分离头检出）或 `unknown` 时省略该行；`--json` 输出保留字段。

### 修复

- **PostgreSQL 上 `Insert` 不回填自增主键**: `LastInsertIdReturningSuffix` 在方言里一直存在，但 `insertBatch` 从未使用它；PostgreSQL 驱动不支持 `LastInsertId()`，于是 `Insert` / `ChunkedInsert` 之后结构体的主键始终是 0（MySQL / SQLite 不受影响）。现在省略主键的插入在返回 RETURNING 后缀的方言上走 `INSERT ... RETURNING <pk>` 并按插入顺序回填。由新的集成测试在真实 PostgreSQL 上发现。
- **pgx v5 的错误识别失效**: `IsTxConflictError` 和 `ChunkedInsert{IgnoreErrors}` 的重复键检测此前只匹配 `github.com/jackc/pgconn`（pgx v4）的错误类型，`jackc/pgx/v5` 返回的是另一个包里的 `PgError`，导致驱动名为 `pgx` 的运行时上这两条路径静默不命中。
- **`IdentifierValidationMode` 默认值静默吞掉违规**: 空值既不是 `strict` 也不是 `warn`，超长标识符被收集后直接丢弃，既不报错也不告警；文档却写着默认 strict。现在空值就是 strict。
- **commit 阶段的明确冲突码现在会重试**: 此前 `WithTx` 对 commit 阶段的任何错误都不重试，而 PostgreSQL 的 `40001` 序列化失败经常在 COMMIT 时才抛（事务已确定回滚，重试安全）。网络类等不确定错误在 commit 阶段仍不重试。
- **执行期日志绕过了 `RuntimeOptions.Logger`**: 批量插入 ID 回填跳过的警告和 chunked insert 忽略重复键的调试日志此前直接写 `slog.Default()`，现在路由到运行时配置的 `Logger`（执行器不属于任何运行时时仍回退到 `slog.Default()`）。
- **Docker 镜像的构建元数据**: `Dockerfile` 的 `-X` 打在不存在的包路径上（与 v4.4.3 修复的 `.goreleaser.yaml` 是同一个 bug 的另一个副本），镜像里 `tsq version` 报告 `unknown`。改为 `internal/buildinfo` 并补上 `-trimpath`；`make release-check` 现在核对三份构建配置里的每个 `-X` 目标。
- **文档引用了不存在的 API**: README 的 `tsq.PageReq`、`tsq.EscapeKeywordSearch`（转义是自动的，没有公开函数）和三处 `tsq.Into`（实际是 `MapInto`）。`make doc-check` 现在把使用者文档里的 `tsq.*` 符号对照 API 快照。

## [4.5.0] - 2026-08-21

### 新增

- **`tsq version` 输出完整构建信息**: 除版本号和构建时间外，还报告 commit、分支、Go 版本和平台；新增 `--short`（只打印版本号，便于脚本取用）和 `--json`（同样字段的 JSON 输出）。此前注入的 commit 与分支没有任何命令会显示。
- **Go 1.27 泛型方法补全**: 新增 `Query.Scalar`、`Query.AsSubquery`、`Runtime.WithTxResult` 和 `PageRequest.Response`，让标量查询、已构建子查询、带返回值事务和分页响应构造都归到拥有状态的 receiver 上；`QueryInt` / `QueryFloat` / `QueryString`、包级 `AsSubquery` / `NewPageResponse`，以及按返回值数量命名的 `WithTx1` / `WithTx2` 保留为弃用兼容包装。

### 变更

- **CLI 描述与输出统一为英文**: `tsq version` 的命令描述和输出标签此前是中文，与 `tsq fmt` / `tsq gen` 不一致；现已对齐。

### 修复

- **Go 1.27 闭包身份兼容**: tracer 配置不再用函数代码指针去重。Go 1.27 允许不同闭包共享代码地址，旧实现会把捕获不同状态的 tracer 误判成同一个并静默丢弃。

## [4.4.3] - 2026-08-21

### 修复

- **发布产物的版本信息**: `.goreleaser.yaml` 的 ldflags 打在了不存在的包路径上，而 Go 链接器对找不到的 `-X` 符号是静默忽略的，因此此前所有 GitHub Release 二进制的构建时间、commit 和分支都是 `unknown`。改为注入 `internal/buildinfo`，并用 `{{ .Tag }}` 保留版本号的前导 `v`。
- **发布产物的构建路径**: 为发布构建补上 `-trimpath`（GoReleaser 不默认添加），不再把 CI 机器的绝对路径嵌进二进制。

## [4.4.2] - 2026-08-21

### 变更

- **智能体 harness**: 新增 `script/` 下的确定性门禁与 `make harness` 汇总目标，覆盖技能同步、项目内存、生成物同步、对外 API 快照、版本一致性和提交信息。
- **开发者技能**: 新增 `.agents/skills/tsq-dev`，承载开发本仓所需的架构、代码地图、代码生成管线、变更影响清单、发版流程和项目内存；`skills/tsq` 继续只服务 TSQ 的使用者。
- **自动发版**: 新增 `make release`，按"定版本号 → 写 CHANGELOG → 重新生成示例 → harness → 提交 → 打 tag → 推送"的顺序发布，并拒绝自动跨主版本。

## [4.4.1] - 2026-08-21

### 修复

- **CI 示例刷新**: coverage job 改用仓库已有的 `make examples` 目标，修复发布流水线因调用不存在的 `make update-examples` 而失败。

## [4.4.0] - 2026-08-21

### 变更

- **Go 1.27**: 最低 Go 版本、CI 和 Docker 构建环境统一升级到 Go 1.27.0，并将 golangci-lint 升级到兼容版本。
- **泛型事务方法**: 新增 `Runtime.WithTx1` / `Runtime.WithTx2` 泛型方法；原包级函数保留为弃用入口。
- **标准库现代化**: 使用 Go 1.27 的 `strings.CutLast`、`slices.Backward` 和 `errors.AsType` 简化实现。

## [4.3.0] - 2026-07-14

### 变更

- **生成文件命名惯例**: 生成文件后缀从 `_tsq.go` / `_result_tsq.go` 改为 `.tsq.go` / `.result.tsq.go`，与 Go 生态复合扩展名惯例（如 `.pb.go`）保持一致。视觉分隔更清晰，IDE 识别更友好。
  - `user_tsq.go` → `user.tsq.go`
  - `userorder_result_tsq.go` → `userorder.result.tsq.go`
  - `runtime_tsq.go` → `runtime.tsq.go`

### 迁移指南

- 删除旧的 `*_tsq.go` / `*_result_tsq.go` 文件，重新运行 `tsq gen` 即可。
- 更新 `.gitignore`、Makefile 或 CI 中引用旧后缀的 glob 模式。

## [4.2.0] - 2026-06-10

### 修复 (Critical)
- **PostgreSQL 自增主键默认值被破坏**: schema reconcile 曾把 BIGSERIAL/SERIAL 主键的 `nextval('..._seq')` 默认值当作漂移并执行 `DROP DEFAULT`，导致重启后插入报 `null value in column "id"`。现在 diff 层（`columnsEqual`）与渲染层（`DDLAlterColumnStatements`）都会忽略自增列的数据库托管默认值。
- **MySQL 主键列 ALTER 必然失败**: `MODIFY COLUMN` 不再重复输出 `PRIMARY KEY`（MySQL error 1068 "Multiple primary key defined"），同时保留 `AUTO_INCREMENT` 与 `NOT NULL`；主键升位（如 INT→BIGINT）现在可以正常执行。
- **SQLite 自增检测大小写错误**: CREATE SQL 匹配因引号/大小写问题永远失败，导致 Reconcile 策略下每次重启都重建所有自增表。现在大小写不敏感，并兼容 `"x"`、`[x]`、反引号与裸标识符等手写 DDL 引用风格。
- **SQLite 表重建事务穿透连接池**: 重建语句改为在单个事务（同一连接）内执行，不再以独立 Exec 发送 `BEGIN`/`COMMIT`（可能落到不同池化连接，留下悬挂事务）。
- **SQLite 表重建丢失二级索引**: 重建会随 `DROP TABLE` 丢弃旧表索引；现在无论索引策略如何，都会在重建事务内恢复仍然适用的二级索引。

### 修复 (噪音/重复 DDL)
- **类型往返不闭合导致永久重复 DDL（类修）**: inspect 会把部分 SQL 类型坍缩为 canonical kind（PG `TEXT`/`CHAR(n)`/`NUMERIC(n,m)`、MySQL `TEXT`/`DECIMAL(n,m)`、SQLite `TEXT` 等），与 `db:"...,type:X"` 声明对账时每次重启都触发 ALTER/重建且永不收敛。`DDLColumnSpec` 新增 `NativeType` 字段记录数据库原始类型，新增 `DDLColumnTypesEquivalent` 做「渲染类型 + 原生类型别名归一」双轨比较。
- **PostgreSQL TEXT 列往返**: inspect 的 `text` 列按 `RawType: "TEXT"` 往返，不再渲染为 `VARCHAR(255)` 触发重复 ALTER。
- **PostgreSQL 仅 nullability 漂移时多发 ALTER TYPE**: 类型比较改用解析后的等价比较，仅 NULL/NOT NULL 漂移不再附带一条会锁表重写的 `ALTER ... TYPE`。

### 新增
- **PostgreSQL identity 列识别**: `GENERATED ... AS IDENTITY` 列（`is_identity = 'YES'`）现在与 SERIAL 一样识别为自增，不再误报 "manual change required"。
- **Reconcile 删列警告日志**: Reconcile/Managed 策略执行 `DROP COLUMN` 前输出 WARN 日志，明确提示该操作会删除列数据。

### 行为变化
- **TEXT 列漂移不再被掩盖（PostgreSQL）**: 数据库列为 `TEXT` 而 Go 端声明为普通 `string`（即 `VARCHAR(255)`）时，Reconcile 现在会如实执行 `ALTER ... TYPE VARCHAR(255)`；若存量数据超长，PostgreSQL 会报错使启动失败（不会截断数据）。如希望保持 TEXT，请为字段添加 `db:"...,type:TEXT"`。

## [4.1.18] - 2026-05-27

### 变更 (Breaking Changes)
- **Runtime 构造签名调整**: `NewRuntime` 现在改为 `NewRuntime(driverName, dsn, tables, ...options)`，运行时会自行打开数据库并按 driver 解析 dialect。
- **Schema 管理策略重命名**: 运行时 schema 管理改为 `SchemaPolicy`，通过 `RuntimeOptions.TablePolicy` / `RuntimeOptions.IndexPolicy` 控制；旧 `IndexInit*` 名称仅保留为兼容别名。

### 新增
- **表管理策略**: 新增表级 `Manual / Validate / CreateMissing / Reconcile / Managed` 五档策略。
- **索引托管增强**: 索引支持缺失创建、定义不一致重建，以及仅在 TSQ 托管范围内清理未声明索引。
- **运行时 schema 元数据**: 生成的 `TSQTables()` 现在同时携带列 schema 与索引声明，供 runtime 直接做校验和 DDL 协调。
- **DDL 日志**: runtime 启动期间会明确记录 manual 提醒和实际执行的 DDL 语句。

### 改进
- **Dialect 检查能力增强**: SQLite / MySQL / PostgreSQL 新增表、列、索引检查能力，支持 runtime schema 对账。
- **示例与文档同步**: README、quickstart、concepts、skill references、academy 示例全部更新到新 runtime API 和 schema policy 模型。
- **测试覆盖扩展**: 新增表创建、表 reconcile、managed 索引清理、仅删除 TSQ 托管表等回归测试。

## [4.1.16] - 2026-05-25

### 改进
- **清理未使用代码**: 删除 `toTSQDDLColumnType` 和 `quoteDialectIdentifier` 未使用函数
- **修复测试**: 修复 `query_chunked_test.go` 中指针语法 `new(int64(1))
- **优化事务选项**: 优化 `normalizeTxOptions` 中选项复制方式
- **SQLite 错误检测**: 改用 `errors.Is` 检测 SQLite 错误码
- **重新生成代码**: 所有生成文件更新到 v4.1.15 版本标记

## [4.1.15] - 2026-05-25

### 变更 (Breaking Changes)
- **谓词 API 重构**: `EQ/NE/GT/GTE/LT/LTE` 现在接受 `RHS[T]` 类型，普通 Go 值需要使用 `EQVal/NEVal/GTVal/GTEVal/LTVal/LTEVal`
- **列比较 API 变更**: `EQCol/NECol/GTCol/GTECol/LTCol/LTECol` 现在直接使用 `EQ/NE/GT/GTE/LT/LTE`
- **子查询 API 引入**: 新增 `Subquery[T]` 类型和 `BuildSubquery/AsSubquery` 构造函数

### 新增
- **类型安全的子查询**: `BuildSubquery/AsSubquery` 构建类型安全的子查询，可用于 `RHS[T]` 位置
- **RHS 接口**: `RHS[T]` 统一标量谓词的右操作数类型，支持列/表达式/子查询
- **子查询文件**: 新增 `subquery.go` 和 `rhs.go`

### 改进
- **API 一致性**: 字符串匹配谓词同时提供 `ContainsVal/HasPrefixVal/HasSuffixVal` 和 `Contains/HasPrefix/HasSuffix`
- **文档全面更新**: README、迁移指南、concepts、skill 等全部同步到新 API
- **测试覆盖**: 新增大量测试覆盖新的子查询和 RHS 接口
- **重新生成代码**: 所有生成文件更新到 v4.1.14 版本标记

### 迁移示例
```go
// 旧代码
Where(User_ID.EQ(1))
Where(User_OrgID.EQCol(Org_ID))

// 新代码
Where(User_ID.EQVal(1))
Where(User_OrgID.EQ(Org_ID))
```

## [4.1.14] - 2026-05-25

### 变更 (Breaking Changes)
- **移除 MatchByInputOrder**: 从公开 API 中删除，现在作为生成代码的内部函数 `matchByInputOrder`
- **ChunkedDeleteByIDs 重构**: 改为 `ChunkedDeleteByPKs`，使用泛型接受 `TypedColumn` 字段而不是字符串表名/列名

### 改进
- **类型安全提升**: `ChunkedDeleteByPKs` 现在在编译期验证主键字段类型和表归属
- **生成代码优化**: `runtime_tsq.go` 现在同时包含 `compactJSON()` 和 `matchByInputOrder()` 内部辅助函数
- **示例更新**: academy 示例同步到新 API
- **重新生成代码**: 所有生成文件更新到 v4.1.13 版本标记

## [4.1.13] - 2026-05-25

### 改进
- **移除 traceManager 中间层**: 直接把 `tracers []Tracer` 放在 `Runtime` 结构体中
- **收紧公开 API**: 隐藏 `CompactJSON`、`Trace1`、`TraceFn`、`Runtime.Trace()` 等内部实现细节
- **模板更新**: 生成的代码现在使用内部的 `compactJSON()` 而不是公开 API
- **简化测试**: 删除 `trace_test.go`，相关测试已整合
- **重新生成代码**: 所有生成文件更新到 v4.1.12 版本标记

## [4.1.12] - 2026-05-22

### 改进
- **移除中间 engine 层**: 删除 `engine.go`，把 `db`、`dialect`、`indexInitMode` 直接内联到 `Runtime` 结构体
- **简化索引初始化**: `upsertIndex()` 和相关函数不再接受 `*engine`，改为直接接受 `*sql.DB`、`Dialect`、`IndexInitMode`
- **代码整洁度提升**: 减少间接访问层级，`runtime.db` 和 `runtime.dialect` 直接可用
- **示例全部更新**: academy 示例的 `OpenSQLiteExampleDB()` 现在返回 `*Runtime`
- **重新生成代码**: 所有生成文件更新到 v4.1.11 版本标记

## [4.1.11] - 2026-05-22

### 变更 (Breaking Changes)
- **Runtime 初始化重构**: 移除全局 `Init()` / `DefaultRuntime()` / `CurrentEngine()` 等 API，改为显式 `NewRuntime(db, dialect, tables)` 构造
- **表注册入口**: `tsq gen` 现在会生成 `runtime_tsq.go`，提供 `TSQTables()` 函数返回当前包所有表的 metadata 切片
- **执行函数签名**: `List()` / `Get()` / `GetOrErr()` / `Page()` / `Load()` / `Insert()` / `Update()` / `Delete()` / `Chunked*` 等现在直接接受 `*Runtime` 或 `SQLExecutor`，不再需要显式 engine

### 新增
- **Runtime 直接构造**: `NewRuntime(db, dialect, tables, ...options)` 一步初始化，返回可用的 `*Runtime`
- **Runtime 实现 SQLExecutor**: `*Runtime` 现在直接实现 `SQLExecutor`，可以直接用于查询执行
- **Runtime 方法**: `Runtime.DB()`、`Runtime.SQLDialect()`、`Runtime.WithTx()`、`Runtime.ValidateIdentifiersForDialect()` 等
- **TableRegistration 类型**: 用于把表 metadata 传递给 `NewRuntime`
- **事务重试增强**: 新增 `IsRetryableNetworkError()`、`IsOptimisticLockError()` 等辅助函数
- **事务重试配置**: `DefaultTxRetryConfig()`、`TxRetryConfig`、`TxRetryPredicate` 等

### 改进
- **生成代码优化**: `tsq gen` 生成的 `runtime_tsq.go` 集中管理当前包所有表的注册
- **文档全面更新**: README、quickstart、concepts、skill references 全部同步到新 API
- **最佳实践更新**: 事务部分补充新的使用模式和示例
- **测试覆盖扩展**: executor_test.go 大幅扩展，覆盖新 Runtime API 的各种场景

### 迁移指南
从 `Init()` + `DefaultRuntime()` 迁移：
```go
// 旧 API
if err := tsq.Init(db, dialect.SQLiteDialect{}); err != nil { ... }
runtime := tsq.DefaultRuntime()

// 新 API
runtime, err := tsq.NewRuntime(db, dialect.SQLiteDialect{}, database.TSQTables())
if err != nil { ... }
```

## [4.1.10] - 2026-05-22

### 新增
- **Agent Skill 支持**: 新增 `skills/tsq/` 目录，可作为 GitHub Copilot、Claude Code、Gemini CLI 的 agent skill 安装使用
- **Skill 文档**: 新增 `docs/skill.md`，说明如何将 TSQ 作为 agent skill 安装到其他项目

### 改进
- **README 完善**: 在 README 中增加 skill 安装说明，扩展文档导航链接

## [4.1.9] - 2026-05-21

### 新增
- **事务支持**: 新增 `tx.go`，提供完整的事务抽象和带重试的事务执行函数
- **事务 API**: `WithTx()`、`WithTxReadOnly()`、`WithTxRetry()`、`Commit()`、`Rollback()` 等
- **最佳实践文档**: 大幅扩展 `BEST_PRACTICES.md`，补充事务使用指南和示例

### 改进
- **Go 语法现代化**: 全面使用 Go 1.26 简洁语法（如 `for range slice` 替代 `for _, _ = range slice`）
- **执行器增强**: `executor_test.go` 大幅扩展，覆盖事务边界条件和并发场景
- **Runtime 完善**: 增加事务支持的内部链路和类型约束

### 修复
- **查询校验**: 修复查询构建阶段的部分边界条件校验问题

## [4.1.3] - 2026-05-20

### 变更 (Breaking Changes)
- **DSL 关键字重命名**: 将 `@TABLE` 和 `@RESULT` 注解中的 `kw` 关键字重命名为更具语义的 `search`。为了保持向后兼容，解析器目前仍会尝试将 `kw` 模糊匹配到 `search` 并给出提示，但建议尽快迁移。

### 改进
- **内部状态机重构**: 将 `builderPhaseKwSearch` 重构为 `builderPhaseSearch`，统一了内部搜索链路的命名规范。
- **文档与示例同步**: 全面更新了 `README.md`、`docs/` 以及所有示例代码中的注解示例，统一使用 `search` 关键字。
- **错误提示增强**: 优化了 DSL 解析错误提示，当用户输入旧的 `kw` 关键字时，解析器会智能提示建议使用 `search`。

## [4.1.2] - 2026-05-20

### 改进
- **内部封装收紧**: 进一步将多个公开类型、常量和函数转为内部可见（如 `QueryBuilder` -> `queryBuilder`、`MaxTracers` -> `maxTracers` 等），减少了 API 暴露面，使包语义更清晰。
- **条件表达统一**: 规范了 `Condition` 接口在内部链路中的使用，移除了部分冗余的 `Predicate` 包装。
- **模板与助手优化**: 更新了代码生成模板中的 `MatchByInputOrder` 使用方式，并同步优化了文档说明。
- **文档完善**: 更新了 `README.md` 和 `MIGRATION_GUIDE.md`，使其与当前的 Build-based 泛型链路描述保持一致。

## [4.1.1] - 2026-05-20

### 改进
- **API 接口收紧**: 将 `CaseBuilder` 重构为 `CaseStage` 接口，并隐藏了多个内部 Builder 实现，进一步提升了 API 的封装性和类型安全性。
- **查询规划优化**: 优化了 `query_plan` 和 `querybuilder` 内部的逻辑流转，通过引入 `queryBuilderCore` 减少了冗余的状态传递。
- **内部元数据管理**: 将 `MaxTracers` 等常量收紧为包私有，并规范了内部变量的命名规范。
- **测试覆盖率提升**: 针对 `CASE` 表达式、CTE 规划和多级 Join 场景补齐了大量边界测试。

## [4.1.0] - 2026-05-20

### 变更 (Breaking Changes)
- **核心架构重构**: 大规模重构了查询规划、列实现和执行引擎。将原本分散的逻辑解耦并按功能模块化，提升了系统的可维护性和扩展性。
- **移除弃用代码**: 清理了旧版的 `typed.go`、`alias.go`、`query_spec.go` 和 `time_helpers.go` 等冗余或已弃用的文件。

### 新增
- **查询规划器 (Query Planner)**: 引入了全新的查询规划机制 (`query_plan.go`)，支持更复杂的查询拓扑和优化。
- **增强的执行器**: 新增 `executor_mutation.go` 和 `executor_wrap.go`，统一并优化了写操作和查询执行流程。
- **模块化列实现**: 采用新的 `column_impl.go` 管理列元数据和扫描映射，支持更灵活的列重绑定和别名。

### 改进
- **测试套件全面升级**: 针对查询规划、Builder 状态机和 Mutation 流程新增了大量测试用例，显著提升了核心逻辑的测试覆盖率。
- **验证逻辑收紧**: 强化了标识符、查询参数和表注册的校验规则。

## [4.0.6] - 2026-05-19

### 变更 (Breaking Changes)
- **Init API 重构**: `tsq.Init` 和 `runtime.Init` 的签名改为 `Init(db *sql.DB, dialect Dialect, options ...*InitOptions) error`。不再接收 `*Engine` 作为首参，改由 `CurrentEngine()` 或 `DefaultEngine()` 获取初始化后的引擎。
- **Dialect 接口解耦**: `Dialect` 接口的方法（如 `EnsureIndex`, `InspectIndexDefinition`）不再依赖具体的 `*Engine` 类型，改为依赖 `SQLExecutor` 接口和 `context.Context`，进一步降低了组件间的耦合。

### 新增
- 新增 `tsq.CurrentEngine()` 用于获取默认运行时的 `*Engine`。
- 新增 `AGENTS.md` 规范项目级 AI Agent 协作准则。

### 改进
- **方言代码拆分**: 将原本臃肿的 `dialect.go` 拆分为 `dialect_sqlite.go`、`dialect_mysql.go` 和 `dialect_postgres.go`，提升了可维护性。
- **SQLite 稳定性修复**: 修复了 SQLite 在索引初始化时由于嵌套查询导致结果集意外关闭的 bug。
- **文档增强**: 在 `README.md` 中添加了 `pkg.go.dev` 文档徽章。

## [4.0.2] - 2026-05-19

### 变更（Breaking Changes）
- `Dialect` 升级为完整闭环合约：方言名称、标识符校验、SQL 能力校验、批量插入 ID 语义、索引管理、DDL 列类型/索引/ALTER COLUMN 规则都必须由 `Dialect` 自身提供
- 移除 `detectDialectName`、`KeywordRegistry`、`DialectExtension`、`DialectValidator` 这类接口外的方言推断/注册表路径，不再保留兼容层
- `ValidateIdentifierForDialect` / `ValidateIdentifierLength` 改为直接接收 `Dialect`，`Runtime.CurrentDialect()` 改为返回 `DialectName`
- 内置能力矩阵收紧为 **SQLite / MySQL / PostgreSQL** 三个实际实现，不再保留仓库内未实现的 Oracle / SQL Server 半支持声明

### 新增
- 增加自动 optimistic locking 与行级锁读取支持

### 改进
- `Runtime`、SQL 执行前能力校验、索引初始化、批量插入主键回填、DDL 快照/增量渲染现在统一走 `Dialect` 合约，消除了字符串和 type switch 旁路
- DDL 生成与 schema diff 现在复用 `Dialect` 的列类型、索引 SQL、ALTER COLUMN 策略，新增方言时不再需要在 `cmd/` 下重复散落分支
- 错误处理切换到 Go 标准库错误语义，并修正 QueryBuilder 状态相关问题
- 示例、文档与生成代码更新为 Academy 示例集和新的运行路径

## [4.0.1] - 2026-05-19

### 修复
- 将 v4 发布线对齐到 Go major version module 约定，模块路径改为 `github.com/tmoeish/tsq/v4`
- 同步更新安装命令、示例导入路径、GoReleaser ldflags 与 golangci module-path，避免发布产物和下游导入不一致
- 将 CI 的 Go 版本提升到 `1.25.0`，与 `go.mod` 保持一致

## [4.0.0] - 2026-05-07

### 变更（Breaking Changes）
- `Col[T]` 升级为 `Col[Owner, T]`，生成列会把所属 owner Struct 类型写入类型参数
- 新增 `Owner` / `Result` 语义，查询结果 owner 不再被迫实现 `tsq.Table`
- `NewCol[Owner, T]` 不再接收显式 table 参数，列所属表由满足 `tsq.Table` 的 `Owner` 类型推导
- `NewCol[Owner, T]` 的 field pointer 收紧为 `func(*Owner) *T`，生成列的扫描目标类型在编译期校验
- `Column` 升级为 `Column[Owner, T]` 泛型接口，异构运行期列集合改用显式的 `AnyColumn`
- `Select` / `QueryBuilder` / `QuerySpec` / `Query` 升级为 owner 泛型 API：`Select[Owner](...)` 只能投影同一 Table 或 Result owner 的字段
- Result projection 现在通过包级 `Into(source, func(*Owner) *T, "json")` 建立，扫描目标 owner 在编译期校验
- `Into(...)` 现在返回只用于投影的 `ResultCol[Owner, T]`，Result 字段不再暴露 `EQVar` / `EQCol` 等条件方法
- 查询现在必须显式调用 `From(table)`，不再从 `Select(...)` 或 Join 链隐式推导主表
- `Join` / `LeftJoin` / `RightJoin` / `FullJoin` 改为直接接收可变 `Condition`，移除旧的 `.Join(...).On(left, right)` 两步 API
- 非 `CROSS JOIN` 必须提供 ON 条件；Join 条件必须同时引用已引入表和当前连接表，提前拒绝缺失连接关系或引用未来表的查询
- `SQLExecutor` 收紧为 `QueryContext` / `QueryRowContext` / `ExecContext` 这组 `database/sql` 共有方法；生成 CRUD 与表级写 helper 也统一接收 `SQLExecutor`
- `Engine` 的执行方法改为显式 `ctx context.Context` 首参，对应的 `tsq.Insert/Update/Delete/Chunked*` helper 也只接受 `Table` mutation target
- `Table` 接口补齐列/主键元数据，表 scan 与 mutation 优先走 field pointer + metadata，不再依赖整结构反射

### 新增
- 增加 `TableColumn[Owner]` / `SearchColumn`，分别约束物理表列与可参与关键词搜索的列

### 改进
- 生成的表 DSL 增加 `Cols()`，让 Build 阶段可以校验列确实属于 `From` / Join 图中的表
- `Result` 生成代码保持为投影描述符；联表是否合法统一由普通 `QueryBuilder` 在 `Build()` 时校验
- 生成列现在使用 `NewCol[TableStruct, FieldType]`，列 owner 信息可被后续 typed query API 使用
- `List` / `Get` / `GetOrErr` / `Page` / `Load` 的扫描目标构建链路改为泛型路径，查询返回类型直接从 `*Query[Owner]` 推导
- `QueryBuilder.Build()` 现在完整保留 owner 类型，子查询比较会按用途校验列数：标量比较与 `IN` 子查询必须单列，`EXISTS` 子查询只要求已构建
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
- `Init` 现在会把索引初始化模式和 schema 事件处理器持久绑定到 `Engine`
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
- 增加 `Engine.Insert/Update/Delete` 的真实批量写入能力，支持多行 `VALUES`、批量 `CASE ... WHEN` 更新和 `IN (...)` 删除
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
- 支持 `Init` 进行更灵活的初始化配置
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
