# 项目内存 — 代码生成与示例

判据与索引在 `../memory.md`。

## 决定：注解是 `//tsq:` 指令行，不是写在注释里的 DSL (2026-09-16，v5)

旧的 `@TABLE(...)` 是写在 doc comment 里的括号 DSL，**gofmt 会重排它**，于是生成器长出一个
`tsq fmt` 命令把注解排回解析器要的样子，外加"先 fmt 再 gen"的规则、670 行格式化器、以及一套把
字节偏移映射回行号的定位器。**没有一件是在解决使用者的问题**，它们都在解决"我们把结构化数据放进
了 gofmt 管辖的地方"这个自找的问题。指令行 gofmt 不碰，这些就全没了。**判断一个辅助工具是不是必要，先问它在解决谁的问题。**

**v5 是全新版本，不做后向兼容也不提供迁移路径**（维护者 2026-09-16 定的）：旧 DSL 解析器、`tsq migrate`、
`ddl.json` 旧状态文件、`--tpl` 自定义模板、`MIGRATION_GUIDE.md` 一律删除。**不要为了"方便升级"把
任何一样加回来**——兼容层在这个仓库里只会积累，v4 就是这么攒出九个 `Deprecated` 的。

语义等价的验证办法是**生成物逐字节相同**：示例全部换成指令后重新生成 diff 为空，这比逐条比对解析结果更强,它覆盖了全部下游推导（索引名、查询名、DDL）。

## 决定：列是表结构体的字段，不是包级变量 (2026-09-19，v5)

`Course_ID` / `Course__Cols` 每表往包里撒十几个带下划线的名字，别名要逐列 `WithTable`，还靠"句柄 → 列
→ Define"三步声明加一个按文件名排序的示例门禁（`academyqueries.go`，已删）守初始化顺序。现在是内嵌
`*TableOf` 的 `CourseTable`：取列必经 `TableCourse`，初始化顺序由 Go 保证；`As` 一次改绑整套列。
**代价**：列字段不能和 `TableOf` 的方法重名，`tsq gen` 报错（`reserved.go`），所以 `Table.Name()` 改成了
`TableName()`，**以后给 `TableOf` 加导出方法都会让某个列名非法**，加之前想清楚。

## 文档承诺的类型，要有一个真的用它的示例 (2026-09-19，2026-09-22，2026-09-28)

`sql.Null[T]`、跨包 result 字段、同名包、`Ctx` 字段、`[N]byte`、`DeviceBinding` 的接收者 `db`……**全是 academy 恰好
没有的形状**，每一处都生成过编译不过的代码。**新的字段形状进 `TestGeneratedCodeCompilesForEveryFieldShape` 的矩阵，
不进示例**；生成器该拒绝的声明进 `TestGenRefusesWhatItCannotGenerate`。**`SameDefault` 曾把 `''` 和无默认值当一回事**（2026-10-08 四代迁移回放）：去掉 `default:''` 不写 `DROP DEFAULT`，下次改类型 PG 才炸；三个引擎都报得出 NULL 对 `''`。**编译门不是运行门**（2026-10-08 随机字段形态 × 真跑）：
`[N]byte` 在矩阵里编译通过，驱动却不收数组；字段形状还要在 `internal/integration` 用手写表真跑三引擎（`TestIntegrationByteArraysAreBoundAndReadAsBytes`）。

## 按字符串批量取数要以数据库的判等为准 (2026-09-19)

`FetchXxxByTitle` 逐字节比对返回的行，MySQL `_ci` 下 `'go'` 匹配 `'Go'` 却报 `sql.ErrNoRows`；现在对不上的**字符串**再单行问一次。**别对整数也这么做**（第一版对 70000 个缺失整数键发了 70000 条查询）。

## 决定：CLI 用标准库 `flag`，不用 cobra (2026-09-19)

三个子命令用不上 cobra；`internal/cmd/command.go` 保留测试依赖的 `SetArgs` / `Execute` / `Help`，每次运行重新声明
flag（cobra 时代包级单例的 `Changed` 位跨测试残留过），并支持参数后的 flag（`tsq gen ./pkg --check`）。

## 生成的 `.sql` 文件头记的是 schema 的出身，别去"修"它 (2026-08-28)

`tsq.json` 的 `version` 比 CLI 新就拒绝（2026-10-08 第十五轮，`refuseNewerStateFile`）：旧 CLI 改写后新 CLI 把丢掉的渲染当变化、再写一段迁移，两边来回。开发构建没有版本号不比较。

`tsq.json` 保存首次建 schema 时的原始 `.sql`，后续变更以带日期的迁移段追加，文件头因此停在首次生成的版本；"修"成当前版本会让每次发版都重写三个 DDL 文件头。
**决定（维护者 2026-10-07，否掉了上一条的后半截）：拼法变化也补段**——`academy` 的初始段在 UTC 默认值和范围约束之后建出的表被运行时判为不匹配，而 `gen --check` 只比模型看不见。`tsq.json` 记每方言每列的渲染（`renderings`），模型没变而渲染变了就写一段 `respell column`（`ddlRespellings`：拿旧渲染冒充被检查的列去调 `AlterColumnSQL`，SQLite 默认值 / 约束变了重建）。第一次只记录。否掉"文件里再放一份当前 schema"：它改了"整个文件建新库"的语义。
已知未处理（2026-10-09）：声明里主键在生成 / `assigned` 之间翻转，迁移段只写 "manual change required for primary key column" 注释，而运行期 `Reconcile` 三方言都会做（MySQL `MODIFY ... AUTO_INCREMENT`、PG 身份列 + `setval`、SQLite 重建）。声明翻转罕见、注释已指明，等有人要再把 `AlterColumnSQL` 的那两条接进 `renderDDLAlterColumnStatements`。
`type:` 覆盖曾跳过一切类型检查（2026-10-09 第二十轮）：`any` 是标识符不是 `interface{}` 语法，过了解析器的"不是列类型"，加 `type:` 就生成、运行期三引擎各读回各的。`uncarriableFieldType` 在 `type:` 分支补上：接口、无 codec 的结构体一律拒。

## 决定：迁移段按"执行者不会停下"来写 (2026-09-28)

`sqlite3` 命令行遇错不停：复制失败后照样删旧表、提交，整表数据清空（审计 P0）。**否掉"调换语句顺序就够了"**——
顺序救不了不会停的执行者，只能让语句本身不会失败。删表删列一律注释掉交给人（维护者定案），因为改名、改 `db` 标签、
把 `//tsq:table` 写成 `// tsq:table` 在生成器眼里都和"删掉"一样。同理（2026-09-29 维护者定案）：改成 NOT NULL 的列在三个方言上
都先把 NULL 填成默认值或零值并注明，否掉"只警告"；PG 同类改类型不写 `USING`，宁可失败也不截断。同名重建成唯一索引是先 `DROP` 后 `CREATE`（2026-10-08）：
运行时用临时名先建再删，文件里做不到（SQLite 没有改名索引、人不一定在事务里跑），于是在 `DROP` 之前写一行点名要查重复的列。改名字段、改名表同理：删加同形状的一对时段首给出注释掉的 `RENAME COLUMN` / `RENAME TO` 加索引改名（`renamedColumnHint` / `renamedTableHint`）；运行时 `Reconcile` 只警告（`renamedColumnPair`），不猜。

## 生成器不能带 `git describe` 的版本号，否则发版是死锁 (2026-08-21)

带 `-X version=$(git describe)` 生成，写对文件头得先打 tag，打 tag 又得先过 `release-check`。所以 `make build-gen`
**故意不带 `$(LDFLAGS)`** 编 `bin/tsq-gen`（报 `internal/buildinfo` 字面量）；`bin/tsq` 给人用，两个二进制别合并。

## 决定：只为主键和唯一索引生成查询 (2026-09-17)

普通索引和前缀的查询要排序、限量，生成器猜不到，照抄就是全表读取。唯一索引生成 `GetByX` / `FindByX` / `FetchByX`
（与主键的 `Get` / `Find` / `Fetch` 对称，2026-09-28）；全文索引生成 `FullTextX()` **方法**——字段在 `As` 之后仍指向原表。
软删除表的 `WithDeleted()` 返回不带 `GetByX` 的 `XxxTableWithDeleted`（2026-09-29）：唯一值只在活行里唯一，
原来那几个方法编译得过、运行必败。生成的列按声明顺序（2026-09-28）；DDL 列序不跟着改，它已写进使用者的迁移历史。SQLite 只在类型亲和性变化时重建（2026-09-29，运行时
`Reconcile` 同理）：重建会丢触发器和手建索引，为它不检查的 `VARCHAR` 长度付这个代价不值。MySQL 行宽上限有两条（2026-10-09）：行格式的 65535
（`VARCHAR` 按最长算，4 字节/字符 + 2）和 InnoDB 每页 8126 的行内上限（超过 40 字节的变长列按 40 + 1 算，实测 195 个 `VARCHAR(20)` 可建、200 个不行），`mysqlRowProblem` 两条都估。

## 决定：`generated` 不带表达式保留，但 TSQ 不替它建表 (2026-09-28)

它表示"库计算、schema 归迁移"（触发器、方言相关的表达式），审计发现它被写成普通 NOT NULL 列，`CreateMissing`
建出的表每次 `Insert` 都失败。**否掉"一律要求写表达式"**：那会让迁移管 schema 的使用者没法声明这种列。改成
TSQ 写的 DDL 留出它（带注释），运行期要建含它的表就报错。`//tsq:search` 写在结果上则直接拒绝，文档改掉——结果没有生成查询，"支持"只能是一个空承诺。

**只对一个方言成立的限制只警告**（2026-09-28）：MySQL 索引键长先做成了 `tsq gen` 报错，等于替只跑 PostgreSQL / SQLite
的使用者拒绝了一个合法索引；改成警告加 `mysql.sql` 注释。
result 字段类型用 go/types 比对（2026-09-28，旧检查拒绝 LEFT JOIN 一侧的 `sql.Null[T]`）；点名多个结构体的错误先排序（解析顺序不稳定，CI 红过）。

## 决定：推导的索引名按列名拼，不按字段名 (2026-09-29)

字段 `SKU` 曾得到 `ux_products_s_k_u`，列名和字段名不同的字段在索引名里根本不出现。索引建在列上，名字跟列走；
改推导是 schema 层面的破坏性变更，所以赶在 v5 发版前改，**发版后别再动它**。

## 决定：示例是按章的教程，每章打印自己的 SQL 并断言输出 (2026-09-29)

旧的三个程序共用 academy 的场景函数、只输出 JSON，维护者读不懂；academy 退为集成夹具。`internal/show` 把
`WithSQLLogging` 打成 `SQL>` 行，`main_test.go` 断言关键输出——示例坏了在 `go test` 里红。`make examples` 不再删
`.sql`：`shop` 用 `//go:embed` 读它们，删了包就加载不了（`tsq.json` 反正会重新渲染）。
