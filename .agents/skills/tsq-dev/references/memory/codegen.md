# 项目内存 — 代码生成与示例

判据与索引在 `../memory.md`。

## 决定：注解是 `//tsq:` 指令行，不是写在注释里的 DSL (2026-09-16，v5)

旧的 `@TABLE(...)` 是写在 doc comment 里的括号 DSL，**gofmt 会重排它**，于是生成器长出一个
`tsq fmt` 命令把注解排回解析器要的样子，外加"先 fmt 再 gen"的规则、670 行格式化器、以及一套把
字节偏移映射回行号的定位器。**没有一件是在解决使用者的问题**，它们都在解决"我们把结构化数据放进
了 gofmt 管辖的地方"这个自找的问题。指令行 gofmt 不碰，这些就全没了。**判断一个辅助工具是不是
必要，先问它在解决谁的问题。**

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
不进示例**；生成器该拒绝的声明进 `TestGenRefusesWhatItCannotGenerate`。

## 按字符串批量取数要以数据库的判等为准 (2026-09-19)

生成的 `FetchXxxByTitle` 逐字节比对返回的行，MySQL 默认 `_ci` 排序规则下 `'go'` 匹配到 `'Go'`，却报
`sql.ErrNoRows`。现在 Go 里对不上的**字符串**再单行问一次，遇到第一个真不存在的就停——**别对整数也这么做**：
第一版对 70000 个缺失整数键发了 70000 条查询。

## 版本号有四个副本，生成物那份最容易忘 (2026-08-21)

**改版本号必须重新生成示例**（文件头和 `tsq.json`），`release.py` 依赖这一点；`TSQFileSuffix` 还被
`changeset.py` 和 `check_release.py` 认着。

## 决定：CLI 用标准库 `flag`，不用 cobra (2026-09-19)

三个子命令用不上 cobra；`internal/cmd/command.go` 保留测试依赖的 `SetArgs` / `Execute` / `Help`，每次运行重新声明
flag（cobra 时代包级单例的 `Changed` 位跨测试残留过），并支持参数后的 flag（`tsq gen ./pkg --check`）。

## 生成的 `.sql` 文件头记的是 schema 的出身，别去"修"它 (2026-08-28)

`tsq.json` 保存首次建 schema 时的原始 `.sql`，后续变更以带日期的迁移段追加，所以文件头停在首次生成的版本。"修"成
当前版本会让每次发版都重写三个 DDL 文件头。示例的 DDL 状态在 2026-09-28 重置过一次（历史里有 SQLite 不接受的段）。
同理，方言改了某种类型的**拼写**（2026-09-28 MySQL 时间 `DATETIME` → `DATETIME(6)`）不是快照差异，迁移历史里不会多一段；
用迁移文件的项目要自己加宽，`CHANGELOG` 写明。

## 决定：迁移段按"执行者不会停下"来写 (2026-09-28)

`sqlite3` 命令行遇错不停：复制失败后照样删旧表、提交，整表数据清空（审计 P0）。**否掉"调换语句顺序就够了"**——
顺序救不了不会停的执行者，只能让复制本身不会失败。删表删列一律注释掉交给人（维护者定案），因为改名、改 `db` 标签、
把 `//tsq:table` 写成 `// tsq:table` 在生成器眼里都和"删掉"一样。

## 生成器不能带 `git describe` 的版本号，否则发版是死锁 (2026-08-21)

用带 `-X version=$(git describe)` 的 `bin/tsq` 生成，文件头记的是 git 描述的版本：想写对头部得先打
tag，想打 tag 得先过 `release-check`。所以 `make build-gen` **故意不带 `$(LDFLAGS)`** 编 `bin/tsq-gen`
（报告 `internal/buildinfo` 字面量）；`bin/tsq` 是给人用的 CLI，两个二进制的分工不要合并。

## 决定：只为主键和唯一索引生成查询 (2026-09-17)

普通索引和前缀的查询要排序、限量，生成器猜不到，照抄就是全表读取。

## 决定：`generated` 不带表达式保留，但 TSQ 不替它建表 (2026-09-28)

它表示"库计算、schema 归迁移"（触发器、方言相关的表达式），审计发现它被写成普通 NOT NULL 列，`CreateMissing`
建出的表每次 `Insert` 都失败。**否掉"一律要求写表达式"**：那会让迁移管 schema 的使用者没法声明这种列。改成
TSQ 写的 DDL 留出它（带注释），运行期要建含它的表就报错。`//tsq:search` 写在结果上则直接拒绝，文档改掉——结果
没有生成查询，"支持"只能是一个空承诺。

**只对一个方言成立的限制只警告**（2026-09-28）：MySQL 索引键长先做成了 `tsq gen` 报错，等于替只跑 PostgreSQL / SQLite
的使用者拒绝了一个合法索引；改成警告加 `mysql.sql` 注释。
result 字段类型用 go/types 比对（2026-09-28）：LEFT JOIN 一侧要把 NOT NULL 列读进 `sql.Null[T]`，按名字比的旧检查拒绝了。
