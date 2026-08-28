# 项目内存

这个文件被提交，所以每台机器、每个 agent 共享同一份记忆。**和它描述的代码在同一波里
更新它。**

只写仓库本身讲不出来的东西：事故及其根本原因、决定及其背后的推理、不明显的运行时行为、
值得不再重复的死胡同。不要复述规则（`AGENTS.md` 拥有规则）、包布局（`architecture.md`
拥有）或使用者契约（`skills/tsq` 拥有）——重复的陈述注定漂移。

每条都标绝对日期。变错的条目**就地更正或删除**，不要追加自相矛盾的内容——过时的内存比
没有内存更糟，因为人会相信它。

## 一条记录什么时候该走

这个文件被每一个 agent 加载进上下文，所以它的长度是**每次会话都要付的成本**。只增不减
必然把它变成一份没人读得完的流水账。但"处理完就删"也是错的：有些记录的价值恰恰在问题
消失之后才开始——它解释了代码为什么长这样。

**判据只有一条：删掉它之后，有人会不会重犯、或者重新调查一遍？**

| 类型 | 例子 | 事情了结之后 |
| --- | --- | --- |
| **搁置项**（`已知未处理：` 开头） | "`branch` 显示 `HEAD`，暂不处理" | **删掉。** 问题没了，记录就没有读者了 |
| **事故 + 根因** | "`-X` 打错包路径是静默失败的" | **留**。有门禁挡着了就压成一行指向那道门 |
| **决定 + 理由** | "`Build()` 只校验结构，方言能力留到执行期" | **永久留**。删了下一个人会把它改回去 |
| **死胡同** | "试过 X，不行，因为 Y" | **永久留**。删了下一个人会重试一遍 |

不要新增"XX 问题已处理"这样的条目——那是把一条记录换成两条，成本翻倍而信息量没变。
处理完搁置项就把那条**删掉**，如果解决过程本身教会了什么，那是一条**新的**事故或决定记录，
按它自己的价值决定写不写。

**删除不是销毁。** 这个文件被提交，任何被删的条目连同日期和删除它的那次提交都留在 git 里，
`git log -p --follow -- agents/skills/tsq-dev/references/memory.md` 一条命令就能翻出来。
所以删除的代价不是"永远失去"，而是"不再默认出现在上下文里"——这让"该删就删"安全得多。
反过来说，agent 不会习惯性去翻 git log，所以**可能还有人需要的，压缩保留，不要删**。

**压缩优于删除的那一档**：一条十几行的事故记录，在有了门禁之后可以塌缩成一行——
"X 曾经发生过，现在由 `make Y` 守着"。指针留下，叙事丢掉。

搁置项一律用 `已知未处理：` 开头，这样 `grep 已知未处理` 就能列出全部待办；顺手处理掉
一条，顺手把它删掉。

`make memory-check` 除了要求每波带记录，还守着这个文件的行数上限。撞上限不是让你随便删，
是提醒你按上面这张表裁剪一遍。

---

## 2026-08-28 — 全库共享的记账表被每个 runtime 整个覆盖，于是它们互删对方的表

`SchemaPolicyManaged` 靠 `_tsq_managed_tables` 记住"我托管过哪些表"，不在当前声明里的就
DROP。但那张记账表是**全库共享**的，而每个 runtime 用**自己那份表集整个覆盖**它。两个服务
共用一个库时来回摧毁对方的表和数据：A 记下 `{a1,a2}` → B 看到它们不在自己的声明里全删掉、
记账改成 `{b1,b2}` → A 重启再删掉 B 的。

**判据**：一份**全局**状态被一个只知道**局部**真相的写入者整个覆盖时，覆盖就是数据丢失。
问题不在 DROP 那段逻辑（它按自己的记账是对的），在记账的**范围**和写入者的范围不一致。

修法是给记账加 owner 维度（`RuntimeOptions.SchemaOwner`），读、删、写都按 owner 划界。
旧的无 owner 记账在启动时就地迁移：它是 TSQ 自己的记账、不含用户数据，所以整表重建而不是
`ADD COLUMN`——老表在 `table_name` 上有主键，加列留着它会让两个 owner 无法记录同名表。
原有行归到 `default` owner，单 runtime 部署行为不变。

**没做**：把 DROP 和记账更新包进一个事务。PG 和 SQLite 支持事务 DDL，**MySQL 每条 DDL 都
隐式提交**，包起来只在三分之二的方言上成立，反而更容易让人误以为它是原子的。
## 2026-08-28 — 生成的 `.sql` 文件头停在旧版本是**有意的**，别去"修"它

`examples/academy/{mysql,postgres,sqlite}.sql` 的头写着 `tsq-v4.1.19`，而 `.tsq.go` 是当前
版本。看起来像"改版本号忘了重新生成"，**不是**：`tsq.json` 保存着首次建 schema 时的原始
`.sql` 内容，聚合文件由它重建、后续变更以带日期的迁移段追加。**文件头记的是这份 schema 的
出身，不是最近一次生成的版本**，所以它就该停在那儿，`gen-check` 也因此是绿的。

本轮审计一度把它当成缺陷。留下这条是为了下一个人别再查一遍、更别"修"成当前版本——那会让
每次发版都重写三个 DDL 文件的头，把真正的 schema 变更淹掉。

## 2026-08-28 — 本轮决定不做的几件事

- **`Exists()` 走 `SELECT COUNT(1)`**（大表全扫），**`Get()` 不加 `LIMIT 1`**（多行匹配时
  返回哪一行不确定）。两者都要包一层 `listSQL`，而它现在可能自带 ORDER BY / LIMIT / 行锁，
  正确写法要连这些一起处理。值得单独一波，别顺手改。
- **没有 `NewRuntimeFromDB` 和连接池选项**：接了 otelsql / 自定义 connector 的人只能用
  `WrapExecutor`，随之失去 `LogSQL`、tracer 和 `MaxPageSize`。真实缺口，属于新特性。
- **`detectSQLCapabilities` 靠字符串匹配**渲染好的 SQL：标识符 base64 编码避开了大部分误判，
  但 `Expr` / `Pred` 的字面量含 ` EXCEPT ` / ` FOR UPDATE` 会误报。正解是从 `querySpec` 导出。
- **CLI 不拆子模块**：`x/tools` 和 `gofumpt` 只被 `internal/` 用却进了使用者的 `go.sum`。
  拆分是破坏性变更，留给 v5。

## 2026-08-28 — "抓住错误继续跑"在 PostgreSQL 的事务里不成立

`ChunkedInsert{IgnoreErrors}` 逐条插入、抓到重复键就 `continue`。这在 SQLite 和 MySQL 上
是对的，在 **PostgreSQL 上必然失败**：PG 在任何语句失败的那一刻就把事务置为 aborted，其后
所有语句以 `25P02` 被拒。第一条被忽略的重复键毒掉整批，而调用方拿到的错误**不是重复键
错误**，看不出源头。

**判据**：错误处理策略的可移植性取决于"失败之后连接还剩下什么状态"，三个数据库答案不一样。
凡是"捕获错误后继续用同一个连接"的代码，都要问一句 PG 上还能不能用。

修法是事务内每行用 savepoint 括起来。选 savepoint 而不是 `ON CONFLICT DO NOTHING` /
`INSERT IGNORE`：后者一条语句搞定且快得多，但 **MySQL 的 `INSERT IGNORE` 会把所有错误降级
成警告**（比"忽略重复键"宽得多），而批量 `DO NOTHING` 配 `RETURNING` 无法按位置把生成的
主键映射回原行，会悄悄破坏主键回填。savepoint 保住原有语义，代价是每行三条额外语句。

事务外**不发** savepoint：PG 用 `25P01` 拒绝事务外的 `SAVEPOINT`，而那时每条插入本来就是
自己的隐式事务。判断走哪条路要**穿过 `wrappedExecutor` 找 `*sql.Tx`**——`WithTx` 交给回调的
是包装过的执行器，只看最外层那个值会对最需要 savepoint 的情形答"不在事务里"。
## 2026-08-28 — 文档描述了一个不存在的阶段，而两侧的门都看不见它

`skills/tsq` 从很早就写着 `OrderBy(...)` / `Limit(...)` / `Offset(...)` 是查询阶段，还给了
可复制的示例。**构建器上从来没有过这三个方法**：`List()` / `Get()` 无法排序，排序的唯一入口
是 `Page()` 里基于字符串的 `PageRequest.OrderBy`；而导出的 `OrderBy` 类型加 `Asc()` / `Desc()`
是一组**零消费方的死 API**（`unused` linter 看不见导出符号）。

两侧的门各差一步，缺口正好在中间：`api-check` 只比对符号快照，`OrderBy` 类型确实在快照里；
`doc-check` 只把文档里的 `tsq.X` 对照快照，而 `.OrderBy(` 是方法调用，不匹配那个模式。
**"文档提到的每个符号都存在"不等于"文档描述的每个用法都成立"**，方法调用是这两者之间的盲区。

修的是实现而不是文档：一个 SQL 构建器不能排序是功能缺失，不是文档错误。顺带救活了那组死 API。

`Page()` 与构建器级分页的冲突选择**报错而不是覆盖**：`Page` 是追加自己的子句，不是替换，
两个 ORDER BY 拼出来的 SQL 在任何方言上都不合法；而"猜调用方想要哪个"比说不清更糟。

## 2026-08-28 — "最紧的那个上限"是个断言，不是常识，要去量

分块的参数上限写死 65535，注释称它是"支持的数据库里最紧的"。**不是**：SQLite 的
`SQLITE_MAX_VARIABLE_NUMBER` 是 **32766**（3.32 起的默认值，实测 modernc 就是这个数），
于是 33 列以上的表按默认 `ChunkSize` 会被 SQLite 拒绝——**而 SQLite 是单元测试唯一跑的库**。
一个错误的常数配一句自信的注释，比没有注释更难被怀疑。

同一波抓到第二个：每行参数数按 `len(Cols())` 估算，那只对 INSERT 成立。批量 UPDATE 渲染成
`col = CASE pk WHEN ? THEN ? ... END`，**每列每行绑两个**，再加 WHERE 的 1~2 个。v4.7.0 那条
"修复分块超参数上限"只修了一半，另一半在同一个函数里躺着。**修一类 bug 时要把这一类的所有
实例都数一遍**——这里的"一类"是"把行数换算成参数数"，换算的两个因子都可能错。

上限现在按方言查表（`dialect.MaxBindParams`），方言未知时取最紧的那个：**分块偏小只多几次
往返，偏大是执行期直接失败**，不对称的代价决定了默认值往哪边倒。
## 2026-08-28 — 转义值和声明转义符是同一件事的两半，只做前一半是静默错误

`escapeKeywordSearch` 一直在转义 `%` / `_`，但渲染出来的谓词是裸 `LIKE ?`。**SQLite 没有
默认的 LIKE 转义字符**，转义前缀于是变成普通字符，搜 `a_b` 在 SQLite 上返回零行；
MySQL / PostgreSQL 默认转义字符恰好是反斜杠才侥幸正确——而单元测试只跑 SQLite，两侧的
端到端关键字用例都是零，所以谁都没发现。**固定进 SQL 文本的子句要三个方言都验。**

转义字符选 `~` 不选反斜杠，两个理由都是硬约束：**MySQL 根本拼不出 `ESCAPE '\'`**
（反斜杠会转义掉字符串字面量的收尾引号，要写 `ESCAPE '\\'`），而三方言写法必须一致，
因为这个子句是 `Build()` 期就固定进 SQL 文本的，那时还不知道会在哪个方言上执行。

**引申**：任何"我们对值做了预处理"的功能，都要问一句"数据库怎么知道我们做了预处理"。
只有值被改了而契约没被声明时，行为由各方言的默认值决定，而默认值本来就是不一样的。

已知未处理：`StartsWithVal` / `ContainsVal` / `EndsWithVal` 及其 `Var` 形式仍然直接把
调用方的字符串拼进 pattern，值里的 `%` / `_` 是活的通配符。这是**有意的**（文档写明由调用方
转义），但和 `Keyword` 的行为不一致，容易踩。要改就是破坏性语义变更，得配一对
`*Literal` 系列或一个开关，值得单独一波做。

## 2026-08-26 — 第三次了：只被自己的测试撑着的代码，在库里是不存在的

一次审计同时抓到三处同一形状的东西：未导出的 `printSQL` context key 加三个 tracer（读路径
八处 `ctx.Value` 在发布出去的库里**永远为假**）；`dialect_validation.go` 里
`canonicalCapabilityName` 的逐行副本加一个零调用入口；以及 `export_compat_test.go` 自己在
`_test.go` 里定义了 `AddTracer` / `Trace1` 和一个包级 `Runtime` 单例——`AGENTS.md` 明令禁止
的东西，而守着它的 `api-check` 只看 `api-surface.txt`，`_test.go` 的导出符号不进快照。
**规则的门在哪，绕过它的路就在哪。**

两条教训，都比"删掉了"值钱：

1. **`unused` linter 看不见这类东西**：`_test.go` 里的引用算使用。它现在开着（顺手清了
   九处真死代码），但它挡不住这一类。判据只能是**排除 `_test.go` 之后 grep 调用方**，
   和 2026-08-26 那条 `LastInsertIdReturningSuffix` 用的是同一招。
2. **一个只被自己的测试引用的符号，测试证明的是它自洽，不是它可达。** 绿色的测试在这里
   是伪装，不是保障。

`change-impact.md` 新增了"在执行路径上加了一个日志或诊断出口"和"新增了一个开关 + 若干
消费点的特性"两条，各带一条可执行的 grep。

## 2026-08-26 — 能力位的 `default` 分支是那道门自己的漏洞

规则写着"新增能力位三个方言都要显式表态"，但三个 `SupportsCapability` 都是 `switch` 加
`default: return false`——漏掉一个方言不编译失败、不 lint 失败、不测试失败，只静默变成
"不支持"。现在是每方言一张表加一个遍历表的测试。

**引申，对所有"必须穷尽"的 switch 都成立**：`default` 分支把"忘了写"和"决定不支持"变成
同一件事，而这两件事需要不同的处理。要穷尽性就别给它兜底分支——用表加一个遍历表的测试。

## 2026-08-26 — 写在 AGENTS.md 里但没有门的规则，几个月都是假的

`AGENTS.md` 要求 "README、`docs/`、`skills/tsq` 用英文"，而实测只有 `skills/tsq` 是对的——
这条规则从写下那天起就没成立过，`doc-check` 当时扫不到 Markdown 的语言。

选择是改规则而不是翻译九百行：分界线按**读者**划才站得住，然后给英文那一侧加了门。
**留下的是判据：写规则的时候就问"谁来发现它被违反了"。** 答不出来的规则不要写进
`AGENTS.md`，写进去只会让下一个读到它的人相信一件假事。

## 2026-08-26 — 同一个 SQLSTATE 在三个驱动里是三个 Go 类型

曾 `errors.AsType[*pgconn.PgError]` 匹配 pgx **v4** 的包。pgx v5 的 `PgError` 是另一个包里的
另一个类型，匹配静默失败，于是 driver 为 `"pgx"` 的运行时上重试和 `IgnoreErrors` 全都不生效
且无任何报错——单元测试的 fixture 恰好也是 v4，所以一直绿。

修法是匹配接口 `interface{ SQLState() string }`（pq / pgx v4 / pgx v5 都实现）。
**驱动错误分类永远按接口，不按具体类型**；`integration_test.go` 用真实 pgx v5 守着。

## 2026-08-26 — 字符串模式的空值落在所有分支之外

`IdentifierValidationMode string` 的默认值 `""` 既不是 `strict` 也不是 `warn`，违规被收集
后**直接丢弃**，而注释写着"strict 是默认"。stringly-typed 的开关每个分支都写 `== "x"`，
空值永远是那个没人写的第四分支。现在是类型化枚举，空值显式映射到 Strict，未知值被拒绝。

## 2026-08-26 — 决定：方言能力位按版本基线表态，否决"版本可配置"

能力位曾按 2018 年前的引擎写死，README 忠实复述了这些错误。考虑过给 `MySQLDialect` 加
`ServerVersion` 字段，**否决**：`Build()` 之前根本不知道会连哪个库，版本只能执行时探测，
那就得每个 `Dialect` 值带状态，和"方言是无状态值类型"冲突。改成按基线表态：MySQL 8.0、
SQLite ≥3.39。代价是更老的引擎拿到数据库报错而不是 `ErrUnsupportedCapability`。

## 2026-08-26 — 决定：commit 阶段只对明确冲突码重试

原来一刀切 `stage != commit`，理由是 commit 失败有歧义。但 PG 的 `40001` **经常在 COMMIT 时
才抛**，且这些码（40001 / 40P01 / 55P03、MySQL 1205 / 1213）保证事务已回滚——PG 最典型的
重试场景被一刀切排除了。现在 commit 阶段只放行明确冲突码，网络类错误仍不重试。

## 2026-08-26 — 接口里"有定义、有实现、零调用"的钩子

`Dialect.LastInsertIdReturningSuffix` 六个版本零调用，PostgreSQL 上 `Insert` 从来没回填过
主键，而唯一的自动化测试是 SQLite 所以一直绿。现在由 `change-impact.md` 的 grep 和
`integration_test.go` 挡着。

**同一天的第二个教训**：`Integration` job 红着，PR #61 还是被 auto-merge 合进了 `main`——
auto-merge 只等**必需**检查。已提升为必需检查。
auto-merge 只等**必需**检查。第一次跑就抓到真 bug 的门不该是可选的，已提升为必需检查。

## 2026-08-26 — 集成测试为什么长这样，以及暂时不做的几件事

`dialect/mysql.go` 和 `postgres.go` 此前覆盖率 0%。核心断言是"托管 schema 第二次启动零
DDL"——v4.2.0 的每个 Critical 事故都表现为它。用 env DSN + `t.Skip` 而不是 build tag，
是为了让 SQLite 目标始终参与。
是为了让 SQLite 目标始终参与、套件每次 `go test` 都被编译执行。

已知决定不做（写在这里免得下一个人重新调查）：

- **Docker 镜像不推送 registry**。`Docker Build` 是必需检查，但产物没人消费；推送要配
  ghcr 权限和 tag 策略，等有真实使用者再说。
- **`NewRuntime` 不改成 functional options**。`options ...*RuntimeOptions` 别扭，但改签名
  是破坏性变更，留给 v5。
- **不替换 `gopkg.in/nullbio/null.v6` 和 `serenize/snaker`**。前者出现在生成代码里
  （`examples/academy/*.tsq.go` import 它），是使用者契约；后者只在生成器里做
  CamelToSnake，换实现等于改所有使用者的表名推导。

## 2026-08-21 — `release-check` 只能查版本倒退，不能查"没前进"

第一版写的是"代码里的版本必须严格大于最新 tag"，把门装反了：合法状态有两个（发版之间
buildinfo 等于最新 tag；`release.py` 跑 harness 时 buildinfo 领先于 tag，因为 tag 要等
harness 全绿才打），这条规则两个都拦。真正的错误状态只有一个：buildinfo **低于**最新 tag。

## 2026-08-21 — 两份技能必须各住各的目录，别为了少一个符号链接把它们并在一起

曾把 `skills/tsq` 软链进开发者技能目录以省一条符号链接。**这是错的**：整套设计的核心是
"两份技能读者不同、所有权严格分开"，而那个布局先告诉看目录的人"它俩是一伙的"——
**布局是文档的一部分**。`.claude/skills/` 只是发现入口，不是它们的家。

改这类路径用 `git mv` 而不是删了重建（`git log --follow` 才追得到），并 `grep -rn` 一遍。

## 2026-08-21 — 任何在合并前后各跑一次的检查，都要确认两次跑的是同一个输入

`commit-msg` 钩子量的是作者写的主题，`commit-check` 在 `main` 上量的是 GitHub squash
之后追加了 ` (#59)` 的那条——同一条信息在两道门里长度不同，作者过了钩子却让 `main` 变红，
而写提交信息时 PR 号还不存在，**没有任何办法提前避免**。现在 `check_change_log.py` 量
长度前剥掉 ` (#\d+)`。

合并会改写提交信息、SHA 和历史形状三件事，同一天各绊了一次。

## 2026-08-21 — 文档里的 make 目标和 CI 里的是两条独立的真相

CI 调了一个不存在的 `make update-examples`；修 CI 时没人问"这个名字还写在哪"，同一个幽灵
在 `README.md` / `CONTRIBUTING.md` 里又活了三个月——**一个被修两次的问题是一个不彻底的
修复。** 现在围栏块里的 `make X` 由 `doc-check` 守着（散文有意不扫），但它**管不到
`.github/workflows/`**，改目标名那里仍要手动 grep。

## 2026-08-21 — 第一次真跑 PR 发版流程暴露的两件事

- **squash 之后不能 `git pull --ff-only`**：squash 在 origin 上造出**新** commit，本地的
  原始提交不在它的历史里，必然报分叉。正确动作是 `git fetch` + `git reset --hard origin/main`。
- **发版 PR 里只该有发版提交**：曾把三个没推的提交一起卷进发版 PR，squash 之后 `main` 上只剩
  一句 `chore: release`，那三条提交信息从 `git log` 消失。`release.py` 现在检查
  `origin/main..main` 为空，不空就拒绝。
- **别把新分支叠在还没合的 PR 分支上**：上游被 squash 后产生新 SHA，你那份原始提交立刻冲突。
  开新分支前先 `git checkout main && git fetch && git reset --hard origin/main`。

三条是同一件事的三个面：**squash 的粒度是 PR，所以 PR 的粒度就是你能保留的历史粒度，
而任何"基于未合并分支"的东西都会在合并那一刻失效。**

## 2026-08-21 — 把并发写入者的改动误判成了工具的 bug

曾断定"`make fmt` 里的 `go fix` 会把树改到编译不过"并删掉它。**结论是错的，已改回来**：
真相是另一个 claude 进程在同一个工作区里边跑边写文件，`go fix` 打印的编译错误是它**遇到**
的，不是它造成的。在 HEAD 的干净副本里复现不出来。

- **"我改了 A，然后 B 坏了"在有并发写入者时什么都不能证明。** 先确认自己是不是唯一写入者：
  `ps aux | grep claude` 加 `lsof -p <pid> -a -d cwd`。当时几次 `git checkout -- '*.go'`
  丢掉了对方未提交的工作。
- **验证要在副本里做**：`git archive HEAD | tar x` 到临时目录再跑可疑命令。
- `make fmt` 末尾的 `go build ./...` 守卫独立成立：这个目标每一步都在改写源码，格式化绝不该
  交回一棵编不过的树。

## 2026-08-21 — 给 main 和 tag 加了 ruleset，发版随之改成 PR 流程

`main` 禁直推、必须走 PR 且五个检查全绿；`refs/tags/v*` 禁删除/移动/强推。两条都对仓库
所有者生效（`bypass_actors` 为空）。**tag 那条比分支那条重要得多**：单人仓的真实风险不是
"别人推了坏代码"，是删掉或移动一个已发布的 tag——Go Proxy 永久缓存内容哈希，那是唯一
不可恢复的操作。

- **必需检查不能放 matrix job**：`Test` 的检查名是 `Test (ubuntu-latest, 1.27.0)`，升 Go
  版本名字就变，变了的名字永远不出现在 PR 上，必需检查永远等不到，**所有 PR 从此合不进去**。
  选了 `Build` / `Docker Build` / `GoReleaser Check`（都 `needs: [test, lint, coverage]`，
  覆盖等价而名字稳定）。同理不能放只在 tag 上跑的 `Release`。
- **用 `gh pr merge --auto`，不要"等 CI 再合"**：PR 刚建出来的头几秒没有任何 check 注册，
  `gh pr checks --watch` 那一刻会以 "no checks reported" 直接退出。
- **验证服务端规则不能用 `git push --dry-run`**——它不联服务端，看起来永远成功。要真推一次；
  测 tag 规则用不合法 semver 的探针 tag（`v-ruleset-probe`），受 `v*` 规则管但 Go Proxy 忽略。

## 2026-08-21 — 版本号是给使用者的，不是给每一次提交的

v4.4.2 只改了 `Makefile`、`script/` 和 `agents/`——纯 harness 和技能，使用者拿到手里和
v4.4.1 一模一样。它是一个不该存在的版本，发它是个错误。

判据不是"这波重不重要"，是"使用者拿到的东西变了没有"。使用者只拿得到两样：`go get` 的
模块和 `tsq` 二进制。`script/release.py` 的 `user_visible_changes` 现在自己算这件事，
没有可见改动就拒绝，`--allow-maintenance` 显式放行。

一个反直觉的点：`internal/` **算**使用者可见。Go 的 import 规则让使用者引用不到它，但
CLI 的全部行为都在那里面——`internal/parser` 改了解析规则，使用者手写的注解就换了含义。
"internal 就是内部实现，不影响外部"这个直觉在有 CLI 的库上是错的。

`## [未发布]` 段就是为"攒着"存在的：不值得单独发版的改动写进去，等下一次真需要发版时一起
出去。这也是为什么 `release-check` 只查倒退不查前进（见上面那条）。

## 2026-08-21 — cobra 的互斥标志组按 `Changed` 位判定，测试里必须手动清

`VersionCmd` 是包级单例，标志在 `init` 里绑定一次，**状态跨 `Execute()` 存活**，而
`MarkFlagsMutuallyExclusive` 判定用的是每个 pflag 的 `Changed` 位，不是 Go 变量值。
清法：`VersionCmd.Flags().Lookup(name).Changed = false`。`go test -shuffle=on` 是发现这类
用例间耦合的标准手段，`test-race` 已经带着它。

## 2026-08-21 — `-X` 打错包路径是**静默**失败的

Go 链接器找不到 `-X` 指定的符号时不报错，直接忽略，于是 build time / commit / branch
全是 `unknown` 而没有任何提示。三份配置（`Makefile`、`.goreleaser.yaml`、`Dockerfile`）
各有一份副本，都犯过。现在 `make release-check` 核对每个 `-X` 的目标路径，CI 的
`Docker Build` 还会 `docker run` 镜像跑 `version --json` 核对值真的进了二进制——
**静态检查证明路径对，跑产物证明值到了，两者缺一不可。**

## 2026-08-21 — 生成器不能带 `git describe` 的版本号，否则发版是死锁

生成器曾用带 `-X ...version=$(git describe)` 的 `bin/tsq`，于是**生成文件头记的是 git 描述
出来的版本，不是即将发布的版本**。第一次真跑 `make release` 就死锁：想让头部写对得先打 tag，
想打 tag 得先过 `release-check`。

修法是 `make build-gen`：**故意不带 `$(LDFLAGS)`** 地编 `bin/tsq-gen`，它报告
`internal/buildinfo` 的字面量。附带好处是生成结果只依赖源码，不再依赖工作区干不干净。
`bin/tsq`（带 ldflags）仍是给人用的 CLI，两个二进制的分工不要合并。

## 2026-08-21 — 决定：两份技能按所有权拆开，不按篇幅

理由不是篇幅是所有权。同一份文件同时服务两拨读者时，写给使用者的部分会因为开发者觉得
"这个细节太内部"而被删掉，反过来也一样。`skill-check` 的每条触发器都是从"哪类改动会让哪份
文档变假"倒推出来的，`hint` 里写着理由。

## 2026-08-21 — 生成物是否同步不能用 `git diff` 判断

`git diff --exit-code -- examples/academy` 是错的：一波变更本来就可能合法地改动生成物，
这道门会对每一波正当改动都失败。判据只能是"拿当前源码重新渲染一遍看结果一不一样"，
即 `tsq gen --check`（`make gen-check`）。

## 2026-08-21 — 版本号有四个副本，生成物那份最容易忘

版本号传导进生成文件头和 `tsq.json`，所以**改版本号必须重新生成示例**。这不是麻烦，是保险：
它让"tag 指向的代码"和"生成物声称的版本"不可能不一致，`release.py` 依赖这一点。

## 2026-08-21 — `make commit-check` 单独存在时是失效的

有未提交代码时 `commit-check` 跳过；代码一提交 `memory-check` 又跳过。每次 `make harness`
只有一道门是活的，而写提交信息那一刻活着的恰好是另一道。提交信息真正被校验的唯一时机是
`commit-msg` 钩子，`make hooks` 每台机器必须跑一次。

## 2026-08-21 — 发版波必须从内存门禁里豁免

发版波唯一的非生成物改动是 `internal/buildinfo/buildinfo.go`，而发版本身教不了项目任何
东西。豁免写在 `check_change_log.py` 的 `RELEASE_ONLY_FILES`，是**精确白名单而不是开关**：
发版波多碰任何一个别的文件，门就重新活过来。

## 2026-08-21（追溯 v4.3.0） — 改生成文件后缀的真实代价

`_tsq.go` → `.tsq.go` 是对的（对齐 `.pb.go` 惯例），但代价是所有使用者的 `.gitignore`、
Makefile glob 和 CI 都要改。后缀常量的唯一来源是 `TSQFileSuffix`，但认这个格式的还有
`changeset.py` 的 `GENERATED_SUFFIXES` 和 `check_release.py` 的 `GENERATED_HEADER`。
再改一次之前先想清楚值不值。

## 2026-08-21（追溯） — 全局 `Init()` 和 engine 中间层是被删掉的，不要重新引入

历史上有过包级全局 `Init()`、`engine` 中间层和 `traceManager` 层，都被删了，换成显式的
`NewRuntime(...)`。全局单例让"这个查询用的是哪个库"不可回答，测试也没法并行；中间层是纯
转发，只让调用栈多一层。**任何"方便起见加个全局默认 runtime"都是在往回走。**
