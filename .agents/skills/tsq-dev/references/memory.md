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

## 2026-08-26 — 同一个 SQLSTATE 在三个驱动里是三个 Go 类型

`IsRetryableTransactionConflictError` 和重复键检测曾 `errors.AsType[*pgconn.PgError]`，
import 的是 `github.com/jackc/pgconn`——那是 pgx **v4** 的包。pgx v5 的 `PgError` 在
`github.com/jackc/pgx/v5/pgconn`，是另一个类型，匹配静默失败；而 `resolveRuntimeDialect`
明确接受 driver `"pgx"`，使用者按文档用 pgx v5 时重试和 `IgnoreErrors` 全部不生效，没有
任何报错。单元测试用的 fixture 恰好也是 pgconn v1，所以一直绿。

修法是匹配接口 `interface{ SQLState() string }`——pq、pgx v4、pgx v5 都实现——顺带把
`lib/pq` 和 `pgconn` 从根包依赖里去掉了。**驱动错误分类永远按接口，不按具体类型**；
`change-impact.md` 有对应条目，`integration_test.go` 用真实 pgx v5 守着。

## 2026-08-26 — 字符串模式的空值落在所有分支之外

`IdentifierValidationMode string` 的默认值 `""` 既不是 `"strict"`（返回错误）也不是
`"warn"`（返回汇总错误让调用方记日志），违规被收集进切片然后**直接丢弃**。注释写着
"strict 是默认"，测试只用过 `"skip"`。stringly-typed 的开关，每个分支都写 `== "x"`，
空值就永远是那个没人写的第四分支。现在是类型化枚举 + `resolveIdentifierValidationMode`，
空值显式映射到 Strict，未知值被 `NewRuntime` 拒绝。

## 2026-08-26 — 决定：方言能力位按版本基线表态，否决"版本可配置"

SQLite 的 FULL JOIN 位和 MySQL 的 CTE / INTERSECT / EXCEPT 位都是按 2018 年前的引擎写的：
SQLite 3.39（2022）起支持 FULL JOIN，modernc 现在 bundle 3.53；MySQL 8.0 支持 CTE，
8.0.31 支持 INTERSECT/EXCEPT，5.7 于 2023-10 EOL。README 的能力矩阵忠实复述了这些错误。

考虑过给 `MySQLDialect` 加 `ServerVersion` 字段按版本判定，**否决**：API 面变大，而且
`Build()` 之前根本不知道会连哪个库，版本只能在执行时探测——那就得每个 `Dialect` 值带状态，
和"方言是无状态值类型"的现状冲突。改成按基线表态：MySQL 8.0、SQLite ≥3.39。代价是
5.7 用户拿到数据库报错而不是 `ErrUnsupportedCapability`，CHANGELOG 写明。

## 2026-08-26 — 决定：commit 阶段只对明确冲突码重试

原来 `shouldRetryTx` 一刀切 `stage != commit`，理由是 commit 失败有歧义（可能已经提交）。
但 PostgreSQL 的 `40001` 序列化失败**经常在 COMMIT 时才抛**，且这些码（40001 / 40P01 /
55P03、MySQL 1205 / 1213）保证事务已回滚——这是 PG 最典型的重试场景，被一刀切排除了。
现在 commit 阶段只放行 `IsRetryableTransactionConflictError` 为真的错误；`io.EOF`、
`driver.ErrBadConn` 之类在 commit 阶段仍不重试。

## 2026-08-26 — 集成测试第一次跑就抓到：PostgreSQL 上 `Insert` 从来没回填过主键

`Dialect.LastInsertIdReturningSuffix` 从有 PostgreSQL 方言那天起就在接口里，`postgres.go`
也老老实实返回 ` RETURNING "id"`，但 `insertBatch` **从来没调用过它**——它只走
`ExecContext` + `result.LastInsertId()`，而 pgx / lib/pq 的 `LastInsertId()` 返回
"not supported"，`assignBatchInsertIDs` 把这个错误吞掉直接返回。结果：PG 上所有
`Insert` 之后主键都是 0，v4 发了六个版本没人发现，因为唯一的自动化测试是 SQLite。
一个接口里"有定义、有实现、没调用"的钩子，和没有它是一样的——`grep` 一下调用方是检查
方言接口时的必做动作。现在 `insertBatchReturning` 接上了，单元测试用一个返回 RETURNING
后缀的 SQLite 测试方言覆盖这条路径（SQLite 3.35+ 也支持 RETURNING）。

**同一天的第二个教训**：`Integration` job 红着，PR #61 还是被 auto-merge 合进了 `main`——
它不在 ruleset 必需检查里，而 auto-merge 只等必需检查。"先观察稳定性再提升"这个决定
在它第一次跑就抓到真 bug 的事实面前站不住：一个能抓到 PG 六个版本没人发现的 bug 的门，
不该是可选的。已提升为必需检查（job 名 `Integration` 不是 matrix，名字稳定）。

## 2026-08-26 — 集成测试为什么长这样，以及暂时不做的几件事

`dialect/mysql.go` 和 `dialect/postgres.go` 在此之前覆盖率 0%——v4.2.0 那批标 "Critical"
的 PG/MySQL reconcile 修复没有任何自动化回归。集成测试的核心断言是"托管 schema 第二次启动
零 DDL"：v4.2.0 的每个事故都表现为它。用 env DSN + `t.Skip` 而不是 build tag，是为了让
SQLite 目标始终参与、套件本身每次 `go test` 都被编译执行。

已知决定不做（写在这里免得下一个人重新调查）：

- **Docker 镜像不推送 registry**。`Docker Build` 是必需检查，但产物没人消费；推送要配
  ghcr 权限和 tag 策略，等有真实使用者再说。
- **`NewRuntime` 不改成 functional options**。`options ...*RuntimeOptions` 别扭，但改签名
  是破坏性变更，留给 v5。
- **不替换 `gopkg.in/nullbio/null.v6` 和 `serenize/snaker`**。前者出现在生成代码里
  （`examples/academy/*.tsq.go` import 它），是使用者契约；后者只在生成器里做
  CamelToSnake，换实现等于改所有使用者的表名推导。

## 2026-08-21 — `release-check` 只能查版本倒退，不能查"没前进"

第一版写的是"代码里的版本必须严格大于最新 tag"，它把门装反了：合法状态有两个，
这条规则两个都拦。

- 发版之间：buildinfo 等于最新 tag。这是常态，绝大多数提交都处在这里。
- `release.py` 跑 harness 的那一刻：buildinfo 已经领先于最新 tag，因为 tag 要等 harness
  全绿才打。

真正的错误状态只有一个：buildinfo **低于**最新 tag，也就是有人把版本号改回去了。
"HEAD 上有 tag 时 tag 必须等于代码里的版本"那条单独守着重打 tag 的情况。

## 2026-08-21 — 两份技能必须各住各的目录，别为了少一个符号链接把它们并在一起

开发者技能一开始放在 `agents/skills/tsq-dev`，然后为了让 `.claude/skills` 一个符号链接就
同时暴露两份技能，往那个目录里又软链了一个 `agents/skills/tsq -> ../../skills/tsq`。
**这是错的**：整套设计的核心就是"两份技能读者不同、所有权严格分开、内容不许互相复制"，
而目录结构直接把这句话推翻了——布局是文档的一部分，看目录的人先看到的是"它俩是一伙的"。

现在的布局，两个位置各自表达自己的归属：

- `.agents/skills/tsq-dev` —— 开发本仓的上下文，点开头，和 `.github/`、`.claude/` 一样是
  仓库的工具带，不是产品。
- `skills/tsq` —— 随发布分发给使用者的说明书，在仓库根，因为它**是**产品的一部分。

`.claude/skills/` 是一个**目录**，里面两条符号链接分别指过去。它只是让 Claude Code 同时
发现两份技能的入口，不是它们的家——多一条符号链接换布局说真话，这个交换划算。

改路径时 `git mv` 而不是删了重建：历史跟着走，`git log --follow` 还能追到搬家之前。
引用这个路径的地方有 12 个文件（脚本常量、Makefile、`.gitignore`、三份文档、技能自身），
`grep -rn` 一遍是唯一可靠的确认方式。

## 2026-08-21 — 钩子和 `commit-check` 校验的是两个不同的字符串

PR #59 合进 `main` 之后，`make harness` 立刻红了：

```
提交主题 77 字符，上限 72：
'docs: rewrite CONTRIBUTING for contributors and gate stale make targets (#59)'
```

我写的主题是 71 字符，本地 `commit-msg` 钩子放行。**GitHub 的 squash 合并往末尾追加了
` (#59)`**，于是同一条提交信息在两道门里长度不同：钩子看的是作者写的（合并前，无后缀），
`commit-check` 看的是 GitHub 改过的（合并后，有后缀）。作者能过钩子，却让 `main` 上的门
变红，而且**没有任何办法提前避免**——写提交信息的时候 PR 号还不存在。

修法是量长度前剥掉 ` (#\d+)` 后缀：那不是作者写的东西，拿它去衡量作者是在量错的东西。

引申：**任何在合并前后各跑一次的检查，都要确认两次跑的是同一个输入。** 合并会改写提交
信息（squash 追加 PR 号）、会改写 SHA（squash 造新 commit）、会改写历史形状（PR 的多个
提交压成一个）。这三件事今天各绊了一次。

## 2026-08-21 — 不存在的 `make update-examples` 在文档里又活了三个月

v4.4.1 修 CI 时只改了 workflow，`README.md` / `CONTRIBUTING.md` 里同一个幽灵没人问"这个
名字还写在哪"。**一个被修两次的问题是一个不彻底的修复。** 现在由 `make doc-check` 守着
围栏代码块里的 `make X`（散文有意不扫，历史叙述要能写出已不存在的名字）；它管不到
`.github/workflows/`，改目标名那里仍要手动 grep。

## 2026-08-21 — 第一次真跑 PR 发版流程暴露的两件事

v4.5.0 是第一个走 PR 流程发出去的版本（`main` 加 ruleset 之后不能直推了）。两处踩坑：

**squash 之后不能 `git pull --ff-only`。** squash 在 origin 上造出一个**新** commit，
本地 release 分支上的原始提交不在它的历史里，`--ff-only` 必然报分叉。合并之后唯一正确的
动作是 `git fetch` + `git reset --hard origin/main`——内容已经全在那个 squash commit 里，
本地要做的是采纳远端历史，不是合并它。

**发版 PR 里只该有发版提交。** v4.5.0 发的时候本地 `main` 还压着三个没推的提交，它们被
一起卷进了发版 PR，squash 之后 `main` 上只剩一句 `chore: release v4.5.0`——那三条讲清楚了
改动的提交信息就此从历史里消失了（PR 里还能翻到，但 `git log` 上没有）。`release.py` 现在
在发版前检查 `origin/main..main` 是不是空的，不空就拒绝，让那些工作先走自己的 PR。

**别把新分支叠在还没合的 PR 分支上。** 同一天又栽了一次：PR #56 是从 PR #55 的分支上直接
切出来的（忘了先回 `main`），#55 被 squash 合并后产生新 SHA，#56 里那份原始提交立刻和
`main` 冲突，只能重做。**开新分支之前先 `git checkout main && git fetch && git reset --hard
origin/main`**，两秒钟的事，省掉一次 rebase。

这三条是同一件事的三个面，对所有走 PR + squash 的仓库都成立：**squash 的粒度是 PR，
所以 PR 的粒度就是你能保留的历史粒度，而任何"基于未合并分支"的东西都会在合并那一刻失效。**

## 2026-08-21 — 把并发写入者的改动误判成了工具的 bug

本会话一度断定"`make fmt` 里的 `go fix ./...` 会把仓库改到编译不过"，并据此从 `make fmt`
里删掉了它。**这个结论是错的，已经改回来。**

当时的症状很有说服力：树验证过 `BUILD OK` → 只跑 `make fmt` → `tx.go` 被改 → 编译失败
`undefined: withTxRuntime1`，而 `go fix` 自己在输出里打印了这个错误。看起来是闭环。

真相是同一时刻有**另一个 claude 进程在同一个工作区里**做泛型重构，边跑边写文件。`go fix`
打印的那行是它**遇到**的编译错误——树已经被并发写入弄成不一致了——不是它造成的。事后在
HEAD 的干净副本里重跑 `go fix ./...`：空操作，build 照常通过，复现不出来。

留下三条：

- **"我改了 A，然后 B 坏了"在有并发写入者时什么都不能证明。** 排查前先确认自己是不是唯一
  的写入者：`ps aux | grep claude` 加 `lsof -p <pid> -a -d cwd`。本会话在错误的嫌疑人上
  绕了很多圈，代价是我几次 `git checkout -- '*.go'` 丢掉了对方未提交的工作（它自己重写
  回来了，没有实际损失，但那是运气）。
- **验证要在副本里做。** `git archive HEAD | tar x` 到临时目录再跑可疑命令，既能复现又不会
  被别人的编辑干扰，也不会误伤别人。
- `make fmt` 末尾的 `go build ./...` 守卫留下了，它独立成立：这个目标里每一步（`go fix`、
  `golangci-lint fmt`、`run --fix`）都在改写源码，格式化绝不该交回一棵编不过的树。没有
  守卫的话，坏改写要等到 `make lint` 才暴露，而那时报的是离成因很远的 typecheck 错误。

## 2026-08-21 — 给 main 和 tag 加了 ruleset，发版随之改成 PR 流程

`main`：禁直推/强推/删除，必须走 PR 且五个检查全绿。`refs/tags/v*`：禁删除/移动/强推。
两条都**对仓库所有者生效**（`bypass_actors` 为空），实测 `git push origin main` 被
`GH013` 拒绝。

tag 那条比分支那条重要得多。单人仓的真实风险从来不是"别人推了坏代码"，是删掉或移动一个
已发布的 tag——Go Proxy 永久缓存内容哈希，那是唯一不可恢复的操作。加上它之后，
"不要删 tag 重打"从一条写在文档里的规则变成了服务端强制的约束。

必需检查的选法有个坑：**不能放 matrix job**。`Test` 的检查名是
`Test (ubuntu-latest, 1.27.0)`，升 Go 版本名字就变，而变了的名字永远不会出现在 PR 上，
必需检查永远等不到，**所有 PR 从此合不进去**。选了 `Build` / `Docker Build` /
`GoReleaser Check`，它们都 `needs: [test, lint, coverage]`，覆盖等价而名字稳定。
同理不能放 `Release`——它只在 tag 上跑，在 PR 上永远不出现。

`release.py` 因此改成 PR 流程，并且用 `gh pr merge --auto` 而不是"等 CI 再合"：PR 刚建
出来的头几秒还没有任何 check 注册，`gh pr checks --watch` 在那一刻会以
"no checks reported" 直接退出。交给 GitHub 自己在必需检查全绿时合并，脚本只轮询结果。

tag 必须打在**合并之后**的 `main` HEAD 上：squash 产生新 SHA，打在 release 分支上的 tag
会指向一个不在 `main` 历史里的 commit。打之前重新读一遍 `buildinfo` 确认版本对得上。

验证这类服务端规则**不能用 `git push --dry-run`**——它只在本地模拟 ref 更新，根本不联
服务端，看起来永远是成功的。要真推一次。测 tag 规则时用一个不合法 semver 的探针 tag
（`v-ruleset-probe`）：它匹配 `v*` 所以受规则管，但 Go Proxy 会忽略它，不会污染版本列表。

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

`internal/cmd/version_test.go` 一开始只在每个用例前把 `versionShortFlag` /
`versionJSONFlag` 两个 Go 变量置回 false，结果 `--json` 那个用例报"两个标志都设了"。

原因是 `VersionCmd` 是包级单例，标志在 `init` 里绑定一次，**状态跨 `Execute()` 存活**。
`MarkFlagsMutuallyExclusive` 判定用的不是变量值，而是每个 pflag 的 `Changed` 位——前一个
用例传过的 `--short` 把它置上就再也没清过，于是下一个用例看起来像同时传了两个。

清法不需要 import pflag：`VersionCmd.Flags().Lookup(name).Changed = false` 直接改返回的
指针的字段就行，没提到类型名就不需要那个包。`go test -shuffle=on` 是发现这类用例间耦合的
标准手段，`make harness` 里的 `test-race` 已经带上了 `-shuffle=on`。

## 2026-08-21 — `-X` 打错包路径是**静默**失败的

Go 链接器找不到 `-X` 指定的符号时不报错，直接忽略。`.goreleaser.yaml` 打在
`github.com/tmoeish/tsq/v4.version` 上（变量实际在 `internal/buildinfo`），发出去的每个
二进制 build time / commit / branch 都是 `unknown`，而 `version` 退回源码字面量恰好是对的，
掩盖了另外三个。`goreleaser check` 只校验 YAML 结构，证明不了这件事；唯一可靠的验证是
`goreleaser build --snapshot --single-target` 之后跑 `tsq version`。2026-08-26 发现
`Dockerfile` 里还有第三个副本犯同样的错，现在 `make release-check` 核对三份配置里的每个
`-X`，CI 的 `Docker Build` job 还会 `docker run` 镜像跑一次 `version --json` 核对 commit
真的进了二进制——静态检查证明路径对，跑产物证明值到了。附带钉死：用 `{{ .Tag }}` 不用 `{{ .Version }}`（后者剥掉前导 `v`），加 `-trimpath`。

## 2026-08-21 — 生成器不能带 `git describe` 的版本号，否则发版是死锁

`make examples` 原本用 `make build` 产出的 `bin/tsq`，而 `build` 会用
`-ldflags -X ...buildinfo.version=$(git describe --tags --always --dirty)` 注入版本号。
于是**生成文件头记的是 git 描述出来的版本，不是即将发布的版本**。

这在第一次真跑 `make release` 时立刻炸了：脚本把 buildinfo 改成 v4.4.2、跑
`make examples`，生成物头部却还是 v4.4.1——因为 v4.4.2 这个 tag 那一刻还不存在，
`git describe` 只能描述出上一个 tag。想让头部写对就得先打 tag，想打 tag 就得先过
`release-check`，死锁。

修法是 `make build-gen`：**故意不带 `$(LDFLAGS)`** 地编一个 `bin/tsq-gen`，它报告
`internal/buildinfo` 里的字面量。`make examples` 和 `make gen-check` 都用它。附带的好处
是生成结果只依赖源码，不再依赖工作区干不干净——在此之前，同一份源码在脏工作区和干净
检出上会生成出不同的文件头，而 CI 的 "ensure generated examples are committed" 一直是靠
运气才绿的。

`bin/tsq`（带 ldflags）仍然是给人用的 CLI，`tsq version` 该报告 git 状态。两个二进制的
分工不要合并。

`.goreleaser.yaml` 曾经犯的是同一类错误的另一面，已于 2026-08-21 修好，见下一条。

## 2026-08-21 — 引入 harness 与 tsq-dev 技能

本仓有两份技能，读者完全不同，之前只有一份：`skills/tsq`（随仓库发布，给**使用** TSQ 的
人和他们的 agent）和新增的 `.agents/skills/tsq-dev`（给**开发本仓**的人和 agent）。分开的
理由不是篇幅，是所有权：前者描述契约，后者描述实现。同一份文件同时服务两拨读者时，
写给使用者的部分会因为开发者觉得"这个细节太内部"而被删掉，反过来也一样。

`make skill-check` 的触发表不是形式主义。每条触发器都是从"哪类改动会让哪份文档变假"倒推
出来的，`hint` 里写着理由。确实判断过不需要动技能时用 `SKIP_SKILL_CHECK=<触发器名>` 豁免，
并在提交正文里写清楚——无理由的总开关不提供，因为那等于没有门。

## 2026-08-21 — 生成物是否同步不能用 `git diff` 判断

第一反应是 `git diff --exit-code -- examples/academy`，那是错的：一波变更本来就可能合法地
改动生成物，而它们在提交之前一直处于未提交状态——这道门会对每一波正当改动都失败。

判据只能是"拿当前源码重新渲染一遍，看结果一不一样"。`tsq gen --check` 正是为此存在，
它在内存里渲染并与磁盘比对。`make gen-check` 用的就是它。

## 2026-08-21 — 版本号有四个副本，生成物那份最容易忘

`internal/buildinfo` 的版本号会传导进生成文件头（`// Code generated by tsq-vX.Y.Z.`）和
`tsq.json`。所以**改版本号必须重新生成示例**，否则 `gen-check` 和 `release-check` 都会失败。

这不是麻烦，是保险：它让"tag 指向的代码"和"生成物声称的版本"不可能不一致。
`script/release.py` 依赖这一点。

## 2026-08-21 — `make commit-check` 单独存在时是失效的

有未提交代码时 `commit-check` 主动跳过；代码一提交，`memory-check` 又轮到跳过。于是每次
`make harness` 只有一道门是活的，而写提交信息的那一刻活着的恰好是另一道。

提交信息真正被校验的唯一时机是 `commit-msg` 钩子。`make hooks` 必须每台机器跑一次，
这不是可选的补充。

## 2026-08-21 — 发版波必须从内存门禁里豁免

`script/release.py` 会跑 `make harness`，而发版波唯一的非生成物改动是
`internal/buildinfo/buildinfo.go`。不豁免的话，每次发版都会撞在 `memory-check` 上，
而发版本身教不了项目任何东西——真正的知识在被发布的那些波里已经记过了。

豁免写在 `check_change_log.py` 的 `RELEASE_ONLY_FILES`，是精确的白名单而不是开关：
发版波多碰了任何一个别的文件，门就重新活过来。

## 2026-08-21（追溯 v4.4.1） — 本地 make 目标和 CI 是两条独立的真相

v4.4.1 是一个纯修复版本：CI 的 coverage job 调用了一个不存在的 `make update-examples`，
仓库里的目标叫 `make examples`。本地全绿，流水线红。

改本地目标名的时候 grep 一遍 `.github/workflows/`。`make -n all` 的冒烟测试挡不住这个——
它只覆盖 `all` 这条链。

## 2026-08-21（追溯 v4.3.0） — 改生成文件后缀的真实代价

`_tsq.go` → `.tsq.go` 是对的（对齐 `.pb.go` 的生态惯例，IDE 识别更好），但代价是所有使用者
的 `.gitignore`、Makefile glob 和 CI 配置都要跟着改。

后缀常量的唯一来源是 `internal/parser/constants.go` 的 `TSQFileSuffix`，但认这个格式的地方
不止一处：`script/changeset.py` 的 `GENERATED_SUFFIXES`、`script/check_release.py` 的
`GENERATED_HEADER`。再改一次之前先想清楚值不值。

## 2026-08-21（追溯） — 全局 `Init()` 和 engine 中间层是被删掉的，不要重新引入

历史上有过包级全局 `Init()`、一个 `engine` 中间层和一个 `traceManager` 层，都被删了，
换成显式的 `NewRuntime(driverName, dsn, tables, opts...)`。

全局单例让"这个查询用的是哪个库"变成不可回答的问题，测试也没法并行。中间层则是纯转发，
它唯一的作用是让调用栈多一层。任何形式的"方便起见加个全局默认 runtime"都是在往回走。
