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

## 2026-08-21 — 那个不存在的 `make update-examples` 在文档里又活了三个月

v4.4.1 是为了修 CI 调用不存在的 `make update-examples` 而发的补丁版本。修的时候只改了
`.github/workflows/go.yml`——**同一个幽灵还留在 `README.md` 和 `CONTRIBUTING.md` 的代码块
里**，谁照着敲都会得到 `No rule to make target`，然后开始怀疑自己的环境，而没有任何东西
会告诉他文档是错的。

这就是"一个被修两次的问题是一个不彻底的修复"的样板：第一次只修了报错的那一处，没有问
"同一个名字还写在哪"。

`make doc-check` 现在守着这条。判据是"读者会不会把这一行复制去执行"：**只扫围栏代码块，
不扫行内反引号**。这条界线让门不需要任何按文件的白名单——`memory.md` 记录事故经过时必须
能写出这个已经不存在的名字，`CHANGELOG.md` 的历史条目同理，而它们都在散文里。

它管不到 `.github/workflows/`（不是 Markdown），改 make 目标名时那里仍然要手动 grep。

## 2026-08-21 — 已知未处理：发布二进制的 `gitBranch` 显示 `HEAD`

`tsq version` 在 GitHub Release 的产物上显示 `branch  HEAD` 而不是 `main`。原因是 tag
触发的 CI 是分离头指针检出，GoReleaser 的 `{{ .Branch }}` 只能解析到 `HEAD`。

**现在不动它**：这不是错（那确实是分离头状态），而且提供不了信息的字段旁边就是权威来源——
`gitCommit` 是完整哈希且确认无误（v4.5.0 上核对过等于发版提交 `9d35efb9`）。为一个纯装饰
字段单独发一个版本，正是 `user_visible_changes` 那道门要拦的那种版本号噪音。

**什么条件下该动**：下次有真正的使用者可见改动要发版时顺手带上。两种改法——从 `version`
的输出里去掉 branch（tag 构建谈分支本来就没意义），或在 `.goreleaser.yaml` 里改成注入
`{{ .Tag }}` 之类真正有信息量的东西。别为它单独跑一次发版。

（这条本身就是"发现了但决定暂不处理"该怎么记的样例，见 `AGENTS.md` § 发版。这个判断
一开始只写在了聊天记录里，那等于没写——下一个 agent 会重新调查一遍 `branch` 为什么是
`HEAD`，然后重新得出同一个结论。）

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

`.goreleaser.yaml` 的 ldflags 一直打的是 `github.com/tmoeish/tsq/v4.version`，而这些变量
实际声明在 `github.com/tmoeish/tsq/v4/internal/buildinfo` 里。**Go 链接器找不到 `-X` 指定的
符号时不报错，直接忽略。** 所以每一个 GoReleaser 发出去的二进制，`buildTime`、`gitCommit`、
`gitBranch` 都是 `"unknown"`，而 `version` 悄悄退回源码字面量——恰好是对的，掩盖了另外三个
是错的。构建日志、`goreleaser check`、CI 全绿，没有任何地方会提示你。

验证方式只有一个：真的构建出来跑一遍。`goreleaser check` 只校验 YAML 结构，证明不了
`-X` 有没有生效。`goreleaser build --snapshot --single-target` 之后看 `tsq version` 的
构建时间是不是还写着 `unknown`——这是判断这类 bug 的唯一可靠信号。

顺带钉死两件事：

- 用 `{{ .Tag }}` 而不是 `{{ .Version }}`。后者会剥掉前导 `v`，而版本号的其他三个副本
  （`internal/buildinfo` 的字面量、CHANGELOG 的标题、生成文件头）都带 `v`。
- 加上 `-trimpath`。GoReleaser **不会**默认加它（`make build` 一直有），少了它发布的二进制
  会嵌进 CI 机器的绝对路径。

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
