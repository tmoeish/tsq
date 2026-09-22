# 项目内存 — 门禁、CI 与发版流程

判据与索引在 `../memory.md`。

## 集成测试为什么长这样，以及暂时不做的几件事 (2026-08-26)

核心断言"托管 schema 第二次启动零 DDL"（v4.2.0 的 Critical 事故都表现为它），谓词按匹配到的行断言。
用 env DSN 而不是 build tag，SQLite 目标因此每次 `go test` 都跑。**不换 `serenize/snaker`**：它做 CamelToSnake，换实现等于改
所有使用者的表名推导。nullbio 只按类型路径识别，模块不依赖它（2026-09-19）。

## 只被自己的测试撑着的代码，在库里是不存在的 (2026-08-26，2026-09-09，2026-09-22)

四次同一形状：永远为假的 `printSQL` tracer、`canonicalCapabilityName` 的副本、测试里的包级 `Runtime`、以及和真实
索引策略已经漂移的第二份 `upsertIndex`。**`unused` linter 看不见**（测试里的引用算使用），**测试证明的是它自洽，
不是它可达**。第四次之后装了门：`deadcode_test.go` 的 `TestNoUnexportedCodeOnlyTestsReach`。只给测试用的数据
（如能力清单）放进 `_test.go`，别给门开豁免。

**2026-09-09 又一次，这次在代码生成侧**：模板 helper 发出 `tsq.TimePtr(...)`，根包没有这个符号，
声明 `*time.Time` 托管字段的使用者拿到的是**自己工程里**的编译错误，而 `skills/tsq` 一直把它列为
受支持；守着它的单元测试断言的正是那个字符串。**模板和 helper 里的字符串不参与本包的类型检查**，
`api-check` 又只看根包快照，缝正好在"生成代码引用的符号存不存在"。门是
`internal/cmd/generated_symbols_test.go`，它装上后立刻抓到第二个（`PageRespType` 渲染的
`tsq.PageResp` 其实叫 `PageResponse`）。

## 写在 AGENTS.md 里但没有门的规则，几个月都是假的 (2026-08-26)

`AGENTS.md` 要求 "README、`docs/`、`skills/tsq` 用英文"，而实测只有 `skills/tsq` 是对的——
这条规则从写下那天起就没成立过，`doc-check` 当时扫不到 Markdown 的语言。

选择是改规则而不是翻译九百行：分界线按**读者**划才站得住，然后给英文那一侧加了门。
**留下的是判据：写规则的时候就问"谁来发现它被违反了"。** 答不出来的规则不要写进
`AGENTS.md`，写进去只会让下一个读到它的人相信一件假事。

## 同一件事跑两遍的检查要看同一个输入；文档与 CI 的 make 目标是两条真相 (2026-08-21，2026-09-22)

squash 会改写提交信息（追加 ` (#59)`）、SHA 和历史形状；`check_change_log.py` 量长度前剥掉 ` (#\d+)`。
`doc-check` 守着文档里的 `make X`，但**管不到 `.github/workflows/`**（CI 调过不存在的目标），改名时手动 grep。
CI 的 gosec 也跑两遍：门禁那遍排除 G201/G304，上传代码扫描的 SARIF 那遍曾不排除，于是门禁已接受的每处读文件都开一条
告警（攒到 8 条），现在两遍用同一组排除。v4 线（`v4` 分支）有同一个修复，外加 `latest_tag()` 按主版本线比。

## 发版波必须从内存门禁里豁免 (2026-08-21)

豁免是 `check_change_log.py` 的 `RELEASE_ONLY_FILES`，**精确白名单而不是开关**：多碰一个别的文件门就活过来。

## `release-check` 两次装反：**先数清楚合法状态有几个** (2026-08-21，2026-09-16)

**一道门要先问"合法状态有几个"，只有一个时才用等号。** 这里错了两次：

- 版本号 vs 最新 tag：第一版要求"严格大于"，但合法状态有两个（发版之间 buildinfo 等于最新 tag；
  `release.py` 跑 harness 时 buildinfo 领先于 tag），两个都被拦。真正的错误状态只有一个：
  buildinfo **低于**最新 tag。
- 版本号 vs 模块主版本：曾要求相等。跨主版本必须分两步（先一波正常变更把 `/vN` 和全部 import 改完，
  再发首个 vN），两步之间模块路径已是 v5 而 buildinfo 还是 4.x——**严格相等把这个合法过渡态拦死，
  于是迁移根本没法作为独立的一波合入**。现在只查 `module_major() < code.major`。

## squash 的粒度是 PR，所以 PR 的粒度就是你能保留的历史粒度 (2026-08-21)

第一次真跑 PR 发版流程，同一天被这一件事绊了三次：`pull --ff-only` 报分叉、卷进发版 PR 的三条
提交信息从 `git log` 消失、新分支叠在未合并分支上冲突。三个动作现在都是 `AGENTS.md` § 发版
里的规则，`release.py` 还会拒绝 `origin/main..main` 不为空的发版。

## 把并发写入者的改动误判成了工具的 bug (2026-08-21)

曾断定 `make fmt` 的 `go fix` 会把树改坏（**错的，已改回**）：另一个 claude 进程在同一工作区边跑边写。**"我改了 A，
然后 B 坏了"在有并发写入者时什么都不能证明**：先 `ps aux | grep claude` 加 `lsof -p <pid> -a -d cwd` 确认自己是唯一
写入者，再在 `git archive HEAD` 的副本里复现；当时几次 `git checkout -- '*.go'` 丢掉了对方未提交的工作。

## 给 main 和 tag 加了 ruleset，发版随之改成 PR 流程 (2026-08-21)

`main` 禁直推、必须走 PR 且五个必需检查全绿；`refs/tags/v*` 禁删除/移动/强推。两条都对仓库
所有者生效。**tag 那条更重要**：删掉或移动已发布的 tag 是唯一不可恢复的操作（Go Proxy 永久缓存）。

- **必需检查不能放 matrix job**（名字带 Go 版本，升版本就永远等不到）；理由和当前选的五个
  检查见 `../impact/harness.md` § 改了 CI 的 job 名字。
- **用 `gh pr merge --auto`，不要"等 CI 再合"**：PR 刚建出来的头几秒没有任何 check 注册，
  `gh pr checks --watch` 那一刻会以 "no checks reported" 直接退出。
- **验证服务端规则不能用 `git push --dry-run`**——它不联服务端，看起来永远成功。要真推一次；
  测 tag 规则用不合法 semver 的探针 tag（`v-ruleset-probe`），受 `v*` 规则管但 Go Proxy 忽略。

## CI 里用 `@latest` 装的工具，会在它发新版本的那天让每个 PR 变红 (2026-09-09)

goreleaser v2.18.1 一发布就要求 Go >= 1.27.1，CI 用 `GOTOOLCHAIN=local` 钉着 1.27.0，`@latest`
随即装不上，**每个 PR 的 GoReleaser Check 都红**且与改动无关。更贵的是 release job 的
`version: latest`：它只在**tag 推送之后**才跑，那一步不可撤销；两处版本不同时 check job 也证明不了 release 会成功。

## `-X` 打错包路径是**静默**失败的 (2026-08-21)

链接器对找不到的 `-X` 符号直接忽略，三份配置（含已删的 Dockerfile）各犯过一次。`release-check` 核对路径，CI 的 `Build` 运行二进制核对值——**静态检查证明路径对，跑产物证明值到了，缺一不可。**

## 决定：两份技能按所有权拆开，不按篇幅 (2026-08-21)

理由不是篇幅是所有权。同一份文件同时服务两拨读者时，写给使用者的部分会因为开发者觉得
"这个细节太内部"而被删掉，反过来也一样。`skill-check` 的每条触发器都是从"哪类改动会让哪份
文档变假"倒推出来的，`hint` 里写着理由。

## Go 1.27 允许组合字面量用提升字段作键 (2026-09-17)

`outer{c: 1}` 这种写法让拆结构体时旧字面量照样编译，别当成改完了。

## 决定：变更影响和项目内存拆成"索引 + 子文件"，其余几份不拆 (2026-09-22)

`change-impact.md` 537 行、`memory.md` 460 行，每一波都要整份读进来，而一波通常只落在一两个域里。照
`ddwiki-dev` 的做法拆成索引加 `impact/`、`memory/` 各六份；`architecture.md`、`feature-map.md`、`codegen.md`、
`release.md` 不拆——**判据是读一份要付的字节数，不是文件数**，它们还没到那个量。拆分的代价是索引漂移：
速查漏一条就等于那条耦合不存在，所以 `check_index_sync` 无条件跑、不给豁免。拆的时候还翻出一条早已失效的
小节引用（`§ 改了分块或批量语句的形状`，小节早改名成"改了批量写"）：**按小节名的交叉引用没有门**，改标题时 grep 它。
