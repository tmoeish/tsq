# 项目内存 — 门禁、CI 与发版流程

判据与索引在 `../memory.md`。

## 集成测试为什么长这样，以及暂时不做的几件事 (2026-08-26)

核心断言"托管 schema 第二次启动零 DDL"（v4.2.0 的 Critical 事故都表现为它），谓词按匹配到的行断言。
用 env DSN 而不是 build tag，SQLite 目标因此每次 `go test` 都跑。**不换 `serenize/snaker`**：它做 CamelToSnake，换实现等于改
所有使用者的表名推导。nullbio 只按类型路径识别，模块不依赖它（2026-09-19）。
**真跑也要换驱动的模式和会话的设置跑**（2026-10-06）：MySQL 的 `interpolateParams=true` 和 pgx 的 `simple_protocol` 把参数写进语句，`[]byte` 成了二进制字面量——"空 `json.RawMessage` 绑成 `[]byte(\"null\")`"在默认模式全绿、换模式就被 JSON 列拒绝；MySQL 的 `sql_mode` 同理（`ANSI_QUOTES` 让探测整个失效、非严格模式让改列类型截断数据，默认模式下都是绿的）。改绑定出口、改解析引擎输出的代码之后，把集成测试在 `DSN + &sql_mode=...` / 上面两种驱动模式下各跑一遍。PostgreSQL 换 `DateStyle`、`standard_conforming_strings`、隔离级别跑过，没有产品问题（剩下的失败是 pgx 简单协议自己的限制）。
**macOS 的时钟只有微秒精度**（2026-10-05）：断言里和纳秒有关的 `time.Now()` 在本地永远是绿的，Linux 的 CI 才红（时间绑定截到微秒那一波因此多跑了一轮）；这类测试手写纳秒（`time.Date(..., 828405417, ...)`）。

## 只被自己的测试撑着的代码，在库里是不存在的 (2026-08-26，2026-09-09，2026-09-22)

四次同一形状（永远为假的 tracer、漂移了的第二份 `upsertIndex`……）：**`unused` linter 看不见测试里的引用，测试证明的是它自洽，不是它可达**。门是 `deadcode_test.go` 的
`TestNoUnexportedCodeOnlyTestsReach`（只给测试用的数据放进 `_test.go`，别给门开豁免）；模板里的字符串不参与类型检查，门是 `internal/cmd/generated_symbols_test.go`。

## 写在 AGENTS.md 里但没有门的规则，几个月都是假的 (2026-08-26)

"README、`docs/`、`skills/tsq` 用英文"从写下起就不成立（`doc-check` 扫不到 Markdown 语言）。改的是规则（按**读者**划界），再给英文
那侧加门。**判据：写规则时就问"谁来发现它被违反了"**，答不出的规则写进 `AGENTS.md` 只会让人相信一件假事。

## 同一件事跑两遍的检查要看同一个输入；文档与 CI 的 make 目标是两条真相 (2026-08-21，2026-09-22)

squash 会改写提交信息（追加 ` (#59)`）、SHA 和历史形状；`check_change_log.py` 量长度前剥掉 ` (#\d+)`。
`doc-check` 守着文档里的 `make X`，但**管不到 `.github/workflows/`**（CI 调过不存在的目标），改名时手动 grep。
CI 的 gosec 也跑两遍：门禁那遍排除 G201/G304，上传代码扫描的 SARIF 那遍曾不排除，于是门禁已接受的每处读文件都开一条
告警（攒到 8 条），现在两遍用同一组排除。v4 线（`v4` 分支）有同一个修复。
**`latest_tag()` 两条线都按模块主版本比**：main 起初没改，v4.10.1 一发，main 的 `release-check` 就把还停在 4.x 的
buildinfo 判成倒退、所有 PR 变红（2026-09-28）。**改发版脚本时两条线一起改。**

## 发版波必须从内存门禁里豁免 (2026-08-21)

豁免是 `check_change_log.py` 的 `RELEASE_ONLY_FILES`，**精确白名单而不是开关**：多碰一个别的文件门就活过来。

## `release-check` 两次装反：**先数清楚合法状态有几个** (2026-08-21，2026-09-16)

**一道门只有一个合法状态时才用等号。** 版本号 vs 最新 tag 的错误态只有"低于"（发版之间相等、`release.py` 跑 harness 时领先都合法），版本号 vs 模块主版本只查
`module_major() < code.major`（路径已是 `/v5`、buildinfo 还是 4.x 是合法过渡态）；两处都曾写得更严，把合法状态拦死。

## 把并发写入者的改动误判成了工具的 bug (2026-08-21)

曾断定 `make fmt` 的 `go fix` 会把树改坏（错的）：另一个 claude 进程在同一工作区边跑边写。**"我改了 A，然后 B 坏了"在有并发写入者时什么都不能证明**：
先 `ps aux | grep claude` + `lsof -p <pid> -a -d cwd` 确认自己是唯一写入者，再在 `git archive HEAD` 的副本里复现；`git checkout -- '*.go'` 丢过对方未提交的工作。

## 给 main 和 tag 加了 ruleset，发版随之改成 PR 流程 (2026-08-21)

规则在 `AGENTS.md` § 发版。坑：**必需检查不能放 matrix job**（名字带 Go 版本，见 `../impact/harness.md`）；
**用 `gh pr merge --auto`**，刚建的 PR 没注册 check，`gh pr checks --watch` 会直接退出；**验证服务端规则不能用
`git push --dry-run`**（不联服务端），测 tag 规则用 Go Proxy 忽略的探针 tag `v-ruleset-probe`。

## 工具链钉版本、`-X` 要跑产物核对 (2026-08-21，2026-09-09)

CI 里 `@latest` 装的工具在它发新版本那天让每个 PR 变红（goreleaser v2.18.1 要求更新的 Go），release job 的 `version: latest` 更是只在 **tag 推送之后**才跑、不可撤销；规则在 `../impact/harness.md` § 改了 CI 里安装的工具。
链接器对找不到的 `-X` 符号**静默忽略**（三份配置各犯过一次）：`release-check` 核对路径，CI 的 `Build` 运行二进制核对值——**静态检查证明路径对，跑产物证明值到了，缺一不可。**

## 决定：两份技能按所有权拆开，不按篇幅 (2026-08-21)

理由不是篇幅是所有权。同一份文件同时服务两拨读者时，写给使用者的部分会因为开发者觉得
"这个细节太内部"而被删掉，反过来也一样。`skill-check` 的每条触发器都是从"哪类改动会让哪份
文档变假"倒推出来的，`hint` 里写着理由。

## 决定：变更影响和项目内存拆成"索引 + 子文件"，其余几份不拆 (2026-09-22)

`change-impact.md` 537 行、`memory.md` 460 行每波整份读，而一波只落在一两个域；拆成索引加 `impact/`、`memory/` 各六份，
其余几份不拆——**判据是读一份要付的字节数，不是文件数**。代价是索引漂移，所以 `check_index_sync` 无条件跑。
**按小节名的交叉引用没有门**（拆分时翻出一条指向已改名小节的引用），改标题时 grep 它。
