# 变更影响 — 门禁、CI 与文档

处理你匹配的每个触发器；索引与 `[门禁]` 标记的含义在 `../change-impact.md`。

## 加了或改了 `-X` ldflags（`Makefile`、`.goreleaser.yaml`）

- 目标必须是 `github.com/tmoeish/tsq/v5/internal/buildinfo.<var>`，`<var>` 必须真的在
  `internal/buildinfo/buildinfo.go` 里声明。链接器对找不到的符号**静默忽略**，二进制会
  把 build time / commit / branch 报成 `unknown` 而没有任何报错。
  `[门禁: release-check 核对两份配置里的每个 -X；CI 的 Build 运行二进制核对值]`
- 三份配置是三个副本，改变量名要一起改。

## 改了面向使用者的文档（README、`docs/`、`skills/tsq`）

- **实质内容只有一个归宿**：`skills/tsq/references/` 是"怎么用这个库"的唯一来源，
  `docs/` 只做索引指过去。`docs/concepts.md` 和 `docs/quickstart.md` 曾各自把同样的内容
  重写了一遍，两份必然漂移，而漂移之后更糟的是看起来还对的那份。
- 语言按**读者**划：`skills/tsq` 随发布装进别人的项目，必须英文；README、`docs/`、
  `CHANGELOG.md`、`CONTRIBUTING.md` 面向本项目读者，中文。
  `[门禁: doc-check 的 check_shipped_skill_language]`
- 这条规则在 `AGENTS.md` 里写反了好几个月（要求 README 和 `docs/` 英文，而它们一直是
  中文），没有任何东西发现过。**没有门的规则不是规则**——改语言规则就要同时改那道门。

## 改了 harness（`script/`、`Makefile`、CI）

- 门禁的顺序、跳过条件写在 `AGENTS.md` § 验证与交接，那是唯一权威处，技能只引用不复述。
- `SKILL.md` 的命令表和 `../release.md` 的流程描述要同步。`[门禁: skill-check harness]`
- 新增门禁要想清楚它在发版波（只改版本号和生成物）里会不会误报——
  `check_change_log.py` 的 `RELEASE_ONLY_FILES` 就是为此存在的。
- `.github/workflows/go.yml` 和本地 `make` 目标是两条独立的真相。改了本地目标名，
  CI 里引用它的地方要一起改（v4.4.1 那次 CI 调了一个不存在的 `make update-examples`）。

## 想往 `make fmt` 里加自动改写工具

`make fmt` 里每一步都在改写源码（`go fix`、`golangci-lint fmt`、`run --fix`），末尾的
`go build ./...` 是守卫：**格式化绝不能交回一棵编不过的树。** 加任何改写工具都必须能过它，
并且要先证明它是幂等的。

排查"某个工具改坏了我的文件"之前，先确认自己是不是唯一的写入者，并在 `git archive HEAD`
出来的副本里复现——本仓有过一次把并发 agent 的编辑误判成 `go fix` bug 的教训，见
`../memory/harness.md` 的"把并发写入者的改动误判成了工具的 bug"。

## 在非测试 Go 源码里写了中文

不行：注释、Go doc、错误文案都是使用者读的。`make doc-check` 扫 `git ls-files` 里全部
非测试、非生成、非 `examples/` 的 `.go` 文件。`[门禁: doc-check]`

## 改名或删除了一个 make 目标

`grep -rn 'make <旧名>' --include='*.md' .` 一遍。文档里的命令是给人复制粘贴的，改名之后
它们会让照做的人得到 `No rule to make target`，然后开始怀疑自己的环境。`make doc-check`
守着围栏代码块里的引用；散文里的历史提及（`memory/`、`CHANGELOG.md` 讲事故经过时）
有意不管。`.github/workflows/` 不是 Markdown，那道门管不到，要单独 grep。

## 改了 CI 的 job 名字

`main` 的 ruleset 按**检查名**要求 `Lint`、`Coverage`、`Build`、
`GoReleaser Check`、`Integration` 全绿。改掉其中任何一个 job 的 `name:`，那个必需检查就再也不会出现在
PR 上，而"等不到的检查"等于**所有 PR 永久合不进去**，包括发版 PR。

改 job 名必须同步 ruleset：

```bash
gh api repos/tmoeish/tsq/rulesets --jq '.[] | "\(.id) \(.name)"'
gh api repos/tmoeish/tsq/rulesets/<id> --jq '.rules[] | select(.type=="required_status_checks")'
```

同理，**不要把 matrix job 加进必需检查**：`Test` 的检查名是
`Test (ubuntu-latest, 1.27.0)`，升 Go 版本就会变成另一个名字。

## 改了 CI 里安装的工具，或它的版本

- **一律钉死版本，不要 `@latest`。** CI 用 `GOTOOLCHAIN=local` 钉着 `GO_VERSION`，工具一旦发布
  要求更新 Go 的版本，`go install ...@latest` 当天就装不上，于是**每个 PR 都红**且与改动无关。
  gosec、govulncheck、golangci-lint、goreleaser 各有自己的版本变量。
- **`GoReleaser Check` 和 `Release` 必须用同一个 goreleaser**：前者跑在 PR 上，后者只在 tag
  推送**之后**跑。版本不同时，前者证明不了后者会成功，而那一步不可撤销。
  `[门禁: release-check]`
- 换版本前先在本地按 CI 的方式装一次（`GOTOOLCHAIN=local go install ...@<版本>`），确认它
  能在当前 `GO_VERSION` 下装上。

## 升级 Go 版本

`go.mod`、`.github/workflows/go.yml` 的 `GO_VERSION` 与 matrix、
`CLAUDE.md` / `AGENTS.md` 里写的版本号，全部一起改。golangci-lint 也要升到兼容版本
（`Makefile` 的 `LINT_BIN` 那行钉死了版本）。

matrix 一改，`Test` 的检查名就跟着变——所以 ruleset 的必需检查里没有它，见「改了 CI 的 job 名字」。
