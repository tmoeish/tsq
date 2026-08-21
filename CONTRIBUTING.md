# 贡献指南

感谢您对 TSQ 的关注。本文档面向**外部贡献者**：从 fork 到 PR 合入需要知道的全部内容。

> 维护者和在本仓工作的编码智能体请读 `AGENTS.md` 与 `.agents/skills/tsq-dev`——那里还包含
> 项目内存、技能维护和发版流程，本文档不重复它们。

## 快速开始

```bash
# 1. 在 GitHub 上 fork https://github.com/tmoeish/tsq，然后克隆你的 fork
git clone https://github.com/YOUR_USERNAME/tsq.git
cd tsq

# 2. 安装 git 钩子（每台机器一次，必须）
make hooks

# 3. 确认环境正常
make test
```

`make hooks` 装的是 `commit-msg` 钩子，它是提交信息真正被校验的唯一时机。不装的话你会在
PR 阶段才发现提交信息不合规。

需要 Go 1.27.x（`go.mod` 写的是 `go 1.27.0`）。

## 开发流程

`main` 是唯一的长期分支——**没有 develop**。所有改动都从 `main` 切一支短命分支，走 PR 合回
`main`，合并后分支自动删除。

```bash
# 1. 从同步过的 main 切分支
git checkout main && git fetch upstream && git reset --hard upstream/main
git checkout -b feat/your-change

# 2. 改代码，编辑期间随时跑
make fmt

# 3. 交接前跑完整门禁
make harness

# 4. 提交、推送、开 PR
git commit          # commit-msg 钩子在这里校验提交信息
git push origin feat/your-change
```

**开新分支之前一定先同步 `main`。** 本仓用 squash 合并，把新分支叠在还没合的分支上，
等那支被合并（产生新 SHA）之后你这支必然冲突，只能重做。

### 分支命名

`<type>/<短横线描述>`，type 用和提交信息**完全相同**的那份词表：

```
feat/scalar-query-method
fix/goreleaser-ldflags
docs/annotation-examples
refactor/split-parser
```

两套词汇必然漂移，所以只留一套。`release/vX.Y.Z` 由维护者的发版脚本创建，请不要手工建。

### 提交规范

[Conventional Commits](https://www.conventionalcommits.org/)，**英文**：

```
<type>(<scope>): <summary>

<body>
```

- type 取 `feat|fix|perf|refactor|docs|test|build|ci|chore|style|revert`
- 主题不超过 72 字符、不以句号结尾、不能是 `wip` / `update` / `fix` 这类说不清改了什么的词
- 空一行后写正文：**改了什么、为什么改、怎么验证的**，至少 3 行、120 字符
- 破坏性变更在 type 后加 `!`，或在正文写 `BREAKING CHANGE:`

示例：

```
feat(parser): accept composite index definitions in @TABLE

`idx=[{fields=["A","B"]}]` 此前只解析第一个字段，复合索引静默退化成单列索引，
而 DDL 和运行期对账都不会报错——建出来的索引和注解写的不是一回事。

解析器改为读取整个 fields 数组，索引名推导跟着带上全部字段。
验证：新增 parser 与 ddl_render 的用例；make harness 全绿。
```

`commit-msg` 钩子会校验这些。merge、revert、fixup、squash 提交豁免。

## 验证

编辑期间跑窄范围的相关检查和 `make fmt`；**交接前跑 `make harness`**。它按"便宜且常失败的
靠前"排序，是 CI 之外唯一权威的检查清单：

```bash
make harness
```

单独跑其中某一项：

```bash
make lint             # golangci-lint
make vet              # go vet
make test             # go test ./...
make test-race        # -race -shuffle=on，跨切面改动必跑
make gen-check        # 生成物是不是当前源码的输出
make api-check        # 对外 Go 契约有没有偏离快照
make examples-run     # 重新生成示例并跑通 full-suite
```

动了生成器、模板、解析器或示例，跑 `make examples` 并确认 `./bin/examples/full-suite`
能跑通。

## 不要手改生成物

以下文件由 `tsq gen` 产出，改结构体、注解、模板或解析器然后重新生成：

```
examples/academy/*.tsq.go
examples/academy/*.result.tsq.go
examples/academy/runtime.tsq.go
examples/academy/tsq.json
examples/academy/{mysql,postgres,sqlite}.sql
```

`examples/academy/mock.sql` 是**手写的** schema 真相源，改示例结构体时它要跟着改。

判断生成物是否同步不要用 `git diff`——一波变更本来就可能合法地改动生成物。用
`make gen-check`。

## 测试

- 主要用标准库 `testing` 加表格测试（62 个测试文件里只有 1 个用 testify，不要因为这份
  文档提到它就引入新依赖）。
- 测试文件名要么对应一个特性，要么对应一个被测文件，没有第三种。按"待办批次"命名的文件
  会从项目的测试清单里掉出去。
- 跨切面的运行时、查询或并发改动，`make test-race` 是必需的而不是可选的：这个库没有集成
  环境兜底，测试是它唯一的安全网。
- 查询构建器是**阶段式**的，约束由 Go 类型系统在编译期强制。新增阶段时
  `compilefail_test.go`（"这些调用必须编译失败"）和 `querybuilder_stages_test.go`
  两边都要加——返回值类型写错，约束会静悄悄地松掉，而普通测试发现不了。

## 使用者可见的改动

改了公开 API、注解语法、CLI flag 或生成代码的形态，请同时更新：

- `skills/tsq/` —— 面向 TSQ 使用者的技能，别人的 agent 照着它写代码
- `README.md`、`docs/`
- `CHANGELOG.md` 的 `## [未发布]` 段（按 `### 新增` / `### 修复` / `### 破坏性变更` 分节）

`make api-check` 会在对外 Go 符号变化时提醒你回头看这几处。

## Pull Request

`main` 受保护：**必须走 PR**，且 `Lint`、`Coverage`、`Build`、`Docker Build`、
`GoReleaser Check` 五个检查全绿才能合入。合并方式是 squash——所以一个 PR 就是 `main` 上的
一个提交，PR 的粒度决定了历史的粒度。

提交 PR 前请确认：

- [ ] `make harness` 全绿
- [ ] 新增或改动的行为有对应测试
- [ ] 生成物已重新生成并一起提交（如果动了生成器/模板/解析器/示例）
- [ ] 使用者可见的改动已同步 `skills/tsq`、`README.md`、`docs/` 和 CHANGELOG 的未发布段
- [ ] 提交信息符合上面的规范
- [ ] PR 描述说清了改了什么、为什么改、怎么验证的

## 报告问题

- **Bug**：先搜索现有 Issues 避免重复；提供可重现的最小例子、TSQ 版本（`tsq version`）、
  Go 版本和数据库方言。
- **功能建议**：说明使用场景和收益，以及为什么现有 API 做不到。

## 发布

发布由维护者负责，走 `make release`。**外部贡献者不需要改版本号，也不要打 tag**——把
面向使用者的说明写进 `CHANGELOG.md` 的 `## [未发布]` 段即可，它会在下次发版时一起出去。

流程细节见 `.agents/skills/tsq-dev/references/release.md`。

## 语言

- **英文**：代码注释、Go doc、`README.md`、`docs/`、`skills/tsq`、提交信息——这些是使用者
  读的东西。
- **中文**：`CHANGELOG.md`、`AGENTS.md`、`CLAUDE.md`、本文档、`.agents/skills/tsq-dev`——
  这些的读者是维护者。

## 获取帮助

- [GitHub Issues](https://github.com/tmoeish/tsq/issues)
- 项目文档：`README.md`、`docs/`、`skills/tsq/`
- 可运行的例子：`examples/`
