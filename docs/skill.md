# TSQ Agent Skill

本仓库除了 `github.com/tmoeish/tsq/v5` 这个 Go module，还发布一份可安装的 agent 技能：

```txt
skills/
  tsq/
    SKILL.md          路由表 + 最容易写错的规则
    references/       按主题拆开的参考，agent 按需读其中一两份
```

这份技能给**在别的 Go 项目里使用 TSQ** 的 coding agent 用。`docs/` 是给浏览本仓库的人看的索引；
随技能安装、被 agent 读取的参考都在 `skills/tsq/` 下。

## 推荐：装进项目

项目级安装是最推荐、也最常见的做法。在使用 TSQ 的 Go 项目根目录运行：

```bash
gh skill install tmoeish/tsq skills/tsq --dir .agents/skills
```

装好后的位置：

```txt
<project-root>/.agents/skills/tsq/
  SKILL.md
  references/
```

显式指定 `.agents/skills/` 的理由：

- 它是很多 agent（GitHub Copilot、Cursor、Codex、Gemini CLI 等）都认的项目级技能目录；
- 一份项目内的副本服务所有 agent 和协作者，不会出现各 agent 各一份、互相漂移的情况；
- TSQ 的指引跟着需要它的项目走，不影响无关项目；
- 安装位置不依赖 CLI 的默认 agent 或交互式选择，总是可预期的。

`--dir` 相对当前目录，所以要在目标项目根目录运行。命令会在安装的技能里记下 GitHub 来源，之后可以
`gh skill update`。

## 更新已安装的技能

```bash
gh skill update tsq --dir .agents/skills --dry-run   # 只预览
gh skill update tsq --dir .agents/skills             # 更新
gh skill update --all --dir .agents/skills           # 更新该目录下所有技能，不提示
gh skill update tsq --dir .agents/skills --unpin     # 用 --pin 装的技能平时会被跳过，这样解除并更新
```

`gh skill update` 依赖 `gh skill install` 写下的来源信息。手工复制的技能没有它，只能重新复制或重新
`gh skill install`。

**技能版本要和项目 `go.mod` 里的 TSQ 版本一致**：技能描述的是那个版本的契约。升级 TSQ 时一起更新技能，
或者用下面的 `@vX.Y.Z` 钉住同一个版本。

## 其他安装范围和位置

TSQ 主要面向 GitHub Copilot、Claude Code 和 Gemini CLI，但上面的共享项目目录适用于任何遵循 Agent Skills
约定的 agent。某个 agent 需要自己的项目目录时，让 `gh skill install` 去解析：

```bash
gh skill install tmoeish/tsq skills/tsq --agent github-copilot --scope project
gh skill install tmoeish/tsq skills/tsq --agent claude-code --scope project
gh skill install tmoeish/tsq skills/tsq --agent gemini-cli --scope project
```

写这份文档时，GitHub Copilot 和 Gemini CLI 的项目范围解析到 `.agents/skills/tsq/`，Claude Code 解析到
`.claude/skills/tsq/`。显式 `--dir` 仍然是不依赖各 agent 解析规则、最清楚的选法。

只有想让当前用户的每个项目都能用时才装到用户范围：

```bash
gh skill install tmoeish/tsq skills/tsq --agent github-copilot --scope user
gh skill install tmoeish/tsq skills/tsq --agent claude-code --scope user
gh skill install tmoeish/tsq skills/tsq --agent gemini-cli --scope user
```

常见的用户级位置有 `~/.copilot/skills/tsq/`、`~/.claude/skills/tsq/`、`~/.gemini/skills/tsq/`、
`~/.agents/skills/tsq/`，具体取决于 agent 和 CLI 版本；`gh skill list` 可以查看。

## 预览和钉住版本

```bash
gh skill preview tmoeish/tsq skills/tsq
gh skill install tmoeish/tsq skills/tsq@v5.0.0 --dir .agents/skills   # 钉在某个发布版本
```

不写版本时，`gh skill install` 取最新的 tag，没有就退回默认分支。`--pin v5.0.0` 是等价写法。

## 从本地克隆安装

```bash
gh skill install /path/to/tsq tsq --from-local --dir .agents/skills
```

文件是复制而不是软链，装好的位置同样是 `<project-root>/.agents/skills/tsq/`。

## 手工安装

不想用 `gh skill install` 时，把整个 `skills/tsq` 目录复制到目标项目的技能目录，推荐
`<project-root>/.agents/skills/tsq/`；有的 agent 也认 `.github/skills/tsq/`、`.claude/skills/tsq/`。
复制后按 agent 自己的方式重新加载技能（GitHub Copilot CLI 一般是 `/skills reload`、`/skills info tsq`）。
手工副本没有自动更新所需的来源信息。

## 使用

安装后 agent 通常根据技能的 `description` 自动启用它；也可以在提示里点名：

```txt
Use the /tsq skill to add TSQ to this Go service.
```

会触发它的任务：把 TSQ 加进 Go 项目；写 `//tsq:` 指令；跑 `tsq gen`；初始化 `tsq.Runtime` 和 schema
策略；写查询、增删改、批量写、按条件写、分页和搜索；事务与重试；方言差异和 TSQ 返回的错误；从 v4 升级。

## 技能里有什么

`SKILL.md` 是一张路由表加一组最容易写错的规则，参考按主题拆成十五份，agent 只读任务需要的那一两份：

| 文件 | 讲什么 |
| --- | --- |
| `references/quickstart.md` | 从零到第一条查询 |
| `references/concepts.md` | 心智模型：表、行、结果、参数、阶段、两个校验点 |
| `references/cli.md` | 安装与钉版本、`tsq gen` / `tsq version` 的全部 flag 和退出码、生成的 `.sql` 迁移段、`tsq.json` |
| `references/annotations.md` | 全部 `//tsq:` 指令和 `db` / `tsq` / `json` tag 选项、DDL 类型推导 |
| `references/generated-code.md` | 生成的表、列、结果和 `TSQTables()`；列与表的类型层级；手写表 |
| `references/runtime.md` | `Open` / `NewRuntime`、DSN 要求、schema 策略、日志、SQL 日志、tracer、`WrapExecutor` |
| `references/queries.md` | 构建器、读方法、按键读取、`ListIn`、`AttachMany`、阶段类型 |
| `references/expressions.md` | 谓词、值与参数、可空列、列函数、`CASE`、`MapInto`、会自己说出改法的编译错误 |
| `references/advanced-queries.md` | 别名与 `Rebind`、分组、子查询、相关子查询、CTE、集合操作、行锁 |
| `references/paging-search.md` | `Page`、`PageRequest`、游标分页、关键词搜索、全文检索 |
| `references/writes.md` | 单行与批量写、upsert、软删与硬删、按条件写、托管列、乐观锁 |
| `references/transactions.md` | `WithTx` / `WithTxResult`、选项与重试、嵌套、回滚不回滚什么 |
| `references/dialects.md` | 三个引擎的能力矩阵、能力检查、各引擎的特有行为 |
| `references/errors.md` | 每个错误的含义和处理 |
| `references/migrating-from-v4.md` | v4 → v5 的对照表 |

`make doc-check` 守着两件事：技能里是英文；以及技能**覆盖全部对外表面**——根包和 `dialect` 的每个导出符号、
CLI 的每个 flag、每条指令、每个托管角色、每个 `db` tag 选项，少了任何一项都会红。

## 文档的分工

1. `docs/skill.md`（本页）讲安装、使用和这份技能的结构，读者是浏览本仓库的人。
2. `skills/tsq/` 是随技能安装的技术参考，是"怎么用 TSQ"的**唯一实质来源**；`docs/`、README 只做索引和
   总览，不复述它——复述过的副本曾漂移成 `tsq gen` 会拒绝的语法。

## 为什么技能放在 `skills/tsq/`

1. Agent Skills 的模型是一个技能一个目录；
2. `gh skill install` 能自动发现它；
3. 参考文件能和 `SKILL.md` 一起打包安装；
4. 装好的 agent 不需要仓库里的 `docs/` 就能理解 TSQ。

所以本仓库既是 TSQ 的源码仓库，也是发布 TSQ 技能的仓库。
