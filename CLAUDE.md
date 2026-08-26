# TSQ

类型安全的 SQL 查询构建器与代码生成器。三个交付物共用一个版本号：库在仓库根包，
CLI 生成器在 `./cmd/tsq`，可运行示例在 `./examples`。

此文件是编码智能体的入口点，并且**没有自身的规则**——它仅用于路由。规则在 `AGENTS.md`，
工程上下文在项目技能里，推理过程在项目内存里；复制到第二个文件里的规则是会发生漂移的规则。

## 改代码前阅读

@AGENTS.md

如果该导入没有把文件带进上下文，请立即阅读 `AGENTS.md`——它是有约束力的规则集，下方没有
任何内容可以替代它。

然后调用 **`tsq-dev`** 技能（住在 `.agents/skills/tsq-dev`，由 `.claude/skills/tsq-dev` 符号链接暴露）。
这是项目记录下来的上下文，任务可以从已知内容开始，而不是从源码里重新考古：

| 文件 | 解答 |
| --- | --- |
| `references/change-impact.md` | **"这个改动还会触及什么？"**——对你匹配的每个触发器逐条处理 |
| `references/feature-map.md` | "X 在哪实现的？"——关注点 → 文件 |
| `references/architecture.md` | 分层、查询阶段机、执行路径、方言、运行时 |
| `references/codegen.md` | 注解 DSL、模板、生成物、DDL 推导 |
| `references/release.md` | 发版、版本号传导、tag 与 Go Proxy 的坑 |
| `references/memory.md` | 为什么会这样：事故、根本原因、决定、死胡同 |
| `references/api-surface.txt` | 对外 Go 符号的当前全集（生成物） |

优先阅读匹配的上下文是在这里工作的预期方式。如果你必须读源码才能知道技能本该告诉你的事，
交接前把它补进去——`AGENTS.md` § 技能维护是变更的一部分，是有约束力的。

## 两份技能，不要弄混

- `.agents/skills/tsq-dev` —— 给**开发本仓**的人和 agent。架构、代码地图、耦合清单、发版
  流程、项目内存。**你现在用的是这份。**
- `skills/tsq`（仓库根，随发布分发）—— 给**在别的项目里使用 TSQ** 的人和他们的 agent。
  注解语法、CLI 用法、查询 API。改了使用者看得见的东西，这份必须跟着改。

前者描述实现，后者描述契约。内容不许互相复制。

## 向下滚动前值得了解的不可妥协项

- 查询构建器是**阶段式**的，约束来自 Go 类型系统而不是运行期检查。`Where(...)` 和
  `Search(...)` 每条链最多各一次，这是编译期强制的——不要"改进"成运行期 if。
- `Build()` 只校验结构；**方言能力在执行时才校验**。这条边界是有意的，它让一个 `*Query`
  能在多个方言上复用。
- 生成物（`*.tsq.go`、`tsq.json`、`*.sql`）不是源码，不要手改。改结构体、注解、模板或
  解析器，然后 `make examples`。生成物是否同步用 `make gen-check`，不要用 `git diff`。
- 生成文件头印着版本号，所以**改版本号必须重新生成示例**。
- `InVar(nil)` 是显式不匹配，`NInVar(nil)` 是显式全匹配。两者都不会静默去掉过滤条件。
- 根包和 `dialect` 的导出符号是这个库的产品。`make api-check` 守着它的快照；它变了就回头看
  `skills/tsq`、`README.md`、`docs/` 还真不真实。
- **代码注释、Go doc 和 `skills/tsq` 用英文**（读者是全世界，`make doc-check` 守着）；
  README、`docs/`、`CHANGELOG.md`、`CONTRIBUTING.md`、`AGENTS.md`、本文件和
  `.agents/skills/tsq-dev` 用中文（读者是这个项目的人）。提交信息用英文
  Conventional Commits。
- 新机器先跑一次 `make hooks`。编辑时跑 `make fmt`，交接前跑 `make harness`——它的权威顺序
  只写在 `AGENTS.md` § 验证与交接。
- 每波工作都以项目内存里一条带日期的记录、被改动导致失效的技能文件更新，以及一条讲清楚的
  提交信息结束；`make memory-check`、`make skill-check` 和 `commit-msg` 钩子分别守着这三项。
- 发版跑 `make release`。**tag 推送之后不可撤销**——Go Proxy 永久缓存内容哈希，发错了用
  `retract` 加新补丁版本，不要删 tag 重打。
