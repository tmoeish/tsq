---
name: tsq-dev
description: 开发 TSQ 仓库本身时加载：查询构建器、代码生成器、方言层和 harness 的工程上下文与约定。承载架构、代码地图、代码生成管线、变更影响清单、发版流程和项目内存，让任务从记录下来的上下文开始，而不是从源码考古开始。与面向 TSQ 使用者的 `tsq` 技能是两回事——那份教别人怎么用这个库，这份教你怎么改这个库。
license: MIT
metadata:
  repository: github.com/tmoeish/tsq
  module: github.com/tmoeish/tsq/v4
---

# tsq-dev

**这份技能是给开发 `github.com/tmoeish/tsq` 这个仓库的 agent 和开发者用的。**

如果你要在**别的**项目里使用 TSQ——写 `@TABLE` 注解、跑 `tsq gen`、构建类型安全查询——
那是仓库根目录 `skills/tsq` 那份技能的事，不是这份。两份技能的读者不同，内容不许互相
复制：`skills/tsq` 描述**契约**（使用者看得见的东西），本技能描述**实现**（怎么改它）。

TSQ 由三件东西组成，它们共用一个仓库和一个版本号：

- **库**：仓库根包 `tsq`，提供阶段式查询构建器、执行器、事务和运行时。
- **生成器**：`./cmd/tsq` CLI，读结构体上的 `@TABLE` / `@RESULT` 注解，产出类型化的列
  元数据、CRUD helper 和 DDL。
- **示例**：`./examples`，可运行的契约。`examples/academy` 的生成物是**被提交的**，
  它同时是回归测试和文档里代码片段的来源。

## 上下文查找 — 看代码之前，先读与任务匹配的那行

| 任务 | 优先读 |
| --- | --- |
| 任何改动 | `references/change-impact.md` — 耦合清单 |
| "X 在哪实现的？" | `references/feature-map.md` |
| 分层、查询阶段机、执行路径、方言 | `references/architecture.md` |
| 注解 DSL、模板、生成物、DDL 推导 | `references/codegen.md` |
| 发版、版本号、tag、Go Proxy | `references/release.md` |
| "为什么是这样？"、过去的事故、死胡同 | `references/memory.md` |
| 对外 Go 符号的当前全集 | `references/api-surface.txt`（生成物，`make api-snapshot` 重写） |
| 有约束力的规则 | `AGENTS.md`（仓库根） |
| 使用者看到的契约 | `README.md`、`docs/`、`skills/tsq/`（仓库根） |

**用这些文件，不要靠猜文件名再从源码重构设计。** 其中某份错了或者少了你不得不自己弄清楚
的东西，在同一波里补上——见下面的*维护这份技能*。

## 工作流

1. 读 `AGENTS.md`，读本技能里覆盖你要碰的领域的上下文文件，读 `references/memory.md`，
   然后才读附近的代码和测试。
2. 改手写源码。生成物（`*.tsq.go`、`*.result.tsq.go`、`tsq.json`、`*.sql`）不是源码：
   改结构体、注解、模板或解析器，然后重新生成。
3. 编辑期间跑窄范围的检查和 `make fmt`。
4. 动了生成器、模板、解析器或示例，跑 `make examples` 并跑 `./bin/examples/full-suite`。
5. 补上聚焦的测试。这个库的测试是它唯一的安全网——没有集成环境可以兜底。
6. 把这波教会你的东西写进本技能对应的文件（见下），把使用者看得见的变化写进
   `skills/tsq`、`README.md`、`docs/` 和 `CHANGELOG.md` 的未发布段。
7. 交接前跑 `make harness`。它的权威顺序只写在 `AGENTS.md` § 验证与交接，本技能只引用
   不复述。
8. 提交（`commit-msg` 钩子校验提交信息），需要发版时跑 `make release`。

## 维护这份技能

这份技能是本项目的上下文存放处。只有当每一波变更都让它比之前更真实，它才有用。
**维护它是变更的一部分，不是后续工作。**

把学到的东西路由到正确的文件——一个事实，一个归宿：

| 你学到了什么 | 去哪 |
| --- | --- |
| bug 的根本原因；为什么看起来对的修法是错的；值得不再重复的死胡同；不明显的运行时行为；一个决定及其理由 | `references/memory.md`，带绝对日期 |
| **发现了但决定暂不处理的问题**——现象、为什么现在不值得动、什么条件下该动 | `references/memory.md`，`已知未处理：` 开头；要排期就再开 GitHub issue |
| **发现了但决定暂不处理的问题**——现象、为什么现在不值得动、什么条件下该动 | `references/memory.md`，带绝对日期；要排期就再开 GitHub issue |
| "改 A 也得改 B"——尤其是你靠弄坏它才发现的 | `references/change-impact.md` |
| 新文件、新入口、职责搬家、新的 CLI 子命令或 flag | `references/feature-map.md` |
| 新组件、新的阶段类型、新方言能力、新的执行路径 | `references/architecture.md` |
| 注解 DSL 的新键、模板结构、生成文件命名、DDL 类型推导规则 | `references/codegen.md` |
| 发版流程、版本号传导、tag 与 Go Proxy 的坑 | `references/release.md` |
| 必须对所有未来变更成立的规则 | `AGENTS.md`（规则住那儿，别复制过来） |
| 使用者需要知道的事 | `skills/tsq/`、`README.md`、`docs/`、`CHANGELOG.md` |

保持它可用的规则：

- **学到的当场写下来。** 你花时间从源码里弄明白的事，不现在记下来，下一个 agent 会花
  同样的时间再来一遍。
- **一个事实，一个归宿。** 复制到第二个文件里的事实注定漂移。交叉引用，不要重复；
  `AGENTS.md` 拥有规则，本技能拥有上下文，`skills/tsq` 拥有对使用者的说明。
- **就地更正，删掉变错的。** 永远不要追加自相矛盾的内容。过时的上下文文件比缺失的更糟，
  因为人会相信它。
- **记录耦合，不要记流水账。** "改 `X` 必须同时改 `Y`，因为 Z" 可复用；"我今天下午重构了
  X" 不可复用。
- **同一类问题被修两次，说明第二次修得也不彻底。** 直到 `change-impact.md` 或 `memory.md`
  阻止了第三次发生，那次修复才算完成。
- `make skill-check` 把最容易忘的几条耦合钉死了；它没覆盖的部分靠这一节。
- **内存里的条目是有寿命的。** 一件事了结之后那条记录是删是留，判据只有一条：删掉之后
  有人会不会重犯、或者重新调查一遍。搁置项处理完就删，事故根因在有门禁挡着之后压成一行，
  决定和死胡同永久保留。**不要新增"XX 已处理"的条目**——那是成本翻倍而信息量没变。
  完整的分类表在 `memory.md` 开头，`make memory-check` 守着行数上限。

## 命令

```bash
make fmt              # go fix + golangci-lint fmt + 自动修复
make lint             # golangci-lint run
make test             # go test ./...
make test-race        # -race -shuffle=on
make examples         # 重新生成 examples/academy 并编译三个示例程序
make gen-check        # 生成物是不是当前源码的输出（tsq gen --check）
make api-check        # 对外 Go 契约有没有偏离快照
make api-snapshot     # 刷新快照
make skill-check      # 技能有没有跟上代码
make memory-check     # 这波有没有留下项目内存，以及内存文件有没有超出行数上限
make release-check    # 版本号四个副本一致
make harness          # 交接前的全部确定性门禁
make release          # 发版（改版本号 → CHANGELOG → 重新生成 → harness → 提交 → tag → push）
make hooks            # 装 commit-msg 钩子，每台机器一次
```

## 生成的文件（不要手改）

- `examples/academy/*.tsq.go`
- `examples/academy/*.result.tsq.go`
- `examples/academy/runtime.tsq.go`
- `examples/academy/tsq.json`
- `examples/academy/{mysql,postgres,sqlite}.sql`
- `.agents/skills/tsq-dev/references/api-surface.txt`

`examples/academy/mock.sql` 是**手写的**——它是示例库的 schema 真相源，和示例结构体必须
保持一致。
