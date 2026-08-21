# 变更影响清单

**开始改之前，逐条扫一遍触发器，命中的那条整条做完。** 这里的每一条要么已经在真实事故里
被踩过，要么是 `make harness` 会拦下但拦得比这里晚得多的东西——在这里发现比在门禁上发现
省一轮返工。

有 `[门禁]` 标记的，`make harness` 会兜底；没有标记的只有这份清单兜着。

---

## 改了根包里任何导出的符号

- `make api-snapshot` 刷新 `references/api-surface.txt`。`[门禁: api-check]`
- 更新 `skills/tsq/`——使用者照着那份技能写代码，新增的 API 要出现在里面，删掉或改签名的
  要从里面消失。`[门禁: api-check 会提示]`
- 更新 `README.md` 和 `docs/` 里出现该符号的地方。
- 在 `CHANGELOG.md` 的 `## [未发布]` 段写一条人话。破坏性变更单独放 `### 破坏性变更`——
  `script/release.py` 靠这个小节名判断要不要跨主版本。
- 破坏性变更还意味着 v5：Go 的语义化导入版本要求改 go.mod 模块路径和全部内部 import。
  见 `release.md`，不要顺手就改。

## 改了查询构建器的阶段（`querybuilder*.go`）

- 阶段接口和具体 builder 的返回值类型必须一起改。**返回值写错，编译期约束会静悄悄松掉**，
  测试不看类型就发现不了。
- `compilefail_test.go` 是"这些调用必须编译失败"的清单，`querybuilder_stages_test.go`
  是阶段转移的清单。新增阶段两边都要加。
- `querybuilder_multi_call_test.go` 覆盖 `Where` / `Search` 只能各调一次这条约束。
- `builderPhase` 只影响错误信息，不是约束来源——别把类型约束改成运行期 if。

## 改了 SQL 渲染或参数绑定

- `sql_render_test.go`、`query_args_test.go` 是黄金输出，改渲染必然改它们。改之前确认
  新输出是**更对**而不是**只是不一样**。
- `sql_render_bench_test.go`、`querybuilder_bench_test.go`、`query_exec_bench_test.go`
  在测热路径。渲染进热路径的字符串拼接要看一眼 bench。
- 参数顺序变了 → `condition_ordering_test.go`。

## 改了校验逻辑

先确定它属于哪一边，这条边界是有意的（见 `architecture.md`）：

- **结构**校验（列属于哪张表、聚合合不合法）→ `query_validation.go` / `query_plan_validate.go`，
  在 `Build()` 时跑。
- **方言能力**校验（CTE、FULL JOIN、行锁）→ `dialect_validation.go`，在**执行**时跑。

把方言校验提前到 `Build()` 会断掉"一个 `*Query` 在多个方言上复用"这个用法。

## 新增或改动方言能力位

- `dialect/dialect.go` 加 `Capability` 常量，**三个方言（mysql / postgres / sqlite）都要
  显式表态**。漏掉一个，默认值会让不支持的方言悄悄放行——那是跑到生产库上才炸的一类错。
- 执行期不支持要返回 `*ErrUnsupportedCapability`，带上能力名和方言名。
- 更新 `skills/tsq` 里"哪条查询能在哪个库上跑"的说明。`[门禁: skill-check dialect]`

## 改了注解 DSL（`internal/parser/`）

- 解析器接受或拒绝什么，就是使用者能写什么。`skills/tsq` 的注解说明必须同步。
  `[门禁: skill-check dsl]`
- 改注解格式还要改 `internal/parser/format.go`（`tsq fmt` 的规范化）和
  `tableinfo.go` 的 `commentLocator`（错误行号映射）。**报错指错行比不报行号更浪费时间。**
- 改索引名推导（`normalizeIndexNames`）会让使用者已经建好的索引对不上。这是 schema 层面
  的破坏性变更，按破坏性变更处理。
- `make examples` 重新生成，`./bin/examples/full-suite` 跑一遍。`[门禁: gen-check]`

## 改了模板（`internal/cmd/*.go.tmpl`）

- 模板决定生成代码长什么样，也就决定了使用者能调用哪些方法。**改模板等于改 API。**
- `make examples` 后看一眼 `examples/academy/*.tsq.go` 的 diff——那就是使用者会看到的变化。
- `skills/tsq` 里凡是提到生成方法名的地方都要同步。`[门禁: skill-check templates]`
- 模板里新用的辅助函数要加进 `template_helpers.go` 并配测试。

## 改了 DDL 推导（`internal/cmd/ddl_render.go`）

- 三个方言的 `.sql` 输出都会变，`tsq.json` 快照也会变。看 diff 确认是预期的。
- 自定义 codec 类型（`driver.Valuer` / `sql.Scanner`）推不出列类型，使用者必须写显式的
  `db:"...,type:..."`。改推导规则前先确认新规则不会让某类类型从"必须显式"变成"猜一个"——
  猜错的列类型在建表那一刻不报错，在写入超长数据那一刻才报错。
- `dialect/ddl_reconcile_test.go` 覆盖运行期对账，生成期变了它可能跟着变。

## 改了生成文件的命名或文件头

- `internal/parser/constants.go` 的 `TSQFileSuffix` 是唯一来源。
- 使用者的 `.gitignore`、Makefile glob 和 CI 都写死了这个后缀。这是破坏性变更，
  要写迁移指引（v4.3.0 那次的写法可以抄）。
- `script/changeset.py` 的 `GENERATED_SUFFIXES` 和 `check_release.py` 的
  `GENERATED_HEADER` 也认这个格式。

## 改了 `internal/buildinfo` 的版本号

- **必须 `make examples`**：生成文件头和 `tsq.json` 都印着版本号。
  `[门禁: gen-check、release-check]`
- 正常情况下不要手改：`make release` 会替你改，并保证四个副本一致。

## 改了 examples/

- `examples/academy/mock.sql` 是手写的 schema 真相源，示例结构体改了它要跟着改。
- `make examples` 重新生成，`./bin/examples/full-suite` 必须能跑通。
- `skills/tsq` 和 `docs/` 里的代码片段是从示例抄的，示例变了片段要跟着变。
  `[门禁: skill-check examples]`
- 三个示例程序各有 `main_test.go`，别只改 `main.go`。

## 改了 harness（`script/`、`Makefile`、CI）

- 门禁的顺序、跳过条件写在 `AGENTS.md` § 验证与交接，那是唯一权威处，技能只引用不复述。
- `SKILL.md` 的命令表和 `release.md` 的流程描述要同步。`[门禁: skill-check harness]`
- 新增门禁要想清楚它在发版波（只改版本号和生成物）里会不会误报——
  `check_change_log.py` 的 `RELEASE_ONLY_FILES` 就是为此存在的。
- `.github/workflows/go.yml` 和本地 `make` 目标是两条独立的真相。改了本地目标名，
  CI 里引用它的地方要一起改（v4.4.1 那次 CI 调了一个不存在的 `make update-examples`）。

## 升级 Go 版本

`go.mod`、`.github/workflows/go.yml` 的 `GO_VERSION` 与 matrix、`Dockerfile`、
`CLAUDE.md` / `AGENTS.md` 里写的版本号，全部一起改。golangci-lint 也要升到兼容版本
（`Makefile` 的 `LINT_BIN` 那行钉死了版本）。

## 加了新的 Go 源文件

- 根包新文件 → `feature-map.md` 要能把人带到它。`[门禁: skill-check library]`
- 有导出符号 → `make api-snapshot`。`[门禁: api-check]`
- 配套的 `_test.go` 文件名要么对应一个特性，要么对应被测文件，没有第三种。
