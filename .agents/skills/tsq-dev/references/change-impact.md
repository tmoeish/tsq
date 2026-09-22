# 变更影响 — 索引

这里的每条耦合要么已经在真实事故里被踩过，要么是 `make harness` 会拦下但拦得比这里晚得多的东西——在这里发现比在门禁上发现省一轮返工。**按你要动的东西挑一到两份读，并在同一波里把匹配到的每个触发器整条做完**；整份加载没有意义，一波改动通常只落在一两个域里。

子文件里有 `[门禁]` 标记的条目，`make harness` 会兜底；没有标记的只有这份清单兜着。

| 你要动的东西 | 读 |
| --- | --- |
| 对外导出符号、兼容包装、未导出的标记方法、退役的名字、开关与消费点、新 Go 文件 | `impact/api.md` |
| 查询阶段、全文检索、相关子查询、单行读取与分页、可空性、列表参数、SQL 渲染与绑定、LIKE、校验归属 | `impact/query.md` |
| 数据库填值的列、删除语义与托管列、upsert、按条件写、批量写 | `impact/write.md` |
| Runtime 选项、schema 托管、方言能力位与 `Dialect` 钩子、驱动错误分类、执行期日志 | `impact/runtime.md` |
| 注解指令、模板、生成的表声明、DDL 推导、生成文件命名、版本号与生成器构建、examples/ | `impact/codegen.md` |
| ldflags、使用者文档、harness 与 `make fmt`、非测试源码的语言、make 目标、CI 的 job 与工具、升级 Go | `impact/harness.md` |

新学到一条"改了 A 还得改 B"时，写进对应的那一份，并在下面的速查里补一行标题。`make skill-check` 无条件比对速查和子文件的 `##` 标题，漏一行、多一行都会红。

## 触发器速查

下面只是标题，条目在各自文件里。用它确认自己该读哪一份；没有一条匹配上，说明这波改动不带已知耦合。

### `impact/api.md`

- 改了根包里任何导出的符号
- 想给根包加一个"兼容包装"或一个不用接收者的方法
- 给 `Operand` / `ListOperand` / `Pattern` / `Executor` 加或改了未导出方法
- 退役了一个使用者写过的名字或写法
- 新增了一个"开关 + 若干消费点"的特性
- 改了根包导出符号的名字，或在使用者文档里引用了 `tsq.X`
- 加了新的 Go 源文件

### `impact/query.md`

- 改了查询构建器的阶段（`querybuilder.go`）
- 改了全文检索
- 改了相关子查询的作用域传递（`Correlate`、`validateJoinGraph`）
- 改了单行读取（`Get` / `Find` / `Exists`）
- 改了 `Page()` 或构建器级分页
- 改了可空性推导或加了新的表达式构造
- 改了 `ListIn` 或列表参数
- 改了 SQL 渲染、中间表示或参数绑定（`sqlexpr.go`、`param.go`、`query_render.go`）
- 改了 LIKE 谓词的渲染，或改了关键字转义
- 改了校验逻辑

### `impact/write.md`

- 改了数据库填值的列（`Fill`、`default:` / `generated:`）
- 改了删除语义或托管列（`rows.go`、`softdelete.go`、`TableSpec`）
- 改了 upsert（`upsert.go`）
- 改了按条件写语句（`mutation.go`）
- 改了批量写（`rows.go`）

### `impact/runtime.md`

- 加了 Runtime 的构造器或选项
- 改了 schema 托管（`runtime_schema.go`、`runtime_index.go`）
- 给查询加了需要方言能力的构造
- 新增或改动方言能力位
- 给 `Dialect` 接口加了钩子，或改了行写入（`rows.go`）
- 在执行路径上加了一个日志或诊断出口
- 加了或改了 `Capability` 常量
- 改了驱动错误分类（`*_errors.go`、`IsRetryable*`）

### `impact/codegen.md`

- 改了注解指令（`internal/parser/directive.go`）
- 改了模板（`internal/cmd/*.go.tmpl`）
- 改了生成的表声明（`table.go.tmpl`、`TableOf.Define`）
- 改了 DDL 推导（`internal/cmd/ddl_render.go`）
- 改了生成文件的命名或文件头
- 改了 `internal/buildinfo` 的版本号
- 改了生成器的构建方式（`make build` / `make build-gen`）
- 改了 examples/

### `impact/harness.md`

- 加了或改了 `-X` ldflags（`Makefile`、`.goreleaser.yaml`）
- 改了面向使用者的文档（README、`docs/`、`skills/tsq`）
- 改了 harness（`script/`、`Makefile`、CI）
- 想往 `make fmt` 里加自动改写工具
- 在非测试 Go 源码里写了中文
- 改名或删除了一个 make 目标
- 改了 CI 的 job 名字
- 改了 CI 里安装的工具，或它的版本
- 升级 Go 版本
