# 代码生成管线

从 Go 源码到磁盘上的生成物，一条线：

```
Go 源文件
  │  go/ast 解析、字段与嵌入解析          internal/parser/{package,struct,field}.go
  ▼
`//tsq:` 指令行
  │  逐行解析、索引命名、排序    internal/parser/directive.go
  ▼
genmodel.StructInfo / TableMeta        internal/genmodel/model.go
  │  校验                               internal/cmd/gen.go
  │  模板渲染                           internal/cmd/*.go.tmpl
  │  DDL 推导（用 go/types 看真实类型）  internal/cmd/ddl_render.go
  ▼
*.tsq.go / *.result.tsq.go / runtime.tsq.go / tsq.json / {mysql,postgres,sqlite}.sql
```

## 两个 CLI 子命令

- `tsq gen <package>`（`internal/cmd/gen.go`）：生成全部产物。
  - `--dry-run`：在内存里渲染，打印哪些文件会变，不落盘。
  - `--check`：在内存里渲染，与磁盘比对，不一致就非零退出。**`make gen-check` 用的就是
    它**——不要退回到 `git diff` 判断生成物是否同步，那会对每一波正当改动都误报。
  - `-v`：打印每个生成文件路径。

## 注解：`//tsq:` 指令

一行一个关注点，`//go:` 那种形态。gofmt 不碰这类指令行——**这就是它取代旧 DSL 的原因**：旧的
`@TABLE(...)` 写在 doc comment 里，gofmt 会重排缩进，于是生成器不得不自带一个 `tsq fmt` 去把它
排回解析器要的样子。

`internal/parser/directive.go` 认得这些：

| 指令 | 含义 |
| --- | --- |
| `//tsq:table [name=X] [pk=Field] [assigned]` | 声明物理表，每个表结构体一次 |
| `//tsq:result [name=X]` | 声明投影结构体，每个 result 一次 |
| `//tsq:managed role[=Field] ...` | `version` / `created_at` / `updated_at` / `deleted_at` |
| `//tsq:unique 字段[,字段] [name=X]` | 唯一索引，可重复 |
| `//tsq:index 字段[,字段] [name=X]` | 普通索引，可重复 |
| `//tsq:search 字段[,字段]` | 参与关键字搜索的字段 |

- 所有字段名都是 **Go 字段名**，不是 SQL 列名。
- `pk` 默认 `ID` 且自增；`assigned` 关掉自增（调用方自己给值）。
- 索引没写 `name=` 时由 `derivedIndexName` 按 `ux`/`idx` 前缀加表名推出来——**索引名是生成物的
  一部分，改这个推导规则会让使用者已经建好的索引对不上**。

指令是使用者手写的，所以**解析器接受或拒绝什么，就是使用者能写什么**。改这里必须同步
`skills/tsq`——`make skill-check` 的 `dsl` 触发器盯着这一条。

## 错误定位

每条指令在收集时就记下 `comment.Slash` 对应的 `token.Position`，出错时报成
`文件:行:列: invalid //tsq: directive: <那一行原文>: <原因>`，并且 `errors.Is(err, parser.ErrInvalidDirective)`。
一行一个关注点，所以"哪一行"不用再算——**不要把指令改成跨行的形态**，那会把定位问题带回来。

## 生成物的形态

- `<struct>.tsq.go`：结构体 `XxxTable`（内嵌 `*tsq.TableOf[Xxx, 主键类型]`，每列一个字段）、构造函数
  `newXxxTable()`（`NewTable` → 列 → `Define`，含 `ColumnSpecs` 与 `Indexes`）、变量 `TableXxx`、改绑全部列的
  `As` / `WithDeleted`、**每个唯一索引**的 `GetBy<字段>` 与 `FetchBy<字段>`（后者转发 `TableOf.FetchBy`；
  可空或切片类型的末字段不生成），以及转发到表的行方法。主键查询在库里（`TableOf.Get/Find/Fetch/Query`），
  不生成。普通索引和唯一索引的前缀**不生成查询**（理由见 `memory.md`）；模板里没有软删除过滤，作用域在库里。
  列字段名的保留字见 `architecture.md` § 表描述符。
- 可空字段（指针、`sql.NullX`、nullbio，即"可扫描、带 `Valid bool` 和唯一数据字段"的结构体）生成
  `tsq.NewNullColumn[值类型]`；值类型由 `generation_plan.go` 的 `resolveNullValues` 用 `go/types` 算出，
  按生成文件的导入别名写进 `FieldInfo.NullValue`（`time.Time` → `tsqtime.Time`，因此
  `NeedsGeneratedTimeImport` 也看 `NullValue`）。库里的 `nullableValueType` 是同一条规则的反射版，两边要一起改。
- `<result>.result.tsq.go`：结构体 `XxxResult`（每字段一个 `tsq.ResultColumn`）、变量 `ResultXxx` 与它的
  `Columns()`；源列写成 `TableYyy.Field`（`normalizeResultColumns`），可空字段用 `tsq.MapIntoNull`。
- `runtime.tsq.go`：只有 `TSQTables() []tsq.Table`。
- 物理 schema（`genmodel.SchemaColumn`）需要 `go/types` 的真实类型，所以不由解析器填，而是
  `generation_plan.go` 在渲染表之前用 `ddlTypeResolver` 补进 `StructInfo.Schema`。
- 托管列的"什么时候盖时间戳"不在模板里，在库的 `rows.go`：模板只声明哪一列扮演哪个角色。
- `db` 标签的 `default:` / `generated:` 变成 `SchemaColumn.Fill` / `Generated`，模板写进 `TableSpec.Schema`
  （`FillRef` 输出完整的 `tsqdialect.Fill*` 常量名）；库在 `Define` 时按它设置 `columnCore.fill`，写入路径据此
  跳过列。列定义的 DDL 只有 `dialect.ColumnDefinitionSQL` 一份，生成器和运行时共用。
- 模板不许拼接符号名（`tsqdialect.Kind{{ .Kind }}`）；`columnKindRef` 这类 helper 写出完整
  名字，`generated_symbols_test.go` 才能核对它真的存在。

## 校验发生在渲染之前

`gen.go` 在渲染前跑一串校验，每一条都是为了让错误在生成期爆掉而不是在使用者的编译期或
运行期：

- `validatePrimaryKeyField` / `validateVersionField`：主键和乐观锁字段存在且类型可用。
- `validateFieldDatabaseCompatibility` / `validateFieldDatabaseType`：字段类型能映射到
  DDL 类型；SQL 关键字冲突的列名被挡住。
- `validateGeneratedFilenameCollisions`：两个结构体不会生成到同一个文件。
- `validateIndexNameCollisions`：索引名在包内唯一。
- `validateGeneratedSymbolCollisions`：生成的标识符不会互相覆盖。
- `validateResultFields` / `isScanCompatible`：`//tsq:result` 的字段能从来源列 scan 出来。
- `validateIdentifierLengths`：表名、列名、索引名对三个方言都不超长。派生索引名最容易超，报错直接给出
  `//tsq:index ... name=` 的写法；运行时构造时还会再查一遍，但那时已经是部署之后。

这些校验看的都是**解析结果**，没有一条看生成出来的 Go 能不能编译。那一层由两道测试守着：
`generated_symbols_test.go` 核对模板里每个 `tsq.X` / `tsqdialect.X` 真实存在，
`TestGeneratedCodeWithDatabaseSQLFieldsCompiles` 在临时模块里真的 `go build`。生成器测试的临时模块
用 `genTestModuleFile` 把 `replace` 指到**仓库根**——它曾经指到 `internal/cmd`，那时候没有任何测试
真正编译过生成物。新增一种字段类型或导入别名，就往后一个测试的模型里加一个字段。

## DDL 推导

`ddl_render.go` 不靠 AST 猜类型，它用 `golang.org/x/tools/go/packages` 加载**真实的**
类型信息（`ddlTypeResolver`）。原因是类型别名、嵌入结构体和自定义 codec 类型光看 AST
看不出来。

- `classifyDDLColumnTypeRecursive` 顺着类型链往下找基础类型，遇到实现了
  `driver.Valuer` / `sql.Scanner` 的类型就停下来——**这类类型 TSQ 推不出 DDL 列类型，
  使用者必须写 `db:"...,type:JSON"` 之类的显式覆盖**。
- `parseDDLTagOptions` 解析 `db` tag 上的 `size:`、`type:` 等选项。
- `normalizeDDLStringSize` 给字符串列一个合理的默认长度。
- 加载生成物本身会形成循环（生成物引用还没生成的符号），`buildDDLGeneratedFileOverlay`
  用 overlay 把它们从加载里摘掉。

## 生成文件命名

`.tsq.go` / `.result.tsq.go` / `runtime.tsq.go`，复合扩展名（对齐 `.pb.go` 的生态惯例）。
常量在 `internal/parser/constants.go` 的 `TSQFileSuffix`。v4.3.0 之前是 `_tsq.go`——
改过一次，代价是所有使用者的 `.gitignore`、Makefile 和 CI glob 都要跟着改。**再改一次
之前先想清楚值不值。**

## 生成文件头带版本号

首行是 `// Code generated by tsq-vX.Y.Z. DO NOT EDIT.`，`tsq.json` 里也有
`generated_by` 和 `version`。版本号从 `internal/buildinfo` 传导过来，所以：

**改了版本号就必须重新生成示例**，否则 `make gen-check` 和 `make release-check` 都会失败。
`script/release.py` 依赖这一点来保证 tag 和生成物是同一个版本。

生成用的是 `make build-gen` 产出的 `bin/tsq-gen`，**故意不带 `$(LDFLAGS)`**。带 ldflags 的
`bin/tsq` 的版本号来自 `git describe`，那既不是即将发布的版本，也会随工作区干不干净而变——
生成结果必须只依赖源码。两个二进制的分工不要合并，理由见 `memory.md` 2026-08-21 那条。
