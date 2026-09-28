# 变更影响 — 代码生成与示例

处理你匹配的每个触发器；索引与 `[门禁]` 标记的含义在 `../change-impact.md`。

## 改了注解指令（`internal/parser/directive.go`）

- 解析器接受或拒绝什么，就是使用者能写什么。`skills/tsq` 的注解说明必须同步。
  `[门禁: skill-check dsl]`
- **不要把指令写成跨行的形态。** 一行一个关注点是这套语法唯一的好处来源：gofmt 不碰它，错误可以
  直接引用那一行，于是既不需要格式化器也不需要把偏移量映射回行号。
- 改索引名推导（`normalizeIndexNames`）会让使用者已经建好的索引对不上。这是 schema 层面
  的破坏性变更，按破坏性变更处理。
- `make examples` 重新生成，`./bin/examples/full-suite` 跑一遍。`[门禁: gen-check]`
- 改完之后确认 `examples/academy/*.tsq.go` 的 diff 为空：指令和旧 DSL 表达同一件事时，生成物应当
  逐字节相同。那是语义等价最直接的证据。

## 改了模板（`internal/cmd/*.go.tmpl`）

- 模板决定生成代码长什么样，也就决定了使用者能调用哪些方法。**改模板等于改 API。**
- `make examples` 后看一眼 `examples/academy/*.tsq.go` 的 diff——那就是使用者会看到的变化。
- `skills/tsq` 里凡是提到生成方法名的地方都要同步。`[门禁: skill-check templates]`
- 模板里新用的辅助函数要加进 `template_funcs.go` 并配测试。
- **生成代码里出现的 `tsq.X` / `tsqdialect.X` 必须是那两个包真实导出的符号。** 模板和 helper 里
  的字符串不参与本包的类型检查，写错了要到使用者自己的工程里才炸；断言"发出了这个字符串"的
  单元测试证明不了这一点。`[门禁: internal/cmd/generated_symbols_test.go]`
- **生成代码里的使用者类型，拼写和 import 必须同一个来源**：类型写 `FieldInfo.Spelled`（`go/types` 按本文件
  的别名限定），import 写 `StructInfo.Imports`，两个模板（table、result）都要写 import 块。
  `examples/academy` 只覆盖同包和标准库类型，**新增一种字段形状就往 `gen_test.go` 的 `shapeModule` 里加一个
  字段**——那个测试真的 `go build` 生成物。`[门禁: TestGeneratedCodeCompilesForEveryFieldShape]`
- 生成函数的参数名（`fieldVarName`）不能和函数自己用的标识符撞：`ctx`、`db`、接收者 `t`、函数体调用的
  `tsq`，都在 `generatedIdentifiers` 里。给生成函数加参数或在函数体里用新的包名，就把它加进去。
- 模板在包级声明的每个名字都登记在 `generatedSymbols` / `runtimeSymbol`（`gen.go`）：生成器靠它拒绝和包里手写
  声明、其他表生成的名字相撞。给模板加一个包级类型、变量或函数，就把它加进去。`[门禁: TestGenRefusesNamesThePackageAlreadyUses]`

## 改了生成的表声明（`table.go.tmpl`、`TableOf.Define`）

- 列只能是 `TableXxx` 的字段：**不要重新生成包级列变量**，也不要把建表挪进 `init()`。初始化顺序
  正确的唯一原因是"取列必经 `TableXxx`"。
- 给 `TableOf` 加导出方法，或给模板加生成方法：同名的列字段从此非法。普通方法反射自动覆盖；
  **泛型方法要加进 `reserved.go` 的 `genericTableMethods`**，生成方法加进 `reservedTableFields`。
  `[门禁: internal/cmd/reserved_test.go 的 TestReservedTableNamesCoverTableOf]`
- 模板在**行类型**上声明的方法（`Insert`、`Update`、`HardDelete`，软删除表再加 `Delete`、`Restore`、`Active`）
  列在 `reserved.go` 的 `rowMethods` 里，同名字段报错。给行类型加方法要加进去，
  `TestRowMethodsMatchTheTemplate` 按模板核对。行方法的参数（`ctx`、`db`、`cols`）和 `tsq` 包名不能被接收者占用，
  见 `receiverName`。
- 新增一种生成文件（像 `runtime.tsq.go` 这样不按结构体命名的），把名字加进 `validateGeneratedFilenameCollisions`
  的初始集合，否则同名结构体的文件会被它覆盖。
- 生成的方法名、参数名进 `validateGeneratedSymbolCollisions` 的清单和 `gen_test.go` 的断言；
  改了形状要 `make examples` 并看 `examples/academy/*.tsq.go` 的 diff。

## 改了 DDL 推导（`internal/cmd/ddl_render.go`）

- 三个方言的 `.sql` 输出都会变，`tsq.json` 快照也会变。看 diff 确认是预期的。
- 自定义 codec 类型（`driver.Valuer` / `sql.Scanner`）推不出列类型，使用者必须写显式的
  `db:"...,type:..."`。改推导规则前先确认新规则不会让某类类型从"必须显式"变成"猜一个"——
  猜错的列类型在建表那一刻不报错，在写入超长数据那一刻才报错。`[门禁: TestGenRefusesToGuessACodecColumnType]`
- 显式 `type:` 分支的可空性必须和 Go 侧 `NullColumn` 的判据（`nullableValueType`）一致；两边各判一次就会
  出现"Go 能写 NULL、列是 NOT NULL"。
- **生成期要替数据库先说出它建表时才会拒绝的东西**：自增主键必须是整数（`validateAutoIncrementKey`，报错）；MySQL
  索引键 3072 字节、`TEXT` 不能进索引（`mysqlIndexProblem`，**只警告**并在 `mysql.sql` 写注释——只对一个方言成立的限制
  不能拦住另外两个方言的使用者）。改 MySQL 的字符串类型映射（`VARCHAR` / `MEDIUMTEXT` 的界）要回来看后者。
- `generated` 不带表达式的列属于迁移：`ColumnDefinitionSQL` 拒绝写它（运行期建表因此报错），生成的 `.sql` 用
  `migrationOwned` 留出它。新的 DDL 渲染路径（重建、加列）要同样跳过它。
- 索引的列：唯一索引和普通索引在软删表上以 `deleted_at` 打头（`indexFieldNames`），**全文索引不加**。
  模板（`FieldsToCols`）和快照（`ddl_state.go` 的 `appendIndexes`）两处要一起改。
- 同一列不许被两个字段映射（`validateColumnNames`，大小写不敏感）：解析器把 `A, B string` 拆成两个字段，
  各自带同一个标签。
- `internal/sqldialect/ddl_reconcile_test.go` 覆盖运行期对账，生成期变了它可能跟着变。
- **迁移段是给不会遇错停下的客户端执行的**（`sqlite3` 命令行会在失败的 INSERT 之后照样 DROP、COMMIT）。所以：
  破坏数据的语句一律经 `destructive()` 注释掉（新增一种会删数据的迁移语句也要走它，摘要的警告按 `drop ` 前缀识别）；
  SQLite 重建按"新表、复制、删旧表、改名"并关外键，复制要保证不会失败（`sqliteZeroLiteral` 填新 NOT NULL 列、
  生成列不复制，填不了就整段写成人工处理）；语句按 `compareDDLChanges` 排序渲染，索引先于它指向的列删除。
  `TestGenCmdAppendsSQLiteRebuildDDLForTypeChange` 用真的 `sqlite3` 命令行跑迁移。
- 改了某个方言对一种类型的拼写（`ColumnTypeSQL`，如 MySQL 时间改成 `DATETIME(6)`）：同一方言的读回
  （`parseMySQLColumnType` 等）要同步，旧拼写读成原始类型，`Reconcile` 才会改它、新拼写才不算漂移；依赖类型的 DEFAULT
  也要跟上（MySQL `DATETIME(6)` 只收 `CURRENT_TIMESTAMP(6)`，见 `DefaultSQL`，比较见 `sameDefault`）；迁移历史不会补段，
  `CHANGELOG` 要写明使用迁移文件的项目怎么手工改。
- 迁移历史存在 `tsq.json` 里，不会随生成器修复重新渲染：修了迁移渲染之后，示例里已经写坏的段要重置示例的
  DDL 状态（删掉 `tsq.json` 和三份 `.sql` 再 `make examples`），使用者自己的历史只能手工改。

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

## 改了生成器的构建方式（`make build` / `make build-gen`）

`bin/tsq-gen` **故意不带 `$(LDFLAGS)`**，这样生成结果只依赖源码。给它加上版本 ldflags
会让发版死锁（生成文件头要写还不存在的 tag），也会让同一份源码在脏工作区和干净检出上
生成出不同的文件。`make examples` 和 `script/check_generated.py` 必须用同一个二进制。

## 改了 examples/

- `examples/academy/mock.sql` 是手写的 schema 真相源，示例结构体改了它要跟着改。
- `make examples` 重新生成，`./bin/examples/full-suite` 必须能跑通。
- `skills/tsq` 和 `docs/` 里的代码片段是从示例抄的，示例变了片段要跟着变。
  `[门禁: skill-check examples]`
- 三个示例程序各有 `main_test.go`，别只改 `main.go`。
