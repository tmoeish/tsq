# TSQ - 类型安全的 Go SQL 查询代码生成工具

```txt
 _____  __    ____
/__   \/ _\  /___ \
  / /\/\ \  //  / /
 / /   _\ \/ \_/ /
 \/    \__/\___,_\
```

[![GitHub release (latest by date)][1]][2]
[![Build Status][3]](https://github.com/tmoeish/tsq/actions)
[![Go Reference](https://pkg.go.dev/badge/github.com/tmoeish/tsq/v5.svg)](https://pkg.go.dev/github.com/tmoeish/tsq/v5)
[![Go Report Card][4]][5]
[![License: MIT][6]][7]

[1]: https://img.shields.io/github/v/release/tmoeish/tsq
[2]: https://github.com/tmoeish/tsq/releases
[3]: https://img.shields.io/github/actions/workflow/status/tmoeish/tsq/go.yml
[4]: https://goreportcard.com/badge/github.com/tmoeish/tsq
[5]: https://goreportcard.com/report/github.com/tmoeish/tsq
[6]: https://img.shields.io/badge/License-MIT-yellow.svg
[7]: https://opensource.org/licenses/MIT

TSQ（Type-Safe Query）把带 `//tsq:` 指令的 Go 结构体生成为**表元数据、CRUD 助手和类型安全的列**，让你用 Go API 组合 SQL，而不是在业务代码里拼字符串。

- **查询构建器是阶段式的**：`Where` 之后拿到的类型上没有 `Where`，约束来自编译器而不是运行期检查。
- **表是一个值**：生成的 `TableXxx` 内嵌 `*tsq.TableOf[Xxx, 主键类型]`，每列一个字段（`TableXxx.Title`），持有列、主键、托管列、索引和物理 schema；按主键读是 `TableXxx.Get(ctx, db, id)`。你的结构体上不需要实现任何接口。
- **参数按名字绑定、有类型**：`TableCourse.ID.EQ(TableCourse.ID.Param())` 写进查询，执行时传 `TableCourse.ID.Bind(5)`；参数错位、类型不对都编译不过或当场报错。
- **SQL 在执行时按方言渲染**：查询是一棵表达式树，第一次在某个方言上执行时渲染并缓存；方言能力（`FULL JOIN`、行锁、CTE）在渲染时按结构检查。
- **显式运行时**：`Runtime` 持有表声明、方言、日志和 tracer；所有执行方法第一个参数是 `context.Context`，第二个是 `tsq.Executor`（`*Runtime`、事务或任何 `*sql.DB`）。

## 先回答三个上手问题

| 问题 | 最短答案 |
| --- | --- |
| **最小要写什么？** | 一个带 `//tsq:table` 指令的 Go struct。 |
| **生成后得到什么？** | 每个表生成一个 `*.tsq.go`；每个 `//tsq:result` 生成一个 `*.result.tsq.go`。 |
| **怎么跑第一条查询？** | `tsq gen ./db` → `runtime, err := tsq.Open(ctx, "sqlite", dsn, database.TSQTables())` → `query, err := tsq.Select(...).From(table).Where(...).Build()` → `query.List(ctx, runtime, ...)`。 |

## 安装

```bash
go install github.com/tmoeish/tsq/v5/cmd/tsq@latest
```

TSQ 本身不附带数据库 driver；你的应用只需要安装自己实际使用的那个 driver。下面的 quickstart 默认用 `modernc.org/sqlite`，是因为它不依赖 CGO，最适合零配置上手。

也可以从源码构建：

```bash
git clone https://github.com/tmoeish/tsq.git
cd tsq
make build
```

### 安装 TSQ agent skill

这个仓库同时发布了一个可安装的 agent skill，适合 GitHub Copilot、Claude Code、Gemini CLI 等 coding agent 在**别的 Go 项目里**学习如何接入和使用 TSQ。

```bash
gh skill install tmoeish/tsq tsq --agent github-copilot --scope user
```

安装方式、手动复制路径和使用示例见 [`docs/skill.md`](docs/skill.md)。

## 5 分钟最小路径

### 1. 定义一个表结构

```go
package database

//tsq:table
//tsq:search Name,Email
type User struct {
	ID    int64  `db:"id" json:"id"`
	Name  string `db:"name" json:"name"`
	Email string `db:"email" json:"email"`
}
```

### 2. 生成代码

```bash
tsq gen ./database
```

`gen` 接受三种输入：

- 模块导入路径：`github.com/acme/app/internal/database`
- 相对目录：`./internal/database`
- 绝对目录：`/path/to/app/internal/database`

生成后通常会看到：

- `database/user.tsq.go`：`User` 表的列、CRUD、分页和查询助手
- `database/runtime.tsq.go`：当前包全部表的 `TSQTables()` metadata 入口
- `database/*.result.tsq.go`：只在你声明 `//tsq:result` 时生成
- `database/sqlite.sql` / `database/mysql.sql` / `database/postgres.sql`：每种内置方言的 schema 文件；首次生成写入初始建表语句，后续变更会按时间顺序追加带日期注释的增量 DDL
- `database/tsq.json`：最新 schema snapshot、初始 schema 文件内容与增量历史记录，用于后续 `tsq gen` 对账

如果目标文件已经存在，TSQ 只会覆盖**已有的生成文件**；遇到手写文件会拒绝覆盖并直接报错。

DDL 的默认字符串映射现在更偏向“常规业务字段”：

- `string`、`sql.NullString`、`null.String` 以及它们的 type alias / 自定义字符串类型，在**没写 `size`** 时默认生成 `VARCHAR(255)`
- 写了 `size:N` 之后，会按方言选更合适的类型；例如 MySQL 超过 `VARCHAR` 安全范围时会自动切到 `MEDIUMTEXT` / `LONGTEXT`
- 如果字段本身是 TSQ 不认识的自定义类型（例如实现了 `driver.Valuer` / `sql.Scanner` 的 JSON slice），可以直接在 `db` tag 里写 `type:JSON`、`type:TEXT`、`type:JSONB` 这类覆盖；TSQ 会原样写入三个方言的 DDL，并把这个 raw type 记录进 runtime/schema snapshot
- `int` / `uint` 以及基于它们的 enum / type alias 默认按常规整型宽度生成（MySQL `INT`，Postgres `INTEGER`）；只有显式 `int64` / `uint64` 才会落到 `BIGINT`

### 3. 跑第一条查询

```go
package main

import (
	"context"
	"log"
	"log/slog"

	_ "modernc.org/sqlite"

	"github.com/tmoeish/tsq/v5"
	"github.com/your/module/database"
)

func main() {
	ctx := context.Background()

	runtime, err := tsq.Open(
		ctx,
		"sqlite",
		"file:app.db?cache=shared",
		database.TSQTables(),
		tsq.WithSchemaPolicy(tsq.SchemaPolicyCreateMissing),
		// Logger 收 bootstrap DDL 和执行期告警。再加 tsq.WithSQLLogging()，
		// 每条渲染出来的 SQL 和它绑定的参数也会以 debug 级进同一个 Logger；
		// 参数是原样打的，敏感数据别开。
		tsq.WithLogger(slog.Default()),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer runtime.Close()

	query, err := tsq.
		Select(database.TableUser.Columns()...).
		From(database.TableUser).
		Where(tsq.Contains(database.TableUser.Name, tsq.Val("alice"))).
		Build()
	if err != nil {
		log.Fatal(err)
	}

	users, err := query.List(ctx, runtime)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("loaded %d users", len(users))
}
```

更完整的从零到 SQLite 示例见 [`docs/quickstart.md`](docs/quickstart.md)。

`tsq.Open` 自己 `sql.Open` 并按 `driverName` 解析方言；已经有连接池时用 `tsq.NewRuntime(ctx, db, dialect.MySQL, tables, ...)`，`Close()` 不会关掉别人的池。默认策略是 **manual**：TSQ 只记录提醒日志，不会自动建表或补索引。`tsq.WithSchemaPolicy(p)` 同时设置表和索引，`WithTablePolicy` / `WithIndexPolicy` 分开设置。

四档策略，从不做到做得最多：

| 策略 | 做什么 |
| --- | --- |
| `SchemaPolicyManual`（默认） | 什么都不改，只记一条提醒日志。**生产用这个**，schema 交给迁移工具 |
| `SchemaPolicyValidate` | 只校验，对不上就启动失败 |
| `SchemaPolicyCreateMissing` | 建缺失的表、列和索引 |
| `SchemaPolicyReconcile` | 再加上把漂移的列改回声明的样子。**开发和测试用这个**，改了结构直接重启就跟上了 |

> **TSQ 只增不减。** 它不会删除一张它没在当前声明里看到的表——一个 runtime 只知道自己声明了
> 什么，分不清"这张表不该存在了"和"这张表是别人的"。v4 有过一档会删表的 `SchemaPolicyManaged`，
> 它靠一张全库共享的记账表工作，于是两个共用数据库的服务会在每次启动时互删对方的表和数据。
> 那一档和那张记账表在 v5 里都没有了；不再需要的表由迁移脚本删。

## 文档导航

| 文档 | 适合什么时候看 |
| --- | --- |
| [`skills/tsq/references/QUICKSTART.md`](skills/tsq/references/QUICKSTART.md) | 从空目录开始，跑通第一条 SQLite 查询（`docs/quickstart.md` 是它的索引页） |
| [`skills/tsq/references/CONCEPTS.md`](skills/tsq/references/CONCEPTS.md) | 想建立 Table 注解、生成文件、查询构建链路、Result、Runtime 的心智模型（`docs/concepts.md` 是它的索引页） |
| [`skills/tsq/references/REFERENCE.md`](skills/tsq/references/REFERENCE.md) | 完整的注解 DSL、查询 API、运行时与方言契约 |
| [`docs/skill.md`](docs/skill.md) | 想把 TSQ 作为一个 agent skill 安装到 Copilot / Claude Code / Gemini CLI |
| [`examples/README.md`](examples/README.md) | 想按 quickstart / cookbook / full-suite 找示例 |
| [`BEST_PRACTICES.md`](BEST_PRACTICES.md) | 想看输入校验、分页、事务、排序和生产环境建议 |

## 能力矩阵（内置 Dialect）

TSQ 当前内置的 `Dialect` 实现只有 **SQLite / MySQL / PostgreSQL**。下面的矩阵描述的是这三者在仓库当前实现下的行为，不再用“完整支持”这种泛化说法。

| 能力 | SQLite | MySQL | PostgreSQL | 说明 |
| --- | --- | --- | --- | --- |
| 生成 CRUD / 分页助手 | ✅ | ✅ | ✅ | 生成层支持一致 |
| 类型安全列与链式查询 | ✅ | ✅ | ✅ | `tsq.Select(...).From(table).Where(...).Build()` |
| `//tsq:result` 结果映射 | ✅ | ✅ | ✅ | 生成 `*.result.tsq.go` |
| 自动乐观锁（`version`） | ✅ | ✅ | ✅ | `Update/Delete` 在执行时按声明的 `version` 列做版本校验 |
| 按条件批量 `UPDATE` / `DELETE`（`tsq.UpdateTable` / `tsq.DeleteFrom`） | ✅ | ✅ | ✅ | 不校验 `version` 但会自增它；只引用目标表，不支持 JOIN / `LIMIT` / `RETURNING` |
| 列表参数 `In(col.ListParam())` / `NotIn(...)` | ✅ | ✅ | ✅ | 执行时按值个数展开 |
| `CASE` 表达式 | ✅ | ✅ | ✅ | 构建与执行都支持 |
| 行锁读取（`FOR UPDATE` / `FOR SHARE`） | ❌ | ✅ | ✅ | 能否执行取决于运行时 dialect |
| 非递归 CTE / `WITH` | ✅ | ✅ | ✅ | MySQL 基线为 8.0（5.7 已 EOL），5.7 上会收到数据库报错而不是 TSQ 的拒绝 |
| `INTERSECT` / `EXCEPT` | ✅ | ✅ | ✅ | MySQL 需要 8.0.31+ |
| `FULL JOIN` 执行 | ✅ | ❌ | ✅ | SQLite 需要 3.39+（内置的 modernc 驱动满足）；MySQL 会在执行前显式拒绝 |

补充说明：

- TSQ 现在只内置 **SQLite / MySQL / PostgreSQL** 三个完整闭环的 `Dialect` 实现。能力位按 **MySQL 8.0、SQLite 3.39+、PostgreSQL** 当前版本表态，不做服务器版本探测。
- 如果你要接入自定义数据库，需要实现完整 `Dialect` 合约，而不是依赖 TSQ 在接口外推断能力、DDL 或索引行为。

## 常见边界和注意事项

每一条的完整说明都在 [`skills/tsq/references/REFERENCE.md`](skills/tsq/references/REFERENCE.md)，这里只列最容易踩的：

- **`Where(...)` / `Search(...)` 每条链最多各一次**，编译期强制。多个参数是 AND，OR 用 `tsq.Or(...)`。
- **`OrderBy` / `Limit` / `Offset` 和 `Page(...)` 二选一**：`Page` 自己决定排序和分页。
- **空的列表参数不会去掉过滤条件**：`In` 匹配不到任何行，`NotIn` 匹配全部。
- **执行期的值都走参数**：`List` / `Get` / `Exec` 只接受 `Bind` 出来的 `tsq.Arg`，按参数匹配而不是按位置。
- **执行器必须知道方言**：`*tsq.Runtime`、`WithTx` 给的执行器，或 `tsq.WrapExecutor(db, dialect.MySQL)`；裸 `*sql.DB` 编译不过。
- **`Build()` 成功不代表所有方言都能执行**：CTE、`FULL JOIN`、行锁在执行时按方言校验，不支持时返回 `*dialect.UnsupportedCapabilityError`。
- **`version` 字段是自动乐观锁**：`Update` / `Delete` 冲突时返回 `*tsq.OptimisticLockError`，这是业务错误，必须处理。`runtime.WithTx(ctx, fn, tsq.WithRetry(tsq.IsOptimisticLockError))` 可以整段重试。
- **声明了 `deleted_at` 的表，`Delete` 是软删除**，而且已删行对**所有**引用这张表的查询和按条件写都不可见（JOIN 里也是）；要看已删行用 `TableXxx.WithDeleted()`，物理删除写 `HardDelete`。没有 `deleted_at` 的表两者同义。
- **列函数是包级泛型函数**：`tsq.Upper(col)`、`tsq.Sum(col)`、`tsq.Contains(col, tsq.Val("x"))`，套在类型不合的列上编译不过。
- **可空列是 `tsq.NullColumn[X, T]`**：按值类型比较，`SetNull` 写 NULL；可能读到 NULL 的值（可空列、外连接的表、没有 GROUP BY 的聚合）只能读进可空字段，否则查询在执行前就报错，而不是等到数据里真有 NULL 才炸。
- **`Page` 吃类型化的 `tsq.Paging`**：HTTP 进来的 `tsq.PageRequest` 先 `Validate`，再用 `req.Paging(允许排序的列...)` 转换。
- **`UpdateTable` / `DeleteFrom` 按条件写**：不校验 `version` 但会自增它。
- **`Batch*` 不自动开事务**：要全有或全无，放进 `runtime.WithTx(...)`。
- **关键词搜索会转义 LIKE 通配符**，这是语义正确性，不是 SQL 注入防护——普通值本来就走绑定参数。
- **相关子查询先 `Correlate(...)` 声明外层表**，不要把外层表 join 进子查询：那会让谓词悄悄失去相关性。
- **生成的查询变量在包初始化时 `MustBuild()`**：注解改了没重新生成，导入包时就会 panic。改完结构体跑 `tsq gen`。

## 示例入口

- **Quickstart**：[`examples/quickstart/README.md`](examples/quickstart/README.md)
- **Advanced**：[`examples/advanced/README.md`](examples/advanced/README.md)
- **Full suite**：[`examples/full-suite/README.md`](examples/full-suite/README.md)

## 开发

```bash
make fmt
make lint
make test
make build
make examples
./bin/examples/full-suite
```

常用目标：

```bash
make help
make build
make test
make test-coverage
make fmt
make vet
make lint
make clean
make install
make examples
```
