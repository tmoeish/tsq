# TSQ v4 → v5 迁移指南

v5 是一次"一次性把该改的都改掉"的大版本。v2、v3、v4 发布在两周之内，那个节奏是个错误；
这一版的目标是发完之后长时间不再破坏使用者。

下面按"你要动多少手"从大到小排。**前两节几乎每个项目都要做，后面的按你用到了什么对照。**

## 0. 升级路径

```bash
go get github.com/tmoeish/tsq/v5@latest
go install github.com/tmoeish/tsq/v5/cmd/tsq@latest
```

模块路径带上了 `/v5`，所以 v4 和 v5 可以在同一个构建里共存——一个包一个包地迁移是可行的。
仓库内所有 import 都要从 `github.com/tmoeish/tsq/v4` 改成 `.../v5`。

## 1. 注解换成 `//tsq:` 指令（有工具）

```bash
tsq migrate ./internal/database   # 每个包跑一次，然后看 diff
```

`tsq migrate` 用 v4 的解析器读旧注解，所以 v4 生成器能接受的都能原样转换；注解上方的散文保留，
v4 会派生的索引名仍然留给推导。

```go
// v4
// @TABLE(
//
//	name="course",
//	pk="ID",
//	created_at,
//	ux=[{fields=["Title"]}],
//	search=["Title", "Summary"],
//
// )

// v5
//tsq:table name=course pk=ID
//tsq:managed created_at
//tsq:unique Title
//tsq:search Title,Summary
```

**`tsq fmt` 没有了**，"先 fmt 再 gen"那条规则也没有了：指令行 gofmt 不碰。

`pk="UID,true"` 这种把两个意思塞进一个字符串的写法换成 `pk=UID`（自增，默认）；要关掉自增写
`assigned`（表示主键由调用方给值）。

迁移完之后 **必须重新生成**：`tsq gen ./internal/database`。

## 2. 删除的语义变了（没有工具，要人读）

这是整个 v5 里最需要你停下来想一想的一条。

| 表声明了 | `Delete` | `HardDelete` |
| --- | --- | --- |
| `deleted_at` | 打墓碑并刷新 `updated_at`，**行留在库里** | 真正删掉 |
| 没有 `deleted_at` | 真正删掉 | 真正删掉（两者同义） |

- v4 里 `Delete` 一律物理删，软删要显式调 `SoftDelete(ctx, db, dt)`。**v5 反过来了。**
- 成对的入口：`tsq.Delete` / `tsq.HardDelete`、`tsq.ChunkedDelete` / `tsq.ChunkedHardDelete`、
  `tsq.ChunkedDeleteByPKs` / `tsq.ChunkedHardDeleteByPKs`、`tsq.DeleteFrom` / `tsq.HardDeleteFrom`。
- 生成的 `SoftDelete(ctx, db, dt)` 删除了。想指定删除时间就先给字段赋值再调 `Delete`。
- 软删除走的是 `UPDATE`，所以乐观锁校验和 `version` 自增照常生效。

**动手前先过一遍代码里所有的 `Delete` 调用**，确认每一处想要的到底是哪种。清理、GDPR 删除、
测试夹具通常要的是 `HardDelete`。

查询这一侧：`QueryActiveXxx` / `ListActiveXxx` 改名为 `QueryXxx` / `ListXxx`，而**带已删行的那套
不再生成**。审计要读已删行时自己写：

```go
var EveryEnrollment = tsq.
	Select(database.Enrollment__Cols...).
	From(database.TableEnrollment).
	MustBuild()
```

## 3. Runtime 的构造

```go
// v4
rt, err := tsq.NewRuntime("sqlite", dsn, database.TSQTables(), &tsq.RuntimeOptions{
	TablePolicy: tsq.SchemaPolicyCreateMissing,
	IndexPolicy: tsq.SchemaPolicyCreateMissing,
	Logger:      slog.Default(),
})

// v5
rt, err := tsq.NewRuntime(ctx, "sqlite", dsn, database.TSQTables(),
	tsq.WithSchemaPolicy(tsq.SchemaPolicyCreateMissing),
	tsq.WithLogger(slog.Default()),
)
```

- `NewRuntimeContext` 没有了，ctx 不再可选。
- 选项：`WithSchemaPolicy` / `WithTablePolicy` / `WithIndexPolicy` / `WithLogger` /
  `WithSQLLogging()` / `WithTracers(...)` / `WithMaxPageSize(n)`。
- 已经自己开好连接池（otelsql、自定义 connector）的，用
  `tsq.NewRuntimeFromDB(ctx, db, dialect, tables, opts...)`。**`Close()` 不会关这样传进来的池。**
- `SchemaPolicyManaged` 和 `SchemaOwner` 删除了。**没有任何策略会删表**——不再声明的表交给迁移脚本。
- `IdentifierValidationMode` 和 `ValidateIdentifiersForDialect()` 删除了，标识符长度恒为严格校验。
  派生索引名过长时用 `//tsq:unique ... name=...` 显式命名。

## 4. 读取与分页

| v4 | v5 | 没查到时 |
| --- | --- | --- |
| `GetOrErr` | `Get` | `sql.ErrNoRows` |
| `Get` | `Find` | `nil, nil` |
| `Load(holder)` | 删除 | — |
| `Count` (int) / `Count64` | `Count` | 只有 `int64` |

**这是最容易静默出错的一条**：v4 的 `Get` 返回 nil 表示没查到，v5 的 `Get` 返回错误。凡是原来写
`if row == nil` 的地方，要么改成 `Find`，要么改成判 `errors.Is(err, sql.ErrNoRows)`。

其余：

- `Get` / `Find` / `Exists` 现在给语句加 `LIMIT 1`；`Exists` 不再走 `COUNT`。
- `NewPageRequest(url.Values)` 和 `PageRequest.ToQuery()` 删除，HTTP 解析交给调用方的 binder
  （结构体上的 `query` tag 还在）。
- `DefaultMaxPageSize` 是默认值而不是硬顶：`WithMaxPageSize(n)` 双向生效。

## 5. 手写 `tsq.Table` 实现

只有自己实现过这个接口的项目需要看。

```go
// v4
func (t T) PrimaryKeys() []string { return []string{"id"} }
func (t T) VersionColumn() string { return "version" }

// v5
func (t T) PrimaryKey() string { return "id" }
func (t T) ManagedColumns() tsq.ManagedColumns {
	return tsq.ManagedColumns{Version: "version", DeletedAt: "deleted_at"}
}
```

生成的代码会自己带上这两个方法，**重新生成即可**。

## 6. 零散改名与删除

| v4 | v5 |
| --- | --- |
| `col.ExistsSub(sq)` / `col.NExistsSub(sq)` | `tsq.Exists(sq)` / `tsq.NotExists(sq)` |
| `IsCommonTransactionRetryableError` | `IsRetryableTxError` |
| `IsRetryableTransactionConflictError` | `IsTxConflictError` |
| `Tracer func(next) func(ctx) error` | `Tracer func(ctx, tsq.TraceOp, next) error` |
| `col.Length()` 返回 `Column[O, T]` | 返回 `Column[O, int64]` |
| `col.Unique()` / `col.NUnique()` / `col.Concat()` / `col.Now()` | 删除，用 `Expr` / `Exprf` |

`EXISTS` 从来不读它挂在哪一列上，所以它不该是方法；那三个列方法在 v4 里也只会返回构建错误，
`Now()` 则把整个列表达式换成 `CURRENT_TIMESTAMP`（`User_Name.Now()` 和 `User_ID.Now()` 完全一样）。

### v4 里标了 `Deprecated` 的，全部删除

| v4 | v5 |
| --- | --- |
| `tsq.AsSubquery(q, col)` | `q.AsSubquery(col)` |
| `tsq.NewPageResponse(req, total, data)` | `req.Response(total, data)` |
| `WithTx1` / `WithTx2`（含包级形式） | `Runtime.WithTxResult`，多个值用一个小结果结构体 |
| `QueryInt` / `QueryFloat` / `QueryString` | `query.Scalar(ctx, exec, col, args...)` |
| `IndexInitMode`、`IndexInitSkip/Validate/Upsert` | `SchemaPolicy` 那组常量 |

## 7. 顺带修掉的 bug（不需要你做什么）

- 字符串字面量里含 ` FOR UPDATE ` / ` EXCEPT ` 之类的词，会让一条完全正常的查询被
  `ErrUnsupportedCapability` 拒绝执行。
- 声明 `*time.Time` 托管时间戳字段的表，**生成的代码编译不过**（模板发出了一个不存在的符号）。

## 建议的迁移顺序

1. `go get .../v5`，把 import 路径批量改掉，先让它编译失败。
2. `tsq migrate ./...` 每个包跑一次，`tsq gen` 重新生成。
3. 按编译错误逐个修——大部分是第 3、4、6 节的机械改名。
4. **然后单独过一遍所有 `Delete` 调用**：这一条编译器帮不了你，它不会报错，只会改变行为。
