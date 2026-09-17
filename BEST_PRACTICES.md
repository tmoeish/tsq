# TSQ Best Practices

这份文档汇总了在生产代码中使用 TSQ 时最值得优先遵守的实践。

## 1. 错误处理

### 1.1 尽早校验输入

- 在构建查询前校验分页、排序和用户输入
- 对外部输入优先使用 `Validate(maxSize)`
- `Build()` 返回错误时立即处理

```go
if err := pageReq.Validate(runtime.MaxPageSize()); err != nil {
	return fmt.Errorf("invalid pagination: %w", err)
}

query, err := tsq.
	Select(database.User__Cols...).
	From(database.TableUser).
	Where(database.User_ID.EQ(1)).
	Build()
if err != nil {
	return fmt.Errorf("build query: %w", err)
}
```

避免：

```go
query, _ := qb.Build()
```

### 1.2 数据库操作带上 context

- 给查询和写操作显式传递 `context.Context` 首参
- 设置 timeout / cancellation，避免数据库调用无限挂起

```go
ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
defer cancel()

users, err := query.List(ctx, runtime)
```

### 1.3 用 `%w` 保留错误上下文

```go
if err != nil {
	return fmt.Errorf("scan user: %w", err)
}
```

### 1.4 需要分支处理时用 `errors.Is` / `errors.As`

```go
var unknownField *tsq.UnknownSortFieldError
if errors.As(err, &unknownField) {
	return fmt.Errorf("sort field %q not found", unknownField.Field)
}
```

## 2. 分页

### 2.1 对 API 输入优先使用 `Validate`

`Normalize(maxSize)` 会把非法值归一化为安全默认值；  
`Validate(maxSize)` 会直接返回错误，更适合 HTTP API 和管理端输入。

```go
if err := pageReq.Validate(runtime.MaxPageSize()); err != nil {
	return nil, err
}
```

### 2.2 不要自己手算 offset

`PageRequest.Offset()` 已经处理了溢出保护。

```go
offset := pageReq.Offset()
```

避免：

```go
offset := page * size
```

### 2.3 UI 逻辑优先用 `HasNext()` / `HasPrev()`

```go
if resp.HasNext() {
	nextButton.Show()
}
```

## 3. 事务

### 3.1 总是 defer rollback

```go
tx, err := db.BeginTx(ctx, nil)
if err != nil {
	return err
}
defer func() {
	if rollErr := tx.Rollback(); rollErr != nil && rollErr != sql.ErrTxDone {
		log.Printf("rollback transaction: %v", rollErr)
	}
}()
```

### 3.2 把提交放在最后

```go
if err := op1(); err != nil {
	return err
}
if err := op2(); err != nil {
	return err
}

return tx.Commit()
```

### 3.3 优先用 `runtime.WithTx(...)` 执行事务里的 TSQ 操作

```go
if err := runtime.WithTx(ctx, nil, func(ctx context.Context, txExec tsq.Executor) error {
	if err := order.Insert(ctx, txExec); err != nil {
		return err
	}

	return nil
}); err != nil {
	return err
}
```

`runtime.WithTx(...)` 会自动包掉 `BeginTx`、失败回滚和成功提交。  
如果你已经启用了 `version` 乐观锁，并且希望冲突时自动重跑整个事务回调，最短写法是：

```go
if err := runtime.WithTx(ctx, &tsq.TxOptions{
	RetryIf: tsq.IsOptimisticLockError,
}, func(ctx context.Context, txExec tsq.Executor) error {
	// 在回调里重新读取、重新计算、重新写入。
	return nil
}); err != nil {
	return err
}
```

注意：重试的是**整个回调**。如果你把旧对象在事务外先读好，再在回调里反复提交同一份过期数据，自动重试也不会帮你成功。

### 3.4 `BatchInsert` / `BatchUpdate` / `BatchDelete` 不会自动开启事务

这是刻意设计。

这些 helper 接收的是 `Executor`，因此事务边界由调用方决定：

- 传 `runtime`（或 `tsq.WrapExecutor(db, dialect)`）：每条语句各自提交，允许部分成功
- 通过 `runtime.WithTx(...)` 提供的事务 executor：让整个批量操作参与同一个事务

```go
if err := runtime.WithTx(ctx, nil, func(ctx context.Context, txExec tsq.Executor) error {
	if err := database.TableOrder.BatchInsert(ctx, txExec, rows, tsq.WithBatchSize(500)); err != nil {
		return err
	}

	return nil
}); err != nil {
	return err
}
```

不要假设 Batch* helper 会替你包一层外部事务；如果你需要“全部成功或全部回滚”，请显式放进 `runtime.WithTx(...)` 或自己管理 `*sql.Tx`。

### 3.5 行锁读取要显式放进事务

`ForUpdate()` / `ForShare()` 适合表达“读取并锁定随后要修改的行”，但只有放在事务里才有实际意义。

```go
if err := runtime.WithTx(ctx, nil, func(ctx context.Context, txExec tsq.Executor) error {
	query, err := tsq.Select(database.User__Cols...).
		From(database.TableUser).
		Where(database.User_ID.EQ(userID)).
		ForUpdate().
		Build()
	if err != nil {
		return err
	}

	user, err := query.Get(ctx, txExec)
	if err != nil {
		return err
	}

	_ = user
	return nil
}); err != nil {
	return err
}
```

不要把行锁查询放到普通自动提交连接里然后期待锁能跨后续写操作继续存在。

### 3.6 `WrapExecutor(...)` 只在你已经手动持有 `*sql.Tx` 时作为底层 escape hatch

大多数业务代码优先用 `runtime.WithTx(...)` 即可。只有你明确需要自己控制 `BeginTx` / `Commit` / `Rollback` 生命周期时，再手动：

```go
tx, err := db.BeginTx(ctx, nil)
if err != nil {
	return err
}
defer func() {
	_ = tx.Rollback()
}()

txExec := tsq.WrapExecutor(tx, runtime.Dialect())
_ = txExec
```

### 3.7 自动乐观锁冲突要按业务错误处理

如果表声明了 `version` 列，`Update(...)` / `Delete(...)` 会自动做版本校验。  
版本不匹配时，TSQ 会返回 `OptimisticLockError`。

```go
if err := user.Update(ctx, runtime); err != nil {
	if tsq.IsOptimisticLockError(err) {
		return fmt.Errorf("record has been modified by another request: %w", err)
	}
	return err
}
```

不要自己再手工拼一层 `WHERE version = ?`，也不要忽略这类冲突再继续覆盖写。

如果你的写逻辑天然支持“重读后重算再提交”，也可以配合 `runtime.WithTx(..., &tsq.TxOptions{RetryIf: tsq.IsOptimisticLockError}, ...)` 把这类冲突交给事务 helper 重试。

### 3.8 按条件批量改写用 `UpdateTable` / `DeleteFrom`，不要先查再逐行 `Update`

“把满足条件的行都改掉”是一条 SQL 的事。先 `List` 再逐行 `Update(...)` 既多一次往返，又让每一行都走一遍乐观锁校验，而那正是这类需求不想要的：

```go
affected, err := tsq.
	UpdateTable(database.TableOrder).
	SetVal(database.Order_Status, "expired").
	SetVal(database.Order_UpdatedAt, null.TimeFrom(time.Now())).
	Where(database.Order_Status.EQVal("pending"), database.Order_CreatedAt.LT(database.Order_CreatedAt.Param())).
	Exec(ctx, runtime, database.Order_CreatedAt.Bind(cutoff))
```

- 这类语句不校验 `version`，但会自增它。批量改动之前加载的对象随后 `Update(...)` 会拿到 `OptimisticLockError`，按 3.7 处理。
- `UpdateTable` 不替你盖 `updated_at`，需要就显式 `SetVal`；`DeleteFrom` 在有 `deleted_at` 的表上是软删除，时间戳在**执行时**盖。
- `Where(...)` 必需。真要全表操作，写显式的 `tsq.And()`，让意图留在代码里。
- 它是单条语句，不分块；列表参数传超大切片会撞方言的参数上限，那种场景用 `tsq.BatchDeleteByPK` 或自己切片。

## 4. Field pointer 和 `MapInto(...)`

### 4.1 field pointer 要能安全处理 nil / 错误类型

```go
fp := func(u *User) *int64 {
	if u == nil {
		return nil
	}
	return &u.ID
}
```

### 4.2 用 `tsq.MapInto(...)` 做结果映射，而不是重复造列

它让扫描目标在编译期被校验。（这一节此前教的是一个从来不存在的 `Into` 泛型函数，
因为这份文档当时不在 `doc-check` 的扫描范围里，所以三个月没人发现。现在它在了。）

```go
userName := tsq.MapInto(database.User_Name, func(r *UserResult) *string {
	return &r.UserName
}, "user_name")
```

## 5. 排序

### 5.1 排序字段必须可验证

```go
var unknownField *tsq.UnknownSortFieldError
if errors.As(err, &unknownField) {
	return fmt.Errorf("unsupported sort field %q", unknownField.Field)
}
```

### 5.2 联表排序时优先用明确列名

当多个表都有 `id` 之类的字段时，尽量让排序字段与返回列保持一致，避免歧义。

### 5.3 把可排序字段写进接口文档

不要把可排序字段留给调用方猜。

## 6. 查询构建复用

### 6.1 高频场景优先复用已构建的 `Query`

```go
query, err := tsq.Select(User_ID, User_Name).
	From(TableUser).
	Where(User_Status.EQ("active")).
	Build()
```

### 6.2 不要在热路径里重复 `Build()`

稳定查询形状应当在初始化阶段或循环外构建一次，然后重复执行。

### 6.3 需要分支时复用 builder 中间态是安全的

当前 builder 会在继续链式调用时隔离分支状态，所以：

- 可以从同一个中间 builder 派生两个不同查询
- 不需要再担心“后一个分支把前一个分支的条件改掉”

但如果查询形状已经稳定，仍然优先缓存 `Build()` 后的 `*tsq.Query[Row]`，而不是在热路径里反复从 builder 往下走：`*tsq.Query` 在第一次跑某个方言时渲染并缓存 SQL。

### 6.4 把生成 helper 的初始化失败当普通错误处理

生成的查询 helper 不会因为导入包直接 `panic`。  
如果内部静态查询初始化失败，错误会在调用 `Get...` / `List...` / `Page...` 这类 helper 时返回。

这意味着：

- 不要假设“能 import 就说明生成查询一定没问题”
- 对生成 helper 的返回错误照常做 `%w` 包装和日志记录

## 7. 生产环境建议

### 7.1 日志分级

- **ERROR**：构建失败、约束错误、不可恢复的数据库错误
- **WARN**：超时、重试、降级
- **INFO**：计数、缓存状态、慢查询摘要

### 7.2 不要把内部查询细节直接暴露给终端用户

对用户返回友好的错误，对日志保留完整上下文。

### 7.3 给分页和查询设置上限

推荐至少限制：

- 最大页大小
- 单次查询超时
- 批量写入的 chunk size

## 8. TSQ 特有的两个提醒

### 8.1 `Where(...)` 和 `Search(...)` 每条链只能出现一次

Builder 采用**阶段型类型系统**：每次调用都会返回不同的具体类型，由编译器限制后续可调用的方法。`Where(...)` 和 `Search(...)` 每条链各最多出现一次——Go 类型系统在编译期强制保证，无法再次调用。

- 同一次 `Where(...)` 调用里传多个条件时，TSQ 按 `AND` 组合
- 需要 `OR` 时，请显式使用 `tsq.Or(...)`
- 需要构建复合子条件时，使用 `tsq.And(...)`
- 两个子句可以按任意顺序共存：`Where(...).Search(...)` 或 `Search(...).Where(...)`

### 8.2 关键词转义规则

- `Page(ctx, exec, pageReq)` 会自动对 `pageReq.Keyword` 转义 LIKE 通配符（`%` 和 `_`）。
- `StartsWith` / `EndsWith` / `Contains`（及其 `Val` 和 `Not` 形式）同样自动转义；`Like` / `LikeVal` 的模式按原样使用，通配符由调用方负责。
- SQL 注入防护来自参数绑定本身，LIKE 通配符转义只防止意外的模糊匹配，两者不能互替。

### 8.3 空的列表参数不是异常，而是“查不到任何结果”

列表参数适合执行时才知道筛选集合的场景：

```go
query, err := tsq.
	Select(database.Course_ID, database.Course_Title).
	From(database.TableCourse).
	Where(database.Course_ID.In(database.Course_ID.ListParam())).
	Build()

courses, err := query.List(ctx, runtime, database.Course_ID.BindList(ids...))
```

执行时：

- 非空列表：展开成 `IN (?, ?, ...)`
- 空列表或 nil：渲染成 `IN (NULL)`，返回空结果
- `NotIn` 的空列表渲染成匹配全部的形状

这套行为是刻意设计的，目的是把“当前没有任何允许值 / 选中值”的情况表达成**显式不匹配**，而不是偷偷跳过过滤条件。

因此：

- 想表达“没有任何候选值，所以结果应为空”时，直接传空切片 / `nil`
- 想表达“没有筛选值，所以不要加这个过滤条件”时，应该在业务层自己分支，不要把这个职责交给列表参数

### 8.4 普通值不要试图手工转成 SQL literal

当前 TSQ 的安全主路径是参数绑定。  
如果你要表达列、函数、子查询，请显式传表达式对象；如果你只是传普通值，就让 TSQ 去绑定参数，不要自己拼 SQL 字符串。

### 8.4 `Build()` 只做结构校验，方言能力在执行时校验

`Build()` 会校验：

- 查询子句顺序是否有效
- 列、表、Result owner 是否正确绑定
- 子查询和投影的形状是否符合 TSQ 规则

但 `Build()` **不会**承诺“这个查询一定能在所有执行器上跑起来”。

对 CTE、`FULL JOIN`、`INTERSECT`、`EXCEPT` 这类能力，TSQ 需要在真正执行时根据 executor 上的 dialect 做判断。原因是：

- 同一个查询值可以被复用到不同数据库
- 运行时可能有多个 registry / runtime，每个都有不同 dialect
- `Build()` 阶段没有足够信息决定最终 SQL 能力边界

实践上应当这样理解：

- `Build()` 成功：说明查询结构合法
- `List/Get/Page/Count/...` 成功：说明结构合法，而且当前 executor dialect 也支持这条 SQL
