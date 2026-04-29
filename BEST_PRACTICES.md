# TSQ Best Practices

这份文档汇总了在生产代码中使用 TSQ 时最值得优先遵守的实践。

## 1. 错误处理

### 1.1 尽早校验输入

- 在构建查询前校验分页、排序和用户输入
- 对外部输入优先使用 `ValidateStrict()`
- `Build()` 返回错误时立即处理

```go
if err := pageReq.ValidateStrict(); err != nil {
	return fmt.Errorf("invalid pagination: %w", err)
}

query, err := qb.Build()
if err != nil {
	return fmt.Errorf("build query: %w", err)
}
```

避免：

```go
query, _ := qb.Build()
```

### 1.2 数据库操作带上 context

- 给查询和写操作设置 timeout / cancellation
- 避免数据库调用无限挂起

```go
ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
defer cancel()

rows, err := db.QueryContext(ctx, sqlStr, args...)
```

### 1.3 用 `%w` 保留错误上下文

```go
if err != nil {
	return fmt.Errorf("scan user: %w", err)
}
```

### 1.4 需要分支处理时用 `errors.Is` / `errors.As`

```go
var unknownField *tsq.ErrUnknownSortField
if errors.As(err, &unknownField) {
	return fmt.Errorf("sort field %q not found", unknownField.Field)
}
```

## 2. 分页

### 2.1 对 API 输入优先使用 `ValidateStrict()`

`Validate()` 会把非法值归一化为安全默认值，适合兼容场景；  
`ValidateStrict()` 会直接返回错误，更适合 HTTP API 和管理端输入。

```go
if err := pageReq.ValidateStrict(); err != nil {
	return nil, err
}
```

### 2.2 不要自己手算 offset

`PageReq.Offset()` 已经处理了溢出保护。

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

### 3.3 事务里复用 TSQ 时，重新包装 `DbMap`

```go
txMap := &tsq.DbMap{Db: tx, Dialect: dbmap.Dialect}
if err := order.Insert(ctx, txMap); err != nil {
	return err
}
```

## 4. Field pointer 和 `Into(...)`

### 4.1 field pointer 要能安全处理 nil / 错误类型

```go
fp := func(holder any) any {
	if holder == nil {
		return nil
	}

	user, ok := holder.(*User)
	if !ok {
		return nil
	}

	return &user.ID
}
```

### 4.2 用 `Into(...)` 做结果映射，而不是重复造列

```go
userID := database.User_ID.Into(func(holder any) any {
	return &holder.(*UserResult).UserID
}, "user_id")
```

## 5. 排序

### 5.1 排序字段必须可验证

```go
var unknownField *tsq.ErrUnknownSortField
if errors.As(err, &unknownField) {
	return fmt.Errorf("unsupported sort field %q", unknownField.Field)
}
```

### 5.2 联表排序时优先用明确列名

当多个表都有 `id` 之类的字段时，尽量让排序字段与返回列保持一致，避免歧义。

### 5.3 把可排序字段写进接口文档

不要把可排序字段留给调用方猜。

## 6. SQL cache

### 6.1 只有在高频构建场景再打开缓存

```go
cache := tsq.NewSQLRenderCache(tsq.SQLCacheConfig{
	Enabled: true,
	MaxSize: 1000,
})
```

### 6.2 监控命中率

如果命中率很低，缓存可能只是增加复杂度。

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

### 8.1 `Where(...)` 和 `KwSearch(...)` 会覆盖之前的设置

如果你想继续加条件，请使用 `And(...)`。

### 8.2 `EscapeKeywordSearch(...)` 只转义 LIKE 通配符

SQL 注入边界来自参数绑定，不来自这个转义函数。
