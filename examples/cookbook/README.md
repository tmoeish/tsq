# Examples / Cookbook

这里放的是更接近真实业务代码的场景，不要求你先理解全部能力。

## 场景 1：HTTP API 分页列表

适合后台列表、管理端搜索、用户列表接口。

```go
func ListUsers(ctx context.Context, dbmap *tsq.DbMap, req ListUsersRequest) (*tsq.PageResp[*database.User], error) {
	pageReq := &tsq.PageReq{
		Page:    req.Page,
		Size:    req.Size,
		OrderBy: "id",
		Order:   "desc",
		Keyword: tsq.EscapeKeywordSearch(req.Keyword),
	}
	if err := pageReq.ValidateStrict(); err != nil {
		return nil, fmt.Errorf("invalid list users page: %w", err)
	}

	return database.PageUser(ctx, dbmap, pageReq)
}
```

这个场景重点看：

- `PageReq.ValidateStrict()`
- `EscapeKeywordSearch(...)` 只处理 LIKE 语义，不替代参数绑定
- 生成的 `PageUser(...)` 足够覆盖很多标准列表页

## 场景 2：事务里写入订单

适合“先写主表，再写明细，再提交”的流程。

```go
func CreateOrder(ctx context.Context, db *sql.DB, dbmap *tsq.DbMap, order *database.Order, items []*database.Item) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	txMap := &tsq.DbMap{Db: tx, Dialect: dbmap.Dialect}

	if err := order.Insert(ctx, txMap); err != nil {
		return fmt.Errorf("insert order: %w", err)
	}

	for _, item := range items {
		if err := item.Insert(ctx, txMap); err != nil {
			return fmt.Errorf("insert order item: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit order transaction: %w", err)
	}

	return nil
}
```

这个场景重点看：

- TSQ 不替你管理事务边界，事务仍由 `database/sql` 控制
- 进入事务后，把 `*sql.Tx` 包进新的 `tsq.DbMap`
- 生成的 `Insert/Update/Delete` 助手可以直接在事务里复用

## 再往下看什么

- 想看真实代码而不是片段：去 `../main.go`
- 想看全部能力边界：去 [`../full-suite/README.md`](../full-suite/README.md)
