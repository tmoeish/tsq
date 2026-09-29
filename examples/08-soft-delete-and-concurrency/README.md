# 08 软删除和并发

```bash
go run ./examples/08-soft-delete-and-concurrency
```

| 小节 | 演示 |
| --- | --- |
| 8.1 | `row.Delete`：打删除标记，行从所有查询里消失；`IsDeleted` |
| 8.2 | 删除已删除的行：`*tsq.RowStateError` |
| 8.3 | `TableXxx.WithDeleted()` 找回来，`row.Restore` |
| 8.4 | `tsq.DeleteFrom(...).Where(...)`、`BatchDeleteByPK` |
| 8.5 | 两个人改同一行，后保存的得到乐观锁冲突 |
| 8.6 | `WithTx(..., tsq.WithRetry(tsq.IsOptimisticLockError))`：冲突后自动重跑 |
| 8.7 | `ForUpdate` 在 SQLite 上执行时报 `*dialect.UnsupportedCapabilityError`；`dialect.Supports` |

## 软删除

结构体声明 `//tsq:managed deleted_at`，这张表就是软删除表：

- `Delete` 只写 `deleted_at`、`updated_at` 和 `version`；真删用 `HardDelete`；
- **任何**提到这张表的查询都自动只看没删除的行——生成的、手写的、连接进来的、子查询里的；
- 需要看已删除的行时写 `WithDeleted()`，它只去掉过滤，删除仍然是软删除；
- 唯一索引以 `deleted_at` 打头，所以已删除的行不占用唯一值；也因此在 `WithDeleted()` 上按唯一键查找会被拒绝，按主键取。

## 乐观锁

结构体声明 `//tsq:managed version`：`Update` / `Delete` 按"主键 + 读到的版本"匹配，匹配不到就返回
`*tsq.OptimisticLockError`。**这是业务错误，必须处理**：忽略它就等于悄悄覆盖了别人的改动。
处理方式是重新读、再改——`WithRetry` 把这件事自动化，前提是回调**自己读它要改的行**。

`RowStateError`（删除已删除的行、恢复没删除的行）不是并发冲突，重试没有用。

## 方言能力在执行时检查

`Build()` 只检查结构；行锁、`FULL JOIN`、CTE 这些方言能力在执行时才检查。这是有意的：同一个 `*Query`
可以在多种方言上复用。要事先分支用 `dialect.Supports`；事务回调里用 `tsq.DialectOf(tx)` 拿方言。

下一章：[09 查找和关联](../09-lookups-and-relations/)
