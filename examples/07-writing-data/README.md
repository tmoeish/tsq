# 07 写数据

```bash
go run ./examples/07-writing-data
```

| 小节 | 演示 |
| --- | --- |
| 7.1 | `Insert` 写回自增主键、时间戳、数据库默认值（`default:`）和生成列（`generated:`） |
| 7.2 | `BatchInsert` + `tsq.WithSkipDuplicates()` |
| 7.3 | `Update` 整行保存（带乐观锁）；`Update(ctx, db, 列...)` 只写这几列 |
| 7.4 | 用窄 `Select` 读出来的行不能整行 `Update` |
| 7.5 | `Upsert(row, tsq.OnConflict(键))` 按唯一键插入或更新整行；`.Update(列...)` 冲突时只改这几列 |
| 7.6 | `tsq.UpdateTable(...).Set(...).Where(...)`、`SetNull` |
| 7.7 | `tsq.HardDeleteFrom(...).Where(...)` |
| 7.8 | `WithTxResult`：一个事务里扣库存、写订单、写明细；返回错误就回滚 |

## 两种写法

| | 行级：`row.Update(ctx, db)` | 按条件：`tsq.UpdateTable(t)...Exec` |
| --- | --- | --- |
| 需要先读出行 | 要 | 不要 |
| 乐观锁 | 校验 `version` | 不校验，但会给 `version` 加一——先读出的行之后保存会冲突 |
| 适合 | 读-改-写一行 | "把所有满足条件的行改成……" |

## 要点

- **读了几列就只写几列**：用窄 `Select` 读进行类型，没读的列是零值。TSQ 记住了这一行是怎么读的，
  整行 `Update` / `Upsert` 会报错并列出读过的列，而不是悄悄把零值写回去。
- **`Upsert` 默认写整行**：字段是 nil 的可空列会被写成 NULL（7.5 里 Ada 的手机号就这样没了）；
  `default:` 列例外，nil 时插入交给默认值、冲突时保留库里的值。
  只想改几列时写 `tsq.OnConflict(键).Update(列...)`，没冲突的行照样整行插入。
- **`default:` 只用在能存 NULL 的字段上**：nil 表示"交给数据库"，零值是一个真实的值。
- **`Batch*` 不自动开事务**：要全有或全无，用 `WithTx` 包起来。
- **事务回滚只撤销数据库，不撤销内存**：回调里 `Insert` 过的结构体保留着主键和时间戳，所以要写的行在回调里构造。
- **`Where` 必须写**：`UpdateTable` / `DeleteFrom` 没有 `Where` 编译不过；真要改全表写 `Where(tsq.And())`。
- 删除一律带 `Hard` 才是真删：`grep HardDelete` 就能找到项目里所有真删数据的地方。

下一章：[08 软删除和并发](../08-soft-delete-and-concurrency/)
