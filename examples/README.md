# TSQ 示例

11 章，由浅入深。每一章是一个能直接运行的程序：它打印每一步做了什么、**TSQ 实际发出的 SQL**
和结果，让你对照代码看每行 Go 背后跑了什么。

```bash
go run ./examples/01-getting-started   # 在仓库根运行，任意一章都一样
go test ./examples/...                  # 每章都有测试，断言它的输出
```

不需要装数据库：示例用纯 Go 的 SQLite（`modernc.org/sqlite`），每次运行在临时目录里新建一个库。

## 学习路线

| 章 | 学什么 | 主要 API |
| --- | --- | --- |
| [01 从结构体开始](01-getting-started/) | 写结构体和注解 → `tsq gen` → 增删改查 | `//tsq:table`、`tsq.Open`、`Insert` / `Get` / `Update` / `HardDelete`、`Select` |
| [02 查询](02-querying/) | 条件、排序、分片、参数，读结果的几种方式 | `Where`、`tsq.Val` / `Vals`、`Or` / `Not`、`Param` / `Bind`、`Limit` / `Offset`、`Get` / `Find` / `Exists` / `Count`、`SelectValue`、`Iter` |
| [03 多表](03-joins-and-results/) | 连接、结果投影、别名、可能为 NULL 的值 | `InnerJoin` / `LeftJoin`、`//tsq:result`、`MapInto` / `MapIntoNull`、`As`、`Coalesce` |
| [04 聚合](04-aggregates-and-case/) | 分组统计、CASE、列函数、自定义 SQL 片段 | `GroupBy` / `Having`、`Count` / `Sum` / `Avg`、`SelectNullValue`、`Case`、`Upper` / `Length`、`SelectDistinct`、`Add` / `Div`、`Pred` |
| [05 子查询](05-subqueries-cte-setops/) | 子查询、关联子查询、CTE、集合运算 | `In(子查询)`、`NotExists` + `Correlate`、`tsq.CTE` + `WithTable`、`Union` / `Intersect` / `Except` |
| [06 分页和搜索](06-paging-and-search/) | 页码分页、HTTP 分页请求、游标分页、关键词和全文搜索 | `Page`、`PageRequest`、`PageKeyset`、`Search` + `tsq.Keyword`、`tsq.Matches` |
| [07 写数据](07-writing-data/) | 插入、更新、Upsert、按条件批量改删、事务 | `BatchInsert`、`Update(cols...)`、`Upsert`、`UpdateTable` / `HardDeleteFrom`、`WithTx` / `WithTxResult` |
| [08 软删除和并发](08-soft-delete-and-concurrency/) | 删除与恢复、乐观锁冲突与重试、行锁 | `Delete` / `Restore` / `WithDeleted`、`DeleteFrom`、`OptimisticLockError`、`WithRetry`、`ForUpdate` |
| [09 查找和关联](09-lookups-and-relations/) | 按主键和唯一键查找、不产生 N+1 的关联加载 | `Get` / `Find` / `Fetch`、`GetByX` / `FetchByX`、`AttachMany` / `AttachOne`、`ListIn` |
| [10 表结构](10-schema-and-migrations/) | 生成的 DDL、启动策略、结构漂移 | `*.sql` / `tsq.json`、`SchemaPolicy*`、`MissingTableError` / `SchemaMismatchError`、`NewRuntime` |
| [11 方言和可观测性](11-dialects-and-observability/) | 同一查询的三种 SQL、方言能力、追踪、外部连接 | `query.SQL`、`dialect.Supports`、`WithTracers`、`WrapExecutor`、`WithMaxPageSize` |

第 1 章自带一张最小的表；第 2 到 11 章共用 [`shop`](shop/) 里的网店模型。

## 网店模型

```
categories  分类，可嵌套（parent_id）
products    商品：软删除、乐观锁、唯一 SKU、关键词搜索、全文索引
customers   顾客：可空的手机号，带数据库默认值的等级
orders      订单：乐观锁
order_items 订单明细：复合唯一键，数据库计算的小计（生成列）
```

- 结构体和注解：[`shop/*.go`](shop/)，每个注解旁边写了它的作用
- 生成物：`shop/*.tsq.go`、`shop/tsq.json`、`shop/{sqlite,mysql,postgres}.sql`——由 `tsq gen` 生成，不要手改
- 种子数据：[`shop/seed.go`](shop/seed.go)，开头列出了每一行的主键，各章按主键引用它们
- 演示脚手架：[`shop/demo.go`](shop/demo.go) 打开临时库并灌数据；[`internal/show`](internal/show/)
  把 SQL 打印出来。它们不是 TSQ 的一部分

输出里的 `‹products 的 12 列›` 是打印时的缩写：TSQ 实际发出的 SQL 总是逐列列出，从不写 `SELECT *`。

## 改了模型之后

在仓库根运行 `make examples`：重新生成 `shop` 和 `01-getting-started/todo`，再编译每一章。
`make examples-run` 还会把每一章跑一遍。

完整的 API 说明在 [`skills/tsq/references/REFERENCE.md`](../skills/tsq/references/REFERENCE.md)。
