# 09 查找和关联

```bash
go run ./examples/09-lookups-and-relations
```

| 小节 | 演示 |
| --- | --- |
| 9.1 | 按主键：`Get`（没有就报错）、`Find`（没有返回 nil）、`Fetch`（多个，按给出的顺序） |
| 9.2 | 按唯一键：`GetByEmail`、`FetchBySKU`、复合键 `GetByOrderIDAndProductID` |
| 9.3 | `GetBy` 用不唯一的列：拒绝，而不是随便返回一行 |
| 9.4 | `tsq.AttachMany`：一次查询给所有订单装上明细 |
| 9.5 | `tsq.AttachOne`：顺着外键给每行明细找到商品 |
| 9.6 | `query.ListIn`：去重、按参数上限切分 |

## 生成了哪些查找方法

| 注解 | 生成 |
| --- | --- |
| 主键（每张表都有） | `TableX.Get` / `Find` / `Fetch` |
| `//tsq:unique Email` | `TableX.GetByEmail` / `FindByEmail` / `FetchByEmail` |
| `//tsq:unique OrderID,ProductID` | `GetByOrderIDAndProductID(ctx, db, o, p)`、`FetchByOrderIDAndProductID(ctx, db, o, ps...)` |
| `//tsq:index CategoryID` | 不生成方法：普通索引上的查询要排序、分页，生成器猜不出来，用 `Select` 写 |

## 关联加载

TSQ 没有关系 DSL，也不往你的结构体里加字段。`AttachMany` / `AttachOne` 做的是一件事：给一批父行
加载子行，**只多一次查询**，而不是每个父行一次（N+1）。

- 子查询由你写，它的 `Where`、`OrderBy`、软删除范围决定哪些子行算数、按什么顺序；
- 它唯一的列表参数就是子键，父行的键会被收集、去重后填进去；
- 装到哪里由回调决定（示例里是一个 map，也可以是父结构体上你自己的字段）；
- 没有父行就不查询。

下一章：[10 表结构](../10-schema-and-migrations/)
