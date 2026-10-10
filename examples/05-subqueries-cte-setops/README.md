# 05 子查询、CTE 和集合运算

```bash
go run ./examples/05-subqueries-cte-setops
```

| 小节 | 演示 |
| --- | --- |
| 5.1 | `col.In(tsq.SelectValue(...)...)`：子查询作为集合 |
| 5.2 | `col.EQ(tsq.SelectValue(tsq.Max(...))...)`：标量子查询 |
| 5.3 | `tsq.NotExists` + `Correlate`：关联子查询 |
| 5.4 | `tsq.CTE` + `col.Rebind(cte)` |
| 5.5 | `Union` / `Intersect` / `Except` |

## 要点

- **`SelectValue` 的阶段就是一个子查询**，类型是它选的那一列的类型：`int64` 的子查询只能放在 `int64` 列的
  `In` / `EQ` 里。它不用单独 `Build`，外层 `Build` 时一起检查。
- **关联必须声明**：子查询引用外层的表，要写 `Correlate(外层表)`，否则 `Build` 报错。这是为了防止
  "以为关联了其实没有"——比如把外层表也 join 进子查询，谓词就悄悄变成了对每一行都一样。
- **CTE 的列按名字找**：`col.Rebind(cte)` 把列重新绑定到 CTE 上。聚合在 CTE 里输出为源列的名字
  （`SUM(total_cents) AS total_cents`），所以同一列的两个聚合不能放进同一个 CTE。
- **集合运算两边的形状必须一样**：读进同一种类型、同样顺序的字段。结果整体排序，`OrderBy` 引用输出列。
- **链式集合运算从左到右求值**：`a.Union(b).Intersect(c)` 是 `(a ∪ b) ∩ c`，三种方言结果相同；
  要 `a ∪ (b ∩ c)` 就写 `a.Union(b.Intersect(c))`。

下一章：[06 分页和搜索](../06-paging-and-search/)
