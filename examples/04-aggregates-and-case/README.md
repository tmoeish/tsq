# 04 聚合

```bash
go run ./examples/04-aggregates-and-case
```

| 小节 | 演示 |
| --- | --- |
| 4.1 | `GroupBy` + `Count` / `Sum` / `Avg` / `Max` |
| 4.2 | `Having`，`OrderBy` 一个聚合 |
| 4.3 | `SelectValue(CountDistinct(...))`；`SelectNullValue(Sum(...))` 读成 `sql.Null[int64]` |
| 4.4 | `tsq.Case(...).When(...).Else(...).End()`，再按它分组 |
| 4.5 | 列函数 `Upper` / `Length` / `Substring`，也能用在 `Where` 里 |
| 4.6 | `SelectDistinct` |
| 4.7 | 逃生舱 `Exprf` / `Pred` |

## 要点

- 聚合函数和列函数是**包级泛型函数**，按列的类型约束：`tsq.Sum` 只接受数值列，`tsq.Upper` 只接受字符串列，
  `tsq.Avg` 返回 `float64`。用错列编译不过。
- `Build()` 检查分组是否合法：选了既没分组也没聚合的列会被拒绝（SQLite 会随便给一行的值，PostgreSQL 直接报错）。
  按一张表的主键分组，这张表的其他列就可以直接选（4.1）。
- 聚合只能出现在选择列表、`Having` 和 `OrderBy` 里；放进 `Where` 会被 `Build()` 拒绝。
- 带绑定值的表达式（4.4 的 CASE）同时出现在 `SELECT` 和 `GROUP BY` / `ORDER BY` 里时，后两者写成列序号
  `GROUP BY 1`：PostgreSQL 给每个占位符重新编号，认不出它们是同一个表达式。
- `Exprf` / `Pred` 的格式串原样发给每种方言，可移植性由你负责；参数仍然是绑定的。

下一章：[05 子查询](../05-subqueries-cte-setops/)
