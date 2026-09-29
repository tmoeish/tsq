# 03 多表

```bash
go run ./examples/03-joins-and-results
```

| 小节 | 演示 |
| --- | --- |
| 3.1 | `InnerJoin` + 生成的结果投影 `ResultProductListing` |
| 3.2 | 四表连接 + `tsq.MapInto` 读进本地结构体 |
| 3.3 | `LeftJoin` + 可空字段 `sql.Null[int64]` |
| 3.4 | 把可能为 NULL 的值读进 `int64`：**执行前**就报错 |
| 3.5 | `As` 起别名做自连接 + `tsq.MapIntoNull` |
| 3.6 | `tsq.Coalesce` 给 NULL 默认值 |

## 结果形状：`//tsq:result` 还是 `MapInto`

查询读进什么类型，由 `Select` 的列决定：

- 选一张表的全部列（`product.Columns()...`），读进表的行类型 `shop.Product`；
- 形状稳定、多处复用的，在结构体上写 `//tsq:result`，每个字段用 `tsq:"表.字段"` 指明来源，
  `tsq gen` 生成 `ResultXxx`（见 [`shop/results.go`](../shop/results.go)）；
- 临时用一次的，`tsq.MapInto(列或表达式, func(r *T) *字段类型 {...})` 现场映射。

## 可能为 NULL 的值

TSQ 知道一个选出来的值什么时候可能是 NULL：可空列、外连接可空一侧的列、没有 `GROUP BY` 的
`SUM` / `MAX`、没有 `Else` 的 `CASE`……把它读进不能存 NULL 的字段，**发出 SQL 之前**就报错并说明原因，
而不是等扫描到第一行 NULL 才失败。三种修法，按推荐顺序：

1. 行一定存在的话，用内连接；
2. `tsq.Coalesce(x, tsq.Val(默认值))`；
3. 可空字段 + `tsq.MapIntoNull`（生成的结果投影会自动这么做）。

下一章：[04 聚合](../04-aggregates-and-case/)
