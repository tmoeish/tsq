# 02 查询

```bash
go run ./examples/02-querying
```

| 小节 | 演示 |
| --- | --- |
| 2.1 | `Select → From → Where → OrderBy`；`Where` 的多个条件是 AND；`tsq.Val` 包住 Go 值 |
| 2.2 | `tsq.Or`、`tsq.Not`、`In(tsq.Vals(...))` |
| 2.3 | `StartsWith` / `Contains` 转义通配符，`Like` 按原样使用模式 |
| 2.4 | 可空列（`*string` 字段生成 `tsq.NullColumn`）用 `IsNull` / `IsNotNull` |
| 2.5 | 参数：`col.Param()` / `col.Bind(v)`、`tsq.NewParam`、`col.ListParam()` / `col.BindList(...)` |
| 2.6 | `OrderBy → Limit → Offset` |
| 2.7 | `Get`（没有就报错）、`Find`（没有返回 nil）、`Exists`、`Count` |
| 2.8 | `SelectValue`：只要一列 |
| 2.9 | `Iter`：逐行遍历，不把结果全读进内存 |

## 要点

- **值总是绑定的**：`tsq.Val(v)` 和参数都变成 `?`，从不拼进 SQL 文本。
- **类型在编译期检查**：`PriceCents` 是 `int64`，`tsq.Val(5000)` 是 `int`，比较它们编译不过，要写
  `tsq.Val(int64(5000))`。编译错误里的方法名告诉你该怎么改，对照表见
  [expressions.md § Values fixed in the code](../../skills/tsq/references/expressions.md#values-fixed-in-the-code)。
- **子句的顺序和次数由类型保证**：`Where` 一条链只能调一次（全部条件传给这一次），
  `Offset` 只能跟在 `Limit` 后面——写错了编译不过。
- **查询构建一次、到处复用**：包级变量 `productsInCategory` 在执行时绑定参数；渲染结果按方言缓存。
- 输出里每条 `products` 查询都多了 `deleted_at = 0`：这张表声明了软删除，任何查询都只看没删除的行（第 8 章）。

下一章：[03 多表](../03-joins-and-results/)
