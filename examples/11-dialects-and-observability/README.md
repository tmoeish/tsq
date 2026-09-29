# 11 方言和可观测性

```bash
go run ./examples/11-dialects-and-observability
```

| 小节 | 演示 |
| --- | --- |
| 11.1 | `query.SQL(方言, 参数...)`：同一个查询渲染成三种方言，不执行 |
| 11.2 | `FULL JOIN` 渲染到 MySQL 得到 `*dialect.UnsupportedCapabilityError`；`dialect.Supports` 能力表 |
| 11.3 | `tsq.WithTracers`：每次操作一个 span，带操作名和表名 |
| 11.4 | `tsq.WrapExecutor` 包装 `*sql.Conn`；`tsq.DialectOf` |
| 11.5 | `tsq.WithMaxPageSize`：分页大小上限 |

## 三种方言

TSQ 只支持 SQLite、MySQL、PostgreSQL。同一个 `*Query` 不绑定方言，执行时按执行器的方言渲染并缓存。
各方言写法不同的地方（占位符、引号、`LENGTH`、`ROUND`、NULL 排序、全文检索、Upsert 语法）由 TSQ 按方言改写，
结果一致；做不到一致的能力（`FULL JOIN`、行锁、`INTERSECT ALL`）在执行时报错，错误里带能力名和方言名。

## 日志和追踪

| | `WithSQLLogging()` + `WithLogger(l)` | `WithTracers(t...)` |
| --- | --- | --- |
| 得到什么 | 每条渲染好的 SQL 和绑定的参数（debug 级别） | 每次操作的开始和结束：`insert`、`list`、`page`、`tx`…… |
| 用来 | 排查问题 | 指标、分布式追踪（OpenTelemetry span） |
| 注意 | 参数原样记录，含个人数据的环境不要开 | 不带 SQL 文本 |

`tsq.Logger` 是 `*slog.Logger` 的一个子集，直接传你的 slog logger。各章的 `SQL>` 输出就是一个自定义 Logger
（[`internal/show`](../internal/show/show.go)）。

`WrapExecutor` 出来的执行器不属于任何 runtime，所以不打 SQL 日志、不走 tracer，分页上限是默认的 `tsq.DefaultMaxPageSize`。

回到[目录](../README.md)
