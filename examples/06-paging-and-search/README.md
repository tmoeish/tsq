# 06 分页和搜索

```bash
go run ./examples/06-paging-and-search
```

| 小节 | 演示 |
| --- | --- |
| 6.1 | `query.Page(ctx, db, tsq.Paging{...})`：总数和本页来自同一个快照 |
| 6.2 | `TableProduct.Query()` + `tsq.Keyword(词)`：生成的关键词搜索 |
| 6.3 | 自己的查询里写 `Search(...)`，搜连接进来的列 |
| 6.4 | HTTP 请求 `tsq.PageRequest` → `Paging(可排序列...)`；非法请求得到 `*tsq.PageRequestError` |
| 6.5 | `PageKeyset`：游标分页 |
| 6.6 | `tsq.Matches`：全文检索 |

## 选哪种分页

| | 页码分页 `Page` | 游标分页 `PageKeyset` |
| --- | --- | --- |
| 能跳到第 N 页 | 能 | 不能，只能"下一页" |
| 有总数 | 有（多一次 COUNT） | 没有 |
| 翻得很深时 | 越来越慢（要跳过前面的行），并发插入会让页错位 | 一样快 |
| 排序要求 | 任意 | 必须包含每张表的主键，列不能为 NULL |

## 要点

- 关键词为空就不过滤，搜索框的值可以原样传进来；关键词里的 `%`、`_` 按字面匹配。
- 可排序的列由接口显式列出：在没索引的列上排序是一种代价，要由接口决定，而不是由客户端决定。
- `PageRequest` 带着 `keyword`，`Page` 会用它搜索，不用再传 `tsq.Keyword`。
- 大小写敏感与否由数据库决定：SQLite 忽略 ASCII 大小写，MySQL 看排序规则，PostgreSQL 区分大小写。
- 全文检索的"匹配"含义每种方言不同：MySQL 是 `MATCH ... AGAINST`，PostgreSQL 是 `to_tsvector`，
  SQLite 退化成子串匹配。同一段代码能在三种方言上跑，但排序和语法不可移植。

下一章：[07 写数据](../07-writing-data/)
