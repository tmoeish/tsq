# Examples / Quickstart

这一层只讲**“一个普通后台列表页和平常 CRUD”**，不引入复杂技巧。

你可以把它理解成 Academy 后台的三个最小需求：

1. 运营新增一条学习路径，再修改说明，最后删除
2. 用户在课程目录里搜 `"SQLite"`
3. 后台筛出 `Backend Engineering` 路径下所有已发布课程

## 对应 demo

| Demo | 业务场景 | 重点能力 |
| --- | --- | --- |
| `runTrackCRUDDemo` | 临时创建一条学习路径，顺带演示自定义 JSON 字段的插入、更新、删除 | 生成的 CRUD helper、`db:"...,type:JSON"` |
| `runCatalogSearchDemo` | 给课程目录做关键词搜索和分页 | `PageCourse(...)`、关键词搜索 |
| `runBackendCatalogDemo` | 列出某条路径下的已发布课程 | 最直接的查询构建链路 |

## 先看哪些文件

1. `../academy/track.go`
2. `../academy/course.go`
3. `../academy/scenarios.go` 里的 `RunQuickstart`
4. `main.go`

## 运行后你会看到什么

输出 JSON 会分成三段：

- `track_crud`：插入的路径 ID、更新后的说明、更新后 round-trip 回来的 `skill_items`、是否删除成功
- `catalog_search`：关键词、总数、当前页课程标题
- `backend_catalog`：指定学习路径下的课程标题

## 这一层建议先理解

- `@TABLE`
- `tsq gen ./examples/academy`
- 生成的 `Insert / Update / Delete / Page...` helper
- `Select(...).From(...).Where(...).Build()`
- 自定义 `driver.Valuer` / `sql.Scanner` 类型如何配合 `db:"...,type:JSON"` 指定 DDL 类型

## 运行

```bash
make examples
./bin/examples/quickstart
go test ./examples/quickstart
```
