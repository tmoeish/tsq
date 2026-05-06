# Examples / Quickstart

如果你只想快速确认 TSQ 的基本流程，按这个顺序看：

1. 根目录 [`docs/quickstart.md`](../../docs/quickstart.md)
2. `../database/user.go`：最小 `@TABLE` 写法
3. `../main.go` 里的 `runCRUDDemo` 和 `runKeywordDemo`

建议你先只理解四件事：

- `@TABLE` 注解
- `tsq gen ./database`
- `tsq.Select(...).From(table).Where(...).Build()`
- `tsq.List(...)` / 生成的 CRUD 助手

先不要同时学习 `Result`、CTE、集合查询和运行时隔离。
