# Examples / Full Suite

这里对应当前仓库的完整 runnable 示例套件。

## 入口文件

- `../main.go`：端到端执行所有示例并输出 JSON summary
- `../main_test.go`：验证 full-suite 的主要路径
- `../database/*.go`：手写表结构、Result 定义、mock schema
- `../database/*_tsq.go`：生成代码

## 覆盖能力

`main.go` 当前覆盖：

- CRUD generated methods
- alias / rebinding queries
- aggregation and `GROUP BY`
- keyword search and pagination
- `@RESULT` join queries
- `InVar()` dynamic `IN (...)`
- public searched `CASE`
- non-recursive CTE / `WITH`
- set operations
- chunked insert / update / delete

## 运行

```bash
make examples
./bin/examples
```

如果你只是第一次接触 TSQ，先不要从这里开始；优先看 `../quickstart/README.md`。
