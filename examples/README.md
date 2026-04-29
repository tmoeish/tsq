# TSQ Examples

这个目录现在按**上手顺序**来看，而不是按“能力展厅”来看。

| 入口 | 适合谁 | 内容 |
| --- | --- | --- |
| [`quickstart/README.md`](quickstart/README.md) | 第一次接触 TSQ | 最短路径、最少概念、先跑通第一条查询 |
| [`cookbook/README.md`](cookbook/README.md) | 已经跑通基础示例 | 更像真实业务代码的分页 API、事务写入场景 |
| [`full-suite/README.md`](full-suite/README.md) | 想系统浏览能力边界 | 对应当前 `examples/main.go` 的完整能力展示 |

## 当前 runnable 示例在哪里

仓库当前可直接运行的示例套件仍然是：

- `main.go`：端到端 runnable full-suite
- `main_test.go`：full-suite smoke tests
- `database/*.go`：手写表结构和 `@RESULT`
- `database/*_tsq.go`：生成代码
- `database/mock.sql`：SQLite 示例 schema

## 运行方式

```bash
make examples
./bin/examples
go test ./examples -v
```

## 生成文件说明

以下文件由 `tsq gen` 生成，不要手改：

- `database/*_tsq.go`
- `database/*_result_tsq.go`

修改 `database/*.go` 后，使用下面的命令重新生成：

```bash
tsq gen ./examples/database
```
