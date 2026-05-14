# Examples / Advanced

这一层不再只做列表页，而是开始做**“像业务代码的查询和批处理”**。

每个 demo 都围绕 Academy 的一个真实需求：

| Demo | 业务场景 | 重点能力 |
| --- | --- | --- |
| `runAliasDemo` | 查课程和它的前置课 | alias / rebinding |
| `runAggregateDemo` | 统计每条学习路径的报名数和平均分 | aggregate、`GroupBy`、`Having` |
| `runInVarDemo` | 前端传入一组课程 ID，后台批量查询 | `InVar()` |
| `runSubqueryDemo` | 用“先查路径，再查报名，再查学员”的方式筛数据 | `InSub`、标量子查询 |
| `runCaseDemo` | 按报名状态和分数打标签 | `CASE` |
| `runCTEDemo` | 先定义一组平台课程，再继续查询 | non-recursive CTE |
| `runSetOpsDemo` | 合并两条路径的课程，或排除有前置课的课程 | `UNION`、`EXCEPT` |
| `runChunkedDemo` | 分块插入、更新、删除报名记录 | chunked helper（事务边界由调用方控制） |

## 怎么读最顺

1. 先看 `../academy/scenarios.go` 中对应函数
2. 再运行 `main.go` 看 JSON 输出
3. 最后回头看 README 里的场景和能力对照

## 输出结构

运行后 JSON 会分成这些节点：

- `alias_prerequisite`
- `track_metrics`
- `dynamic_in`
- `subquery`
- `case_labels`
- `cte`
- `set_ops`
- `chunked`

它们一一对应上面的 demo，方便你边读代码边对照结果。

`runChunkedDemo` 主要演示 chunked helper 的调用方式。它默认直接使用示例里的 `engine`，没有额外包外层事务；如果你的业务需要整批原子提交，请改为显式创建 `*sql.Tx` 并把事务 executor 传给这些 helper。

## 运行

```bash
make examples
./bin/examples/advanced
go test ./examples/advanced
```
