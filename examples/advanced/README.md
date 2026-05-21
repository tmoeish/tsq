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
| `runChunkedDemo` | 在一个事务里分块插入、更新、删除报名记录 | `runtime.WithTx(...)` + chunked helper |
| `runOptimisticLockDemo` | 先触发一次过期版本失败，再自动重试更新报名记录 | `tsq.WithTx1(...)`、`IsOptimisticLockError`、自动乐观锁重试 |

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
- `optimistic_lock`

它们一一对应上面的 demo，方便你边读代码边对照结果。

`runChunkedDemo` 主要演示 chunked helper 和 `runtime.WithTx(...)` 的配合方式。  
如果你的业务需要整批原子提交，推荐直接把整段 chunked 流程放进 `WithTx(...)`；只有在你已经手动持有 `*sql.Tx` 时，才需要退回到底层事务 executor 方案。

`runOptimisticLockDemo` 演示的是 **SQLite 下可以真实运行的锁语义**：自动乐观锁。  
它会先制造一个过期快照，让第一次事务尝试因为版本冲突失败；随后 `tsq.WithTx1(...)` 在 `Retry: tsq.IsOptimisticLockError` 的配置下自动重试，重新加载新版本并完成更新。

这里**不会**执行 `ForUpdate()` / `ForShare()` 之类的行锁 query，因为 examples 统一跑在 SQLite 上，而 SQLite 不支持这些语句；行锁示例请放到 MySQL 或 PostgreSQL runtime 中演示。

## 运行

```bash
make examples
./bin/examples/advanced
go test ./examples/advanced
```
