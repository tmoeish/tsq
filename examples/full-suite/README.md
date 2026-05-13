# Examples / Full Suite

这一层把前两层能力收敛成一个完整报表：**学习旅程看板**。

把它想成后台里的一个页面：

- 左边先选一组学员
- 再选几条学习路径
- 后台返回这些学员在这些路径上的报名明细
- 每行都要带课程、讲师、得分、状态、报名时间
- 结果还要支持分页

## 这个示例到底在演示什么

| 需求 | TSQ 能力 |
| --- | --- |
| 把 `Learner + Enrollment + Course + Track + Instructor` 组装成一行结果 | joins |
| 让 API 直接消费结果结构 | `@RESULT` |
| 只保留“参与度足够高”的课程 | 子查询 |
| 给看板列表做分页和排序 | `tsq.Page(...)` |

## 核心文件

| 文件 | 作用 |
| --- | --- |
| `../academy/learningjourney.go` | 定义 `LearningJourney` Result 和预构建分页查询 |
| `../academy/scenarios.go` 的 `runComprehensive` | 传入筛选条件并执行分页查询 |
| `main.go` | 打开 SQLite 示例库并输出完整 JSON |

## 输出结构

`full-suite` 的输出分三层：

1. `quickstart`：基础 demo 结果
2. `advanced`：进阶 demo 结果
3. `comprehensive`：最终学习旅程看板

其中 `comprehensive.first` 是当前页第一条记录，方便直接看 `@RESULT` 映射出来的最终形状。

## 运行

```bash
make examples
./bin/examples/full-suite
go test ./examples/full-suite
```

如果你是第一次接触 TSQ，先看 `../quickstart/README.md`，再看 `../advanced/README.md`。
