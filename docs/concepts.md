# Concepts：从哪读起

TSQ 的核心心智模型——注解 DSL、代码生成、查询构建、运行时注册这几层怎么咬合——
写在随发布分发的使用者技能里：

**[`skills/tsq/references/concepts.md`](../skills/tsq/references/concepts.md)**

那份文件是这个主题的**唯一实质来源**。它讲：

| 小节 | 回答 |
| --- | --- |
| Main flow | 从 Go struct 到可执行查询，中间经过哪些步骤，每一步由哪份参考细讲 |
| Tables, rows and results | 行类型、表描述符 `TableOf` 和结果投影的分工 |
| Operands, parameters and arguments | 右值、参数和执行参数怎么对上 |
| Stages: the type system is the validator | 阶段式构建器为什么让写错的顺序编译不过 |
| Two validation points | `Build()` 校验什么，执行期才校验什么 |
| Runtime and executors | `tsq.Runtime` 与 `Executor` 的边界 |
| Semantics that never change silently | 空列表、软删除作用域、乐观锁、UTC 时间 |
| Adopting TSQ in an existing project | 在已有项目里一条路径一条路径地迁 |

其余主题（CLI、注解、生成代码、运行时、查询、表达式、写入、事务、分页与搜索、方言、错误、
v4 迁移）各有一份，入口是 [`skills/tsq/SKILL.md`](../skills/tsq/SKILL.md) 的路由表。

## 为什么这里只有一个链接

这一页曾经把上面的内容用中文重写了一遍。两份讲同一件事的文档必然漂移，而漂移之后
更糟的那份是看起来还对的那份。所以 `docs/` 现在只做索引，实质内容留在 `skills/tsq/`——
那是**随发布分发出去、被别人 `gh skill install` 装进自己项目**的东西，必须是最真的一份。
分工写在 [`AGENTS.md`](../AGENTS.md) 的所有权一节。

## 相邻的入口

- [`quickstart.md`](quickstart.md)——想直接跑起来
- [`skill.md`](skill.md)——想把这份技能装进自己的项目
- [`../README.md`](../README.md)——总览与 API 速查
- [`../BEST_PRACTICES.md`](../BEST_PRACTICES.md)——输入校验、分页、事务、生产环境建议
