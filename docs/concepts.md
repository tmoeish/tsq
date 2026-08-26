# Concepts：从哪读起

TSQ 的核心心智模型——注解 DSL、代码生成、查询构建、运行时注册这几层怎么咬合——
写在随发布分发的使用者技能里：

**[`skills/tsq/references/CONCEPTS.md`](../skills/tsq/references/CONCEPTS.md)**

那份文件是这个主题的**唯一实质来源**。它讲：

| 小节 | 回答 |
| --- | --- |
| Main flow | 从 Go struct 到可执行查询，中间经过哪些步骤 |
| `@TABLE` / `@RESULT` | 两种注解各自声明什么 |
| Generated files | `*.tsq.go`、`*.result.tsq.go`、`runtime.tsq.go` 各是什么 |
| Owner model | `Owner` / `Table` / `Result` 三层语义的分工 |
| Runtime and execution | `tsq.Runtime` 与 `SQLExecutor` 的边界 |
| Query lifecycle | `Build()` 校验什么，执行期才校验什么 |
| Two boundaries to remember | `Where` / `Search` 不是 append；`InVar(nil)` 不是"忽略" |

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
