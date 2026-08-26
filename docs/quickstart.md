# Quickstart：从哪读起

从空目录到第一条能跑的查询，完整步骤写在随发布分发的使用者技能里：

**[`skills/tsq/references/QUICKSTART.md`](../skills/tsq/references/QUICKSTART.md)**

那份文件是这个主题的**唯一实质来源**。它按顺序走完：

1. 把 TSQ 加进 module
2. 选一个放数据库模型的包
3. 写第一个带 `@TABLE` 注解的表结构
4. `tsq fmt` 然后 `tsq gen`
5. 初始化 `tsq.Runtime`
6. 跑通第一条查询
7. 需要时加上事务
8. 出问题时先查什么——找不到包、生成 helper 报初始化错误、构建成功但执行报方言错误

## 想看真的能跑的代码

`examples/` 是**可运行的契约**，不是片段：

```bash
go run ./examples/quickstart     # 最小可运行示例
go run ./examples/advanced       # 连接、子查询、CASE、集合操作
go run ./examples/full-suite     # 覆盖面最广的一份
```

`examples/academy` 是它们共用的模型包，生成物被提交，所以打开就能看到 `tsq gen`
真实的输出长什么样。

## 为什么这里只有一个链接

见 [`concepts.md`](concepts.md) 的同名小节：实质内容只许有一个归宿。

## 相邻的入口

- [`concepts.md`](concepts.md)——先建立心智模型
- [`skill.md`](skill.md)——把这份技能装进自己的项目
- [`../README.md`](../README.md)——总览与 API 速查
