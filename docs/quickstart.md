# Quickstart：从哪读起

从空目录到第一条能跑的查询，完整步骤写在随发布分发的使用者技能里：

**[`skills/tsq/references/quickstart.md`](../skills/tsq/references/quickstart.md)**

那份文件是这个主题的**唯一实质来源**。它按顺序走完：

1. 把 TSQ 加进 module
2. 选一个放数据库模型的包
3. 写第一个带 `//tsq:table` 指令的表结构
4. `tsq gen`
5. 初始化 `tsq.Runtime`
6. 跑通第一条查询
7. 需要时加上事务
8. 出问题时先查什么——找不到包、生成 helper 报初始化错误、构建成功但执行报方言错误

## 想看真的能跑的代码

`examples/` 是**可运行的契约**，不是片段：

```bash
go run ./examples/01-getting-started   # 从一个结构体开始
go run ./examples/02-querying          # 然后按章往下走，一共 11 章
```

每章打印它执行的 SQL。第 2 到 11 章共用 `examples/shop` 的网店模型，生成物被提交，
所以打开就能看到 `tsq gen` 真实的输出长什么样。目录见 [`examples/README.md`](../examples/README.md)。

## 为什么这里只有一个链接

见 [`concepts.md`](concepts.md) 的同名小节：实质内容只许有一个归宿。

## 相邻的入口

- [`concepts.md`](concepts.md)——先建立心智模型
- [`skill.md`](skill.md)——把这份技能装进自己的项目
- [`../README.md`](../README.md)——总览与 API 速查
