# 10 表结构

```bash
go run ./examples/10-schema-and-migrations
```

| 小节 | 演示 |
| --- | --- |
| 10.1 | `tsq gen` 为三种方言各生成一份 DDL |
| 10.2 | 生产做法：迁移工具执行 DDL，`tsq.NewRuntime` + `SchemaPolicyValidate` 启动时校验 |
| 10.3 | 空库 + `Validate`：`*tsq.MissingTableError` |
| 10.4 | 库里的表少了列：`*tsq.SchemaMismatchError` 列出差异 |
| 10.5 | `SchemaPolicyCreateMissing` 补上缺的列和索引 |
| 10.6 | 表描述符上的 `ColumnSpecs()` / `Indexes()` |

## 生成的 DDL

`tsq gen` 在包目录里写 `sqlite.sql`、`mysql.sql`、`postgres.sql` 和 `tsq.json`：

- 每份 `.sql` 先是完整的建表语句；之后每次 `tsq gen` 改动了表结构，追加一段以 `-- Migration:` 开头的迁移。
  新库执行完整的那段，已有的库执行还没执行过的迁移段——交给你的迁移工具；
- 破坏性的语句（删表、删列）写成注释，标着 `-- DESTRUCTIVE`，`tsq gen` 会警告，要你确认后手动执行；
- `tsq.json` 是渲染这些迁移用的历史，**要提交**；
- CI 里跑 `tsq gen --check`，生成物过期时以退出码 2 失败。

示例里 [`shop/schema.go`](../shop/schema.go) 用 `//go:embed` 把三份 DDL 编进程序。

## 四种启动策略

| 策略 | 做什么 | 适合 |
| --- | --- | --- |
| `SchemaPolicyManual`（默认） | 什么都不做，记一条日志 | 生产：表结构由迁移管理 |
| `SchemaPolicyValidate` | 对不上就拒绝启动 | 生产：确认迁移已经执行 |
| `SchemaPolicyCreateMissing` | 建缺的表、列、索引；已有的列不一致仍然拒绝启动 | 开发、测试 |
| `SchemaPolicyReconcile` | 再把列改回声明的样子，删掉不再声明的列（连同数据） | 原型：改完结构体重启就行 |

任何策略都**从不删表**：一个服务只知道自己声明的表，分不清"这张表过时了"和"这张表是别的服务的"。

`tsq.Open` 自己开连接池；池子是你开的（带监控、特殊配置），用 `tsq.NewRuntime(ctx, db, 方言, 表, 选项...)`，
它不接管这个池子，`Close` 不会关它。

下一章：[11 方言和可观测性](../11-dialects-and-observability/)
