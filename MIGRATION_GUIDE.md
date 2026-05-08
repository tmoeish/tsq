# TSQ v4 Migration Guide

TSQ v4 是一个重大的架构升级，旨在通过 Go 泛型提供更强的编译期类型安全、更清晰的查询语义以及更完善的结果映射机制。

## 核心变更概览

| 特性 | v3 (及更早) | v4 |
| --- | --- | --- |
| **列类型** | `Col[T]` | `Col[Owner, T]` (强绑定所属结构) |
| **QueryBuilder** | 弱类型，推导式 | `QueryBuilder[Owner]` (强绑定结果类型) |
| **主表声明** | 隐式 (从 Select/Join 推导) | **显式** (必须调用 `From(table)`) |
| **Join API** | `Join(t).On(l, r)` (两步) | `Join(t, conds...)` (一步) |
| **结果映射** | `Col.Into(fp, json)` | `tsq.Into[Owner](source, fp, json)` |
| **执行 API** | `WithContext(ctx)` | `Method(ctx, ...)` (显式 ctx 首参) |

---

## 详细迁移步骤

### 1. 显式声明主表

在 v4 中，`From(table)` 不再是可选的。QueryBuilder 需要明确知道主表是谁以进行权限和列归属校验。

**旧代码：**
```go
tsq.Select(User_Name).Where(User_ID.EQ(1)).Build()
```

**新代码：**
```go
tsq.Select(User_Name).From(TableUser).Where(User_ID.EQ(1)).Build()
```

### 2. 升级 Join API

旧的 `.Join(...).On(...)` 链式调用被简化为单次 `Join(...)` 调用，且 ON 条件现在必须显式提供（`CROSS JOIN` 除外）。

**旧代码：**
```go
tsq.Select(cols...).From(TableUser).LeftJoin(TableOrg).On(User_OrgID, Org_ID)
```

**新代码：**
```go
tsq.Select(cols...).From(TableUser).LeftJoin(TableOrg, User_OrgID.EQCol(Org_ID))
```

### 3. 结果映射 `Into` 迁移

`Into` 不再是列的方法，而是一个包级泛型函数。这确保了投影目标字段确实属于指定的 Result Owner。

**旧代码：**
```go
userName := User_Name.Into(func(h any) any {
    return &h.(*MyResult).Name
}, "user_name")
```

**新代码：**
```go
userName := tsq.Into[MyResult](User_Name, func(r *MyResult) *string {
    return &r.Name
}, "user_name")
```

### 4. 执行 API 的 Context 处理

移除了 `WithContext`，所有执行方法（`List`, `Get`, `Insert`, `Update`, `Delete` 等）现在都要求 `context.Context` 作为第一个参数。

**旧代码：**
```go
tsq.WithContext(ctx).List(db, query)
user.WithContext(ctx).Insert(db)
```

**新代码：**
```go
tsq.List(ctx, db, query)
user.Insert(ctx, db)
```

### 5. 生成代码的重新生成

由于生成的符号（如 `TableUserCols`）现在带有泛型参数，你**必须**重新生成所有代码。

```bash
make examples  # 或者 tsq gen ./your/package
```

---

## Breaking Changes 细节

### Owner 泛型约束
`Select[Owner](cols...)` 接收的列必须满足 `Owner` 约束。这意味着你不能在一个 `Select[User]` 中直接混入 `Org_Name` 而不使用 `Into` 将其映射到 `User`（或者定义一个包含两者的 `Result` 结构）。

### 显式 Table 接口
所有的 `Insert`, `Update`, `Delete` 现在只接受实现了 `tsq.Table` 的结构体。对于纯结果集（Result），请使用 `Query` 进行查询。

### 提前错误校验
v4 的 QueryBuilder 在 `Build()` 阶段会进行更严格的校验：
- Join 条件必须引用已经 `From` 或 `Join` 引入的表。
- 投影的列必须在查询涉及的表中。

---

## 常见问题解答 (FAQ)

**Q: 为什么我的 `tsq.Select(...)` 报错说类型不匹配？**
A: 请确保你传入的所有列都属于同一个 Owner。如果是联表查询，请先定义一个 `@RESULT` 结构体，并使用生成或手写的 `Into` 投影到该结构体。

**Q: 我可以使用 v3 的生成代码配合 v4 的库吗？**
A: 不可以。v4 的核心接口（`SQLColumn`, `BoundColumn`）已经改变，旧代码无法编译。

**Q: `WithContext` 真的没了吗？**
A: 是的。为了符合 Go 的主流实践（Context 作为首参），我们彻底移除了 `WithContext` 链式调用。
