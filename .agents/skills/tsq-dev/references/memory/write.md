# 项目内存 — 写入、删除与乐观锁

判据与索引在 `../memory.md`。

## 决定：按条件写语句不校验 `version` 但自增它；`Set*` 是泛型方法 (2026-09-03)

`UpdateTable(t)` / `DeleteFrom(t)` 是给"调用方手里没有行对象"的场景的，校验版本没有
意义；但**自增不能省**：不自增，批量改动之前加载的对象随后 `Update(...)` 时版本号仍然对得上，
会静默覆盖掉批量改动——那正是使用者声明 `version` 想防的事。显式赋值版本列是构建错误。

否掉的两个替代：给 `Update(item)` 加"跳过版本校验"开关，解决不了"手里没有对象"的真正
场景，还把乐观锁变成可选项；让 `*Query[O]` 长出 `.Update()`，`Query` 背着 SELECT 列和分页
语义，两边校验都说不清。

`Set*` 做成 Go 1.27 泛型方法是为了把"列和值类型一致"从运行期校验升成编译错误，代价是它们
只能住在 `Where` 之前的具体类型上（接口方法不能带类型参数）。`Where` 必需由类型强制，全表
操作要写显式 `And()`——"静默去掉过滤条件"在写路径同样不允许。

## 决定：软删除是一种表类型，`Delete` 恒软删、物理删除恒带 `Hard` (2026-09-09，2026-09-22 改为类型)

判据是真实用法：软删除的行在业务上就是删掉了，只有审计才回头看。`softDeleted()` 曾同时回答"要不要活行过滤"和
"软删还是硬删"，`WithDeleted().Delete` 于是静默成了物理删除（审计发现，零测试）。现在只有 `SoftDeleteTableOf` 有
`Delete*` / `Restore` / `WithDeleted`、`DeleteFrom` 只收它，`grep HardDelete` 即全部物理删除点；**别为少一个类型合回去**。

**已删行不可见是表的默认作用域，不是生成查询里的过滤条件**（2026-09-17）。模板加 `deleted_at = 0` 时，
手写查询和 JOIN 里的软删除表全都漏掉。作用域放 WHERE 会把 LEFT JOIN 变成 INNER JOIN，放 ON 挡不住
RIGHT JOIN 被保留侧的已删行，所以有 RIGHT / FULL JOIN 时整张表改成活行派生表；`WithDeleted()` 是唯一出口。

- **软删除不再复用 update 路径**：复用时 `Update` 要写 `deleted_at`，没有 `version` 的表上旧副本一次 `Update`
  就把行复活，手工构造的行还会清零 `created_at`。现在 `Update` 不碰这两列，`Delete` / `Restore` 只写托管列。
- **墓碑值靠 `applyTombstone` 按字段形态分派**，最后一环 `sql.Scanner.Scan(now)` 同时吃下
  `sql.NullTime` 和 `null.Time`，**根包因此不必 import nullbio**。

此前端到端零覆盖（和 `*time.Time` 那个 bug 同一盲区），门是 `runSoftDeleteDemo`。

## 决定：部分列读出的行不许整行写回，靠弱引用记住它们 (2026-09-19)

`partial.go`：查询只选某表的部分列、读进该表行类型时，每行以 `weak.Pointer` 为键登记，`runtime.AddCleanup`
在行被回收时删掉；不带列的 `Update` / `BatchUpdate` / `Upsert` 查到就报错。否决过"禁止部分列读进表行类型"（最常见的轻查询会变啰嗦）。

## 决定：v5 设计收尾——写入、删除与乐观锁 (2026-09-17)

- **Upsert 在 MySQL 上遇到别的唯一键也可能冲突就拒绝**：`ON DUPLICATE KEY UPDATE` 没有冲突目标，会静默更新
  无关的行。批量同键两行报错（PG 不许一条语句改一行两次），批量不回读。
- **写入热路径用列自带的类型化取值函数**，不用反射：100 行批量 INSERT 快约 19%、UPDATE 约 28%（`write_bench_test.go`）。
- **没匹配到行分两种错误**：版本不符 `OptimisticLockError`（可重试），状态不符 `RowStateError`（重试无用）。软删除 / 恢复的
  语句同时校验两者，曾一律报后者；现在失败后按字段类型回读版本来区分（`tombstoneMismatch`，只在错误路径上多一次查询）。
- 已知未处理（2026-09-22）：`Insert` 的 `assignInsertIDs` 和 #33d 修掉的 Upsert 一样吞掉 `LastInsertId` 的错误。MySQL /
  SQLite 驱动实际不会失败、PG 走 `RETURNING`，所以现在碰不到；**加第四种驱动或方言之前**改成返回错误。
- **SQLite 表达式深度上限 1000 恰等于默认批量大小**（2026-09-22）：每行一个 `OR` 的版本匹配从 998 行起被拒。批量 WHERE
  只用扁平形状（`IN`、`CASE`）；不用行值 `IN`（SQLite 要求右侧子查询，MySQL 要 `ROW(...)`）。门 `TestBatchWritesFitTheDefaultBatchOnSQLite`。
