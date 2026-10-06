# 项目内存 — 写入、删除与乐观锁

判据与索引在 `../memory.md`。

## 决定：按条件写语句不校验 `version` 但自增它；`Set*` 是泛型方法 (2026-09-03)

`UpdateTable(t)` / `DeleteFrom(t)` 是给"调用方手里没有行对象"的场景的，校验版本没有
意义；但**自增不能省**：不自增，批量改动之前加载的对象随后 `Update(...)` 时版本号仍然对得上，
会静默覆盖掉批量改动——那正是使用者声明 `version` 想防的事。显式赋值版本列是构建错误。

否掉：给 `Update(item)` 加"跳过版本校验"开关（把乐观锁变成可选项）；让 `*Query[O]` 长出 `.Update()`（两边校验说不清）。

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

此前端到端零覆盖（和 `*time.Time` 那个 bug 同一盲区），门是 `examples/08-soft-delete-and-concurrency`。

## 决定：部分列读出的行不许整行写回，靠弱引用记住它们 (2026-09-19)

`partial.go`：读进表行类型却没填满写回列的行以 `weak.Pointer` 登记，`runtime.AddCleanup` 回收；整行写回查到就报错。否决过"禁止
部分列读进表行类型"（轻查询变啰嗦）。行不知道自己来自哪张表，所以**一个行类型只描述一张表**（2026-09-29 审计 P0：共用时按先定义的表
判断，`Update` 清零了别的表的列；维护者选了"禁止共用"而不是按查询来源追踪），同名重定义替换登记。

## 决定：v5 设计收尾——写入、删除与乐观锁 (2026-09-17)

- **Upsert 在 MySQL 上遇到别的唯一键也可能冲突就拒绝**：`ON DUPLICATE KEY UPDATE` 没有冲突目标，会静默更新
  无关的行。批量同键两行报错（PG 不许一条语句改一行两次），批量不回读。
- **写入热路径用列自带的类型化取值函数**，不用反射：100 行批量 INSERT 快约 19%（`write_bench_test.go`）。
- **没匹配到行分两种错误**：版本不符 `OptimisticLockError`（可重试），状态不符 `RowStateError`（重试无用），失败路径上回读来区分。
- **SQLite 表达式深度上限 1000 恰等于默认批量大小**（2026-09-22）：每行一个 `OR` 的版本匹配从 998 行起被拒。批量 WHERE
  只用扁平形状（`IN`、`CASE`）；不用行值 `IN`（SQLite 要求右侧子查询，MySQL 要 `ROW(...)`）。门 `TestBatchWritesFitTheDefaultBatchOnSQLite`。

## 决定：批量写部分失败时回读，不自动开事务 (2026-09-28)

`Batch*` 不开事务是规则，所以一行过期时其余行已写进库；曾把整块行退回旧状态，内存和库对不上、重试永不收敛。现在错误路径上
回读：版本是"加载值 + 1"**且**写入的每个值都对得上才算写成，时间按 1µs 容差比（曾跳过时间列：别人只改了时间就被当成自己写的，
版本前移后下一次 `Update` 覆盖了别人——2026-09-29 审计 P0）。无版本列时 MySQL 写原值报零行，只把回读不到的行算缺失；"本来就在目标状态"和"刚写成"分不出，只报短缺不点名。
批量更新**否掉 RETURNING**（MySQL 没有）；单行写入和按主键删用它、MySQL 另走回查。没删到的行报 `RowStateError`（2026-09-29，
按主键删与行级硬删一致，维护者定案；要静默用 `DeleteFrom`）。`BatchUpsert` 不回读，成功后托管列还原成传入值，免得行看起来最新。先只修了 `BatchUpdate`，同形的删除 / 恢复 / 跳过重复又被审计找出：**一次只修一条写路径必漏**，短缺处理集中在 `shortfalls`。**回读的比较按值不按文本**（2026-10-06 随机写序列差分找到）：MySQL 和 JSONB 把 JSON 规范化（排键、加空格），文本比较把写成的行当成没写成、版本留在原地、下一次写报假冲突；JSON 走 `sameJSON`，新的引擎会改写的类型（CHAR 去尾空格之类）要进同一处。

## 决定：批量更新是"表连接行列表"，类型各方言各有一个定法 (2026-10-05)

每列一个按主键分支的 `CASE` 让一批的代价是行数的平方（SQLite 默认批 1000 比批 50 慢 12 倍）；维护者选了换语句形状而不是调小默认批。行列表没有类型，**三个定法都是试出来的，别互相套**：
PG 的裸 `VALUES` 全读成 text，第一行放每列的类型化 NULL（`(SELECT col FROM t WHERE FALSE)`，不需要类型名）；MySQL 的 `VALUES ROW` 把值转成文本、拒绝非 UTF-8 的字节，改成"表上空分支 + `UNION ALL SELECT ?`"；
SQLite 不需要类型，但 `UNION` 链有 500 项上限，只能用 `VALUES`。门是 `TestBatchUpdateJoinsTheRowsOnEveryDialect` 和 `TestIntegrationBatchUpdateCarriesEveryValue`。
**MySQL 的空分支带着列的字符集**（2026-10-06）：列是 latin1 / gbk / ascii 而连接是 utf8mb4 时，`UNION` 定不出类型，1267（参数不是常量，服务器不肯转），整条语句在写行之前被拒。不知道列的字符集就写不出 `CONVERT(? USING …)`，把空分支转成 utf8mb4 又会让键的比较换排序规则（`_bin` 的键 `a` / `A` 会串行）——所以是**见到 1267 / 1270 / 1271 就逐行写**（`updateOneByOne`），不是换一种联接写法。

**时间戳截断到微秒、MySQL 用 `DATETIME(6)`；"当前时间"默认值写成 UTC 表达式**（PG/MySQL 的 `CURRENT_TIMESTAMP` 是会话本地时间，MySQL
还要带精度，否则 1067——只跑 SQLite 时漏了，CI 才发现）。已有的 `DATETIME` 列读回为原始类型，`Reconcile` 会加宽它。
**`default:` 只许可空字段**（2026-09-29 维护者定案）：零值曾被当成"未设置"，`false`/`0` 永远写不进；否掉"总是写非空字段"（`default:` 对
TSQ 自己的插入变成摆设）。只有自增主键的表写 `(pk) VALUES (DEFAULT)`（SQLite 写 `NULL`）；
MySQL 的 `SET` 从左到右求值，读前面赋值过的列的赋值**拒绝而不重排**——有环（交换）时无法重排。
