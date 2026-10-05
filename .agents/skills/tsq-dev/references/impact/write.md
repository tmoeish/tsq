# 变更影响 — 写入路径

处理你匹配的每个触发器；索引与 `[门禁]` 标记的含义在 `../change-impact.md`。

## 改了数据库填值的列（`Fill`、`default:` / `generated:`）

- 三条路径都要一致：插入的列清单（`insertColumns`，按行分组，因为"未设置"是逐行的）、`Update` /
  `Upsert` 的 SET 清单、单行写入后的回读（`reloadColumns`）。漏一处就会写进一个数据库该自己算的列。
  **`Upsert` 曾自己拼列清单**，有生成列的表（示例的 `Course`）因此一次都 upsert 不了，而规则就写在这里；
  现在 `upsertColumns` 调 `insertColumns`（只多一个恒写的 `deleted_at`），回读用 `databaseFilled`。
  别再给某条写路径单独拼列清单。
- "交给默认值"只看 `holdsNull`（nil / `Value()==nil`），**不看** `isUnset`（后者把零值和指向零值的指针也当未设置，
  只给托管时间戳用）。`default:` 只许可空字段，生成器（`validateDatabaseFilledFields`）和 `Define` 各拒一次。
- **生成列不参与 schema 对账**（`diffTableColumns` 里过滤）：SQLite 的 `table_info` 根本不列它，
  MySQL/PG 报的类型和默认值也和声明不同，比较的结果是每次启动都想改一次。
- 端到端的门是 `examples/07-writing-data` 的 7.1（`default:` 和 `generated:` 读回）和 `TestIntegrationDatabaseFilledColumns`
  （后者还断言第二次启动零 DDL）。

## 改了删除语义或托管列（`rows.go`、`softdelete.go`、`TableSpec`）

- **删除语义由表的类型决定，不由调用点、也不由 scope 决定**：有 `deleted_at` 的表是
  `SoftDeleteTableOf`，它的 `Delete*` 恒写墓碑；`TableOf` 上根本没有 `Delete*`，只有恒 DELETE 的 `Hard*`。
  加一个软删除入口就放在 `SoftDeleteTableOf` 上、同时在 `TableOf` 上加它的 `Hard*` 对偶，并在
  `compilefail_test.go` 加一条"普通表上编译不过"。**不要让任何删除路径读 `softDeleted()` 来选软删还是
  硬删**：它只回答"要不要活行过滤"，一旦兼任，`WithDeleted()` 就会把删除变成物理删除（v5 发版前出过，
  见 `../memory/write.md` 的软删除那条）。`TestWithDeletedOnlyDropsTheLiveRowFilter` 守着 `WithDeleted()` 只改可达的行。
- 生成器按 `DeletedAtField` 在两种表类型之间选（`table.go.tmpl` 的 `$base` / `$bind`），`reserved.go`
  按同一个判据取方法集；三处的判据必须是同一个字段。
- **软删除和恢复的状态不符报 `RowStateError`，版本不符报 `OptimisticLockError`**：前者重试不能解决，后者能。
  语句同时校验两者，匹配不上时 `tombstoneMismatch` 回读版本号判别；新的"校验状态又校验版本"的写路径要用同一个判别，
  门是 `TestStaleSoftDeletesAreVersionConflicts`。
- **软删除和恢复只写托管列**（`setTombstone`），自带版本校验和自增；`Update` 永远不写 `created_at` /
  `deleted_at` 且只匹配活行。`softdelete_test.go` 用一张没有 `version` 的表守着"旧副本复活已删行"。
- **托管时间戳和墓碑在库里维护**（`applyTimestamp` / `applyTombstone` / `isUnset`）。新增一种
  字段形态要同时加进这三个函数、`timestamps_test.go` 的类型表、生成器的
  `validateTimestampField` / `validateSoftDeleteField`，以及 `skills/tsq` 的"Supported field
  types"。
- `Insert` 只在字段**未设置**时盖 `created_at` / `updated_at`（导入历史数据时不能丢调用方的
  时间），`Update` **总是**刷新 `updated_at`。
- 给 `TableSpec` 加字段不是破坏性变更；给 `Table` 接口加方法也不影响使用者（它是封闭的），但
  两个实现（`TableOf`，`SoftDeleteTableOf` 经内嵌自动跟上；`cteTable`）都要跟上。`[门禁: api-check]`
- 软删除的端到端门是 `examples/08-soft-delete-and-concurrency`（删除、`RowStateError`、`WithDeleted` + `Restore`、`DeleteFrom`）。
- **软删除作用域在渲染里，不在调用点**：新增一种表出现的位置（新的 JOIN 类型、`UPDATE ... FROM`、
  新的集合形态）必须在 `writeFromWhere` 里表态它的作用域放 WHERE、ON 还是派生表，并在
  `TestSoftDeleteScope` 和 `TestIntegrationSoftDeleteScopeJoins` 里各加一条。给 `Table` 接口加实现
  也要实现 `softDeleted()`。

## 改了 upsert（`upsert.go`）

- `upsertRows` 先按相邻同形状分组、写之前检查每一组（不许写到一半才报错）；只有自增主键要写的组交给 `insertChunk`
  （零值主键不会冲突）。批量成功后 `snapshot.restore(nil)` 把托管列还原成传入值：批量不回读，盖上的戳只会让行
  看起来是最新的。`Conflict.Update` 点名、但这批行没进 INSERT 的列（nil 的 `default:` 列）写成 `NULL`。

- **MySQL 的 `ON DUPLICATE KEY UPDATE` 匹配所有唯一键**，`checkUpsertRows` 因此在行可能撞上别的唯一键
  时拒绝。放宽它之前先想清楚：那一行会静默地更新一条和指定键无关的行。
- 三个方言的语句形状只有 `TestIntegrationUpsert` 能证明，包括"值没变时 MySQL 仍报出主键"。本地没有
  MySQL 时，SQLite 绿不代表什么：第一版的 `version = version + 1` 在 MySQL 上和行别名 `tsq_new` 的同名列
  冲突（1052 ambiguous），**引用已有行的列一律带表名**。
- 更新时的列清单（不写键、主键、`created_at`，`version` 自增）和 `UpdateTable` 的语义保持一致；
  改一边要看另一边。
- 冲突目标不是主键、主键又不是自增时，语句拿不回被更新那一行的主键：`adoptStoredKeys` 按目标列回读并改写行的主键，
  批量和 MySQL 的单行走它；多列目标每行一个 `OR`，按 `maxOrTerms` 分段（SQLite 深度上限）。PostgreSQL / SQLite 的
  单行走 `upsertReturning`：`RETURNING` 主键加 `upsertReadBack` 的列，拿到的就是被写那一行的，不再回读。
- `Conflict.Update(cols)` 在 `upsertStatement` 里收窄 SET 列表，`updated_at` 和 `deleted_at` 照写（和行级
  `Update(cols)` 一致）；`upsertUpdate` 拒绝键、主键、`created_at`、托管列和生成列。插入那一半永远是整行。

## 改了按条件写语句（`mutation.go`）

- **`version` 自增不校验是契约**。去掉自增会让并发的乐观锁静默失效，加上校验会让它退化成
  逐行更新。`exec_test.go` 的 `TestConditionalWrites` 守着"之前加载的行随后冲突"。
- 读列值用 `value(row, col)`（走 `columnCore.get`），不要回到 `field(row, col).Interface()`：那是反射路径，
  批量写会按列×行付成本。`write_bench_test.go` 是量它的地方。
- **托管时间戳是执行时绑定的内置参数**（`deletedAtParam` / `updatedAtParam`；`UpdateTable` 在调用方没 `Set` 时也刷新 `updated_at`）。改成构建时
  求值，包级语句就会永远写进程启动时间——v4 就是这么错的。
- **语句形状要在三个方言上真跑**：SET 左侧不带表限定、WHERE 带表限定，靠
  `internal/integration` 的 `TestIntegrationMutationsByCondition` 证明。
- 只能引用目标表本身：`Build` 按 `tableDef` 指针加表名比较 `allTables()`，别名会被拒，
  `WithDeleted()` 视为同一张表。放开这一点要先为
  三个方言各设计一种 `UPDATE ... FROM` 写法。
- `Set` 是泛型方法，所以 `UpdateStage` / `SetStage` 必须是具体类型；`Where` 之后才是接口。给 `UpdateStage` 加方法
  时想清楚它能不能出现在第一个 `Set` 之前。
- 使用者文档三处要同步：`skills/tsq/references/REFERENCE.md` §8 与 §13、`README.md`
  "常见边界"、`BEST_PRACTICES.md` §3.8。

## 改了批量写（`rows.go`）

- 硬删的短缺（`hardDeleteShortfall`）按回读分两类：还在的行是版本冲突，不在的行是 `RowStateError{RowExists}`，
  没有 `version` 列的表也查——行级硬删和 `BatchHardDeleteByPK` 必须对"行不存在"说同一件事。按主键删时数据库按
  自己的排序规则匹配键（MySQL `_ci`），`missingKeys` 先比行数、再按不分大小写和尾部空格比，别退回 Go `==`。

- 分批的单位是**行**，数据库数的是**占位符**：
  - **上限按方言**（`sqldialect.MaxBindParams`）：MySQL / PostgreSQL 65535，**SQLite 32766**。
  - **每行占位符数按操作算**：INSERT 每列一个；UPDATE 每列两个（`CASE pk WHEN ? THEN ?`）加
    WHERE 的 `keyMatchParams`；DELETE 每行 `keyMatchParams`。每条语句一次的参数（墓碑、时间戳）从上限里
    扣掉。改了语句形状就要回来核对 `effectiveChunkSize` 的实参。
- **占位符不是唯一的上限，表达式深度是第二个**：SQLite 拒绝深于 1000 层的表达式，恰好等于
  `defaultBatchSize`。批量语句的 WHERE 只许用扁平形状（`IN` 列表、`CASE` 分支），每行一个 `OR` 就是
  每行深一层。`TestBatchWritesFitTheDefaultBatchOnSQLite` 用默认批量大小真写 1000 行；新的批量写路径
  要进这个测试，形状要进 `TestIntegrationBatchWritesMatchByVersion` 在三方言上跑。
- **`WithSkipDuplicates` 的错误处理不可移植**：事务内必须用 savepoint 括住每一行（PostgreSQL
  一条语句失败就 aborted），事务外**不能**发 savepoint（`25P01`）。事务与否读 `execScope.tx`。
  别改成 `INSERT IGNORE` / `ON CONFLICT DO NOTHING`：前者在 MySQL 上吞掉所有错误，后者让
  `RETURNING` 无法按位置回填主键。`TestIntegrationBatchInsertIgnoresDuplicatesInsideTransaction`
  只有在真实 PostgreSQL 上才有意义。
- `BatchDeleteByPK` / `BatchHardDeleteByPK`（`deleteByPK` → `deleteKeys`）要知道哪些键删到了：有 `RETURNING` 的方言
  从语句里读，MySQL 软删按本次墓碑回查、硬删在删除前查存在的键。没删到的键进 `RowStateError.Keys`。
- **一条 INSERT 只合并相邻的同形状行**（`insertGroups`）：按形状全局分组会让自增主键不按切片顺序分配。
  单行 `Insert` 在有 `RETURNING` 的方言上走 `insertReadingBack`，数据库填的列在同一条语句里取回；
  `Returning(...)` 返回空串（MySQL）就退回 `reloadColumns`。`startVersion` 让为零的 `version` 从 1 开始，与 DDL 默认值一致。
- **写之前改了行上的字段，写失败就要放回去**：`Insert` / `Upsert` 用 `snapshotFields` 记下托管列和主键，只放回没写成
  的行；`BatchUpdate` 靠 `updateMismatch` 的回读判断哪些行写成了。新增一条会盖戳的写路径要走同一套，
  `TestFailedWritesLeaveRowsAsTheyWere` / `TestBatchUpdateWithAStaleRowSaysWhichAndKeepsTheRest` 守着。
- 给多行 `*time.Time` 字段写时间要**每行各自** `applyTimestamp`，不要 `Set` 同一个 `reflect.Value`（指针会被共享）。
- `batch_test.go` 的宽表用例是门：它真的写一张 200 列的表。UPDATE 的求值开销约是行数² × 列数，
  所以表做宽、行做少；它在 `-race` 下跳过（转译的 SQLite 慢约四十倍，且没有并发可查），
  普通 `test` 里照跑。
- **批量写的短缺一律回读**：`BatchUpdate`（`updateMismatch`）、`BatchDelete` / `BatchRestore`（`tombstoneShortfall`）、
  `BatchHardDelete`（`hardDeleteShortfall`）在匹配行数不够时回读这一块，只给写成的行改内存状态，`Keys` 列出其余的行；
  跨语句的汇总走 `shortfalls`（后面的语句出别的错时也不丢前面的 `Keys`）。新增一条"一条语句写多行"的写路径要接进同一套。
  回读比较跳过时间列（`isTimeField`），数据库可能按更粗的精度存。
- `insert` 的 `written` 集合要由每条真正写成行的路径登记，`WithSkipDuplicates` 也不例外，否则出错时会把已入库的行恢复成旧值。
