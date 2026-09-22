# 项目内存 — 查询语义

判据与索引在 `../memory.md`。

## 决定：相关子查询靠 `Correlate(...)` 显式声明，不靠推断 (2026-09-03)

`validateJoinGraph` 要求每张被提到的表都在 FROM/JOIN 图里，相关引用天生不满足。旧报错建议
`use CrossJoin`，**照做会静默改变语义**：join 进来的表遮蔽外层同名表，谓词不再相关，而且不报错。

**否掉"自动放行未知表"**：那等于把打错的表名一起放行。既 `Correlate` 又 join 同一张表是构建错误；带
`Correlate` 的查询不能单独执行。`api-check` 看不见方法调用，语义由
`TestCorrelatedSubqueryCarriesItsParameters` 守着。

## 两个测试各自编码了相反的意图，代码同时满足它们 (2026-09-16)

一条测试断言 `WithMaxPageSize(5000)` 能放行 3000，另一条断言"没有 runtime 能抬高绝对上限"，两条都绿：
`Validate` 把上限夹到 1000 而 `Normalize` 不夹。**一对矛盾的断言可以同时为真，只要实现里有两条路径各
满足一条**；同一个概念的两个入口要放在一个用例里比。后来只留一条路径：`PageRequest.Paging` 只拒绝没有
意义的输入，大小由 `Page` 按 runtime 上限封顶（2026-09-19），`Validate` / `Normalize` / `MaxPageSize()` 删除。

定案取名字：`DefaultMaxPageSize` 是**默认**，`WithMaxPageSize(n)` 是这个 runtime 的上限，双向生效。
把常量当硬顶会让 `WithMaxPageSize(5000)` 变成一句空话——库不该用一个编译期常量去否决调用方明确的选择。

## 决定：读单行只留两个入口，语义写在名字里 (2026-09-09，v5)

`Get`（无行报 `sql.ErrNoRows`）和 `Find`（`nil, nil`）；删掉的 `Load(holder)` 没法不比较错误就表达
"没查到"。`Count` 只留 `int64`（截断是静默的）。单行读取加 `LIMIT 1`，**必须在行锁之前**——这道门
在 v5 核心重写时随旧测试文件一起丢过，现在是 `TestSingleRowReadsLimitBeforeTheLock`。`Exists` 不用
`COUNT`：它要访问每个匹配行，去回答第一行就能定的问题。

## 决定：v5 设计收尾——查询语义 (2026-09-17)

- **列函数是包级泛型函数**（`tsq.Upper(col)`），用 `Text` / `Number` 约束（方法不能约束类型参数）；搜索列
  只能是 `string`。
- **固定值是 `tsq.Val(v)` 右值，`*Val` 方法全删**（一度否决，后由维护者拍板）：类型只由值推断，`int64` 列上
  写 `tsq.Val(int64(90))`，别因为要写转换改回去。
- **`Page` 吃 `Paging`**，字符串形态的 `PageRequest` 只在 HTTP 边界；`Paging(sortable...)` 要求列出可排序
  列，v4 按选出来的列放行，未建索引的列也能被客户端拿来排序。
- **可空性：类型区分表列，表达式在运行期推导**（`exprInfo.null`）。外连接和无 GROUP BY 的聚合只在查询
  上下文里可知，检查因此在读行前而不在 `Build`（会拒掉合法子查询）。否决值类型包成 `Null[T]`：一个值不能
  同时是两种 `RHS`。
- **关联装配不引入关系 DSL**：`AttachMany` 只做收键、一次查询、按键分组，子查询仍由调用方给出。
- **派生表达式不是列**：`derived` 不留扫描目标，`Select(tsq.Date(时间列))` 在编译期就写不出来（以前运行期
  扫描失败）；单值查询走 `SelectValue`。
- 已知未处理（2026-09-22）：同一 CTE 里两个派生项来自同一源列（`SUM(amount)` 与 `MAX(amount)`）会得到同一个
  `AS "amount"`，外层引用时数据库报歧义（响亮，不静默）；要支持得让使用者给输出列起名，等有人真需要再做。
- **派生选择项写 `AS <Name()>`，不用 JSON 名**（2026-09-22）：CTE 的列靠 `源列.WithTable(cte)` 按源列名查找，集合操作的
  `ORDER BY` 也按它。`ResultColumn` 因此有 `Asc` / `Desc`（排序不是谓词），按选中的投影给集合操作排序。
- **`driver.Value` 是定义类型**（2026-09-22）：手写的 `interface{ Value() (any, error) }` 永远不匹配 `driver.Valuer`，两处
  NULL 检查因此从未生效（审计发现）。只断言 `driver.Valuer`，门是 `TestValuersAreComparedByTheirValue`。
- **超长列表参数用显式 `ListIn`，否决自动分块**：`OR`、`NOT IN`、排序、聚合、LIMIT 分块后语义都变。
- **游标分页展开成 `a < ? OR (a = ? AND b > ?)`，不用行值比较**：后者只在所有列同向时成立。最后一列必须
  是主键（否则同值行会被跳过或重复）；游标带排序指纹。
- **关键词是执行参数 `tsq.Keyword`**（放在 `Paging` 里就没法 `Iter` / `Count`），但 `PageRequest.Paging` 把请求的关键词
  带进未导出字段，由 `Page` 补上：否则忘传就悄悄返回不搜索的结果。
- **`Page` 的一致性靠只读快照事务，不靠 `COUNT(*) OVER()`**：窗口函数在 `DISTINCT` 前求值、PG 不能和
  `FOR UPDATE` 同用、越界页没有行带回总数。代价是一对 BEGIN/COMMIT（单语句的 `ListIn` 因此不开事务）。
