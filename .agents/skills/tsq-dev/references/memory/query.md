# 项目内存 — 查询语义

判据与索引在 `../memory.md`。

## 决定：相关子查询靠 `Correlate(...)` 显式声明，不靠推断 (2026-09-03)

`validateJoinGraph` 要求每张被提到的表都在 FROM/JOIN 图里，相关引用天生不满足。旧报错建议
`use CrossJoin`，**照做会静默改变语义**：join 进来的表遮蔽外层同名表，谓词不再相关，而且不报错。

**否掉"自动放行未知表"**：那等于把打错的表名一起放行。既 `Correlate` 又 join 同一张表是构建错误；带
`Correlate` 的查询不能单独执行。`api-check` 看不见方法调用，语义由
`TestCorrelatedSubqueryCarriesItsParameters` 守着。

## 两个测试各自编码了相反的意图，代码同时满足它们 (2026-09-16)

`WithMaxPageSize(5000)` 放行 3000 与"没有 runtime 能抬高上限"都绿（`Validate` 夹到 1000，`Normalize` 不夹）。**同一概念的
两个入口放在一个用例里比**；`DefaultMaxPageSize` 是默认不是硬顶。同类：`Page(Paging.OrderBy)` 曾不查集合运算的输出列、
`Count` 曾不认 `List` 的参数——新增与构建器等价的执行期入口时，把构建器的校验调用一遍。

## 决定：读单行只留两个入口，语义写在名字里 (2026-09-09，v5)

`Get`（无行报 `sql.ErrNoRows`）和 `Find`（`nil, nil`）；删掉的 `Load(holder)` 没法不比较错误就表达
"没查到"。`Count` 只留 `int64`（截断是静默的）。单行读取加 `LIMIT 1`，**必须在行锁之前**——这道门
在 v5 核心重写时随旧测试文件一起丢过，现在是 `TestSingleRowReadsLimitBeforeTheLock`。`Exists` 不用
`COUNT`（要访问每个匹配行），也不扫描那一行（否则会因可空性检查拒绝回答）。

## 决定：v5 设计收尾——查询语义 (2026-09-17)

- **列函数是包级泛型函数**（`tsq.Upper(col)`），用 `Text` / `Number` 约束（方法不能约束类型参数）；搜索列
  是底层为 `string` 的类型（生成器按 go/types 判断，不比类型名）。
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
- **CTE 输出列重名在 `Build` 拒绝，不加起别名的 API**（2026-09-28）：`SUM(amount)` 与 `MAX(amount)` 都叫 `amount`，
  而 CTE 的列靠名字找。普通 SELECT 里的重名改写成 `tsq_c<位置>`（读行按位置），CTE 里不能这么做——那会让
  `amount.WithTable(cte)` 静默拿到第一个。
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

## 决定：集合运算链从左到右求值 (2026-09-28)

SQL 标准和 MySQL / PostgreSQL 让 `INTERSECT` 比 `UNION` / `EXCEPT` 结合得紧，SQLite 严格从左到右，于是平铺
的 `a.Union(b).Intersect(c)` 三个方言返回不同的行（审计 P0）。选从左到右：它是链式调用读起来的顺序，
也和嵌套写法 `a.Union(b.Intersect(c))` 各表达一种意思，不需要新 API。`regroupAt` 只在"`INTERSECT` 前面有
`UNION` / `EXCEPT`"时把前缀包成派生表，其余形态照旧平铺。**否掉"拒绝混用"**：那让一个标准的查询写不出来。

空 `In` 同理（2026-09-29）：`IN (NULL)` 是 UNKNOWN，`Not(col.In(空))` 因此零行；现在空列表由派生的守卫参数补成确定的 FALSE / TRUE，
非空时守卫不渲染任何东西。`GetBy` 类查找要求唯一靠条件上的 `pins`（`col = 值`，像 `inList` 一样不合并、`Not` 清掉）。
空 `NotIn` 同理只能交给引擎验证：`NOT IN (SELECT 1 WHERE 1 = 0)` 的渲染断言全绿，PostgreSQL 在 varchar 列上
比较 `varchar = integer` 报错。换成 `(col NOT IN (NULL) OR <守卫>)`：`NULL` 字面量能适配任何列类型，守卫在绑定时
按列表空否写 `1 = 1` / `1 = 0`。

## 决定：部分读取和 keyset 唯一性都按"行实际是什么"判断 (2026-09-28)

部分读取曾按"选的都是同一张表的普通列"判断，经 CTE 或 `MapInto` 读回的表行漏网、`Update` 写零值；现在比较扫描实际
填了行类型的哪些字段。keyset 曾只要求"最后一列是主键"，一对多 join 里父表主键重复、静默跳行；现在要求每张来源表的主键。

**分组合法性在 `Build` 查**（2026-09-28；2026-09-29 补上错位的聚合、DISTINCT 排序、外连接上的行锁，`Page` 的排序复用 `validate`）：它是结构，不是方言能力；SQLite 静默取任意行、PG 报错，两种都不该发生。
分组了表主键即放行同表其他列（PG / MySQL 认的函数依赖）。`NotIn(可空子查询)` 同样在 `Build` 拒绝，**否掉自动改写成
`NOT EXISTS`**：那会换掉使用者写的查询和它的执行计划。`Count` 数的是 `List` 返回的行，带 `Limit` 的包一层再数。
