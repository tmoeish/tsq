# 变更影响 — 对外契约、命名与符号

处理你匹配的每个触发器；索引与 `[门禁]` 标记的含义在 `../change-impact.md`。

## 改了根包里任何导出的符号

- `make api-snapshot` 刷新 `references/api-surface.txt`。`[门禁: api-check]`
- 更新 `skills/tsq/`——使用者照着那份技能写代码，新增的 API 要出现在里面，删掉或改签名的
  要从里面消失。`[门禁: api-check 会提示]`
- 更新 `README.md` 和 `docs/` 里出现该符号的地方。
- 在 `CHANGELOG.md` 的 `## [未发布]` 段写一条人话。破坏性变更单独放 `### 破坏性变更`——
  `script/release.py` 靠这个小节名判断要不要跨主版本。
- 破坏性变更还意味着 v5：Go 的语义化导入版本要求改 go.mod 模块路径和全部内部 import。
  见 `../release.md`，不要顺手就改。

## 想给根包加一个"兼容包装"或一个不用接收者的方法

- **不要加 `Deprecated` 别名。** v5 之前攒了九个，没有任何门禁会提醒它们该走；删除它们本身就是
  大版本的理由之一。要改名就改名，把旧名写进 `CHANGELOG.md` 的破坏性变更段。
- **方法不用接收者，就说明它不该是方法。** `ExistsSub` 曾经长在每个列上却从不读那个列，逼着
  调用方随便挑一列。判据可执行：新增列方法时 grep 一下方法体里有没有出现 `c.`。
- **参数类型必须能被使用者写出名字。** 未导出的接口做参数类型时，调用能编译，但没人能声明变量或
  写 helper。要么导出成密封接口（方法保持未导出），要么换成具体类型。`[门禁: api-check 会显示它]`

## 给 `Operand` / `ListOperand` / `Pattern` / `Executor` 加或改了未导出方法

- 这些方法名就是使用者看到的编译错误：Go 报**按字母序第一个**缺失的方法，类型参数不符时报签名不对的那个。
  `needsTsqVal` / `needsTsqVals` / `needsRuntimeOrWrapExecutor` 是只为报错存在的标记方法，必须排在同一接口
  其他未导出方法之前（新方法别起 `a…`–`m…` 开头的名字），承载类型的那个叫 `valueOfType(T)` / `valuesOfType(T)`。
- 每个实现（列、`Param`、`Value`、阶段、`*Query`、`*Runtime`、`boundExecutor`）都要实现标记方法。
  `[门禁: compilefail_test.go 的 "a literal names tsq.Val" 等用例]`

## 退役了一个使用者写过的名字或写法

- 把旧写法加进 `script/check_docs.py` 的 `RETIRED`：它扫使用者文档、示例 README、`CONTRIBUTING.md` 和
  生成器源码（CLI 帮助文本在那里）。`api-check` 和 `tsq.*` 符号检查只认符号，认不出散文和帮助里的旧词。
  `[门禁: doc-check 的 check_retired_vocabulary]`

## 新增了一个"开关 + 若干消费点"的特性

- 开关必须从**导出的** `With*` 选项一路接到消费点。中途任何一段不可达，
  那个特性在发布出去的库里就不存在，而源码看着像它能用。
- 判据同 `runtime.md` 的"给 `Dialect` 接口加了钩子"那条：**grep 一遍调用方**。只被 `_test.go` 引用的
  未导出符号是这类缺陷的典型形态——`unused` linter 看不见它（测试里的引用算使用）。
  `[门禁: deadcode_test.go 的 TestNoUnexportedCodeOnlyTestsReach]`；它按名字匹配、不看类型，同名的
  两个声明会互相遮住，所以接进导出选项这一段仍要自己 grep。
- `printSQL` context key 加它的三个未导出 tracer 就是这样活了很久：八处
  `ctx.Value(printSQL)` 在库里永远为假，唯一能设置它的 `printSQLTracer` 没导出。

## 改了根包导出符号的名字，或在使用者文档里引用了 `tsq.X`

`make doc-check` 把 `README.md`、`docs/`、`skills/tsq/` 里每个 `tsq.X`（围栏块和行内
反引号都算）对照 `api-surface.txt` 的根包段落。改名先 `make api-snapshot`，再改文档，
否则门会把新名字当成不存在。`[门禁: doc-check]`

## 加了新的 Go 源文件

- 根包新文件 → `../feature-map.md` 要能把人带到它。`[门禁: skill-check library]`
- 有导出符号 → `make api-snapshot`。`[门禁: api-check]`
- 配套的 `_test.go` 文件名要么对应一个特性，要么对应被测文件，没有第三种。
