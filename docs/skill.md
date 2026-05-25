# TSQ Agent Skill

这个仓库除了是 `github.com/tmoeish/tsq/v4` 的源码仓库，也额外发布了一个可安装的 agent skill，目录在：

```txt
skills/
  tsq/
    SKILL.md
    references/
```

这个 skill 面向 **其他 Go 项目中的 coding agent**。仓库里的 `docs/` 主要给人类读者和仓库访客看；skill 真正随安装一起分发、能被 agent 读取的参考资料，都放在 `skills/tsq/` 目录内。

## 适用 agent

- GitHub Copilot
- Claude Code
- Gemini CLI

`gh skill install` 当前也支持更多 agent，但上面三类是这个 skill 的主要目标。

## 推荐安装方式

推荐使用 GitHub CLI 的 `gh skill install`，因为它会把 skill 安装到对应 agent 约定的位置。

### GitHub Copilot

```bash
gh skill install tmoeish/tsq tsq --agent github-copilot --scope user
```

### Claude Code

```bash
gh skill install tmoeish/tsq tsq --agent claude-code --scope user
```

### Gemini CLI

```bash
gh skill install tmoeish/tsq tsq --agent gemini-cli --scope user
```

说明：

- `--scope user`：当前用户全局可用
- `--scope project`：只安装到当前项目
- 需要固定版本时，可用 `tsq@vX.Y.Z` 或 `--pin vX.Y.Z`

例如：

```bash
gh skill install tmoeish/tsq tsq@v4.0.0 --agent github-copilot --scope user
```

安装前建议先预览：

```bash
gh skill preview tmoeish/tsq tsq
```

## 从本地仓库安装

如果你已经 clone 了本仓库，也可以直接从本地目录安装：

```bash
gh skill install /path/to/tsq tsq --from-local --agent github-copilot --scope user
```

例如在仓库根目录执行：

```bash
gh skill install . tsq --from-local --agent github-copilot --scope user
```

## 手动安装

如果你不想依赖 `gh skill install`，也可以手动把 `skills/tsq` 目录复制到 agent 的 skills 目录中。

### GitHub Copilot 项目级

复制到目标项目的任一目录：

- `.github/skills/tsq`
- `.claude/skills/tsq`
- `.agents/skills/tsq`

### GitHub Copilot / Claude Code / Gemini CLI 用户级

常见位置：

- `~/.copilot/skills/tsq`
- `~/.agents/skills/tsq`

手动复制后，按 agent 的方式重载 skills。对于 GitHub Copilot CLI，通常可以使用：

```txt
/skills reload
/skills info tsq
```

## 使用这个 skill

安装后，一般不需要每次显式点名，agent 会根据 `description` 自动判断是否启用。  
如果你想强制使用，可以在 prompt 里直接提到它：

```txt
Use the /tsq skill to add TSQ to this Go service.
```

适合触发这个 skill 的任务包括：

- 在 Go 项目中接入 TSQ
- 给 struct 添加 `@TABLE` / `@RESULT`
- 运行 `tsq fmt` / `tsq gen`
- 初始化 `tsq.Runtime`
- 编写 Build-based 查询
- 使用 CRUD / 分页 / 搜索 helper
- 处理事务、`InVar()` / `NInVar()`、CTE、`FULL JOIN` 等边界

skill 内部的参考资料见：

- `skills/tsq/SKILL.md`
- `skills/tsq/references/QUICKSTART.md`
- `skills/tsq/references/CONCEPTS.md`
- `skills/tsq/references/REFERENCE.md`

## 这份文档和 skill reference 的分工

为了避免 agent 因文档歧义写出错误代码，这里采用两层说明：

1. `docs/skill.md`：给仓库访客和使用者看的安装、使用和风险说明
2. `skills/tsq/references/*.md`：随 skill 一起分发的**技术参考正文**

如果你关心 **DSL key、managed field 语义、查询 DSL、事务和方言边界**，请直接把 `skills/tsq/references/REFERENCE.md` 视为当前 skill 的 canonical reference。

## 关键正确性约束

下面这些点如果写错，agent 很容易产出错误代码，因此这里明确写出来。

### 1. DSL 里填的是 Go struct field name，不是 SQL column name

无论是：

- `pk`
- `version`
- `created_at`
- `updated_at`
- `deleted_at`
- `ux[].fields`
- `idx[].fields`
- `search`

这些 DSL key 引用的都是 **Go struct 字段名**。  
真正的 SQL 列名仍然来自字段的 `db` tag 或生成器的列命名规则。

### 2. `version` 不是任意字段，它是自动乐观锁字段

`version` 的 DSL 形式包括：

```txt
version
version=true
version="Version"
version="CustomField"
```

含义：

- `version` / `version=true`：启用乐观锁，默认 Go 字段名为 `Version`
- `version="CustomField"`：启用乐观锁，并指定 Go struct 字段名
- `version=false`：等价于不配置

要求：

- 引用的字段必须存在
- 必须是**非指针整数类型**
- 不应使用 string、time、nullable wrapper、slice/array

语义：

- `Update(...)` / `Delete(...)` 会按 `pk + version` 匹配
- 更新成功后版本会自动 `+1`
- 冲突时返回 `ErrOptimisticLockConflict`

### 3. `created_at` / `updated_at` / `deleted_at` 也是有类型要求的

- `created_at` / `updated_at` 支持：
  - `time.Time`
  - `*time.Time`
  - `sql.NullTime`
  - `null.Time`
- `deleted_at` 支持：
  - `int64`
  - `uint64`
  - `*time.Time`
  - `sql.NullTime`
  - `null.Time`

其中：

- `created_at`：生成的 insert helper 会自动写当前时间
- `updated_at`：insert/update/soft-delete helper 会自动刷新
- `deleted_at`：用于 soft delete；list/get/page helper 会自动带 active-row 过滤

如果表还声明了 unique index，`deleted_at` 更推荐用 `int64/uint64` tombstone 语义，而不是 nullable time。

### 4. `@RESULT` 和 `@TABLE` 不一样

正常情况下，`@RESULT` 只应使用：

- `name`
- `search`

并通过字段 tag：

```go
tsq:"Struct.Field"
```

来声明投影来源。

对 `@RESULT` 来说，像下面这些 table-only key：

- `pk`
- `version`
- `created_at`
- `updated_at`
- `deleted_at`
- `ux`
- `idx`

不应作为正常设计的一部分使用。对 agent 来说，应该把它们视为 **unsupported / no-op in normal usage**，不要依赖这些 key 在 result model 上产生表语义。

### 5. 这个 skill 故意不带管理脚本

这个 skill 没有提供 `scripts/` 去包装：

- TSQ CLI 安装/升级
- `tsq fmt`
- `tsq gen`

这是刻意的。原因是这些动作都强依赖目标项目的模块布局和包路径，通用脚本更容易：

- 在错误目录执行
- 修改错误包
- 让 agent 误以为“应该无脑运行脚本”

正确做法是：agent 先检查目标项目的包结构，再显式运行：

```bash
go install github.com/tmoeish/tsq/v4/cmd/tsq@latest
tsq fmt ./your/package
tsq gen ./your/package
```

## 建议阅读顺序

如果你想确认这个 skill 是否足够完整，按下面顺序阅读最省时间：

1. `docs/skill.md`：安装、使用方式、关键风险
2. `skills/tsq/SKILL.md`：agent 触发条件和工作规则
3. `skills/tsq/references/QUICKSTART.md`：最短接入路径
4. `skills/tsq/references/REFERENCE.md`：完整 DSL 和语义说明

## 仓库内布局说明

这个 skill 放在 `skills/tsq/`，而不是仓库根目录，原因是：

1. 更符合 Agent Skills 规范里“一个 skill 是一个目录”的模型
2. 更适合 `gh skill install` 从仓库中自动发现
3. skill 的引用资料可以和 `SKILL.md` 一起打包安装
4. 安装后的 agent 不需要依赖仓库根目录 `docs/` 才能获得 TSQ 用法说明

也就是说，这个仓库既是 TSQ 的源码仓库，也是一个**包含 TSQ skill 的发布仓库**。
