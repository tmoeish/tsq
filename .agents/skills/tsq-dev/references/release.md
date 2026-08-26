# 发版

**在 Go 生态里，发版是不可逆的。** tag 一旦推送，Go Proxy 就永久缓存了那个版本的内容
哈希。删掉重打同一个号，会让已经拉取过的用户全部 `checksum mismatch`，而且没有办法让他们
恢复——唯一的补救是 `retract` 加一个新的补丁版本。所以这里的每一道检查都在 push 之前。

## 版本号有四个副本

| 副本 | 位置 | 谁写 |
| --- | --- | --- |
| 代码里的 | `internal/buildinfo/buildinfo.go` 的 `var version` | `script/release.py` |
| 变更日志 | `CHANGELOG.md` 置顶的 `## [X.Y.Z] - YYYY-MM-DD` | `script/release.py` |
| 生成文件头 | `examples/academy/*.tsq.go` 首行、`tsq.json` 的 `version` | `make examples` 用 `bin/tsq-gen` 从 buildinfo 传导 |
| git tag | `vX.Y.Z` | `script/release.py` |

`make release-check` 校验四者一致，外加：主版本号必须和 go.mod 的模块路径匹配；
版本号不能倒退；HEAD 上已有 tag 时 tag 必须等于代码里的版本。

## 什么时候**不**发版

绝大多数波都不该发版。harness 不要求发版：`release-check` 只校验四个副本一致且没有倒退，
发版之间 buildinfo 等于最新 tag 是常态。

判据是"使用者拿到的东西变了没有"。使用者只拿得到 `go get` 的模块和 `tsq` 二进制：

| 改了什么 | 发版？ |
| --- | --- |
| `*.go`（非测试）、`*.tmpl`、`go.mod`/`go.sum`、`.goreleaser.yaml`、`Dockerfile` | 是 |
| `agents/`、`script/`、`skills/`、`docs/`、`.github/`、`Makefile`、`*.md`、`*_test.go` | 否 |

`internal/` 算使用者可见（CLI 的全部行为都在那里），但 `internal/` 里**纯粹的**重构不值得
单独占一个版本号——攒进 `## [未发布]` 段，等下一次真有使用者可见的改动一起发。

`release.py` 自己算这件事，没有使用者可见的改动就拒绝发版；纯维护版本加
`--allow-maintenance`。这道门是为了防版本号噪音：使用者拿到手里毫无区别的版本只会让版本
列表变长，让「我该升到哪个版本」变难回答。**v4.4.2 就是这样一个不该存在的版本**——它只改了
`Makefile`、`script/` 和 `agents/`，是在这道门加上之前发的。

## 平时怎么做

每波变更把人话写进 `CHANGELOG.md` 的 `## [未发布]` 段，按小节分：

```markdown
## [未发布]

### 新增

- 具体写新增了什么能力，使用者怎么用得上

### 修复

- 具体写修了什么，什么情况下会触发

### 破坏性变更

- 具体写什么不兼容了，怎么迁移
```

小节名决定递增级别：`### 破坏性变更` → major，`### 新增` → minor，其余 → patch。
没有未发布段时，`release.py` 退而从上一个 tag 之后的 Conventional Commits 推断并自动
生成条目——那份条目远不如手写的好读，所以**推荐路径是写未发布段**。

## 发版

```bash
make release-dry-run     # 先看一眼会发成什么版本、条目长什么样
make release             # 真的发
```

`script/release.py` 按顺序做这些事，任何一步失败就停下：

1. 确认在 `main` 分支（维护旧大版本时用 `--allow-branch`）。
2. 确认工作区干净，**且本地没有未推送的提交**——发版 PR 里只该有发版提交。本地攒着没推的
   提交会被一起卷进发版 PR，squash 之后 `main` 上只剩一句 `chore: release vX.Y.Z`，那些
   讲清楚了改动的提交信息就从 `git log` 上消失了。先让它们走自己的 PR 合进 `main`。
3. 确认 HEAD 上还没有 tag，且上一个 tag 之后有使用者可见的改动。
4. 算出新版本号和 CHANGELOG 条目。
5. 切 `release/vX.Y.Z` 分支。
6. 写 `internal/buildinfo/buildinfo.go` 和 `CHANGELOG.md`。
7. `make examples`——让生成文件头带上新版本号。它用的是 `make build-gen` 产出的
   `bin/tsq-gen`，**故意不带 `$(LDFLAGS)`**：`bin/tsq` 的版本号来自 `git describe`，
   而即将发布的 tag 那一刻还不存在，用它生成会让 `release-check` 永远失败（想写对头部
   要先打 tag，想打 tag 要先过 release-check）。见 `memory.md` 2026-08-21 那条。
8. `make harness`——全绿才继续。
9. 提交，推 release 分支，开 PR，开启自动合并。
10. 等 CI 全绿、PR 被 squash 合入。
11. 回 `main`，`git fetch` + `git reset --hard origin/main` 采纳合并结果——squash 造出
    的是新 commit，`git pull --ff-only` 会报分叉。**重新读一遍 `buildinfo` 确认版本对得
    上**，在合并后的 HEAD 上打 tag 并推送。

## `main` 和 tag 都有 ruleset

- **`main`**：禁止直推、禁止强推、禁止删除；必须走 PR，且 `Lint`、`Coverage`、`Build`、
  `Docker Build`、`GoReleaser Check` 五个检查全绿。规则对仓库所有者也生效（没有配
  bypass actor），所以 `git push origin main` 一定会被拒——这就是发版走 PR 的原因。
- **`refs/tags/v*`**：禁止删除、禁止移动、禁止强推。这条比分支保护重要得多：Go Proxy
  永久缓存每个 tag 的内容哈希，删掉重打会让全球用户 checksum 校验失败。**"不要删 tag
  重打"从此是被强制的，不是靠人记得的。**
- 必需检查里**故意不包含** `Test`：它是 matrix job，检查名叫
  `Test (ubuntu-latest, 1.27.0)`，升 Go 版本时名字会变，而变了的名字永远不会出现在 PR 上，
  必需检查就永远等不到——PR 从此合不进去。`Build` / `Docker Build` / `GoReleaser Check`
  都 `needs: [test, lint, coverage]`，测试挂了它们就不会绿，覆盖是等价的且名字稳定。
- 必需检查里也**不包含** `Release`：它 `if: startsWith(github.ref, 'refs/tags/')`，
  在 PR 上永远不会跑。要求一个永不出现的检查就是把 PR 永久卡死。
- 真需要清理非发布用途的 `v*` tag：先把 tag ruleset 停用，删完立刻恢复。
  ```bash
  gh api repos/tmoeish/tsq/rulesets --jq '.[] | "\(.id) \(.name)"'
  gh api repos/tmoeish/tsq/rulesets/<id> -X PUT -f enforcement=disabled
  git push origin :refs/tags/<tag>
  gh api repos/tmoeish/tsq/rulesets/<id> -X PUT -f enforcement=active
  ```

- `Integration`（真实 MySQL/PG）**在**必需检查里：它第一次跑就抓到了 PG 上六个版本没人
  发现的 bug，而在它成为必需检查之前 auto-merge 曾在它红着的时候合入了 PR #61。
  `Vulncheck` 不在必需检查里（上游漏洞库的新条目会让无关 PR 变红）。

推送 tag 会触发 `.github/workflows/go.yml` 的 `release` job，由 GoReleaser 构建三平台
二进制并创建 GitHub Release。CI 的 `lint` job 跑的是 `make lint`（Makefile 钉的版本），
`test` job 跑的是 `make test-race`——本地目标就是 CI 的定义，不要在 workflow 里另写一份。

发布产物的版本信息由 `.goreleaser.yaml` 的 ldflags 注入进
`github.com/tmoeish/tsq/v4/internal/buildinfo`。**改那几行的时候必须真的构建一次来验证**：
`-X` 打错包路径时链接器不报错、直接忽略，`goreleaser check` 也只校验 YAML 结构——这个 bug
在仓库里活了很久，谁都没看见。

```bash
goreleaser check
goreleaser build --snapshot --clean --single-target
./dist/default_*/tsq version    # 构建时间还写着 unknown 就说明 -X 没生效
```

参数：`--version vX.Y.Z` 显式指定版本；`--dry-run` 只打印；`--no-push` 提交并打 tag 但
不推送（tag 还留在本地，可以删）。

## 跨主版本（v4 → v5）不自动做

Go 的语义化导入版本要求：

1. `go.mod` 的 `module github.com/tmoeish/tsq/v4` 改成 `/v5`。
2. 仓库内所有 import 路径跟着改。
3. `README.md`、`docs/`、`skills/tsq`、`CHANGELOG.md` 的迁移说明全部更新。
4. 然后才打 `v5.0.0`。

漏掉第 1 步就打 tag，Go Proxy 会判这个版本非法，使用者 `go get` 收到 `invalid version`，
而这个 tag 已经不能重用了。所以 `release.py` 检测到主版本跨越会直接拒绝，让你把它当成一波
正常的代码变更做完、提交，再发版。

## 发错了怎么办

**不要删 tag 重打。** 正确做法（Go 1.16+）：

1. 在 `main` 上修好这个 bug。
2. `go.mod` 末尾加撤回声明，注释里写清原因：
   ```go
   retract v4.4.2 // 生成的 UPDATE 语句漏掉了乐观锁条件
   ```
3. 提交，发一个新的补丁版本。

## 分支策略

`main` 是唯一的长期分支，**没有 develop、没有 master**。任何改动都从 `main` 切一支短命
分支，走 PR 合回 `main`，合完分支自动删除（仓库开了 `delete_branch_on_merge`）。

**分支命名用 Conventional Commit 的 type 作前缀**：`<type>/<短横线描述>`，type 取
`feat|fix|perf|refactor|docs|test|build|ci|chore|style|revert`——和 `commit-msg` 钩子强制的
是同一份词表。两套词汇必然漂移，所以只留一套。

```
feat/scalar-query-method
fix/goreleaser-ldflags
docs/route-deferred-decisions
build/memory-line-budget
release/v4.5.0          ← 唯一的例外，由 `script/release.py` 自己创建，不要手工建
```

- **开新分支之前先回 `main` 同步**：`git checkout main && git fetch && git reset --hard
  origin/main`。叠在还没合的 PR 分支上，等那个 PR 被 squash 之后你这支必然冲突。
- 分支名没有门禁守着，这是有意的：名字取错不会导致任何下游故障，分支合并后就消失了。
  它是约定，不是正确性问题。
- 维护旧大版本（**唯一的长期分支例外**）：基于最后一个该版本的 tag 切分支
  （`git checkout -b v3 v3.9.5`），
  在上面修、在上面打 tag，发版时加 `--allow-branch`。
- 旧次版本的紧急修复：基于出问题的 tag 切临时分支，修完打 tag，**再把修复反向合并回
  `main`**，然后删临时分支。忘了反向合并，下一个次版本会把同一个 bug 再放出去一次。
- 大型实验特性优先用特性开关或 `//go:build` 构建标签，不要长期不合并的分支。
