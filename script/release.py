#!/usr/bin/env python3
"""把一次发版跑完：定版本号、写 CHANGELOG、重新生成、过 harness、提交、打 tag、推送。

发版在 Go 生态里是**不可逆**的：tag 一旦推送，Go Proxy 就永久缓存了那个版本的
内容哈希。删掉重打同一个号会让已经拉过的用户全部 checksum 校验失败，唯一的补救是
`retract` 加一个新补丁版本。所以这个脚本把顺序写死，并且在推送之前就把所有能在本地
发现的问题拦下来——`make harness` 必须全绿才会有 tag。

`main` 上有 ruleset：禁止直推，必须走 PR 且 CI 全绿。所以发版是这样的：切
`release/vX.Y.Z` 分支 → 改版本号和 CHANGELOG → 重新生成 → 本地 harness → 提交 → 推分支
→ 开 PR → 等 CI → squash 合并 → 回到 main 拉最新 → 在**合并后的 main HEAD** 上打 tag
→ 推 tag。tag 必须打在合并之后的那个 commit 上：squash 会产生新的 SHA，打在分支上的
tag 会指向一个不在 main 历史里的 commit。

版本号从哪来，按优先级：
1. 命令行显式给的 `--version vX.Y.Z`。
2. `CHANGELOG.md` 里的 `## [未发布]` 段落——这是推荐路径：每波变更顺手把人话写进
   未发布段，发版时它被原样提升为正式条目，递增级别由段落里出现的小节推断。
3. 兜底：从上一个 tag 到 HEAD 的 Conventional Commits 推断，并自动生成条目。

主版本跨越（v4 → v5）不自动做。Go 的语义化导入版本要求把 `/v5` 写进 go.mod 的模块
路径和仓库内所有 import，那是一次真实的代码改动，不该藏在发版脚本里。
"""

from __future__ import annotations

import argparse
import datetime as dt
import re
import subprocess
import sys
import time
from collections.abc import Sequence
from typing import Final

from changeset import PROJECT_ROOT, ChangesetError, git_output
from version import (
    BUILDINFO_PATH,
    CHANGELOG_PATH,
    Version,
    VersionError,
    buildinfo_version,
    head_tag,
    latest_tag,
    module_major,
    write_buildinfo_version,
)

RELEASE_BRANCH: Final = "main"
MERGE_TIMEOUT_SECONDS: Final = 30 * 60
MERGE_POLL_SECONDS: Final = 20
UNRELEASED_HEADING: Final = re.compile(
    r"^## \[(?:未发布|Unreleased)\]\s*$", re.MULTILINE
)
RELEASED_HEADING: Final = re.compile(r"^## \[\d+\.\d+\.\d+\]", re.MULTILINE)
SUBJECT: Final = re.compile(
    r"^(?P<type>[a-z]+)(?P<scope>\([^)]*\))?(?P<breaking>!)?: (?P<summary>.+)$"
)

# CHANGELOG 沿用中文小节名；顺序即输出顺序。
SECTIONS: Final = (
    ("新增", ("feat",)),
    ("修复", ("fix",)),
    ("变更", ("refactor", "perf", "build", "ci", "chore", "style")),
)
BREAKING_SECTION: Final = "破坏性变更"


# 使用者拿得到的东西只有两样：`go get` 到的模块，和 `go install .../cmd/tsq` 或 GitHub
# Release 下载到的 CLI 二进制。这两样的内容完全由下面这些文件决定。
#
# 注意 `internal/` **算**用户可见：Go 的 import 规则让使用者引用不到它，但 CLI 的全部行为
# 都在那里面——`internal/parser` 改了解析规则，使用者手写的注解就换了含义。
USER_VISIBLE_SUFFIXES: Final = (".go", ".tmpl")
USER_VISIBLE_FILES: Final = frozenset(
    {"go.mod", "go.sum", ".goreleaser.yaml", "Dockerfile"}
)
# 这些只服务维护者，改它们不会让任何使用者拿到不同的东西。
MAINTAINER_ONLY_PREFIXES: Final = ("agents/", "script/", ".github/", "skills/", "docs/")


class ReleaseError(RuntimeError):
    """发版前置条件不满足时抛出。"""


def run(command: Sequence[str], *, capture: bool = False) -> str:
    completed = subprocess.run(
        list(command),
        cwd=PROJECT_ROOT,
        check=False,
        capture_output=capture,
        text=capture,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or "").strip() if capture else ""
        raise ReleaseError(
            f"`{' '.join(command)}` 失败（退出码 {completed.returncode}）"
            + (f"：\n{detail}" if detail else "")
        )

    return (completed.stdout or "") if capture else ""


def wait_for_merge(url: str) -> None:
    """等 PR 被自动合并；被关掉或超时都要停下来而不是继续往下打 tag。"""
    deadline = time.monotonic() + MERGE_TIMEOUT_SECONDS
    while True:
        state = run(
            ["gh", "pr", "view", url, "--json", "state", "--jq", ".state"],
            capture=True,
        ).strip()

        if state == "MERGED":
            print("  已合并。")

            return

        if state == "CLOSED":
            raise ReleaseError(
                f"{url} 被关闭而没有合并。工作还在 release 分支上；"
                "查清楚原因再重来。"
            )

        if time.monotonic() > deadline:
            raise ReleaseError(
                f"等了 {MERGE_TIMEOUT_SECONDS // 60} 分钟，{url} 还没合并。"
                "多半是某个必需检查红了或者卡住了。去看一眼；PR 上的自动合并还开着，"
                "检查转绿后它会自己合，那之后重跑发版即可。"
            )

        time.sleep(MERGE_POLL_SECONDS)


def current_branch() -> str:
    return git_output(["rev-parse", "--abbrev-ref", "HEAD"]).decode().strip()


def worktree_dirty() -> list[str]:
    raw = git_output(["status", "--porcelain", "-z"]).decode(errors="replace")

    return [entry[3:] for entry in raw.split("\0") if entry.strip()]


def commits_since(previous: Version | None) -> list[tuple[str, str]]:
    span = f"{previous}..HEAD" if previous is not None else "HEAD"
    raw = git_output(["log", span, "--no-merges", "--format=%s%x1f%b%x1e"]).decode(
        "utf-8", errors="replace"
    )
    entries: list[tuple[str, str]] = []
    for chunk in raw.split("\x1e"):
        if not chunk.strip():
            continue

        subject, _, body = chunk.strip().partition("\x1f")
        entries.append((subject.strip(), body.strip()))

    return entries


def user_visible_changes(previous: Version | None) -> list[str]:
    """上一个 tag 之后，改动里真正会让使用者拿到不同东西的那些文件。"""
    span = f"{previous}..HEAD" if previous is not None else "HEAD"
    raw = git_output(
        ["diff", "--name-only", "-z", "--diff-filter=ACDMRTUXB", span]
    ).decode("utf-8", errors="surrogateescape")

    visible: list[str] = []
    for path in (entry for entry in raw.split("\0") if entry):
        if path.startswith(MAINTAINER_ONLY_PREFIXES):
            continue

        if path.endswith("_test.go"):
            continue

        if path.endswith(USER_VISIBLE_SUFFIXES) or path in USER_VISIBLE_FILES:
            visible.append(path)

    return sorted(visible)


def infer_level(entries: Sequence[tuple[str, str]]) -> str:
    level = "patch"
    for subject, body in entries:
        match = SUBJECT.match(subject)
        if match is None:
            continue

        if match.group("breaking") or "BREAKING CHANGE" in body:
            return "major"

        if match.group("type") == "feat":
            level = "minor"

    return level


def changelog_text() -> str:
    return (PROJECT_ROOT / CHANGELOG_PATH).read_text(encoding="utf-8")


def unreleased_body() -> str | None:
    """`## [未发布]` 段落的正文，没有这个段落时返回 None。"""
    text = changelog_text()
    opened = UNRELEASED_HEADING.search(text)
    if opened is None:
        return None

    rest = text[opened.end() :]
    closed = RELEASED_HEADING.search(rest)
    body = rest[: closed.start()] if closed else rest

    return body.strip("\n")


def level_from_body(body: str) -> str:
    if BREAKING_SECTION in body:
        return "major"

    if "### 新增" in body:
        return "minor"

    return "patch"


def render_body(entries: Sequence[tuple[str, str]]) -> str:
    buckets: dict[str, list[str]] = {}
    breaking: list[str] = []
    for subject, body in entries:
        match = SUBJECT.match(subject)
        if match is None:
            continue

        kind = match.group("type")
        if kind in {"docs", "test"}:
            continue

        line = f"- {match.group('summary')}"
        if match.group("breaking") or "BREAKING CHANGE" in body:
            breaking.append(line)
            continue

        for name, kinds in SECTIONS:
            if kind in kinds:
                buckets.setdefault(name, []).append(line)
                break

    blocks: list[str] = []
    if breaking:
        blocks.append(f"### {BREAKING_SECTION}\n\n" + "\n".join(breaking))

    for name, _ in SECTIONS:
        if buckets.get(name):
            blocks.append(f"### {name}\n\n" + "\n".join(buckets[name]))

    if not blocks:
        raise ReleaseError(
            "上一个 tag 之后只有 docs / test 类型的提交，没有任何值得写进 CHANGELOG "
            "的条目。发一个使用者拿到手里毫无区别的版本，只会让版本列表变长、"
            "让「该升到哪个版本」变难回答。"
        )

    return "\n\n".join(blocks)


def write_changelog(version: Version, body: str, today: str) -> None:
    text = changelog_text()
    entry = f"## [{version.major}.{version.minor}.{version.patch}] - {today}\n\n{body}\n"

    opened = UNRELEASED_HEADING.search(text)
    if opened is not None:
        closed = RELEASED_HEADING.search(text[opened.end() :])
        end = opened.end() + (closed.start() if closed else len(text) - opened.end())
        updated = text[: opened.start()] + entry + "\n" + text[end:].lstrip("\n")
    else:
        first = RELEASED_HEADING.search(text)
        if first is None:
            raise ReleaseError(f"{CHANGELOG_PATH} 里没有任何已发布条目，无法定位插入点")

        updated = text[: first.start()] + entry + "\n" + text[first.start() :]

    (PROJECT_ROOT / CHANGELOG_PATH).write_text(updated, encoding="utf-8")


def commit_message(version: Version, body: str) -> str:
    highlights = "\n".join(
        line for line in body.splitlines() if line.startswith("- ")
    )

    return (
        f"chore: release {version}\n"
        "\n"
        f"发布 {version}。本次变更：\n"
        f"{highlights or '- 维护性更新'}\n"
        "\n"
        f"版本号同步到 {BUILDINFO_PATH} 与 {CHANGELOG_PATH}，"
        "并重新生成 examples/academy 让生成文件头带上新版本。\n"
        "验证：make harness 全绿（fmt/lint/vet/test/test-race/gen-check/"
        "api-check/release-check/examples）。\n"
    )


def resolve_version(explicit: str | None, previous: Version | None) -> tuple[Version, str, bool]:
    """返回（新版本号、CHANGELOG 正文、正文是否来自未发布段）。"""
    body = unreleased_body()
    from_unreleased = bool(body)

    if not from_unreleased:
        entries = commits_since(previous)
        if not entries:
            raise ReleaseError(
                f"上一个 tag {previous} 之后没有任何提交，没有东西可发。"
            )

        body = render_body(entries)
        level = infer_level(entries)
    else:
        level = level_from_body(body or "")

    base = previous or buildinfo_version()
    version = Version.parse(explicit) if explicit else base.bump(level)

    return version, body or "", from_unreleased


def main(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", help="显式指定 vX.Y.Z，不指定则自动推断")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="只算出版本号和 CHANGELOG 条目并打印，不改文件、不提交、不推送",
    )
    parser.add_argument(
        "--no-push",
        action="store_true",
        help="在 release 分支上提交，但不推送、不开 PR、不打 tag",
    )
    parser.add_argument(
        "--allow-branch",
        action="store_true",
        help=f"允许在 {RELEASE_BRANCH} 以外的分支发版",
    )
    parser.add_argument(
        "--allow-maintenance",
        action="store_true",
        help="即使这波没有任何使用者可见的改动也发版",
    )
    namespace = parser.parse_args(argv)

    branch = current_branch()
    if branch != RELEASE_BRANCH and not namespace.allow_branch:
        raise ReleaseError(
            f"当前在 {branch} 分支。发版默认只在 {RELEASE_BRANCH} 上做——"
            "tag 指向哪个 commit 决定了使用者拿到什么代码。"
            "确实要在这个分支发（例如维护旧的大版本），加 --allow-branch。"
        )

    dirty = worktree_dirty()
    if dirty and not namespace.dry_run:
        listed = "\n".join(f"  - {path}" for path in dirty[:12])
        raise ReleaseError(
            f"工作区还有未提交的改动：\n{listed}\n"
            "先把这波工作按正常流程提交（内存、技能、提交信息都过门禁），"
            "发版只负责把已经提交的东西打包。"
        )

    if head_tag() is not None:
        raise ReleaseError(
            f"HEAD 上已经有 tag {head_tag()}，这个 commit 已经发过了。"
        )

    previous = latest_tag()

    # 版本号只对使用者有意义。文档、技能、harness、CI 和纯测试改动不会让任何人拿到
    # 不同的东西，为它们发一个版本只是在制造噪音——`## [未发布]` 段本来就是给这种情况
    # 攒着用的：等到下一次真有使用者可见的改动，一起发出去。
    visible = user_visible_changes(previous)
    if not visible and not namespace.allow_maintenance:
        raise ReleaseError(
            f"{previous or 'HEAD'} 之后没有任何使用者可见的改动，这波不该发版。\n"
            "使用者拿得到的只有 `go get` 的模块和 `tsq` 二进制，它们的内容由 *.go、"
            "*.tmpl、go.mod/go.sum、.goreleaser.yaml 和 Dockerfile 决定；"
            "agents/、script/、skills/、docs/、.github/ 和 *_test.go 不算。\n"
            "把这波攒在 CHANGELOG 的 `## [未发布]` 段里，等下次真有使用者可见的改动一起"
            "发。确实要发一个纯维护版本（比如只为了让 .goreleaser.yaml 的修复生效），"
            "加 --allow-maintenance。"
        )

    version, body, from_unreleased = resolve_version(namespace.version, previous)

    if previous is not None and version <= previous:
        raise ReleaseError(
            f"算出的版本 {version} 不高于已发布的 {previous}。版本号只能前进。"
        )

    if version.major != module_major():
        raise ReleaseError(
            f"这次要发 {version}，但 go.mod 声明的是主版本 v{module_major()}。\n"
            "跨主版本不自动做：Go 的语义化导入版本要求先把 `/v"
            f"{version.major}` 写进 go.mod 的模块路径，再把仓库内所有 import 跟着改，"
            "还要更新 README、skills/tsq 和 CHANGELOG 的迁移说明。这是一次真实的代码"
            "改动，把它当作一波正常变更做完并提交，然后再发版。"
        )

    today = dt.date.today().isoformat()
    source = "CHANGELOG 的未发布段" if from_unreleased else "提交历史推断"
    print(f"发布版本：{version}（上一个 {previous or '无'}，条目来自{source}）")
    if visible:
        print(f"使用者可见的改动：{len(visible)} 个文件，含 {visible[0]}")
    else:
        print("使用者可见的改动：无（--allow-maintenance 放行的纯维护版本）")
    print(f"日期：{today}\n")
    print(body)

    if namespace.dry_run:
        print("\n--dry-run：什么都没改。")

        return 0

    release_branch = f"release/{version}"
    print(f"\n切到 {release_branch}……")
    run(["git", "checkout", "-b", release_branch])

    write_buildinfo_version(version)
    write_changelog(version, body, today)

    print("重新生成示例，让生成文件头带上新版本号……")
    run(["make", "--no-print-directory", "examples"])

    print("跑 make harness……")
    run(["make", "--no-print-directory", "harness"])

    message = commit_message(version, body)
    run(["git", "add", "-A"])
    run(["git", "commit", "-m", message])

    if namespace.no_push:
        print(
            f"\n已在 {release_branch} 上提交。--no-push：没有推送，也没有开 PR。\n"
            f"回到 {branch}：git checkout {branch} && git branch -D {release_branch}"
        )

        return 0

    print(f"推送 {release_branch}……")
    run(["git", "push", "-u", "origin", release_branch])

    subject, _, pr_body = message.partition("\n\n")
    print("开 PR……")
    url = run(
        [
            "gh", "pr", "create",
            "--base", branch,
            "--head", release_branch,
            "--title", subject,
            "--body", pr_body.strip(),
        ],
        capture=True,
    ).strip()
    print(f"  {url}")

    # 用 --auto 而不是"等 CI 再合"：PR 刚建出来的头几秒还没有任何 check 注册，
    # `gh pr checks --watch` 在那一刻会以"no checks reported"退出。交给 GitHub 自己
    # 在必需检查全绿时合并，这里只负责等结果。
    print("开启自动合并（CI 全绿即 squash 合入）……")
    run(
        [
            "gh", "pr", "merge", url,
            "--squash",
            "--auto",
            "--delete-branch",
            "--subject", subject,
            "--body", pr_body.strip(),
        ]
    )

    print(f"等 CI 与合并……（最多 {MERGE_TIMEOUT_SECONDS // 60} 分钟）")
    wait_for_merge(url)

    print(f"回到 {branch} 并拉取合并结果……")
    run(["git", "checkout", branch])
    run(["git", "pull", "--ff-only", "origin", branch])

    merged = git_output(["rev-parse", "HEAD"]).decode().strip()
    if buildinfo_version() != version:
        raise ReleaseError(
            f"合并后的 {branch} 上版本号是 {buildinfo_version()}，不是 {version}。"
            "tag 绝不能打在对不上的 commit 上——先查清楚 PR 合进去的是什么。"
        )

    print(f"在合并后的 {branch} HEAD（{merged[:8]}）上打 tag {version}……")
    run(["git", "tag", "-a", str(version), "-m", f"tsq {version}"])
    run(["git", "push", "origin", str(version)])

    print(
        f"\n{version} 已发布。CI 的 release job 会由 tag 触发并跑 GoReleaser。\n"
        "从现在起这个版本号在 Go Proxy 上不可撤销：发现问题用 go.mod 的 retract "
        "加一个新补丁版本，不要删 tag 重打（tag 的 ruleset 也会拦下删除和移动）。"
    )

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except (ReleaseError, VersionError, ChangesetError) as error:
        print(f"发版失败：{error}", file=sys.stderr)
        raise SystemExit(1) from error
