#!/usr/bin/env python3
"""每波变更都必须留下一条项目内存记录和一条讲清楚的提交信息。"""

from __future__ import annotations

import argparse
import re
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Final

from changeset import (
    MEMORY_PATH,
    PROJECT_ROOT,
    ChangesetError,
    changed_functional_files,
    changed_paths,
    git_output,
    git_paths,
)

SUBJECT_LIMIT: Final = 72
BODY_MIN_LINES: Final = 3
BODY_MIN_CHARS: Final = 120

# 项目内存的行数上限。定得宽松是有意的：一道从没触发过的门，触发的那次才有意义。
MEMORY_MAX_LINES: Final = 400

# 本仓是公开 OSS，提交历史面向使用者，因此沿用英文 Conventional Commits。
CONVENTIONAL: Final = re.compile(
    r"^(?P<type>feat|fix|perf|refactor|docs|test|build|ci|chore|style|revert)"
    r"(?P<scope>\([a-z0-9.,/\- ]+\))?(?P<breaking>!)?: (?P<summary>.+)$"
)
VAGUE_SUMMARIES: Final = frozenset(
    {
        "wip",
        "fix",
        "fixes",
        "fixed",
        "bugfix",
        "update",
        "updates",
        "updated",
        "change",
        "changes",
        "cleanup",
        "clean up",
        "refactor",
        "refactoring",
        "misc",
        "tmp",
        "temp",
        "stuff",
        "improvements",
        "minor fixes",
        "small fixes",
        "code review",
    }
)
EXEMPT_PREFIXES: Final = ("Merge ", "Revert ", "fixup!", "squash!")

# GitHub 的 squash 合并会往主题末尾追加 " (#123)"。那不是作者写的，PR 号在写提交信息的
# 时候也不可能知道，所以量长度之前要把它剥掉——否则 `commit-msg` 钩子（合并前，没有后缀）
# 和这里（合并后，有后缀）校验的是两个不同的字符串，作者能过钩子却让 main 上的门变红。
SQUASH_REFERENCE: Final = re.compile(r"\s*\(#\d+\)$")

# 发版提交只碰这些非生成物文件。
RELEASE_ONLY_FILES: Final = frozenset(
    {Path("internal/buildinfo/buildinfo.go")}
)


class ChangeLogError(RuntimeError):
    """本波缺少内存记录或提交信息不合格时抛出。"""


def memory_insertions() -> int:
    """项目内存这一波净新增了多少行。

    比较基准必须是 HEAD 而不是索引：`changed_paths()` 按 `diff HEAD` 算本波范围，
    这里若只看未暂存改动，`git add` 过的内存记录就会被当成没写。

    内存文件还没被跟踪时（第一波，或者有人删了重建），`git diff HEAD` 一行都不报，
    于是整份新写的内存会被当成"没写"。这时候整个文件都是新增。
    """
    absolute = PROJECT_ROOT / MEMORY_PATH
    if MEMORY_PATH in git_paths(["ls-files", "--others", "--exclude-standard", "-z"]):
        return len(absolute.read_text(encoding="utf-8").splitlines())

    raw = git_output(["diff", "--numstat", "HEAD", "--", MEMORY_PATH.as_posix()])
    for line in raw.decode("utf-8", errors="replace").splitlines():
        fields = line.split("\t")
        if len(fields) == 3 and fields[0].isdigit():
            return int(fields[0])

    return 0


def check_memory_budget() -> None:
    """项目内存的长度是每次会话都要付的上下文成本，所以它有上限。

    这道检查和"这波带没带记录"无关，因此**无条件**跑：文件是不是太长，跟当前这波改了
    什么没有关系。撞上限不是让你随便删，是提醒你按 memory.md 开头那张表裁剪一遍——
    搁置项处理完就删，事故根因在有门禁挡着之后压成一行，决定和死胡同永久保留。
    """
    absolute = PROJECT_ROOT / MEMORY_PATH
    if not absolute.is_file():
        return

    lines = len(absolute.read_text(encoding="utf-8").splitlines())
    if lines <= MEMORY_MAX_LINES:
        print(f"项目内存 {lines} 行，上限 {MEMORY_MAX_LINES}。")

        return

    raise ChangeLogError(
        f"{MEMORY_PATH} 有 {lines} 行，超过上限 {MEMORY_MAX_LINES}。\n"
        "这个文件被每个 agent 加载进上下文，长度是每次会话都要付的成本。按它开头那张表"
        "裁剪一遍：\n"
        "  - `已知未处理：` 开头的搁置项，已经处理掉的就删掉整条\n"
        "  - 事故 + 根因：现在有门禁挡着的，压成一行指向那道门，叙事丢掉\n"
        "  - 决定 + 理由、死胡同：留着，删了下一个人会改回去或者重试一遍\n"
        "判据只有一条：删掉它之后，有人会不会重犯、或者重新调查一遍。\n"
        "删除不是销毁——`git log -p --follow` 能还原任何被删的条目。"
    )


def check_memory() -> None:
    check_memory_budget()

    code = changed_functional_files()
    if not code:
        print("项目内存检查跳过：没有未提交的功能性改动。")

        return

    # 发版波只改版本号和生成物，它教不了项目任何东西——真正的知识在被发布的那些波里
    # 已经记过了。不豁免的话 `script/release.py` 跑 `make harness` 必然撞在这道门上。
    if set(code) <= RELEASE_ONLY_FILES:
        print("项目内存检查跳过：这波只有版本号改动（发版）。")

        return

    listed = "\n".join(f"  - {path}" for path in code)
    if MEMORY_PATH not in changed_paths():
        raise ChangeLogError(
            f"以下文件改了，但项目内存没有更新：\n{listed}\n"
            f"把这波变更教给项目的东西写进 {MEMORY_PATH}——根本原因、决定及其理由、"
            "或者不明显的行为。如果这波确实没教会任何持久的东西，就在提交正文里"
            "说明，提交之后重跑。"
        )

    if memory_insertions() <= 0:
        raise ChangeLogError(
            f"{MEMORY_PATH} 被改动了，但没有任何新增行：\n{listed}\n"
            "删除过时条目是好事，但一波功能性改动总该留下点什么。补一条带绝对日期"
            "的记录，或者在提交正文里说明为什么这波没有。"
        )

    print(f"项目内存随 {len(code)} 个改动文件一起更新了。")


def commit_message(message_file: str | None) -> str:
    if message_file is not None:
        try:
            return Path(message_file).read_text(encoding="utf-8")
        except OSError as error:
            raise ChangeLogError(f"读取提交信息 {message_file}：{error}") from error

    return git_output(["log", "-1", "--format=%B"]).decode("utf-8", errors="replace")


def meaningful_lines(lines: Sequence[str]) -> list[str]:
    return [
        line for line in lines if line.strip() and not line.lstrip().startswith("#")
    ]


def check_commit(message_file: str | None) -> None:
    if message_file is None and changed_functional_files():
        print("提交信息检查跳过：改动还没有提交。")

        return

    lines = commit_message(message_file).splitlines()
    raw_subject = lines[0].strip() if lines else ""

    if not raw_subject:
        raise ChangeLogError("提交信息没有主题行")

    if raw_subject.startswith(EXEMPT_PREFIXES):
        print("提交信息检查跳过：merge、revert 或 fixup 提交。")

        return

    subject = SQUASH_REFERENCE.sub("", raw_subject)

    if len(subject) > SUBJECT_LIMIT:
        raise ChangeLogError(
            f"提交主题 {len(subject)} 字符，上限 {SUBJECT_LIMIT}：{subject!r}"
        )

    match = CONVENTIONAL.match(subject)
    if match is None:
        raise ChangeLogError(
            "提交主题要用 Conventional Commits 格式 "
            f"`type(scope): summary`（见 AGENTS.md § 语言与提交）：{subject!r}"
        )

    summary = match.group("summary").strip()
    if summary.endswith("."):
        raise ChangeLogError(f"提交主题不要以句号结尾：{subject!r}")

    if summary.lower().rstrip("!") in VAGUE_SUMMARIES:
        raise ChangeLogError(f"提交主题没说清改了什么：{subject!r}")

    if len(lines) > 1 and lines[1].strip():
        raise ChangeLogError("提交主题和正文之间要空一行")

    body_lines = meaningful_lines(lines[1:])
    body = "\n".join(body_lines)
    if len(body_lines) < BODY_MIN_LINES or len(body) < BODY_MIN_CHARS:
        raise ChangeLogError(
            "提交正文太单薄，说不清这次改动；写清楚改了什么、为什么改、怎么验证的"
            f"（至少 {BODY_MIN_LINES} 行、{BODY_MIN_CHARS} 字符，"
            f"当前 {len(body_lines)} 行、{len(body)} 字符）"
        )

    print("提交信息符合 Conventional Commits 且带有详细正文。")


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("memory")
    commit = subparsers.add_parser("commit")
    commit.add_argument(
        "--message-file",
        help="校验这个文件而不是 HEAD（供 commit-msg 钩子使用）",
    )

    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    namespace = parse_args(sys.argv[1:] if argv is None else argv)
    if namespace.command == "memory":
        check_memory()
    else:
        check_commit(namespace.message_file)

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ChangeLogError, ChangesetError) as error:
        print(f"变更记录检查失败：{error}", file=sys.stderr)
        raise SystemExit(1) from error
