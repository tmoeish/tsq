#!/usr/bin/env python3
"""本波变更包含哪些文件——所有门禁共用的单一定义。

这个模块存在的理由是防止分歧：内存门禁、技能门禁和发版门禁如果各自定义"什么算
一波功能性改动"，同一次提交就会被三道门判成三种结果。判据只有一条：**非 Markdown
且非生成物**。文档改动不需要重新生成示例，生成物的重新生成也从不携带值得记住的
知识——它只是源码改动的影子。

CHANGELOG.md 是例外中的例外：它是 Markdown，因此不算功能性改动（改一句发版说明
不该要求重跑 harness），但发版门禁会单独读它。
"""

from __future__ import annotations

import subprocess
import sys
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Final

if sys.version_info < (3, 12):
    raise SystemExit(
        "本仓库的检查脚本需要 Python 3.12 及以上"
        f"（类型形参与新式泛型语法），当前是 {sys.version.split()[0]}"
    )

PROJECT_ROOT: Final = Path(__file__).resolve().parent.parent

# 重新生成这些文件从不携带值得记住的知识。
GENERATED_SUFFIXES: Final = (".tsq.go", ".result.tsq.go")
GENERATED_PATHS: Final = frozenset(
    {
        Path("examples/academy/tsq.json"),
        Path("examples/academy/mysql.sql"),
        Path("examples/academy/postgres.sql"),
        Path("examples/academy/sqlite.sql"),
        Path("agents/skills/tsq-dev/references/api-surface.txt"),
    }
)

# 用户可见的公开契约。改到这里就意味着使用者读的东西变了。
PUBLIC_API_ROOTS: Final = ("dialect",)

# 面向 TSQ 使用者的技能（仓库根 skills/），与面向本仓开发者的技能（agents/skills/）。
USER_SKILL_DIR: Final = Path("skills/tsq")
DEV_SKILL_DIR: Final = Path("agents/skills/tsq-dev")
MEMORY_PATH: Final = DEV_SKILL_DIR / "references/memory.md"


class ChangesetError(RuntimeError):
    """无法确定本波变更范围时抛出。"""


def git_output(arguments: Sequence[str]) -> bytes:
    completed = subprocess.run(
        ["git", *arguments],
        cwd=PROJECT_ROOT,
        check=False,
        capture_output=True,
    )
    if completed.returncode != 0:
        detail = completed.stderr.decode("utf-8", errors="replace").strip()
        raise ChangesetError(f"git {' '.join(arguments)} 失败：{detail}")

    return completed.stdout


def git_paths(arguments: Sequence[str]) -> set[Path]:
    return {
        Path(raw.decode("utf-8", errors="surrogateescape"))
        for raw in git_output(arguments).split(b"\0")
        if raw
    }


def is_generated(path: Path) -> bool:
    return path in GENERATED_PATHS or path.name.endswith(GENERATED_SUFFIXES)


def changed_paths() -> set[Path]:
    """工作树相对 HEAD 的全部改动，含未跟踪文件。"""
    tracked = git_paths(
        ["diff", "--name-only", "--diff-filter=ACDMRTUXB", "-z", "HEAD"]
    )
    untracked = git_paths(["ls-files", "--others", "--exclude-standard", "-z"])

    return tracked | untracked


def functional_files(paths: Iterable[Path]) -> list[Path]:
    """本波里需要门禁关心的文件：非 Markdown、非生成物。"""
    return sorted(
        (
            path
            for path in paths
            if path.suffix.lower() != ".md" and not is_generated(path)
        ),
        key=lambda path: path.as_posix(),
    )


def changed_functional_files() -> list[Path]:
    return functional_files(changed_paths())


def committed_functional_files() -> list[Path]:
    """HEAD 这次提交自身改动的文件。"""
    return functional_files(
        git_paths(
            [
                "diff-tree",
                "--no-commit-id",
                "--name-only",
                "-r",
                "-z",
                "--diff-filter=ACDMRTUXB",
                "HEAD",
            ]
        )
    )


def under(path: Path, directory: Path) -> bool:
    return path == directory or directory in path.parents


def public_go_files() -> list[Path]:
    """构成对外 Go 契约的非测试源码：根包加 PUBLIC_API_ROOTS。"""
    tracked = git_paths(["ls-files", "-z", "*.go"])

    return sorted(
        (
            path
            for path in tracked
            if not path.name.endswith("_test.go")
            and not is_generated(path)
            and (
                len(path.parts) == 1
                or path.parts[0] in PUBLIC_API_ROOTS
            )
        ),
        key=lambda path: path.as_posix(),
    )
