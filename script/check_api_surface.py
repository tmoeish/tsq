#!/usr/bin/env python3
"""把对外 Go 契约钉成一份快照，改动时强制回头看使用者读的那份技能。

TSQ 是一个被别的项目 import 的库，它的公开符号就是它的产品。`skills/tsq` 是使用者
（和使用者的 agent）读的说明书，`.agents/skills/tsq-dev` 是开发者读的上下文——两份都
会因为一次导出符号的增删改而变得不真实，而 diff 里看不出这一点：删掉一个方法和改一行
内部实现在 `git status` 里长得一模一样。

判据是快照比对而不是 `git diff`：一波变更本来就可能合法地改动公开 API，快照存在的
意义不是禁止改，而是让"改了"这件事无法悄悄发生。

用 `go doc -all` 而不是自己解析 AST：它已经是 Go 官方对"什么算公开契约"的定义。
文档正文（四空格缩进）和注释被剔除，所以改一句注释不会惊动这道门。
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Final

from changeset import (
    DEV_SKILL_DIR,
    PROJECT_ROOT,
    PUBLIC_API_ROOTS,
    USER_SKILL_DIR,
    ChangesetError,
    changed_paths,
    under,
)

SNAPSHOT_PATH: Final = DEV_SKILL_DIR / "references/api-surface.txt"
PACKAGES: Final = (".", *(f"./{root}" for root in PUBLIC_API_ROOTS))
DECL_PREFIXES: Final = ("package ", "func ", "type ", "const ", "var ", ")", "}")
SECTION_HEADERS: Final = frozenset(
    {"CONSTANTS", "FUNCTIONS", "TYPES", "VARIABLES"}
)


class APISurfaceError(RuntimeError):
    """无法生成 API 快照时抛出。"""


def package_surface(package: str) -> list[str]:
    completed = subprocess.run(
        ["go", "doc", "-all", package],
        cwd=PROJECT_ROOT,
        check=False,
        capture_output=True,
    )
    if completed.returncode != 0:
        detail = completed.stderr.decode("utf-8", errors="replace").strip()
        raise APISurfaceError(f"go doc -all {package} 失败：{detail}")

    kept: list[str] = []
    for raw in completed.stdout.decode("utf-8", errors="replace").splitlines():
        # 四空格缩进是 go doc 的文档正文；顶格和制表符缩进才是声明本身。
        if raw.startswith("    "):
            continue

        line = raw.rstrip()
        stripped = line.strip()
        if not stripped or stripped.startswith("//"):
            continue

        # 顶格的散文（包文档首句）与声明长得一样，靠开头的关键字区分。
        if not raw.startswith("\t") and not stripped.startswith(DECL_PREFIXES):
            if stripped not in SECTION_HEADERS:
                continue

        kept.append(line)

    return kept


def render() -> str:
    lines = [
        "# TSQ 对外 Go 契约快照——由 `make api-snapshot` 生成，不要手改。",
        "# 它变了就说明使用者读的 skills/tsq 可能不再真实。",
    ]
    for package in PACKAGES:
        lines.append("")
        lines.append(f"## {package}")
        lines.extend(package_surface(package))

    return "\n".join(lines) + "\n"


def write() -> int:
    (PROJECT_ROOT / SNAPSHOT_PATH).parent.mkdir(parents=True, exist_ok=True)
    (PROJECT_ROOT / SNAPSHOT_PATH).write_text(render(), encoding="utf-8")
    print(f"已写入 {SNAPSHOT_PATH}。")

    return 0


def diff_lines(before: str, after: str) -> tuple[list[str], list[str]]:
    old = set(before.splitlines())
    new = set(after.splitlines())

    return sorted(new - old), sorted(old - new)


def check() -> int:
    absolute = PROJECT_ROOT / SNAPSHOT_PATH
    current = render()
    committed = (
        absolute.read_text(encoding="utf-8") if absolute.is_file() else ""
    )

    if current == committed:
        print(f"对外 API 契约与 {SNAPSHOT_PATH} 一致。")

        return 0

    added, removed = diff_lines(committed, current)
    print("对外 Go 契约变了，但快照还是旧的。新增：")
    for line in added or ["  （无）"]:
        print(f"  + {line.strip()}" if line.strip() else "")

    print("移除：")
    for line in removed or ["  （无）"]:
        print(f"  - {line.strip()}" if line.strip() else "")

    absolute.parent.mkdir(parents=True, exist_ok=True)
    absolute.write_text(current, encoding="utf-8")
    print(f"\n已替你刷新 {SNAPSHOT_PATH}。")

    paths = changed_paths()
    touched_user_skill = any(under(path, USER_SKILL_DIR) for path in paths)
    if not touched_user_skill:
        print(
            f"这波还没有动 {USER_SKILL_DIR}/。使用者和使用者的 agent 是照着那份技能"
            "写代码的：新增的 API 要出现在里面，删掉或改签名的 API 要从里面消失，"
            "顺带检查 README.md、docs/ 和 CHANGELOG.md 的迁移说明。"
        )

        return 1

    print(f"{USER_SKILL_DIR}/ 这波也改了。确认刷新后的快照无误，一起提交。")

    return 1


def main(argv: list[str]) -> int:
    command = argv[0] if argv else "check"
    if command == "write":
        return write()

    if command == "check":
        return check()

    print(f"未知子命令 {command!r}，可用：check、write", file=sys.stderr)

    return 2


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except (APISurfaceError, ChangesetError) as error:
        print(f"API 契约检查失败：{error}", file=sys.stderr)
        raise SystemExit(1) from error
