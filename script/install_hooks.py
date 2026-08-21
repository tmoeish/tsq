#!/usr/bin/env python3
"""安装本仓库的 git 钩子。

`make commit-check` 作为 Makefile 目标存在结构性失效：有未提交代码时它主动跳过，
而代码一旦提交，`make memory-check` 又轮到跳过——于是每次 `make harness` 只有一道门
是活的，且写提交信息的那一刻活着的恰好是另一道。提交信息真正被校验的唯一时机是
`commit-msg` 钩子，所以这个钩子必须真的装上，而不只是写在文档里。

每台机器克隆后跑一次 `make hooks`。
"""

from __future__ import annotations

import stat
import subprocess
import sys
from pathlib import Path
from typing import Final

from changeset import PROJECT_ROOT, ChangesetError

MARKER: Final = "# tsq-managed-hook"
HOOKS: Final = {
    "commit-msg": f"""#!/bin/sh
{MARKER}
exec python3 "$(git rev-parse --show-toplevel)/script/check_change_log.py" \\
	commit --message-file "$1"
""",
}


def hooks_dir() -> Path:
    completed = subprocess.run(
        ["git", "rev-parse", "--git-path", "hooks"],
        cwd=PROJECT_ROOT,
        check=False,
        capture_output=True,
    )
    if completed.returncode != 0:
        detail = completed.stderr.decode("utf-8", errors="replace").strip()
        raise ChangesetError(f"定位 git 钩子目录失败：{detail}")

    relative = Path(completed.stdout.decode("utf-8").strip())

    return relative if relative.is_absolute() else PROJECT_ROOT / relative


def install(name: str, body: str, directory: Path) -> str:
    path = directory / name
    if path.exists():
        existing = path.read_text(encoding="utf-8", errors="replace")
        if MARKER not in existing:
            raise ChangesetError(
                f"{path} 已经存在，且不是本仓库管理的钩子。"
                "先备份或删除它，再重新运行 `make hooks`——"
                "覆盖别人的钩子不是这个目标该做的事。"
            )

        if existing == body:
            return f"  {name}：已是最新"

    path.write_text(body, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

    return f"  {name}：已安装"


def main() -> int:
    directory = hooks_dir()
    directory.mkdir(parents=True, exist_ok=True)

    print(f"git 钩子目录：{directory}")
    for name, body in HOOKS.items():
        print(install(name, body, directory))

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ChangesetError as error:
        print(f"安装 git 钩子失败：{error}", file=sys.stderr)
        raise SystemExit(1) from error
