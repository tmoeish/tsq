#!/usr/bin/env python3
"""文档里写的 `make X` 必须真的存在。

这道门是从一次真实事故倒推出来的：CI 的 coverage job 调用了 `make update-examples`，
而仓库里的目标叫 `make examples`。本地全绿、流水线红，只能发一个补丁版本 v4.4.1 去修。
事后发现同一个幽灵还活在 `CONTRIBUTING.md` 里——**文档抄了一个从未存在过的命令，而没有
任何东西会告诉你**。照着做的人得到的是 "No rule to make target"，然后开始怀疑自己的环境。

**只扫围栏代码块（```）里的 `make X`。** 判据是"读者会不会把这一行复制去执行"：围栏块是
可运行的命令清单，行内反引号则多半是散文里的引用——`memory.md` 记录事故经过时就必须能写出
`make update-examples` 这个已经不存在的名字，`CHANGELOG.md` 的历史条目同理。这条界线让门
不需要任何按文件的白名单，也不会被英文散文里的 "make sure" 误伤。
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Final

from changeset import PROJECT_ROOT, ChangesetError, git_paths

FENCED: Final = re.compile(r"```.*?```", re.DOTALL)
MAKE_CALL: Final = re.compile(r"\bmake\s+([a-z][a-z0-9-]*)")
# Makefile 里的目标定义：行首、非缩进、冒号前。
TARGET_DEF: Final = re.compile(r"^([a-zA-Z][a-zA-Z0-9_-]*)\s*:(?!=)", re.MULTILINE)
# 传给 make 的选项值不是目标。
NOT_TARGETS: Final = frozenset({"no-print-directory"})


class DocsError(RuntimeError):
    """文档引用了不存在的 make 目标时抛出。"""


def makefile_targets() -> set[str]:
    text = (PROJECT_ROOT / "Makefile").read_text(encoding="utf-8")

    return set(TARGET_DEF.findall(text))


def code_spans(markdown: str) -> list[str]:
    """围栏代码块——读者会整块复制去执行的地方。"""
    return FENCED.findall(markdown)


def main() -> int:
    targets = makefile_targets()
    documents = sorted(
        path
        for path in git_paths(["ls-files", "-z", "*.md"])
        if path.suffix == ".md"
    )

    missing: dict[str, list[str]] = {}
    for path in documents:
        text = (PROJECT_ROOT / path).read_text(encoding="utf-8", errors="replace")
        for span in code_spans(text):
            for name in MAKE_CALL.findall(span):
                if name in targets or name in NOT_TARGETS:
                    continue

                missing.setdefault(name, []).append(path.as_posix())

    if not missing:
        print(f"文档检查通过：{len(documents)} 份文档引用的 make 目标都存在。")

        return 0

    print("文档引用了 Makefile 里不存在的 make 目标：")
    for name, where in sorted(missing.items()):
        places = ", ".join(sorted(set(where)))
        print(f"  - `make {name}`  出现在 {places} 的代码块里")

    print(
        "\n照着文档敲的人会得到 `No rule to make target`，然后开始怀疑自己的环境。"
        "改文档，或者补上这个目标。"
    )

    return 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (DocsError, ChangesetError) as error:
        print(f"文档检查失败：{error}", file=sys.stderr)
        raise SystemExit(1) from error
