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
from pathlib import Path
import sys
from pathlib import Path
from typing import Final

from changeset import PROJECT_ROOT, ChangesetError, git_paths, is_generated, under

FENCED: Final = re.compile(r"```.*?```", re.DOTALL)
# 只认真正处在"被调用"位置的 make：行首、`$ ` 提示符之后、或 `&&` / `;` / `|` 之后。
# 代码块里不全是命令——报错输出、日志、被引用的提交信息都可能落在围栏里，而英文散文里
# 的 "gate stale make targets" 会被朴素的 `\bmake\s+\w+` 误判成调用 `make targets`
# （这道门加上的当天就自己踩了一次）。
MAKE_CALL: Final = re.compile(
    r"(?:^|&&|;|\|)[ \t]*(?:\$[ \t]*)?make[ \t]+([a-z][a-z0-9-]*)", re.MULTILINE
)
# Makefile 里的目标定义：行首、非缩进、冒号前。
TARGET_DEF: Final = re.compile(r"^([a-zA-Z][a-zA-Z0-9_-]*)\s*:(?!=)", re.MULTILINE)
# 传给 make 的选项值不是目标。
NOT_TARGETS: Final = frozenset({"no-print-directory"})


# 使用者文档里引用的 `tsq.X` 必须是根包真实导出的符号。README 曾同时教人用 `tsq.PageReq`、
# `tsq.EscapeKeywordSearch` 和 `tsq.Into`——三个都不存在（分别是 PageRequest、内部自动转义、
# MapInto），而 api-check 只守快照本身，守不住文档对快照的引用。行内反引号也扫：那条
# `tsq.Into` 就写在散文里。CHANGELOG 和项目内存有意不扫，它们讲历史。
API_SURFACE: Final = ".agents/skills/tsq-dev/references/api-surface.txt"
USER_DOCS: Final = ("README.md", "docs", "skills/tsq")
TSQ_SYMBOL: Final = re.compile(r"\btsq\.([A-Z][A-Za-z0-9_]*)")
API_TOP_LEVEL: Final = re.compile(
    r"^(?:func|type|var|const)\s+([A-Z][A-Za-z0-9_]*)|^\t([A-Z][A-Za-z0-9_]*)\b", re.MULTILINE
)
# 只认根包那一段：快照里 `## dialect` 之后是另一个包的符号，`tsq.X` 引用不到它们。
API_SECTION: Final = re.compile(r"^## (?P<name>\S+)$", re.MULTILINE)

# 非测试 Go 源码（注释和字符串）必须是英文：这是公开库，pkg.go.dev 和 CLI 报错都是使用者读的。
# 测试文件和 examples/ 不管——前者没有读者，后者的中文注释是示例故事的一部分。
CJK: Final = re.compile(r"[\u4e00-\u9fff]")
CJK_EXEMPT_DIRS: Final = (Path("examples"),)

# `skills/tsq` 随发布分发给使用者，被别的项目安装进 `.agents/skills/`，读者是全世界的
# 开发者和他们的 agent——所以它必须是英文。README、docs/、CHANGELOG.md、CONTRIBUTING.md
# 面向的是本项目的中文读者，AGENTS.md § Go 代码风格 写明了这条分界，这里只守英文那一侧。
#
# 这道门存在的理由：分界线在 AGENTS.md 里写了几个月，而 README 和 docs/ 一直是中文，
# 没有任何东西发现过。没有门的规则不是规则。
ENGLISH_ONLY_DOC_DIRS: Final = (Path("skills/tsq"),)

class DocsError(RuntimeError):
    """文档引用了不存在的 make 目标时抛出。"""


def makefile_targets() -> set[str]:
    text = (PROJECT_ROOT / "Makefile").read_text(encoding="utf-8")

    return set(TARGET_DEF.findall(text))


def code_spans(markdown: str) -> list[str]:
    """围栏代码块——读者会整块复制去执行的地方。"""
    return FENCED.findall(markdown)


def api_symbols() -> set[str]:
    text = (PROJECT_ROOT / API_SURFACE).read_text(encoding="utf-8")
    sections = list(API_SECTION.finditer(text))
    root = next((m for m in sections if m.group("name") == "."), None)
    if root is None:
        raise DocsError(f"{API_SURFACE} 里找不到根包段落 `## .`，先跑 `make api-snapshot`")

    following = [m for m in sections if m.start() > root.start()]
    end = following[0].start() if following else len(text)
    body = text[root.end() : end]

    return {a or b for a, b in API_TOP_LEVEL.findall(body)}


def user_documents() -> list[Path]:
    documents = []
    for path in git_paths(["ls-files", "-z", "*.md"]):
        if path.suffix != ".md":
            continue

        if path.as_posix() == "README.md" or any(under(path, Path(d)) for d in USER_DOCS[1:]):
            documents.append(path)

    return sorted(documents)


def check_make_targets() -> list[str]:
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

        return []

    lines = ["文档引用了 Makefile 里不存在的 make 目标："]
    for name, where in sorted(missing.items()):
        places = ", ".join(sorted(set(where)))
        lines.append(f"  - `make {name}`  出现在 {places} 的代码块里")

    lines.append("照着文档敲的人会得到 `No rule to make target`，然后开始怀疑自己的环境。改文档，或者补上这个目标。")

    return lines


def check_api_references() -> list[str]:
    symbols = api_symbols()
    documents = user_documents()

    missing: dict[str, set[str]] = {}
    for path in documents:
        text = (PROJECT_ROOT / path).read_text(encoding="utf-8", errors="replace")
        for name in TSQ_SYMBOL.findall(text):
            if name not in symbols:
                missing.setdefault(name, set()).add(path.as_posix())

    if not missing:
        print(f"文档检查通过：{len(documents)} 份使用者文档引用的 tsq.* 符号都在 API 快照里。")

        return []

    lines = ["使用者文档引用了根包里不存在的符号："]
    for name, where in sorted(missing.items()):
        lines.append(f"  - `tsq.{name}`  出现在 {', '.join(sorted(where))}")

    lines.append(
        f"对照 {API_SURFACE}（`make api-snapshot` 刷新）。照着文档写的代码会编译失败，"
        "使用者不会怀疑文档，只会怀疑自己。"
    )

    return lines


def check_go_source_language() -> list[str]:
    sources = sorted(
        path
        for path in git_paths(["ls-files", "-z", "--cached", "--others", "--exclude-standard", "*.go"])
        if path.suffix == ".go"
        and not path.name.endswith("_test.go")
        and not is_generated(path)
        and not any(under(path, d) for d in CJK_EXEMPT_DIRS)
    )

    offenders: list[str] = []
    for path in sources:
        for number, line in enumerate((PROJECT_ROOT / path).read_text(encoding="utf-8", errors="replace").splitlines(), 1):
            if CJK.search(line):
                offenders.append(f"{path.as_posix()}:{number}: {line.strip()}")

    if not offenders:
        print(f"文档检查通过：{len(sources)} 个非测试 Go 源文件没有中文注释或文案。")

        return []

    shown = offenders[:20]
    lines = ["非测试 Go 源码里有中文（注释、Go doc 和错误文案都必须是英文——它们是使用者读的）："]
    lines.extend(f"  - {item}" for item in shown)
    if len(offenders) > len(shown):
        lines.append(f"  ……另外还有 {len(offenders) - len(shown)} 处")

    return lines


def check_shipped_skill_language() -> list[str]:
    documents = sorted(
        path
        for path in git_paths(["ls-files", "-z", "--cached", "--others", "--exclude-standard", "*.md"])
        if path.suffix == ".md" and any(under(path, d) for d in ENGLISH_ONLY_DOC_DIRS)
    )

    offenders: list[str] = []
    for path in documents:
        for number, line in enumerate((PROJECT_ROOT / path).read_text(encoding="utf-8", errors="replace").splitlines(), 1):
            if CJK.search(line):
                offenders.append(f"{path.as_posix()}:{number}: {line.strip()}")

    if not offenders:
        print(f"文档检查通过：{len(documents)} 个随发布分发的技能文件没有中文。")

        return []

    shown = offenders[:20]
    lines = [
        "随发布分发的技能里有中文（`skills/tsq` 装进别人的项目，读者是全世界的开发者，必须是英文）："
    ]
    lines.extend(f"  - {item}" for item in shown)
    if len(offenders) > len(shown):
        lines.append(f"  ……另外还有 {len(offenders) - len(shown)} 处")

    return lines


def main() -> int:
    failures: list[str] = []
    for check in (
        check_make_targets,
        check_api_references,
        check_go_source_language,
        check_shipped_skill_language,
    ):
        failures.extend(check())

    if not failures:
        return 0

    print("\n".join(failures))

    return 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (DocsError, ChangesetError) as error:
        print(f"文档检查失败：{error}", file=sys.stderr)
        raise SystemExit(1) from error
