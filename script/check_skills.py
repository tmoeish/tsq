#!/usr/bin/env python3
"""改了某些东西，就必须同步对应的技能——这道门只负责让"忘了"变成不可能。

本仓有两份技能，读者完全不同：

- `skills/tsq`（仓库根，随 GitHub 发布）是**使用者**和使用者的 agent 读的说明书。
  注解语法、CLI 命令、生成文件命名、查询 API 用法在这里；它落后于代码，别人就照着
  错的写。
- `.agents/skills/tsq-dev`（本文件所在体系）是**开发本仓的 agent** 读的工程上下文。
  架构、代码地图、耦合清单和项目内存在这里；它落后于代码，下一个 agent 就得重新做
  一遍源码考古。

两份都不该靠人记得更新。下面的触发表把"改了 A 就得改 B"写死：每条触发器匹配到本波
改动时，被点名的技能文件里至少要有一个也在本波里改过。这不是形式主义——触发器都是
从"哪类改动会让哪份文档变假"倒推出来的，见每条的 `hint`。

真的判断过、确认技能不需要动时，用 `SKIP_SKILL_CHECK=<触发器名,...>` 逐条豁免，并在
提交正文里说明理由。整道门不提供无理由的总开关。

另一半和本波改了什么无关、**无条件**跑：`change-impact.md` 与 `memory.md` 是索引，子文件在
`impact/`、`memory/` 下。速查漏一条触发器、路由表漏一份子文件，读的人就以为自己看过了全部，
所以索引与子文件对不上时直接失败，这一半不提供豁免。
"""

from __future__ import annotations

import os
import re
import sys
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from changeset import (
    DEV_SKILL_DIR,
    MEMORY_DIR,
    MEMORY_INDEX,
    PROJECT_ROOT,
    USER_SKILL_DIR,
    ChangesetError,
    changed_functional_files,
    changed_paths,
    is_generated,
    under,
)

SKIP_ENV: Final = "SKIP_SKILL_CHECK"

DEV_ARCHITECTURE: Final = DEV_SKILL_DIR / "references/architecture.md"
DEV_FEATURE_MAP: Final = DEV_SKILL_DIR / "references/feature-map.md"
DEV_CODEGEN: Final = DEV_SKILL_DIR / "references/codegen.md"
DEV_CHANGE_IMPACT: Final = DEV_SKILL_DIR / "references/change-impact.md"
DEV_IMPACT_DIR: Final = DEV_SKILL_DIR / "references/impact"
DEV_RELEASE: Final = DEV_SKILL_DIR / "references/release.md"


@dataclass(frozen=True)
class Trigger:
    name: str
    matches: Callable[[Path], bool]
    required: tuple[Path, ...]
    hint: str


def prefix(*prefixes: str) -> Callable[[Path], bool]:
    return lambda path: path.as_posix().startswith(prefixes)


def suffix(*suffixes: str) -> Callable[[Path], bool]:
    return lambda path: path.as_posix().endswith(suffixes)


def root_go(path: Path) -> bool:
    return (
        len(path.parts) == 1
        and path.suffix == ".go"
        and not path.name.endswith("_test.go")
    )


def either(*predicates: Callable[[Path], bool]) -> Callable[[Path], bool]:
    return lambda path: any(predicate(path) for predicate in predicates)


TRIGGERS: Final = (
    Trigger(
        name="dsl",
        matches=prefix("internal/parser/"),
        required=(USER_SKILL_DIR, DEV_CODEGEN),
        hint=(
            "`//tsq:` 指令是使用者直接手写的东西。解析器接受或拒绝"
            "的内容变了，使用者技能里的注解说明就成了错的。"
        ),
    ),
    Trigger(
        name="templates",
        matches=suffix(".go.tmpl"),
        required=(USER_SKILL_DIR, DEV_CODEGEN),
        hint=(
            "模板决定生成代码长什么样，也就决定了使用者能调用哪些方法。"
            "改模板等于改 API。"
        ),
    ),
    Trigger(
        name="cli",
        matches=either(prefix("cmd/tsq/"), prefix("internal/cmd/")),
        required=(USER_SKILL_DIR, DEV_FEATURE_MAP),
        hint=(
            "`tsq gen` / `tsq version` 的子命令、flag 和生成物文件名是使用者每天敲的"
            "命令，也是他们 CI 里写死的东西。"
        ),
    ),
    Trigger(
        name="dialect",
        matches=either(prefix("dialect/"), prefix("internal/sqldialect/")),
        required=(USER_SKILL_DIR, DEV_ARCHITECTURE),
        hint=(
            "方言能力（CTE、FULL JOIN、行锁、DDL 类型映射）是在执行期才校验的，"
            "使用者只能靠文档提前知道哪条查询能在哪个库上跑。"
        ),
    ),
    Trigger(
        name="examples",
        matches=lambda path: (
            path.as_posix().startswith("examples/") and not is_generated(path)
        ),
        required=(USER_SKILL_DIR, DEV_FEATURE_MAP),
        hint=(
            "examples/ 是可运行的契约，使用者技能里的代码片段就是从这里抄的。"
            "示例结构体或场景变了，片段要跟着变。"
        ),
    ),
    Trigger(
        name="library",
        matches=root_go,
        required=(DEV_ARCHITECTURE, DEV_FEATURE_MAP, DEV_CHANGE_IMPACT, DEV_IMPACT_DIR),
        hint=(
            "根包就是这个库本身。新增或移动文件、改查询阶段机、改执行路径之后，"
            "开发者技能的代码地图必须还能把人带到正确的文件。"
            "（对外符号的增删改另有 `make api-check` 把关。）"
        ),
    ),
    Trigger(
        name="harness",
        matches=either(
            prefix("script/", ".github/workflows/"),
            lambda path: path.as_posix()
            in {"Makefile", ".goreleaser.yaml", ".golangci.yml"},
        ),
        required=(DEV_SKILL_DIR / "SKILL.md", DEV_RELEASE),
        hint=(
            "harness 和发版流水线本身就是开发者技能描述的东西。门禁的顺序、"
            "跳过条件和发版步骤改了，技能里那份说明就会把下一个 agent 带进沟里。"
        ),
    ),
)


TRIGGER_HEADING: Final = re.compile(r"^## (?P<title>.+)$")
INDEX_ENTRY: Final = re.compile(r"^### `(?P<file>impact/[a-z-]+\.md)`$")
INDEX_BULLET: Final = re.compile(r"^- (?P<title>.+)$")


def read(path: Path) -> list[str]:
    return (PROJECT_ROOT / path).read_text(encoding="utf-8").splitlines()


def index_triggers() -> dict[str, list[str]]:
    """`change-impact.md` 速查里按子文件列出的触发器标题。"""
    listed: dict[str, list[str]] = {}
    current: str | None = None
    for line in read(DEV_CHANGE_IMPACT):
        if entry := INDEX_ENTRY.match(line):
            current = entry.group("file")
            listed.setdefault(current, [])
            continue

        if current and (bullet := INDEX_BULLET.match(line)):
            listed[current].append(bullet.group("title"))

    return listed


def check_index_sync() -> list[str]:
    """索引必须能替代整份加载：速查漏一条，那条耦合就等于不存在。

    拆分的代价全在这里——一份和子文件对不上的索引比不拆更糟，读的人以为自己看过了全部
    触发器。所以这道检查无条件跑，和"这波改了什么"无关。
    """
    failures: list[str] = []
    listed = index_triggers()
    shards = sorted((PROJECT_ROOT / DEV_IMPACT_DIR).glob("*.md"))
    for shard in shards:
        key = f"impact/{shard.name}"
        actual = [
            match.group("title")
            for line in read(DEV_IMPACT_DIR / shard.name)
            if (match := TRIGGER_HEADING.match(line))
        ]
        if key not in listed:
            failures.append(f"  {DEV_CHANGE_IMPACT} 的速查里没有 `{key}` 这一节")
            continue

        failures.extend(
            f"  {key} 有触发器「{title}」，但速查里没有"
            for title in actual
            if title not in listed[key]
        )
        failures.extend(
            f"  速查里的「{title}」在 {key} 里已经没有了"
            for title in listed[key]
            if title not in actual
        )

    names = {f"impact/{shard.name}" for shard in shards}
    failures.extend(f"  速查指向的 {key} 不存在" for key in listed if key not in names)

    for directory, index in (
        (DEV_IMPACT_DIR, DEV_CHANGE_IMPACT),
        (MEMORY_DIR, MEMORY_INDEX),
    ):
        body = "\n".join(read(index))
        failures.extend(
            f"  {index} 的路由表里没有 {directory.name}/{shard.name}"
            for shard in sorted((PROJECT_ROOT / directory).glob("*.md"))
            if f"{directory.name}/{shard.name}" not in body
        )

    return failures


def skipped() -> frozenset[str]:
    raw = os.environ.get(SKIP_ENV, "")

    return frozenset(name.strip() for name in raw.split(",") if name.strip())


def satisfied(required: Iterable[Path], paths: set[Path]) -> bool:
    return any(
        any(under(path, target) for path in paths) for target in required
    )


def main() -> int:
    unsynced = check_index_sync()
    if unsynced:
        print("技能的索引和子文件对不上了（这一半无条件检查，不提供豁免）：\n")
        print("\n".join(unsynced))

        return 1

    changed = changed_functional_files()
    if not changed:
        print("技能同步检查跳过：没有未提交的功能性改动。")

        return 0

    paths = changed_paths()
    exempt = skipped()
    unknown = exempt - {trigger.name for trigger in TRIGGERS}
    if unknown:
        print(
            f"{SKIP_ENV} 里有不存在的触发器名：{', '.join(sorted(unknown))}",
            file=sys.stderr,
        )

        return 2

    failures: list[str] = []
    for trigger in TRIGGERS:
        fired = [path for path in changed if trigger.matches(path)]
        if not fired:
            continue

        if trigger.name in exempt:
            print(f"触发器 {trigger.name}：已显式豁免，理由写进提交正文。")
            continue

        if satisfied(trigger.required, paths):
            print(f"触发器 {trigger.name}：技能已同步。")
            continue

        listed = "\n".join(f"      {path}" for path in fired[:8])
        more = "" if len(fired) <= 8 else f"\n      …… 另有 {len(fired) - 8} 个"
        targets = "\n".join(f"      {path}" for path in trigger.required)
        failures.append(
            f"  [{trigger.name}] 改了：\n{listed}{more}\n"
            f"    但下列位置这波都没动：\n{targets}\n"
            f"    为什么要管：{trigger.hint}"
        )

    if not failures:
        print("技能同步检查通过。")

        return 0

    print("技能落后于代码了。修完下面每一条再交接：\n")
    print("\n\n".join(failures))
    print(
        f"\n确认某条确实不需要动技能，用 {SKIP_ENV}=<触发器名> 豁免，"
        "并在提交正文里写清楚为什么。"
    )

    return 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ChangesetError as error:
        print(f"技能同步检查失败：{error}", file=sys.stderr)
        raise SystemExit(1) from error
