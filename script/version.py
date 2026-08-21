#!/usr/bin/env python3
"""版本号在本仓有四个副本，这里是它们唯一的读写入口。

四处：`internal/buildinfo/buildinfo.go` 的 `version`、`CHANGELOG.md` 的置顶条目、
生成文件头里的 `tsq-vX.Y.Z`、以及 git tag。它们必须一致，因为使用者拿到的 `tsq
version` 输出、`go get` 到的模块版本和生成文件里印的版本号是同一件事的三种说法。

生成文件那一份不在这里写：它由 `make examples` 从 buildinfo 传导过去，所以发版必须
重新生成示例。
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Self

from changeset import PROJECT_ROOT, git_output

BUILDINFO_PATH: Final = Path("internal/buildinfo/buildinfo.go")
CHANGELOG_PATH: Final = Path("CHANGELOG.md")
GO_MOD_PATH: Final = Path("go.mod")

BUILDINFO_PATTERN: Final = re.compile(
    r'(?P<prefix>^var version = ")(?P<version>[^"]+)(?P<suffix>"$)', re.MULTILINE
)
CHANGELOG_HEADING: Final = re.compile(
    r"^## \[(?P<version>\d+\.\d+\.\d+)\] - (?P<date>\d{4}-\d{2}-\d{2})\s*$",
    re.MULTILINE,
)
SEMVER: Final = re.compile(r"^v(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)$")


class VersionError(RuntimeError):
    """版本号读不出来或者对不上时抛出。"""


@dataclass(frozen=True, order=True)
class Version:
    major: int
    minor: int
    patch: int

    @classmethod
    def parse(cls, raw: str) -> Self:
        match = SEMVER.match(raw.strip())
        if match is None:
            raise VersionError(f"不是 vX.Y.Z 形式的版本号：{raw!r}")

        return cls(
            int(match.group("major")),
            int(match.group("minor")),
            int(match.group("patch")),
        )

    def bump(self, level: str) -> Self:
        if level == "major":
            return type(self)(self.major + 1, 0, 0)

        if level == "minor":
            return type(self)(self.major, self.minor + 1, 0)

        if level == "patch":
            return type(self)(self.major, self.minor, self.patch + 1)

        raise VersionError(f"未知的版本递增级别：{level!r}")

    def __str__(self) -> str:
        return f"v{self.major}.{self.minor}.{self.patch}"


def read_text(path: Path) -> str:
    return (PROJECT_ROOT / path).read_text(encoding="utf-8")


def buildinfo_version() -> Version:
    match = BUILDINFO_PATTERN.search(read_text(BUILDINFO_PATH))
    if match is None:
        raise VersionError(f"{BUILDINFO_PATH} 里找不到 `var version = \"...\"`")

    return Version.parse(match.group("version"))


def write_buildinfo_version(version: Version) -> None:
    absolute = PROJECT_ROOT / BUILDINFO_PATH
    text = absolute.read_text(encoding="utf-8")
    updated, count = BUILDINFO_PATTERN.subn(
        lambda match: f"{match.group('prefix')}{version}{match.group('suffix')}",
        text,
    )
    if count != 1:
        raise VersionError(
            f"{BUILDINFO_PATH} 里 `var version` 出现了 {count} 次，预期 1 次"
        )

    absolute.write_text(updated, encoding="utf-8")


def changelog_version() -> tuple[Version, str]:
    match = CHANGELOG_HEADING.search(read_text(CHANGELOG_PATH))
    if match is None:
        raise VersionError(f"{CHANGELOG_PATH} 里找不到 `## [X.Y.Z] - YYYY-MM-DD` 条目")

    return Version.parse(f"v{match.group('version')}"), match.group("date")


def module_major() -> int:
    """go.mod 声明的主版本号。v2+ 必须体现在模块路径里。"""
    for line in read_text(GO_MOD_PATH).splitlines():
        fields = line.strip().split()
        if len(fields) == 2 and fields[0] == "module":
            tail = fields[1].rsplit("/", 1)[-1]
            if tail.startswith("v") and tail[1:].isdigit():
                return int(tail[1:])

            return 1

    raise VersionError("go.mod 里没有 module 指令")


def tags() -> list[Version]:
    raw = git_output(["tag", "--list", "v*"]).decode("utf-8", errors="replace")
    parsed: list[Version] = []
    for line in raw.splitlines():
        try:
            parsed.append(Version.parse(line))
        except VersionError:
            continue  # 预发布或历史遗留的非规范 tag 不参与比较。

    return sorted(parsed)


def latest_tag() -> Version | None:
    existing = tags()

    return existing[-1] if existing else None


def head_tag() -> Version | None:
    raw = git_output(
        ["tag", "--points-at", "HEAD", "--list", "v*"]
    ).decode("utf-8", errors="replace")
    for line in raw.splitlines():
        try:
            return Version.parse(line)
        except VersionError:
            continue

    return None
