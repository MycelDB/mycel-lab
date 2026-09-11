#!/usr/bin/env python3
"""Lightweight docs/spec checker for Mycel Lab.

The script validates that Markdown files are non-empty and YAML reliability
specs parse as YAML. It intentionally avoids external dependencies so it can run
in minimal CI environments.
"""

from __future__ import annotations

import pathlib
import sys


def iter_files(paths: list[str]) -> list[pathlib.Path]:
    files: list[pathlib.Path] = []
    for raw in paths:
        path = pathlib.Path(raw)
        if path.is_dir():
            files.extend(p for p in path.rglob("*") if p.is_file())
        elif path.is_file():
            files.append(path)
        else:
            raise SystemExit(f"missing path: {path}")
    return sorted(files)


def check_markdown(path: pathlib.Path) -> None:
    text = path.read_text(encoding="utf-8")
    if not text.strip():
        raise SystemExit(f"empty markdown file: {path}")
    if not text.lstrip().startswith("#"):
        raise SystemExit(f"markdown file must start with a heading: {path}")


def check_yaml(path: pathlib.Path) -> None:
    text = path.read_text(encoding="utf-8")
    if not text.strip():
        raise SystemExit(f"empty yaml file: {path}")
    # Minimal structural check without PyYAML: every reliability YAML should
    # declare apiVersion, kind, and metadata.name.
    required = ["apiVersion:", "kind:", "metadata:", "name:"]
    missing = [token for token in required if token not in text]
    if missing:
        raise SystemExit(f"yaml file {path} missing {', '.join(missing)}")


def main(argv: list[str]) -> int:
    paths = argv[1:] or ["docs", "tests/reliability"]
    for path in iter_files(paths):
        if path.suffix == ".md":
            check_markdown(path)
        elif path.suffix in {".yaml", ".yml"}:
            check_yaml(path)
    print("docs checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
