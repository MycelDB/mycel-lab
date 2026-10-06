#!/usr/bin/env python3
"""Lightweight docs/spec checker for Mycel Lab.

The script validates that Markdown files are non-empty and YAML reliability
specs parse as YAML. It intentionally avoids external dependencies so it can run
in minimal CI environments.
"""

from __future__ import annotations

import pathlib
import re
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


def check_scenario_docs(root: pathlib.Path) -> None:
    scenarios = root / "tests" / "reliability" / "scenarios"
    if not scenarios.exists():
        return
    for yaml_path in sorted(scenarios.glob("*.y*ml")):
        raise SystemExit(f"scenario YAML must live in a same-named directory: {yaml_path}")
    required_headings = [
        "## Purpose",
        "## Topology",
        "## Scenario phases",
        "## Actors",
        "## Tunable parameters",
        "## Evidence and artifacts",
        "## Common failure modes",
    ]
    for scenario_dir in sorted(p for p in scenarios.iterdir() if p.is_dir()):
        name = scenario_dir.name
        yaml_path = scenario_dir / f"{name}.yaml"
        md_path = scenario_dir / f"{name}.md"
        if not yaml_path.exists():
            raise SystemExit(f"scenario directory missing same-named YAML: {yaml_path}")
        if not md_path.exists():
            raise SystemExit(f"scenario directory missing same-named Markdown: {md_path}")
        yaml_text = yaml_path.read_text(encoding="utf-8")
        match = re.search(r"(?m)^metadata:\n(?:  .+\n)*?  name:\s*([^\s#]+)", yaml_text)
        if not match or match.group(1).strip('"\'') != name:
            raise SystemExit(f"scenario metadata.name must match directory name {name}: {yaml_path}")
        text = md_path.read_text(encoding="utf-8")
        missing = [heading for heading in required_headings if heading not in text]
        if missing:
            raise SystemExit(f"scenario doc {md_path} missing headings: {', '.join(missing)}")
        if "```mermaid" not in text:
            raise SystemExit(f"scenario doc {md_path} must include a Mermaid diagram")


def main(argv: list[str]) -> int:
    paths = argv[1:] or ["docs", "tests/reliability"]
    for path in iter_files(paths):
        if path.suffix == ".md":
            check_markdown(path)
        elif path.suffix in {".yaml", ".yml"}:
            check_yaml(path)
    check_scenario_docs(pathlib.Path.cwd())
    print("docs checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
