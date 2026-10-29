#!/usr/bin/env python3
"""Fail closed unless every retained Mycel Lab run has a clean redaction report."""

from __future__ import annotations

import hashlib
import json
import re
import sys
from pathlib import Path

MAX_FILE_BYTES = 64 * 1024 * 1024
RUN_MARKERS = ("harness-build.json", "result.json")
SENSITIVE_PATTERNS = {
    "api-key": re.compile(rb"\bsk-[A-Za-z0-9_-]{16,}\b"),
    "bearer-token": re.compile(rb"(?i)\bbearer[ \t]+[a-z0-9._~+/=-]{8,}"),
    "pem-material": re.compile(rb"(?s)-----BEGIN [A-Z0-9 ]+-----.*?-----END [A-Z0-9 ]+-----"),
}


def load_object(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return value


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: verifyArtifactSafety.py <artifact-root>", file=sys.stderr)
        return 2

    requested_root = Path(sys.argv[1])
    if requested_root.is_symlink():
        print(f"artifact root cannot be a symlink: {requested_root}", file=sys.stderr)
        return 1
    root = requested_root.resolve()
    if not root.is_dir():
        print(f"artifact root does not exist: {root}", file=sys.stderr)
        return 1

    entries = sorted(root.rglob("*"))
    failures = [
        f"{path.relative_to(root).as_posix()}: artifact symlink is forbidden"
        for path in entries
        if path.is_symlink()
    ]
    if failures:
        for failure in failures:
            print(failure, file=sys.stderr)
        return 1

    run_roots = sorted(
        {
            marker.parent
            for marker_name in RUN_MARKERS
            for marker in root.rglob(marker_name)
            if marker.is_file()
        }
    )
    if not run_roots:
        print("no run markers found; refusing artifact upload", file=sys.stderr)
        return 1

    failures = []
    for run_root in run_roots:
        result_path = run_root / "result.json"
        redaction_path = run_root / "redaction-scan.json"
        try:
            result = load_object(result_path)
            redaction = load_object(redaction_path)
        except ValueError as exc:
            failures.append(str(exc))
            continue

        if result.get("status") not in {"passed", "failed"}:
            failures.append(f"{result_path}: missing terminal run status")
        if redaction.get("ok") is not True:
            failures.append(f"{redaction_path}: redaction scan did not pass")
        findings = redaction.get("findings")
        if findings is not None and (not isinstance(findings, list) or findings):
            failures.append(f"{redaction_path}: redaction findings are present or malformed")

    if failures:
        for failure in failures:
            print(failure, file=sys.stderr)
        return 1

    inventory_path = root / "artifact-upload-sha256.txt"
    rows: list[tuple[str, str]] = []
    for path in entries:
        if path == inventory_path or path.is_dir():
            continue
        relative = path.relative_to(root).as_posix()
        if not path.is_file():
            failures.append(f"{relative}: non-regular artifact file is forbidden")
            continue
        size = path.stat().st_size
        if size > MAX_FILE_BYTES:
            failures.append(f"{relative}: artifact exceeds the 64 MiB per-file safety bound")
            continue
        data = path.read_bytes()
        for category, pattern in SENSITIVE_PATTERNS.items():
            if pattern.search(data):
                failures.append(f"{relative}: direct safety scan found {category}")
        rows.append((relative, hashlib.sha256(data).hexdigest()))

    if failures:
        for failure in failures:
            print(failure, file=sys.stderr)
        return 1
    inventory_path.write_text(
        "".join(f"{digest}  {relative}\n" for relative, digest in rows),
        encoding="utf-8",
    )
    print(f"artifact safety verified for {len(run_roots)} run(s) and {len(rows)} file(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
