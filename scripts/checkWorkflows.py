#!/usr/bin/env python3
"""Static SGT10 policy checks for CI and protected reliability workflows."""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github" / "workflows"
PINNED_ACTION = re.compile(r"^\s*uses:\s*[^\s@]+@([0-9a-f]{40})(?:\s+#.*)?$", re.MULTILINE)
ANY_ACTION = re.compile(r"^\s*uses:\s*([^\s]+)", re.MULTILINE)


def require(condition: bool, message: str, failures: list[str]) -> None:
    if not condition:
        failures.append(message)


def read(name: str) -> str:
    return (WORKFLOWS / name).read_text(encoding="utf-8")


def main() -> int:
    failures: list[str] = []
    expected = {"ci.yml", "semantic-live.yml", "release-gate.yml"}
    actual = {path.name for path in WORKFLOWS.glob("*.yml")}
    require(expected <= actual, f"missing workflows: {sorted(expected - actual)}", failures)

    texts: dict[str, str] = {}
    for name in sorted(expected & actual):
        text = read(name)
        texts[name] = text
        require("pull_request_target:" not in text, f"{name}: pull_request_target is forbidden", failures)
        require(re.search(r"(?m)^permissions:\n\s+contents: read\s*$", text) is not None, f"{name}: permissions must be contents: read", failures)
        for action in ANY_ACTION.findall(text):
            require(re.fullmatch(r"[^\s@]+@[0-9a-f]{40}", action) is not None, f"{name}: action is not pinned to a full commit: {action}", failures)
        require(len(PINNED_ACTION.findall(text)) == len(ANY_ACTION.findall(text)), f"{name}: malformed action pin", failures)

    ci = texts.get("ci.yml", "")
    require("pull_request:" in ci and "branches: [develop, main]" in ci, "ci.yml: PR and protected-branch triggers are required", failures)
    require("secrets." not in ci, "ci.yml: default CI must not consume secrets", failures)
    require("--dry-run" in ci and "--confirm-destructive" not in ci, "ci.yml: reliability execution must remain dry-run only", failures)
    require("compose-semantic-graph-contract" in ci, "ci.yml: deterministic semantic contract dry-run is required", failures)
    require("compose-procedure-inference-control-emulator" in ci, "ci.yml: protocol-emulator dry-run is required", failures)
    require("semantic-live-provider" in ci, "ci.yml: live-profile planning dry-run is required", failures)
    require("verifyArtifactSafety.py" in ci, "ci.yml: artifact safety verification is required", failures)

    live = texts.get("semantic-live.yml", "")
    release = texts.get("release-gate.yml", "")
    for name, text, environment in (
        ("semantic-live.yml", live, "semantic-live"),
        ("release-gate.yml", release, "release-gate"),
    ):
        require("workflow_dispatch:" in text, f"{name}: workflow_dispatch is required", failures)
        require("pull_request:" not in text and re.search(r"(?m)^\s*push:\s*$", text) is None, f"{name}: protected execution cannot run on push or pull request", failures)
        require(f"environment: {environment}" in text, f"{name}: protected environment {environment} is required", failures)
        require("group: mycel-reliability-destructive" in text and "cancel-in-progress: false" in text, f"{name}: serialized non-cancelling concurrency is required", failures)
        require("@sha256:[0-9a-f]{64}" in text, f"{name}: immutable daemon-image validation is required", failures)
        require("io.myceldb.source.dirty" in text and "org.opencontainers.image.revision" in text, f"{name}: daemon provenance validation is required", failures)
        require("verifyArtifactSafety.py" in text, f"{name}: artifact safety verification is required", failures)
        require("steps.safety.outputs.safe == 'true'" in text, f"{name}: uploads must be gated by artifact safety", failures)
        require("--confirm-destructive" in text, f"{name}: explicit destructive confirmation is required", failures)

    require("runs-on: ubuntu-24.04-arm" in live, "semantic-live.yml: native ARM64 hosted runner is required", failures)
    require("test \"$(uname -m)\" = aarch64" in live and "docker info --format '{{.Architecture}}'" in live, "semantic-live.yml: native ARM64 host preflight is required", failures)
    require("secrets.OPENAI_API_KEY" in live, "semantic-live.yml: protected OpenAI secret is required", failures)
    require(live.count("secrets.OPENAI_API_KEY") == 1, "semantic-live.yml: secret must be scoped to one run step", failures)
    require("maxRequests: 128" in live and "maxInputTokens: 20000" in live and "maxEstimatedCostMicros: 1000000" in live and "totalTimeout: 5m" in live, "semantic-live.yml: fixed live ceilings are required", failures)
    require("secrets." not in release, "release-gate.yml: cluster gate must remain credential-free", failures)
    require("runs-on: [self-hosted, linux, x64, mycel-reliability]" in release, "release-gate.yml: isolated reliability runner is required", failures)
    require("cluster-release-gate" in release, "release-gate.yml: canonical suite is required", failures)

    profile = (ROOT / "tests" / "reliability" / "provider-profiles" / "openai-compatible-live.yaml").read_text(encoding="utf-8")
    for value in ("maxRequests: 128", "maxInputTokens: 20000", "maxEstimatedCostMicros: 1000000", "totalTimeout: 5m"):
        require(value in profile, f"live provider profile is missing {value}", failures)

    if failures:
        for failure in failures:
            print(failure, file=sys.stderr)
        return 1
    print(f"SGT10 workflow policy passed for {len(expected)} workflows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
