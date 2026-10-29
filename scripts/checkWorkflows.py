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
LIVE_ARM64_RUNNER = re.compile(r"^    runs-on: ubuntu-24\.04-arm$", re.MULTILINE)
LIVE_ARM64_PREFLIGHT = re.compile(
    r"^      - name: Verify native ARM64 execution host\n"
    r"        run: \|\n"
    r"          set -euo pipefail\n"
    r"          test \"\$\(uname -m\)\" = aarch64\n"
    r"          test \"\$\(docker info --format '\{\{\.Architecture\}\}'\)\" = aarch64$",
    re.MULTILINE,
)
FIRST_ACTION_STEP = re.compile(r"^\s+uses:\s*", re.MULTILINE)
LIVE_SECRET = "${{ secrets.OPENAI_API_KEY }}"
RELEASE_AMD64_RUNNER = re.compile(r"^    runs-on: ubuntu-24\.04$", re.MULTILINE)
RELEASE_AMD64_PREFLIGHT = re.compile(
    r"^      - name: Verify native AMD64 execution host\n"
    r"        run: \|\n"
    r"          set -euo pipefail\n"
    r"          test \"\$\(uname -m\)\" = x86_64\n"
    r"          test \"\$\(docker info --format '\{\{\.Architecture\}\}'\)\" = x86_64$",
    re.MULTILINE,
)
RELEASE_K3D_INSTALL = re.compile(
    r"^      - name: Install pinned k3d\n"
    r"        run: \|\n"
    r"          set -euo pipefail\n"
    r"          install -d \"\$RUNNER_TEMP/bin\"\n"
    r"          curl --fail --silent --show-error --location \\\n"
    r"            https://github\.com/k3d-io/k3d/releases/download/v5\.9\.0/k3d-linux-amd64 \\\n"
    r"            --output \"\$RUNNER_TEMP/bin/k3d\"\n"
    r"          echo '06d8f25bc3a971c4eb29e0ff08429b180402db0f4dec838c9eac427e296800a0  k3d' \\\n"
    r"            \| \(cd \"\$RUNNER_TEMP/bin\" && sha256sum --check --strict -\)\n"
    r"          chmod \+x \"\$RUNNER_TEMP/bin/k3d\"\n"
    r"          echo \"\$RUNNER_TEMP/bin\" >> \"\$GITHUB_PATH\"$",
    re.MULTILINE,
)
RELEASE_IMAGE_PLATFORM_ASSERTION = re.compile(
    r"^          test \"\$\(docker image inspect \"\$DAEMON_IMAGE\" --format '\{\{\.Os\}\}/\{\{\.Architecture\}\}'\)\" = linux/amd64$",
    re.MULTILINE,
)
RELEASE_DESTRUCTIVE_RUN = re.compile(
    r"^          \./bin/mycel-lab run suite cluster-release-gate \\\n"
    r"            --confirm-destructive \\\n"
    r"            --image \"\$DAEMON_IMAGE\" \\\n"
    r"            --artifact-root \"\$ARTIFACT_ROOT\"$",
    re.MULTILINE,
)
MINIO_HELPER_IMAGE = "docker.io/myceldb/mycel-minio-test-helper@sha256:f33b406adb70891f722e0465ec085056b036f8f5494e4eff33317a9612cf24ff"
MC_HELPER_IMAGE = "docker.io/myceldb/mycel-mc-test-helper@sha256:9646d8b5d019fd20e8f4e1d782b29a56581d51c5a367e7c43156c0faac1a69f0"


def exact_block(block: str) -> re.Pattern[str]:
    return re.compile(r"^" + re.escape(block) + r"$", re.MULTILINE)


def helper_preflight_pattern(architecture: str) -> re.Pattern[str]:
    label = architecture.upper()
    block = f'''      - name: Verify immutable {label} object-store helpers
        env:
          MINIO_IMAGE: {MINIO_HELPER_IMAGE}
          MINIO_MC_IMAGE: {MC_HELPER_IMAGE}
        run: |
          set -euo pipefail
          docker pull "$MINIO_IMAGE"
          docker pull "$MINIO_MC_IMAGE"
          test "$(docker image inspect "$MINIO_IMAGE" --format '{{{{.Os}}}}/{{{{.Architecture}}}}')" = linux/{architecture}
          test "$(docker image inspect "$MINIO_MC_IMAGE" --format '{{{{.Os}}}}/{{{{.Architecture}}}}')" = linux/{architecture}
          test "$(docker image inspect "$MINIO_IMAGE" --format '{{{{index .Config.Labels "org.opencontainers.image.revision"}}}}')" = 07c3a429bfed433e49018cb0f78a52145d4bedeb
          test "$(docker image inspect "$MINIO_MC_IMAGE" --format '{{{{index .Config.Labels "org.opencontainers.image.revision"}}}}')" = 7394ce0dd2a80935aded936b09fa12cbb3cb8096
          test "$(docker image inspect "$MINIO_IMAGE" --format '{{{{index .Config.Labels "io.myceldb.source.dirty"}}}}')" = false
          test "$(docker image inspect "$MINIO_MC_IMAGE" --format '{{{{index .Config.Labels "io.myceldb.source.dirty"}}}}')" = false
          docker run --rm "$MINIO_IMAGE" --version | grep -F 'RELEASE.2025-09-07T16-13-09Z (commit-id=07c3a429bfed433e49018cb0f78a52145d4bedeb)'
          docker run --rm "$MINIO_MC_IMAGE" --version | grep -F 'RELEASE.2025-08-13T08-35-41Z (commit-id=7394ce0dd2a80935aded936b09fa12cbb3cb8096)'
'''.rstrip("\n")
    return exact_block(block)


LIVE_HELPER_RUN_ENV = exact_block(
    f'''        env:
          ARTIFACT_ROOT: ${{{{ runner.temp }}}}/mycel-lab-semantic-live
          DAEMON_IMAGE: ${{{{ inputs.daemon_image }}}}
          MINIO_IMAGE: {MINIO_HELPER_IMAGE}
          MINIO_MC_IMAGE: {MC_HELPER_IMAGE}
          OPENAI_API_KEY: ${{{{ secrets.OPENAI_API_KEY }}}}'''
)
RELEASE_HELPER_RUN_ENV = exact_block(
    f'''        env:
          ARTIFACT_ROOT: ${{{{ runner.temp }}}}/mycel-lab-cluster-release-gate
          DAEMON_IMAGE: ${{{{ inputs.daemon_image }}}}
          MINIO_IMAGE: {MINIO_HELPER_IMAGE}
          MINIO_MC_IMAGE: {MC_HELPER_IMAGE}'''
)


def require(condition: bool, message: str, failures: list[str]) -> None:
    if not condition:
        failures.append(message)


def read(name: str) -> str:
    return (WORKFLOWS / name).read_text(encoding="utf-8")


def job_block(workflow: str, job_name: str) -> str | None:
    header = re.compile(rf"^  {re.escape(job_name)}:\s*$", re.MULTILINE)
    matches = list(header.finditer(workflow))
    if len(matches) != 1:
        return None
    start = matches[0].start()
    next_job = re.search(r"^  [A-Za-z0-9_-]+:\s*$", workflow[matches[0].end() :], re.MULTILINE)
    end = matches[0].end() + next_job.start() if next_job else len(workflow)
    return workflow[start:end]


def check_live_arm64_preflight(live: str, failures: list[str]) -> None:
    live_job = job_block(live, "live-semantic")
    require(live_job is not None, "semantic-live.yml: exactly one live-semantic job is required", failures)
    if live_job is None:
        return

    runner = LIVE_ARM64_RUNNER.search(live_job)
    preflight = LIVE_ARM64_PREFLIGHT.search(live_job)
    first_action = FIRST_ACTION_STEP.search(live_job)
    secret_position = live_job.find(LIVE_SECRET)

    require(runner is not None, "semantic-live.yml: live-semantic must use the native ARM64 hosted runner", failures)
    require(preflight is not None, "semantic-live.yml: live-semantic requires the exact native ARM64 host preflight", failures)
    require(
        preflight is not None
        and first_action is not None
        and secret_position >= 0
        and preflight.start() < first_action.start()
        and preflight.start() < secret_position,
        "semantic-live.yml: live-semantic ARM64 preflight must precede checkout/action use and secret exposure",
        failures,
    )


def check_helper_images(
    workflow: str,
    job_name: str,
    architecture: str,
    run_env_pattern: re.Pattern[str],
    workflow_name: str,
    failures: list[str],
) -> None:
    job = job_block(workflow, job_name)
    if job is None:
        return

    preflight = helper_preflight_pattern(architecture).search(job)
    run_env = run_env_pattern.search(job)
    destructive = job.find("--confirm-destructive")
    secret = job.find(LIVE_SECRET)

    require(preflight is not None, f"{workflow_name}: {job_name} requires the exact immutable {architecture} helper-image preflight", failures)
    require(run_env is not None, f"{workflow_name}: {job_name} must pass the preflighted helper digests to the protected run step", failures)
    require(
        preflight is not None and destructive >= 0 and preflight.start() < destructive,
        f"{workflow_name}: helper-image preflight must precede destructive execution",
        failures,
    )
    if secret >= 0:
        require(
            preflight is not None and preflight.start() < secret,
            f"{workflow_name}: helper-image preflight must precede credential exposure",
            failures,
        )
    require(job.count(f"MINIO_IMAGE: {MINIO_HELPER_IMAGE}") == 2, f"{workflow_name}: MinIO helper digest must appear exactly in preflight and protected run env", failures)
    require(job.count(f"MINIO_MC_IMAGE: {MC_HELPER_IMAGE}") == 2, f"{workflow_name}: mc helper digest must appear exactly in preflight and protected run env", failures)


def check_release_amd64_preflight(release: str, failures: list[str]) -> None:
    release_job = job_block(release, "cluster-release-gate")
    require(release_job is not None, "release-gate.yml: exactly one cluster-release-gate job is required", failures)
    if release_job is None:
        return

    runner = RELEASE_AMD64_RUNNER.search(release_job)
    preflight = RELEASE_AMD64_PREFLIGHT.search(release_job)
    first_action = FIRST_ACTION_STEP.search(release_job)
    k3d_install = RELEASE_K3D_INSTALL.search(release_job)
    image_platform = RELEASE_IMAGE_PLATFORM_ASSERTION.search(release_job)
    destructive_run = RELEASE_DESTRUCTIVE_RUN.search(release_job)

    require(runner is not None, "release-gate.yml: cluster-release-gate must use the native AMD64 hosted runner", failures)
    require(preflight is not None, "release-gate.yml: cluster-release-gate requires the exact native AMD64 host preflight", failures)
    require(
        preflight is not None
        and first_action is not None
        and preflight.start() < first_action.start(),
        "release-gate.yml: cluster-release-gate AMD64 preflight must precede checkout/action use",
        failures,
    )
    require(k3d_install is not None, "release-gate.yml: cluster-release-gate requires the exact checksum-pinned AMD64 k3d install", failures)
    require(image_platform is not None, "release-gate.yml: cluster-release-gate requires the exact linux/amd64 daemon-image assertion", failures)
    require(destructive_run is not None, "release-gate.yml: cluster-release-gate requires the exact canonical destructive command", failures)
    require(
        k3d_install is not None
        and image_platform is not None
        and destructive_run is not None
        and k3d_install.start() < destructive_run.start()
        and image_platform.start() < destructive_run.start(),
        "release-gate.yml: pinned k3d install and image-platform assertion must precede destructive execution",
        failures,
    )


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

    check_live_arm64_preflight(live, failures)
    check_release_amd64_preflight(release, failures)
    check_helper_images(live, "live-semantic", "arm64", LIVE_HELPER_RUN_ENV, "semantic-live.yml", failures)
    check_helper_images(release, "cluster-release-gate", "amd64", RELEASE_HELPER_RUN_ENV, "release-gate.yml", failures)
    require("secrets.OPENAI_API_KEY" in live, "semantic-live.yml: protected OpenAI secret is required", failures)
    require(live.count("secrets.OPENAI_API_KEY") == 1, "semantic-live.yml: secret must be scoped to one run step", failures)
    require("maxRequests: 128" in live and "maxInputTokens: 20000" in live and "maxEstimatedCostMicros: 1000000" in live and "totalTimeout: 5m" in live, "semantic-live.yml: fixed live ceilings are required", failures)
    require("secrets." not in release, "release-gate.yml: cluster gate must remain credential-free", failures)

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
