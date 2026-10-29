#!/usr/bin/env python3
"""Behavior tests for the fail-closed SGT10 artifact upload guard."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import checkWorkflows

SCRIPT = Path(__file__).resolve().with_name("verifyArtifactSafety.py")
SEMANTIC_LIVE = Path(__file__).resolve().parents[1] / ".github" / "workflows" / "semantic-live.yml"
RELEASE_GATE = Path(__file__).resolve().parents[1] / ".github" / "workflows" / "release-gate.yml"


class WorkflowPolicyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.live = SEMANTIC_LIVE.read_text(encoding="utf-8")
        self.release = RELEASE_GATE.read_text(encoding="utf-8")

    def check_arm64_policy(self, text: str) -> list[str]:
        failures: list[str] = []
        checkWorkflows.check_live_arm64_preflight(text, failures)
        return failures

    def test_native_arm64_preflight_precedes_action_and_secret_use(self) -> None:
        self.assertEqual(self.check_arm64_policy(self.live), [])

    def test_arm64_preflight_after_action_use_is_rejected(self) -> None:
        match = checkWorkflows.LIVE_ARM64_PREFLIGHT.search(self.live)
        self.assertIsNotNone(match)
        assert match is not None
        block = match.group(0)
        moved = self.live[: match.start()] + self.live[match.end() :] + "\n" + block + "\n"
        self.assertIn(
            "semantic-live.yml: live-semantic ARM64 preflight must precede checkout/action use and secret exposure",
            self.check_arm64_policy(moved),
        )

    def test_comment_only_arm64_controls_are_rejected(self) -> None:
        match = checkWorkflows.LIVE_ARM64_PREFLIGHT.search(self.live)
        self.assertIsNotNone(match)
        assert match is not None
        commented = "\n".join(f"# {line}" for line in match.group(0).splitlines())
        text = self.live[: match.start()] + commented + self.live[match.end() :]
        text = text.replace("    runs-on: ubuntu-24.04-arm", "    runs-on: ubuntu-24.04\n#     runs-on: ubuntu-24.04-arm")
        failures = self.check_arm64_policy(text)
        self.assertIn("semantic-live.yml: live-semantic must use the native ARM64 hosted runner", failures)
        self.assertIn("semantic-live.yml: live-semantic requires the exact native ARM64 host preflight", failures)

    def test_compliant_decoy_job_cannot_mask_noncompliant_live_job(self) -> None:
        match = checkWorkflows.LIVE_ARM64_PREFLIGHT.search(self.live)
        self.assertIsNotNone(match)
        assert match is not None
        live_without_preflight = self.live[: match.start()] + self.live[match.end() :]
        live_without_preflight = live_without_preflight.replace("    runs-on: ubuntu-24.04-arm", "    runs-on: ubuntu-24.04", 1)
        decoy = (
            "jobs:\n"
            "  architecture-decoy:\n"
            "    runs-on: ubuntu-24.04-arm\n"
            "    steps:\n"
            f"{match.group(0)}\n"
        )
        text = live_without_preflight.replace("jobs:\n", decoy, 1)
        failures = self.check_arm64_policy(text)
        self.assertIn("semantic-live.yml: live-semantic must use the native ARM64 hosted runner", failures)
        self.assertIn("semantic-live.yml: live-semantic requires the exact native ARM64 host preflight", failures)

    def check_amd64_policy(self, text: str) -> list[str]:
        failures: list[str] = []
        checkWorkflows.check_release_amd64_preflight(text, failures)
        return failures

    def test_native_amd64_preflight_precedes_action_use(self) -> None:
        self.assertEqual(self.check_amd64_policy(self.release), [])

    def test_amd64_preflight_after_action_use_is_rejected(self) -> None:
        match = checkWorkflows.RELEASE_AMD64_PREFLIGHT.search(self.release)
        self.assertIsNotNone(match)
        assert match is not None
        block = match.group(0)
        moved = self.release[: match.start()] + self.release[match.end() :] + "\n" + block + "\n"
        self.assertIn(
            "release-gate.yml: cluster-release-gate AMD64 preflight must precede checkout/action use",
            self.check_amd64_policy(moved),
        )

    def test_comment_only_amd64_controls_are_rejected(self) -> None:
        match = checkWorkflows.RELEASE_AMD64_PREFLIGHT.search(self.release)
        self.assertIsNotNone(match)
        assert match is not None
        commented = "\n".join(f"# {line}" for line in match.group(0).splitlines())
        text = self.release[: match.start()] + commented + self.release[match.end() :]
        text = text.replace("    runs-on: ubuntu-24.04", "    runs-on: ubuntu-24.04-arm\n#     runs-on: ubuntu-24.04", 1)
        failures = self.check_amd64_policy(text)
        self.assertIn("release-gate.yml: cluster-release-gate must use the native AMD64 hosted runner", failures)
        self.assertIn("release-gate.yml: cluster-release-gate requires the exact native AMD64 host preflight", failures)

    def test_compliant_decoy_job_cannot_mask_noncompliant_release_job(self) -> None:
        match = checkWorkflows.RELEASE_AMD64_PREFLIGHT.search(self.release)
        self.assertIsNotNone(match)
        assert match is not None
        release_without_preflight = self.release[: match.start()] + self.release[match.end() :]
        release_without_preflight = release_without_preflight.replace("    runs-on: ubuntu-24.04", "    runs-on: ubuntu-24.04-arm", 1)
        decoy = (
            "jobs:\n"
            "  architecture-decoy:\n"
            "    runs-on: ubuntu-24.04\n"
            "    steps:\n"
            f"{match.group(0)}\n"
        )
        text = release_without_preflight.replace("jobs:\n", decoy, 1)
        failures = self.check_amd64_policy(text)
        self.assertIn("release-gate.yml: cluster-release-gate must use the native AMD64 hosted runner", failures)
        self.assertIn("release-gate.yml: cluster-release-gate requires the exact native AMD64 host preflight", failures)

    def test_removed_k3d_url_or_checksum_is_rejected(self) -> None:
        without_url = self.release.replace(
            "https://github.com/k3d-io/k3d/releases/download/v5.9.0/k3d-linux-amd64",
            "https://github.com/k3d-io/k3d/releases/download/v5.9.0/not-k3d",
            1,
        )
        without_checksum = self.release.replace(
            "06d8f25bc3a971c4eb29e0ff08429b180402db0f4dec838c9eac427e296800a0",
            "0" * 64,
            1,
        )
        message = "release-gate.yml: cluster-release-gate requires the exact checksum-pinned AMD64 k3d install"
        self.assertIn(message, self.check_amd64_policy(without_url))
        self.assertIn(message, self.check_amd64_policy(without_checksum))

    def test_commented_or_late_k3d_install_is_rejected(self) -> None:
        match = checkWorkflows.RELEASE_K3D_INSTALL.search(self.release)
        self.assertIsNotNone(match)
        assert match is not None
        block = match.group(0)
        commented = "\n".join(f"# {line}" for line in block.splitlines())
        commented_text = self.release[: match.start()] + commented + self.release[match.end() :]
        self.assertIn(
            "release-gate.yml: cluster-release-gate requires the exact checksum-pinned AMD64 k3d install",
            self.check_amd64_policy(commented_text),
        )
        late_text = self.release[: match.start()] + self.release[match.end() :] + "\n" + block + "\n"
        self.assertIn(
            "release-gate.yml: pinned k3d install and image-platform assertion must precede destructive execution",
            self.check_amd64_policy(late_text),
        )

    def test_removed_commented_or_late_image_platform_assertion_is_rejected(self) -> None:
        match = checkWorkflows.RELEASE_IMAGE_PLATFORM_ASSERTION.search(self.release)
        self.assertIsNotNone(match)
        assert match is not None
        line = match.group(0)
        removed = self.release[: match.start()] + self.release[match.end() :]
        self.assertIn(
            "release-gate.yml: cluster-release-gate requires the exact linux/amd64 daemon-image assertion",
            self.check_amd64_policy(removed),
        )
        commented = self.release[: match.start()] + "# " + line + self.release[match.end() :]
        self.assertIn(
            "release-gate.yml: cluster-release-gate requires the exact linux/amd64 daemon-image assertion",
            self.check_amd64_policy(commented),
        )
        late = removed + "\n" + line + "\n"
        self.assertIn(
            "release-gate.yml: pinned k3d install and image-platform assertion must precede destructive execution",
            self.check_amd64_policy(late),
        )

    def test_supply_chain_decoy_job_cannot_mask_noncompliant_release_job(self) -> None:
        install = checkWorkflows.RELEASE_K3D_INSTALL.search(self.release)
        platform = checkWorkflows.RELEASE_IMAGE_PLATFORM_ASSERTION.search(self.release)
        self.assertIsNotNone(install)
        self.assertIsNotNone(platform)
        assert install is not None and platform is not None
        real_without_controls = self.release[: install.start()] + self.release[install.end() :]
        platform_after_install_removal = checkWorkflows.RELEASE_IMAGE_PLATFORM_ASSERTION.search(real_without_controls)
        self.assertIsNotNone(platform_after_install_removal)
        assert platform_after_install_removal is not None
        real_without_controls = (
            real_without_controls[: platform_after_install_removal.start()]
            + real_without_controls[platform_after_install_removal.end() :]
        )
        decoy = (
            "jobs:\n"
            "  release-control-decoy:\n"
            "    runs-on: ubuntu-24.04\n"
            "    steps:\n"
            f"{install.group(0)}\n"
            "      - name: Decoy image check\n"
            "        run: |\n"
            f"{platform.group(0)}\n"
        )
        text = real_without_controls.replace("jobs:\n", decoy, 1)
        failures = self.check_amd64_policy(text)
        self.assertIn("release-gate.yml: cluster-release-gate requires the exact checksum-pinned AMD64 k3d install", failures)
        self.assertIn("release-gate.yml: cluster-release-gate requires the exact linux/amd64 daemon-image assertion", failures)

    def test_removed_or_commented_canonical_destructive_command_is_rejected(self) -> None:
        match = checkWorkflows.RELEASE_DESTRUCTIVE_RUN.search(self.release)
        self.assertIsNotNone(match)
        assert match is not None
        block = match.group(0)
        removed = self.release[: match.start()] + self.release[match.end() :]
        commented = self.release[: match.start()] + "\n".join(f"# {line}" for line in block.splitlines()) + self.release[match.end() :]
        message = "release-gate.yml: cluster-release-gate requires the exact canonical destructive command"
        self.assertIn(message, self.check_amd64_policy(removed))
        self.assertIn(message, self.check_amd64_policy(commented))

    def test_canonical_destructive_step_before_controls_is_rejected(self) -> None:
        step_start = self.release.index("      - name: Run approved cluster release gate once\n")
        step_end = self.release.index("      - name: Verify artifacts are safe to upload\n", step_start)
        step = self.release[step_start:step_end]
        without_step = self.release[:step_start] + self.release[step_end:]
        insert_at = without_step.index("      - name: Check out Mycel Lab\n")
        moved = without_step[:insert_at] + step + without_step[insert_at:]
        self.assertIn(
            "release-gate.yml: pinned k3d install and image-platform assertion must precede destructive execution",
            self.check_amd64_policy(moved),
        )

    def test_destructive_command_decoy_job_cannot_mask_missing_real_command(self) -> None:
        match = checkWorkflows.RELEASE_DESTRUCTIVE_RUN.search(self.release)
        self.assertIsNotNone(match)
        assert match is not None
        without_real = self.release[: match.start()] + self.release[match.end() :]
        decoy = (
            "jobs:\n"
            "  destructive-command-decoy:\n"
            "    runs-on: ubuntu-24.04\n"
            "    steps:\n"
            "      - name: Decoy destructive command\n"
            "        run: |\n"
            f"{match.group(0)}\n"
        )
        text = without_real.replace("jobs:\n", decoy, 1)
        self.assertIn(
            "release-gate.yml: cluster-release-gate requires the exact canonical destructive command",
            self.check_amd64_policy(text),
        )


class ArtifactSafetyTests(unittest.TestCase):
    def run_guard(self, root: Path) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(SCRIPT), str(root)],
            check=False,
            text=True,
            capture_output=True,
        )

    def write_run(self, root: Path, status: str = "passed", findings: list[dict] | None = None, name: str = "scenario-20261029-000000") -> Path:
        run = root / name
        run.mkdir(parents=True)
        (run / "result.json").write_text(json.dumps({"status": status}) + "\n", encoding="utf-8")
        (run / "redaction-scan.json").write_text(
            json.dumps({"ok": not findings, "findings": findings}) + "\n",
            encoding="utf-8",
        )
        return run

    def test_passed_run_is_inventoried(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.write_run(root)
            result = self.run_guard(root)
            self.assertEqual(result.returncode, 0, result.stderr)
            inventory = root / "artifact-upload-sha256.txt"
            self.assertTrue(inventory.is_file())
            self.assertIn("result.json", inventory.read_text(encoding="utf-8"))

    def test_failed_but_sanitized_run_is_preserved(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.write_run(root, status="failed")
            result = self.run_guard(root)
            self.assertEqual(result.returncode, 0, result.stderr)

    def test_redaction_finding_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.write_run(root, findings=[{"category": "api-key", "path": "log.txt"}])
            result = self.run_guard(root)
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse((root / "artifact-upload-sha256.txt").exists())

    def test_direct_scan_rejects_file_added_after_redaction(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run = self.write_run(root)
            (run / "late.log").write_text("key=sk-examplecredential123456789\n", encoding="utf-8")
            result = self.run_guard(root)
            self.assertNotEqual(result.returncode, 0)
            self.assertNotIn("sk-example", result.stderr)

    def test_incomplete_sibling_run_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.write_run(root, name="scenario-complete")
            incomplete = root / "scenario-incomplete"
            incomplete.mkdir()
            (incomplete / "harness-build.json").write_text("{}\n", encoding="utf-8")
            result = self.run_guard(root)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("scenario-incomplete/result.json", result.stderr)
            self.assertFalse((root / "artifact-upload-sha256.txt").exists())

    def test_redaction_only_sibling_run_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.write_run(root, name="scenario-complete")
            incomplete = root / "scenario-redaction-only"
            incomplete.mkdir()
            (incomplete / "redaction-scan.json").write_text(
                json.dumps({"ok": False, "findings": [{"kind": "unscannable"}]}) + "\n",
                encoding="utf-8",
            )
            result = self.run_guard(root)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("scenario-redaction-only/result.json", result.stderr)
            self.assertFalse((root / "artifact-upload-sha256.txt").exists())

    @unittest.skipUnless(hasattr(os, "symlink"), "symlinks are unavailable")
    def test_file_and_directory_symlinks_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run = self.write_run(root)
            target_file = run / "target.log"
            target_file.write_text("safe\n", encoding="utf-8")
            target_directory = run / "target-directory"
            target_directory.mkdir()
            os.symlink(target_file, run / "file-link")
            os.symlink(target_directory, run / "directory-link")
            result = self.run_guard(root)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("file-link: artifact symlink is forbidden", result.stderr)
            self.assertIn("directory-link: artifact symlink is forbidden", result.stderr)
            self.assertFalse((root / "artifact-upload-sha256.txt").exists())

    def test_missing_terminal_evidence_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = self.run_guard(root)
            self.assertNotEqual(result.returncode, 0)


if __name__ == "__main__":
    unittest.main()
