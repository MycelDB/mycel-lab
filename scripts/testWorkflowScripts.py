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


class WorkflowPolicyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.live = SEMANTIC_LIVE.read_text(encoding="utf-8")

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
            "semantic-live.yml: ARM64 preflight must precede checkout/action use and secret exposure",
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
        self.assertIn("semantic-live.yml: native ARM64 hosted runner is required", failures)
        self.assertIn("semantic-live.yml: exact native ARM64 host preflight is required", failures)




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
