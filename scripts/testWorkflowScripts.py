#!/usr/bin/env python3
"""Behavior tests for the fail-closed SGT10 artifact upload guard."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().with_name("verifyArtifactSafety.py")


class ArtifactSafetyTests(unittest.TestCase):
    def run_guard(self, root: Path) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(SCRIPT), str(root)],
            check=False,
            text=True,
            capture_output=True,
        )

    def write_run(self, root: Path, status: str = "passed", findings: list[dict] | None = None) -> Path:
        run = root / "scenario-20261029-000000"
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

    def test_missing_terminal_evidence_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = self.run_guard(root)
            self.assertNotEqual(result.returncode, 0)


if __name__ == "__main__":
    unittest.main()
