from __future__ import annotations

import json
import subprocess
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class GeneratedWorkflowTest(unittest.TestCase):
    def test_app_runs(self) -> None:
        result = subprocess.run(
            [sys.executable, "app.py"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=True,
        )
        payload = json.loads(result.stdout)
        self.assertEqual(payload["workflow"], "{{WORKFLOW_NAME}}")
        self.assertEqual(payload["runtime"], "{{RUNTIME_NAME}}")
        self.assertEqual(payload["status"], "completed")
        self.assertTrue((ROOT / "workflow.yaml").exists())
        self.assertTrue((ROOT / "runtime" / "__init__.py").exists())
        self.assertTrue((ROOT / "artifacts" / "{{WORKFLOW_SLUG}}" / "last-run.json").exists())
