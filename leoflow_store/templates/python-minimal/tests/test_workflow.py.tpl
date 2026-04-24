from __future__ import annotations

import json
import re
import subprocess
import sys
import unittest
from pathlib import Path

try:
    import yaml
except ImportError as exc:  # pragma: no cover - exercised only in broken envs
    raise RuntimeError("PyYAML is required. Install dependencies with `pip install -r requirements.txt`.") from exc

ROOT = Path(__file__).resolve().parents[1]


class GeneratedWorkflowTest(unittest.TestCase):
    def test_generated_structure(self) -> None:
        spec = yaml.safe_load((ROOT / "workflow.yaml").read_text(encoding="utf-8"))

        self.assertTrue((ROOT / "workflow.yaml").exists())
        self.assertTrue((ROOT / "runtime" / "__init__.py").exists())
        self.assertTrue((ROOT / "runtime" / "README.md").exists())
        self.assertTrue((ROOT / "runtime" / "task_support.py").exists())
        self.assertTrue((ROOT / "runtime" / "task_runtime.py").exists())
        self.assertTrue((ROOT / "tasks" / "README.md").exists())
        self.assertTrue((ROOT / "resources" / "README.md").exists())
        region = spec["data"]["region"]
        if isinstance(region, str) and region.endswith((".geojson", ".json")):
            self.assertTrue(region.startswith("resources/"))
            self.assertTrue((ROOT / region).exists())

        data_task = ROOT / "tasks" / "data" / f"{self._slug(spec['data']['source'])}.py"
        self.assertTrue(data_task.exists())

        for step in spec["preprocessing"]:
            name = next(iter(step.keys()))
            if name == "command":
                continue
            self.assertTrue((ROOT / "tasks" / "preprocessing" / f"{self._slug(name)}.py").exists())

        for feature_name in spec["features"]:
            self.assertTrue((ROOT / "tasks" / "features" / f"{self._slug(feature_name)}.py").exists())

        self.assertTrue((ROOT / "tasks" / "model" / f"{self._slug(spec['model']['output'])}.py").exists())
        for metric_name in spec["evaluation"]["metrics"]:
            self.assertTrue((ROOT / "tasks" / "evaluation" / f"{self._slug(metric_name)}.py").exists())

    def test_app_runs_when_workflow_is_executable(self) -> None:
        spec = yaml.safe_load((ROOT / "workflow.yaml").read_text(encoding="utf-8"))
        if not self._is_executable(spec):
            self.skipTest("generated scaffold bundle; add executable data/model/evaluation config or replace generated tasks")

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
        self.assertTrue((ROOT / "artifacts" / "{{WORKFLOW_SLUG}}" / "reports" / "last-run.json").exists())
        self.assertTrue((ROOT / "artifacts" / "{{WORKFLOW_SLUG}}" / "reports" / "io-manifest.json").exists())
        self.assertEqual(
            Path(payload["inputs"]["workflow_yaml"]).resolve(),
            (ROOT / "workflow.yaml").resolve(),
        )
        self.assertIn(
            str((ROOT / "artifacts" / "{{WORKFLOW_SLUG}}" / "reports" / "last-run.json").resolve()),
            payload["reports"]["files"],
        )
        self.assertTrue(Path(payload["prediction"]["artifact"]).exists())

    def _slug(self, value: object) -> str:
        if isinstance(value, dict):
            if value.get("name"):
                value = str(value["name"])
            elif value.get("collection"):
                value = f"{value.get('kind', 'source')}_{value['collection']}"
            elif value.get("kind"):
                value = str(value["kind"])
            else:
                value = "data_source"
        return re.sub(r"[^a-z0-9]+", "_", str(value).lower()).strip("_")

    def _is_executable(self, spec: dict) -> bool:
        return (
            isinstance(spec.get("data", {}).get("source"), dict)
            and bool(spec.get("model", {}).get("executor"))
            and bool(spec.get("evaluation", {}).get("executor"))
        )
