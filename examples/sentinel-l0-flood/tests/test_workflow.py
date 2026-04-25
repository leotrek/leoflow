from __future__ import annotations

import importlib.util
import json
import re
import shlex
import subprocess
import sys
import unittest
import urllib.request
from pathlib import Path

try:
    import yaml
except ImportError as exc:  # pragma: no cover - exercised only in broken envs
    raise RuntimeError("PyYAML is required. Install dependencies with `pip install -r requirements.txt`.") from exc

ROOT = Path(__file__).resolve().parents[1]


class GeneratedWorkflowTest(unittest.TestCase):
    def test_asf_download_opener_uses_cookie_session(self) -> None:
        module = self._load_data_task_module()
        opener = module._build_authenticated_opener("earthdata-user", "earthdata-pass")
        handler_types = {type(handler) for handler in opener.handlers}

        self.assertIn(urllib.request.HTTPCookieProcessor, handler_types)
        self.assertIn(urllib.request.HTTPBasicAuthHandler, handler_types)

    def test_named_commands_use_current_python_executable(self) -> None:
        app_module = self._load_app_module()
        command = app_module.app._resolve_preprocess_command("focus_level0")
        rendered = command["run"].format_map(  # type: ignore[index]
            app_module.app._command_context({}, {"input_dir": ROOT / "artifacts" / "input"})
        )

        self.assertTrue(rendered.startswith(f"{shlex.quote(sys.executable)} "))
        self.assertNotIn("python3 tasks/custom", rendered)

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

        self.assertEqual(payload["workflow"], "sentinel-l0-flood")
        self.assertEqual(payload["runtime"], "python-minimal")
        self.assertEqual(payload["status"], "completed")
        self.assertTrue((ROOT / "artifacts" / "sentinel-l0-flood" / "reports" / "last-run.json").exists())
        self.assertTrue((ROOT / "artifacts" / "sentinel-l0-flood" / "reports" / "io-manifest.json").exists())
        self.assertEqual(
            Path(payload["inputs"]["workflow_yaml"]).resolve(),
            (ROOT / "workflow.yaml").resolve(),
        )
        self.assertIn(
            str((ROOT / "artifacts" / "sentinel-l0-flood" / "reports" / "last-run.json").resolve()),
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

    def _load_data_task_module(self):
        module_path = ROOT / "tasks" / "data" / "sentinel_1_asf_datapool_raw.py"
        return self._load_module("sentinel_1_asf_datapool_raw", module_path)

    def _load_app_module(self):
        return self._load_module("sentinel_l0_flood_app", ROOT / "app.py")

    def _load_module(self, name: str, module_path: Path):
        spec = importlib.util.spec_from_file_location(name, module_path)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"could not load module from {module_path}")
        module = importlib.util.module_from_spec(spec)
        sys.path.insert(0, str(ROOT))
        try:
            spec.loader.exec_module(module)
        finally:
            sys.path.pop(0)
        return module
