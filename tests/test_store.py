from __future__ import annotations

import subprocess
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path

from leoflow_store.core.generator import generate_project
from leoflow_store.core.parser import load_workflow
from leoflow_store.core.registry import WorkflowRegistry
from leoflow_store.core.validator import resolve_version, validate_workflow_spec


ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / "examples" / "wildfire-detection.yaml"


class StoreSmokeTest(unittest.TestCase):
    def test_publish_search_download_roundtrip(self) -> None:
        spec = validate_workflow_spec(load_workflow(EXAMPLE))
        version = resolve_version(spec)

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            build_dir = temp_root / "build"
            registry_root = temp_root / "registry"
            download_dir = temp_root / "download"

            generate_project(spec, version, "python-minimal", build_dir, workflow_path=EXAMPLE)
            self.assertTrue((build_dir / "app.py").exists())
            self.assertTrue((build_dir / "workflow.yaml").exists())
            self.assertTrue((build_dir / "runtime" / "__init__.py").exists())
            self.assertTrue((build_dir / "runtime" / "README.md").exists())
            self.assertTrue((build_dir / "runtime" / "task_support.py").exists())
            self.assertTrue((build_dir / "runtime" / "task_runtime.py").exists())
            self.assertTrue((build_dir / "tests" / "test_workflow.py").exists())
            self.assertTrue((build_dir / "tasks" / "README.md").exists())
            self.assertTrue((build_dir / "tasks" / "lib" / "__init__.py").exists())
            self.assertTrue((build_dir / "tasks" / "data" / "stac_sentinel_2.py").exists())
            self.assertTrue((build_dir / "tasks" / "preprocessing" / "cloud_mask.py").exists())
            self.assertTrue((build_dir / "tasks" / "preprocessing" / "resample.py").exists())
            self.assertTrue((build_dir / "tasks" / "preprocessing" / "align_time.py").exists())
            self.assertTrue((build_dir / "tasks" / "features" / "ndvi.py").exists())
            self.assertTrue((build_dir / "tasks" / "features" / "ndwi.py").exists())
            self.assertTrue((build_dir / "tasks" / "model" / "fire_mask.py").exists())
            self.assertTrue((build_dir / "tasks" / "evaluation" / "iou.py").exists())
            self.assertTrue((build_dir / "tasks" / "evaluation" / "temporal_consistency.py").exists())
            self.assertTrue((build_dir / "resources" / "README.md").exists())
            self.assertTrue((build_dir / "resources" / "polygon.geojson").exists())
            self.assertIn("resources/polygon.geojson", (build_dir / "workflow.yaml").read_text(encoding="utf-8"))
            self.assertIn("api_url: https://earth-search.aws.element84.com/v1/search", (build_dir / "workflow.yaml").read_text(encoding="utf-8"))
            polygon = (build_dir / "resources" / "polygon.geojson").read_text(encoding="utf-8")
            self.assertIn("[19.3, 34.8]", polygon)
            data_task_source = (build_dir / "tasks" / "data" / "stac_sentinel_2.py").read_text(encoding="utf-8")
            self.assertIn("from runtime.task_runtime import task", data_task_source)
            self.assertNotIn("import argparse", data_task_source)
            self.assertIn('source=ctx.spec["data"]["source"]', data_task_source)

            generated_tests = subprocess.run(
                [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-p", "test_*.py"],
                cwd=build_dir,
                capture_output=True,
                text=True,
                check=True,
            )
            test_output = generated_tests.stdout + generated_tests.stderr
            self.assertIn("OK", test_output)
            self.assertIn("skipped=1", test_output)

            registry = WorkflowRegistry(registry_root)
            published = registry.publish(spec, version, "python-minimal", build_dir)
            self.assertEqual(published["version"], "0.1.0")
            with zipfile.ZipFile(registry_root / "wildfire-detection" / "0.1.0" / "template.zip") as archive:
                names = archive.namelist()
            self.assertFalse(any(name.startswith("artifacts/") for name in names))
            self.assertFalse(any("__pycache__" in name for name in names))
            self.assertFalse(any(name.endswith(".pyc") for name in names))
            self.assertFalse(any(name.endswith(".DS_Store") for name in names))

            results = registry.search("wildfire")
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0]["name"], "wildfire-detection")

            registry.download("wildfire-detection", "0.1.0", download_dir)
            self.assertTrue((download_dir / "app.py").exists())
            self.assertTrue((download_dir / "workflow.yaml").exists())
            self.assertTrue((download_dir / "runtime" / "__init__.py").exists())
            self.assertTrue((download_dir / "runtime" / "README.md").exists())
            self.assertTrue((download_dir / "runtime" / "task_support.py").exists())
            self.assertTrue((download_dir / "runtime" / "task_runtime.py").exists())
            self.assertTrue((download_dir / "tests" / "test_workflow.py").exists())
            self.assertTrue((download_dir / "tasks" / "README.md").exists())
            self.assertTrue((download_dir / "resources" / "README.md").exists())

            downloaded_tests = subprocess.run(
                [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-p", "test_*.py"],
                cwd=download_dir,
                capture_output=True,
                text=True,
                check=True,
            )
            self.assertIn("OK", downloaded_tests.stdout + downloaded_tests.stderr)


if __name__ == "__main__":
    unittest.main()
