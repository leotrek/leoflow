from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from leoflow_store.core.generator import generate_project
from leoflow_store.core.validator import resolve_version, validate_workflow_spec


class ExecutableRuntimeTest(unittest.TestCase):
    def test_generated_app_downloads_data_and_model_and_runs_commands(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            fixture = self._build_fixture(root / "fixture")

            spec = validate_workflow_spec(
                {
                    "workflow": {"name": "wildfire-download-runner"},
                    "data": {
                        "source": {
                            "kind": "stac",
                            "search_results_path": str(fixture / "search-results.json"),
                            "collection": "sentinel-2-l2a",
                            "assets": ["B04"],
                            "limit": 1,
                        },
                        "region": "polygon.geojson",
                        "time": "2024-06-01/2024-08-01",
                        "resolution": "10m",
                    },
                    "preprocessing": [
                        {
                            "command": {
                                "run": (
                                    'mkdir -p "{preprocess_dir}" && '
                                    'cp "{data_dir}/item-1/B04.tif" "{preprocess_dir}/B04.tif"'
                                ),
                                "output": "{preprocess_dir}",
                            }
                        }
                    ],
                    "features": ["ndvi"],
                    "model": {
                        "type": "segmentation",
                        "input": "patches(256x256)",
                        "output": "fire_mask",
                        "source": {
                            "kind": "huggingface",
                            "endpoint": fixture.as_uri(),
                            "repo_id": "hf/acme/fire-model",
                            "revision": "main",
                            "filename": "model.onnx",
                        },
                        "executor": {
                            "run": 'mkdir -p "{prediction_dir}" && cp "{model_path}" "{prediction_dir}/fire_mask.onnx"',
                            "artifact": "{prediction_dir}/fire_mask.onnx",
                        },
                    },
                    "evaluation": {
                        "metrics": ["iou"],
                        "executor": {
                            "run": (
                                'mkdir -p "{outputs_dir}/evaluation" && '
                                'printf "iou ok\\n" > "{outputs_dir}/evaluation/evaluation.json"'
                            ),
                            "artifact": "{outputs_dir}/evaluation/evaluation.json",
                        },
                    },
                }
            )
            version = resolve_version(spec)

            project_dir = root / "build"
            generate_project(spec, version, "python-minimal", project_dir)

            result = subprocess.run(
                [sys.executable, "app.py"],
                cwd=project_dir,
                capture_output=True,
                text=True,
                check=True,
            )
            payload = json.loads(result.stdout)

            self.assertEqual(payload["workflow"], "wildfire-download-runner")
            self.assertEqual(payload["status"], "completed")
            self.assertIn("[start] stage load_data", result.stderr)
            self.assertIn("[finish] stage evaluate", result.stderr)
            self.assertIn("[start] task features/ndvi", result.stderr)
            self.assertIn("[finish] task features/ndvi", result.stderr)
            self.assertIn("duration=", result.stderr)
            self.assertTrue((project_dir / "workflow.yaml").exists())
            self.assertEqual(
                Path(payload["inputs"]["workflow_yaml"]).resolve(),
                (project_dir / "workflow.yaml").resolve(),
            )
            self.assertEqual(
                payload["inputs"]["data"]["source"]["collection"],
                "sentinel-2-l2a",
            )
            self.assertIn(
                str((project_dir / "resources" / "polygon.geojson").resolve()),
                payload["inputs"]["local_files"],
            )
            self.assertIn(
                str((fixture / "search-results.json").resolve()),
                payload["inputs"]["local_files"],
            )
            self.assertIn(
                str(
                    (
                        project_dir
                        / "artifacts"
                        / "wildfire-download-runner"
                        / "inputs"
                        / "data"
                        / "raw"
                        / "item-1"
                        / "B04.tif"
                    ).resolve()
                ),
                payload["inputs"]["raw_data_files"],
            )
            self.assertIn(
                str(
                    (
                        project_dir
                        / "artifacts"
                        / "wildfire-download-runner"
                        / "inputs"
                        / "models"
                        / "model.onnx"
                    ).resolve()
                ),
                payload["inputs"]["model_files"],
            )
            self.assertTrue(
                (
                    project_dir
                    / "artifacts"
                    / "wildfire-download-runner"
                    / "inputs"
                    / "data"
                    / "raw"
                    / "item-1"
                    / "B04.tif"
                ).exists()
            )
            self.assertTrue(
                (
                    project_dir
                    / "artifacts"
                    / "wildfire-download-runner"
                    / "inputs"
                    / "models"
                    / "model.onnx"
                ).exists()
            )
            self.assertTrue(
                (
                    project_dir
                    / "artifacts"
                    / "wildfire-download-runner"
                    / "outputs"
                    / "preprocessed"
                    / "B04.tif"
                ).exists()
            )
            self.assertTrue(
                (
                    project_dir
                    / "artifacts"
                    / "wildfire-download-runner"
                    / "outputs"
                    / "predictions"
                    / "fire_mask.onnx"
                ).exists()
            )
            self.assertTrue(
                (
                    project_dir
                    / "artifacts"
                    / "wildfire-download-runner"
                    / "outputs"
                    / "evaluation"
                    / "evaluation.json"
                ).exists()
            )
            self.assertEqual(
                Path(payload["prediction"]["artifact"]).resolve(),
                (
                    project_dir
                    / "artifacts"
                    / "wildfire-download-runner"
                    / "outputs"
                    / "predictions"
                    / "fire_mask.onnx"
                ).resolve(),
            )
            self.assertEqual(
                Path(payload["reports"]["report_root"]).resolve(),
                (
                    project_dir
                    / "artifacts"
                    / "wildfire-download-runner"
                    / "reports"
                ).resolve(),
            )
            self.assertIn(
                str(
                    (
                        project_dir
                        / "artifacts"
                        / "wildfire-download-runner"
                        / "reports"
                        / "io-manifest.json"
                    ).resolve()
                ),
                payload["reports"]["files"],
            )
            self.assertIn(
                str(
                    (
                        project_dir
                        / "artifacts"
                        / "wildfire-download-runner"
                        / "reports"
                        / "last-run.json"
                    ).resolve()
                ),
                payload["reports"]["files"],
            )
            self.assertIn(
                str(
                    (
                        project_dir
                        / "artifacts"
                        / "wildfire-download-runner"
                        / "outputs"
                        / "evaluation"
                        / "evaluation.json"
                    ).resolve()
                ),
                payload["outputs"]["primary_files"],
            )
            self.assertIn(
                str(
                    (
                        project_dir
                        / "artifacts"
                        / "wildfire-download-runner"
                        / "outputs"
                        / "predictions"
                        / "fire_mask.onnx"
                    ).resolve()
                ),
                payload["outputs"]["primary_files"],
            )
            self.assertNotIn(
                str(
                    (
                        project_dir
                        / "artifacts"
                        / "wildfire-download-runner"
                        / "inputs"
                        / "data"
                        / "raw"
                        / "item-1"
                        / "B04.tif"
                    ).resolve()
                ),
                payload["outputs"]["files"],
            )
            self.assertNotIn(
                str(
                    (
                        project_dir
                        / "artifacts"
                        / "wildfire-download-runner"
                        / "inputs"
                        / "models"
                        / "model.onnx"
                    ).resolve()
                ),
                payload["outputs"]["files"],
            )

    def _build_fixture(self, root: Path) -> Path:
        asset_path = root / "assets" / "B04.tif"
        model_path = root / "hf" / "acme" / "fire-model" / "resolve" / "main" / "model.onnx"
        asset_path.parent.mkdir(parents=True, exist_ok=True)
        model_path.parent.mkdir(parents=True, exist_ok=True)
        asset_path.write_bytes(b"band-4")
        model_path.write_bytes(b"model-bytes")

        search_result = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "id": "item-1",
                    "assets": {"B04": {"href": asset_path.as_uri()}},
                }
            ],
        }
        (root / "search-results.json").write_text(json.dumps(search_result), encoding="utf-8")
        return root


if __name__ == "__main__":
    unittest.main()
