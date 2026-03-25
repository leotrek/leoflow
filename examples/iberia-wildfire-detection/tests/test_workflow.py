from __future__ import annotations

import json
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import numpy as np
import rasterio
from rasterio.transform import from_origin

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
        self.assertTrue((ROOT / "tasks" / "lib" / "eo.py").exists())

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

    def test_offline_task_pipeline_with_synthetic_data(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            project = Path(tmpdir)
            workflow_path = self._write_synthetic_workflow(project)
            raw_dir = project / "raw"
            self._write_scene_raster(raw_dir / "pre_fire" / "scene-1" / "nir.tif", np.full((4, 4), 0.72, dtype="float32"))
            self._write_scene_raster(raw_dir / "pre_fire" / "scene-1" / "swir22.tif", np.full((4, 4), 0.18, dtype="float32"))
            self._write_scene_raster(raw_dir / "pre_fire" / "scene-2" / "nir.tif", np.full((4, 4), 0.78, dtype="float32"))
            self._write_scene_raster(raw_dir / "pre_fire" / "scene-2" / "swir22.tif", np.full((4, 4), 0.22, dtype="float32"))

            post_nir = np.full((4, 4), 0.16, dtype="float32")
            post_swir = np.full((4, 4), 0.52, dtype="float32")
            post_nir[0, 0] = 0.55
            post_swir[0, 0] = 0.18
            self._write_scene_raster(raw_dir / "post_fire" / "scene-1" / "nir.tif", post_nir)
            self._write_scene_raster(raw_dir / "post_fire" / "scene-1" / "swir22.tif", post_swir)

            workspace = project / "preprocessed" / "pre_fire_composite"
            self._run_module(
                "tasks.preprocessing.pre_fire_composite",
                workflow_path,
                "--input-dir",
                raw_dir,
                "--output-dir",
                workspace,
            )
            self._run_module(
                "tasks.preprocessing.post_fire_composite",
                workflow_path,
                "--input-dir",
                workspace,
                "--output-dir",
                project / "preprocessed" / "post_fire_composite",
            )
            self._run_module(
                "tasks.preprocessing.reproject",
                workflow_path,
                "--input-dir",
                workspace,
                "--output-dir",
                project / "preprocessed" / "reproject",
            )
            self._run_module(
                "tasks.preprocessing.resample",
                workflow_path,
                "--input-dir",
                workspace,
                "--output-dir",
                project / "preprocessed" / "resample",
            )
            self._run_module(
                "tasks.preprocessing.align_grid",
                workflow_path,
                "--input-dir",
                workspace,
                "--output-dir",
                project / "preprocessed" / "align_grid",
            )

            features_dir = project / "features"
            self._run_module(
                "tasks.features.pre_fire_nbr",
                workflow_path,
                "--input-dir",
                workspace,
                "--output-dir",
                features_dir,
            )
            self._run_module(
                "tasks.features.post_fire_nbr",
                workflow_path,
                "--input-dir",
                workspace,
                "--output-dir",
                features_dir,
            )
            delta_payload = self._run_module(
                "tasks.features.delta_nbr",
                workflow_path,
                "--input-dir",
                workspace,
                "--output-dir",
                features_dir,
            )
            self.assertTrue(Path(delta_payload["artifact"]).exists())

            model_payload = self._run_module(
                "tasks.model.burned_area_mask",
                workflow_path,
                "--input-dir",
                features_dir,
                "--output-path",
                project / "model" / "burned_area_mask.json",
            )
            prediction_path = Path(model_payload["artifact"])
            self.assertTrue(prediction_path.exists())
            self.assertGreater(model_payload["burned_area_ha"], 0)

            burned_area_payload = self._run_module(
                "tasks.evaluation.burned_area_ha",
                workflow_path,
                "--prediction-path",
                prediction_path,
                "--output-path",
                project / "evaluation" / "burned_area_ha.json",
            )
            self.assertEqual(burned_area_payload["status"], "completed")
            self.assertGreater(burned_area_payload["burned_area_ha"], 0)

            gwis_payload = self._run_module(
                "tasks.evaluation.gwis_overlay",
                workflow_path,
                "--prediction-path",
                prediction_path,
                "--output-path",
                project / "evaluation" / "gwis_overlay.json",
            )
            self.assertEqual(gwis_payload["status"], "unavailable")
            self.assertTrue(Path(gwis_payload["artifact"]).exists())

    def test_app_runs_when_workflow_is_executable(self) -> None:
        spec = yaml.safe_load((ROOT / "workflow.yaml").read_text(encoding="utf-8"))
        if not self._is_executable(spec):
            self.skipTest("network-backed bundle; run app.py manually when you want to execute the real STAC workflow")

        result = subprocess.run(
            [sys.executable, "app.py"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=True,
        )
        payload = json.loads(result.stdout)

        self.assertEqual(payload["workflow"], "iberia-wildfire-detection")
        self.assertEqual(payload["runtime"], "python-minimal")
        self.assertEqual(payload["status"], "completed")
        self.assertTrue((ROOT / "artifacts" / "iberia-wildfire-detection" / "last-run.json").exists())
        self.assertTrue(Path(payload["prediction"]["artifact"]).exists())

    def _run_module(self, module: str, workflow_path: Path, *args: object) -> dict:
        command = [sys.executable, "-m", module, "--workflow", str(workflow_path)]
        for value in args:
            command.append(str(value))
        result = subprocess.run(
            command,
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=True,
        )
        return json.loads(result.stdout)

    def _write_synthetic_workflow(self, project: Path) -> Path:
        workflow = yaml.safe_load((ROOT / "workflow.yaml").read_text(encoding="utf-8"))
        workflow["workflow"]["name"] = "synthetic-iberia-wildfire"
        workflow["data"]["region"] = "resources/aoi/feature.geojson"
        workflow["data"]["crs"] = "EPSG:4326"
        workflow["data"]["resolution"] = 0.05

        resources_dir = project / "resources" / "aoi"
        resources_dir.mkdir(parents=True, exist_ok=True)
        polygon = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {},
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [
                            [
                                [0.0, 0.0],
                                [0.2, 0.0],
                                [0.2, 0.2],
                                [0.0, 0.2],
                                [0.0, 0.0],
                            ]
                        ],
                    },
                }
            ],
        }
        (resources_dir / "feature.geojson").write_text(json.dumps(polygon), encoding="utf-8")

        workflow_path = project / "workflow.yaml"
        workflow_path.write_text(yaml.safe_dump(workflow, sort_keys=False), encoding="utf-8")
        return workflow_path

    def _write_scene_raster(self, path: Path, data: np.ndarray) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        profile = {
            "driver": "GTiff",
            "height": int(data.shape[0]),
            "width": int(data.shape[1]),
            "count": 1,
            "dtype": "float32",
            "crs": "EPSG:4326",
            "transform": from_origin(0.0, 0.2, 0.05, 0.05),
            "nodata": np.nan,
            "compress": "lzw",
        }
        with rasterio.open(path, "w", **profile) as dst:
            dst.write(data.astype("float32"), 1)

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
            and "search_results_path" in spec.get("data", {}).get("source", {})
            and (
                bool(spec.get("model", {}).get("executor"))
                or (ROOT / "tasks" / "model" / f"{self._slug(spec['model']['output'])}.py").exists()
            )
            and (
                bool(spec.get("evaluation", {}).get("executor"))
                or all(
                    (ROOT / "tasks" / "evaluation" / f"{self._slug(metric)}.py").exists()
                    for metric in spec.get("evaluation", {}).get("metrics", [])
                )
            )
        )


if __name__ == "__main__":
    unittest.main()
