from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
from pathlib import Path

import leoflow_store.cli as cli
from leoflow_store.core.parser import dump_workflow, load_workflow
from leoflow_store.core.registry import WorkflowRegistry
from leoflow_store.core.validator import resolve_version, validate_workflow_spec

ROOT = Path(__file__).resolve().parents[1]
CLI = [sys.executable, "-m", "leoflow_store.cli"]


class CliSmokeTest(unittest.TestCase):
    def test_help_create_build_list_test_delete(self) -> None:
        help_result = self._run("help")
        self.assertIn("create", help_result.stdout)
        self.assertIn("build", help_result.stdout)
        self.assertIn("run", help_result.stdout)
        self.assertIn("delete", help_result.stdout)
        self.assertIn("list", help_result.stdout)

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            project_dir = root / "wildfire-demo"
            build_dir = root / "build" / "wildfire-demo"
            registry_root = root / "registry"

            list_result = self._run("list", cwd=root)
            template_names = list_result.stdout.strip().splitlines()
            self.assertEqual(
                template_names,
                ["iberia-wildfire-detection", "sentinel-l0-flood", "wildfire-detection"],
            )

            create_result = self._run("create", "wildfire-demo", project_dir, "--template", "wildfire-detection", cwd=root)
            self.assertIn("created workflow project", create_result.stdout)
            self.assertTrue((project_dir / "workflow.yaml").exists())
            self.assertTrue((project_dir / "resources" / "polygon.geojson").exists())
            self.assertTrue((project_dir / "README.md").exists())
            self.assertTrue((project_dir / "tasks" / "README.md").exists())
            self.assertTrue((project_dir / "tasks" / "data" / "stac_sentinel_2.py").exists())
            self.assertTrue((project_dir / "app.py").exists())
            workflow_text = (project_dir / "workflow.yaml").read_text(encoding="utf-8")
            self.assertIn("name: wildfire-demo", workflow_text)

            build_result = self._run("build", project_dir, "--output", build_dir, cwd=root)
            self.assertIn("built wildfire-demo 0.1.0", build_result.stdout)
            self.assertTrue((build_dir / "app.py").exists())
            self.assertTrue((build_dir / "tests" / "test_workflow.py").exists())

            test_result = self._run("test", build_dir, cwd=root)
            combined_output = test_result.stdout + test_result.stderr
            self.assertIn("OK", combined_output)
            self.assertIn("tests passed", combined_output)

            spec = validate_workflow_spec(load_workflow(project_dir / "workflow.yaml"))
            version = resolve_version(spec)
            WorkflowRegistry(registry_root).publish(spec, version, "python-minimal", build_dir)

            list_result = self._run("list", "wildfire", "--registry", "--registry-root", registry_root, cwd=root)
            self.assertEqual(list_result.stdout.strip(), "wildfire-demo")

            delete_build = self._run("delete", build_dir, "--yes", cwd=root)
            self.assertIn("deleted", delete_build.stdout)
            self.assertFalse(build_dir.exists())

            delete_registry = self._run(
                "delete",
                "wildfire-demo",
                "--registry",
                "--registry-root",
                registry_root,
                "--yes",
                cwd=root,
            )
            self.assertIn("deleted", delete_registry.stdout)
            self.assertFalse((registry_root / "wildfire-demo").exists())

    def test_help_topic(self) -> None:
        result = self._run("help", "create")
        self.assertIn("Create a workflow project in the given output directory", result.stdout)
        self.assertIn("output", result.stdout)
        self.assertIn("--template", result.stdout)
        self.assertIn("--workflow", result.stdout)
        self.assertIn("--runtime-template", result.stdout)
        self.assertNotIn("--starter", result.stdout)

    def test_help_run_topic(self) -> None:
        result = self._run("help", "run")
        self.assertIn("Run a generated workflow project directly", result.stdout)
        self.assertIn("--setup", result.stdout)
        self.assertIn("--venv-dir", result.stdout)

    def test_build_generates_runtime_that_reuses_current_python_for_commands(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            workflow_dir = self._write_runnable_fixture_workflow(root / "workflow")
            build_dir = root / "direct-build"

            self._run("build", workflow_dir, "--output", build_dir, cwd=root)
            runtime_core = (build_dir / "runtime" / "core.py").read_text(encoding="utf-8")

            self.assertIn('"python_executable": shlex.quote(sys.executable)', runtime_core)
            self.assertIn('{python_executable}', runtime_core)

    def test_run_generated_project_and_generate_from_spec(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            workflow_dir = self._write_runnable_fixture_workflow(root / "workflow")
            direct_build_dir = root / "direct-build"

            build_result = self._run("build", workflow_dir, "--output", direct_build_dir, cwd=root)
            self.assertIn("built wildfire-download-runner 0.1.0", build_result.stdout)

            run_result = self._run("run", direct_build_dir, cwd=root)
            combined_direct = run_result.stdout + run_result.stderr
            self.assertIn('"status": "completed"', combined_direct)
            self.assertIn("ran ", combined_direct)
            self.assertIn("direct-build", combined_direct)
            self.assertTrue(
                (
                    direct_build_dir
                    / "artifacts"
                    / "wildfire-download-runner"
                    / "reports"
                    / "last-run.json"
                ).exists()
            )

            generated_run_result = self._run("run", workflow_dir, "--keep-build", cwd=root)
            combined_generated = generated_run_result.stdout + generated_run_result.stderr
            self.assertIn('"status": "completed"', combined_generated)
            self.assertIn("generated and ran build/wildfire-download-runner", combined_generated)
            self.assertTrue(
                (
                    root
                    / "build"
                    / "wildfire-download-runner"
                    / "artifacts"
                    / "wildfire-download-runner"
                    / "reports"
                    / "last-run.json"
                ).exists()
            )

    def test_resolve_python_for_run_prefers_existing_virtualenv(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            project_dir = Path(temp_dir)
            venv_python = cli._venv_python_path(project_dir / ".venv")
            venv_python.parent.mkdir(parents=True, exist_ok=True)
            venv_python.write_text("", encoding="utf-8")

            resolved = cli._resolve_python_for_run(project_dir, setup=False, venv_dir=".venv")

            self.assertEqual(resolved, venv_python)

    @mock.patch("leoflow_store.cli._install_requirements")
    @mock.patch("leoflow_store.cli._ensure_virtualenv")
    def test_resolve_python_for_run_setup_creates_env_and_installs_requirements(
        self,
        ensure_virtualenv: mock.Mock,
        install_requirements: mock.Mock,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            project_dir = Path(temp_dir)
            requirements_path = project_dir / "requirements.txt"
            requirements_path.write_text("PyYAML>=6.0\n", encoding="utf-8")

            resolved = cli._resolve_python_for_run(project_dir, setup=True, venv_dir=".venv")
            expected_python = cli._venv_python_path(project_dir / ".venv")

            self.assertEqual(resolved, expected_python)
            ensure_virtualenv.assert_called_once_with(project_dir / ".venv")
            install_requirements.assert_called_once_with(project_dir, requirements_path, expected_python)

    @mock.patch("leoflow_store.cli.subprocess.run")
    def test_run_workflow_requires_setup_when_current_python_misses_requirements(self, run: mock.Mock) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            project_dir = Path(temp_dir)
            (project_dir / "requirements.txt").write_text("definitely-missing-package-xyz123>=1.0\n", encoding="utf-8")

            with self.assertRaisesRegex(
                RuntimeError,
                "definitely-missing-package-xyz123.*Run `lf run .* --setup`",
            ):
                cli._run_workflow_in_dir(project_dir, setup=False, venv_dir=".venv")

            run.assert_not_called()

    def test_requirement_import_name_uses_known_aliases(self) -> None:
        self.assertEqual(cli._requirement_import_name("PyYAML"), "yaml")
        self.assertEqual(cli._requirement_import_name("opencv-python"), "cv2")
        self.assertEqual(cli._requirement_import_name("rasterio"), "rasterio")

    def test_create_requires_name_and_output(self) -> None:
        result = subprocess.run(
            [*CLI, "create"],
            cwd=ROOT,
            capture_output=True,
            env=_cli_env(),
            text=True,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("usage: lf create", result.stderr)

    def test_create_from_workflow_yaml(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source_workflow_dir = self._write_runnable_fixture_workflow(root / "source-workflow")
            project_dir = root / "created-from-workflow"

            create_result = self._run(
                "create",
                "sar-demo",
                project_dir,
                "--workflow",
                source_workflow_dir / "workflow.yaml",
                cwd=root,
            )

            self.assertIn("created workflow project", create_result.stdout)
            self.assertTrue((project_dir / "workflow.yaml").exists())
            self.assertTrue((project_dir / "resources" / "polygon.geojson").exists())
            self.assertTrue((project_dir / "app.py").exists())
            workflow_text = (project_dir / "workflow.yaml").read_text(encoding="utf-8")
            self.assertIn("name: sar-demo", workflow_text)
            self.assertIn("search_results_path:", workflow_text)

    def _run(self, *args: object, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [*CLI, *[str(arg) for arg in args]],
            cwd=cwd or ROOT,
            capture_output=True,
            env=_cli_env(),
            text=True,
            check=True,
        )

    def _write_runnable_fixture_workflow(self, root: Path) -> Path:
        fixture = self._build_fixture(root / "fixture")
        resources_dir = root / "resources"
        resources_dir.mkdir(parents=True, exist_ok=True)
        (resources_dir / "polygon.geojson").write_text(
            json.dumps(
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [
                            [
                                [-121.95, 38.40],
                                [-121.75, 38.40],
                                [-121.75, 38.55],
                                [-121.95, 38.55],
                                [-121.95, 38.40],
                            ]
                        ],
                    },
                    "properties": {"name": "cli-test-region"},
                }
            ),
            encoding="utf-8",
        )

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
                    "region": "resources/polygon.geojson",
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
        dump_workflow(spec, root / "workflow.yaml")
        return root

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


def _cli_env() -> dict[str, str]:
    env = os.environ.copy()
    pythonpath = env.get("PYTHONPATH")
    root = str(ROOT)
    env["PYTHONPATH"] = f"{root}{os.pathsep}{pythonpath}" if pythonpath else root
    return env


if __name__ == "__main__":
    unittest.main()
