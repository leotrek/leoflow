from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from leoflow_store.core.parser import load_workflow
from leoflow_store.core.registry import WorkflowRegistry
from leoflow_store.core.validator import resolve_version, validate_workflow_spec

ROOT = Path(__file__).resolve().parents[1]
CLI = [sys.executable, "-m", "leoflow_store.cli"]


class CliSmokeTest(unittest.TestCase):
    def test_help_create_build_list_test_delete(self) -> None:
        help_result = self._run("help")
        self.assertIn("create", help_result.stdout)
        self.assertIn("build", help_result.stdout)
        self.assertIn("delete", help_result.stdout)
        self.assertIn("list", help_result.stdout)

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            project_dir = root / "wildfire-demo"
            build_dir = root / "build" / "wildfire-demo"
            registry_root = root / "registry"

            list_result = self._run("list", cwd=root)
            template_names = list_result.stdout.strip().splitlines()
            self.assertEqual(template_names, ["iberia-wildfire-detection", "wildfire-detection"])

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
        self.assertIn("--runtime-template", result.stdout)
        self.assertNotIn("--starter", result.stdout)

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

    def _run(self, *args: object, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [*CLI, *[str(arg) for arg in args]],
            cwd=cwd or ROOT,
            capture_output=True,
            env=_cli_env(),
            text=True,
            check=True,
        )


def _cli_env() -> dict[str, str]:
    env = os.environ.copy()
    pythonpath = env.get("PYTHONPATH")
    root = str(ROOT)
    env["PYTHONPATH"] = f"{root}{os.pathsep}{pythonpath}" if pythonpath else root
    return env


if __name__ == "__main__":
    unittest.main()
