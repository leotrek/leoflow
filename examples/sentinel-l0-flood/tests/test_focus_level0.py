from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def _load_focus_module():
    module_path = ROOT / "tasks" / "custom" / "focus_level0.py"
    spec = importlib.util.spec_from_file_location("sentinel_focus_level0", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(ROOT))
    try:
        spec.loader.exec_module(module)
    finally:
        sys.path.pop(0)
    return module


class FocusLevel0Test(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.module = _load_focus_module()

    def test_runs_external_toolchain_when_no_post_event_rasters_exist(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            workflow_path = self._write_workflow(
                root,
                toolchain_command=(
                    f'"{sys.executable}" "{root / "emit_focus_outputs.py"}" '
                    '--output "{output_dir}" --safe "{safe_dir}" --archive "{archive_path}"'
                ),
            )
            (root / "emit_focus_outputs.py").write_text(
                "\n".join(
                    [
                        "from pathlib import Path",
                        "import argparse",
                        "parser = argparse.ArgumentParser()",
                        "parser.add_argument('--output', required=True)",
                        "parser.add_argument('--safe', required=True)",
                        "parser.add_argument('--archive', required=True)",
                        "args = parser.parse_args()",
                        "out = Path(args.output)",
                        "out.mkdir(parents=True, exist_ok=True)",
                        "(out / 'vv.tif').write_bytes(b'vv')",
                        "(out / 'vh.tif').write_bytes(b'vh')",
                        "(out / 'local_incidence_angle.tif').write_bytes(b'angle')",
                        "print(args.safe)",
                        "print(args.archive)",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            input_dir = self._prepare_raw_input(root)
            output_dir = root / "output"

            exit_code = self.module.main(
                ["--workflow", str(workflow_path), "--input", str(input_dir), "--output", str(output_dir)]
            )

            self.assertEqual(exit_code, 0)
            self.assertTrue((output_dir / "vv.tif").exists())
            self.assertTrue((output_dir / "vh.tif").exists())
            self.assertTrue((output_dir / "local_incidence_angle.tif").exists())
            manifest = json.loads((output_dir / "focus_level0.json").read_text(encoding="utf-8"))
            self.assertIn("emit_focus_outputs.py", manifest["toolchain"]["command"])
            self.assertEqual(Path(manifest["toolchain"]["cwd"]).resolve(), root.resolve())

    def test_existing_post_event_rasters_skip_external_toolchain(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            workflow_path = self._write_workflow(
                root,
                toolchain_command=f'"{sys.executable}" "{root / "should_not_run.py"}" "{root / "ran.txt"}"',
            )
            (root / "should_not_run.py").write_text(
                "\n".join(
                    [
                        "from pathlib import Path",
                        "import sys",
                        "Path(sys.argv[1]).write_text('ran\\n', encoding='utf-8')",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            input_dir = self._prepare_raw_input(root)
            (input_dir / "vv.tif").write_bytes(b"vv",)
            (input_dir / "vh.tif").write_bytes(b"vh",)
            output_dir = root / "output"

            exit_code = self.module.main(
                ["--workflow", str(workflow_path), "--input", str(input_dir), "--output", str(output_dir)]
            )

            self.assertEqual(exit_code, 0)
            self.assertTrue((output_dir / "vv.tif").exists())
            self.assertTrue((output_dir / "vh.tif").exists())
            self.assertFalse((root / "ran.txt").exists())
            manifest = json.loads((output_dir / "focus_level0.json").read_text(encoding="utf-8"))
            self.assertIsNone(manifest["toolchain"])

    def test_external_toolchain_must_create_post_event_pair(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            workflow_path = self._write_workflow(
                root,
                toolchain_command=f'"{sys.executable}" "{root / "noop_focus.py"}"',
            )
            (root / "noop_focus.py").write_text("print('ok')\n", encoding="utf-8")

            input_dir = self._prepare_raw_input(root)
            output_dir = root / "output"

            with self.assertRaisesRegex(
                RuntimeError,
                "external SAR toolchain finished, but no post-event VV/VH rasters were found",
            ):
                self.module.main(["--workflow", str(workflow_path), "--input", str(input_dir), "--output", str(output_dir)])

    def _prepare_raw_input(self, root: Path) -> Path:
        input_dir = root / "input"
        (input_dir / "downloads").mkdir(parents=True, exist_ok=True)
        (input_dir / "downloads" / "scene.zip").write_bytes(b"zip")
        (input_dir / "extracted" / "scene.SAFE").mkdir(parents=True, exist_ok=True)
        (input_dir / "overrides" / "pre_event").mkdir(parents=True, exist_ok=True)
        return input_dir

    def _write_workflow(self, root: Path, *, toolchain_command: str) -> Path:
        region_path = root / "region.geojson"
        region_path.write_text(
            json.dumps(
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [
                            [
                                [16.0, 48.0],
                                [16.1, 48.0],
                                [16.1, 48.1],
                                [16.0, 48.1],
                                [16.0, 48.0],
                            ]
                        ],
                    },
                    "properties": {},
                }
            )
            + "\n",
            encoding="utf-8",
        )

        workflow_path = root / "workflow.yaml"
        workflow_path.write_text(
            yaml.safe_dump(
                {
                    "workflow": {"name": "focus-test"},
                    "data": {
                        "region": str(region_path),
                        "acquisition": {"archive_type": "RAW"},
                        "download": {"filename": "scene.zip"},
                        "toolchain": {
                            "focus": {
                                "command": toolchain_command,
                                "cwd": "{workflow_dir}",
                            }
                        },
                    },
                },
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        return workflow_path


if __name__ == "__main__":
    unittest.main()
