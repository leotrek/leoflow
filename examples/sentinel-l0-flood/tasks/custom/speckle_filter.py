from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from runtime.task_support import load_workflow, write_json
from tasks.lib.sar import copy_raster, find_raster, mean_filter, read_raster, write_float_raster


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workflow", required=True)
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    spec = load_workflow(args.workflow)
    input_dir = Path(args.input)
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    artifacts: list[str] = []
    for name in ("sigma0_vv", "sigma0_vh", "pre_event_sigma0_vv", "pre_event_sigma0_vh"):
        source_path = find_raster(input_dir, name, required=False)
        if source_path is None:
            continue
        values, profile = read_raster(source_path)
        filtered = mean_filter(values, kernel_size=3)
        output_path = output_dir / f"{name}.tif"
        write_float_raster(output_path, filtered, profile)
        artifacts.append(str(output_path))

    if not artifacts:
        raise RuntimeError("speckle_filter could not find any sigma0 rasters to smooth")

    angle_path = find_raster(input_dir, "local_incidence_angle", required=False)
    if angle_path is not None:
        artifacts.append(str(copy_raster(angle_path, output_dir / "local_incidence_angle.tif")))

    manifest_path = write_json(
        output_dir / "speckle_filter.json",
        {
            "task": "speckle_filter",
            "workflow": spec["workflow"]["name"],
            "input_dir": str(input_dir),
            "output_dir": str(output_dir),
            "artifacts": artifacts,
            "implementation": "3x3 mean filter on sigma0 rasters",
        },
    )
    print(
        json.dumps(
            {
                "task": "speckle_filter",
                "input_dir": str(input_dir),
                "output_dir": str(output_dir),
                "manifest": str(manifest_path),
                "artifacts": artifacts + [str(manifest_path)],
                "status": "completed",
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
