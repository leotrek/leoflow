from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
from rasterio.enums import Resampling

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from runtime.task_support import load_workflow, write_json
from tasks.lib.sar import (
    apply_aoi_mask,
    find_raster,
    load_region_geometries,
    reproject_raster,
    target_profile,
    to_linear_backscatter,
    write_float_raster,
)


def _project_and_mask(workflow_path: str, source_path: Path, profile: dict[str, object], *, nearest: bool = False) -> np.ndarray:
    geometries = load_region_geometries(workflow_path, target_crs=profile["crs"])
    projected = reproject_raster(
        source_path,
        profile,
        resampling=Resampling.nearest if nearest else Resampling.bilinear,
    )
    return apply_aoi_mask(projected, profile, geometries)


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

    profile = target_profile(args.workflow)
    artifacts: list[str] = []

    for source_name, target_name in (
        ("vv", "sigma0_vv"),
        ("vh", "sigma0_vh"),
        ("pre_event_vv", "pre_event_sigma0_vv"),
        ("pre_event_vh", "pre_event_sigma0_vh"),
    ):
        source_path = find_raster(input_dir, source_name, required=False)
        if source_path is None:
            continue
        values = _project_and_mask(args.workflow, source_path, profile)
        linear = to_linear_backscatter(values)
        output_path = output_dir / f"{target_name}.tif"
        write_float_raster(output_path, linear, profile)
        artifacts.append(str(output_path))

    if not artifacts:
        raise RuntimeError("calibrate_and_geocode could not find VV/VH rasters in the focusing output")

    angle_path = find_raster(input_dir, "local_incidence_angle", "incidence_angle", "angle", required=False)
    if angle_path is not None:
        angle = _project_and_mask(args.workflow, angle_path, profile, nearest=True)
    else:
        default_angle = float(
            spec["model"].get("parameters", {}).get("water_detection", {}).get("default_incidence_angle_deg", 35.0)
        )
        reference = _project_and_mask(args.workflow, find_raster(input_dir, "vv"), profile)
        angle = np.full(reference.shape, default_angle, dtype="float32")
        angle[~np.isfinite(reference)] = np.nan
    angle_output = output_dir / "local_incidence_angle.tif"
    write_float_raster(angle_output, angle, profile)
    artifacts.append(str(angle_output))

    manifest_path = write_json(
        output_dir / "calibrate_and_geocode.json",
        {
            "task": "calibrate_and_geocode",
            "workflow": spec["workflow"]["name"],
            "input_dir": str(input_dir),
            "output_dir": str(output_dir),
            "artifacts": artifacts,
            "implementation": "reproject available VV/VH rasters to the AOI grid and normalize to linear sigma0",
        },
    )
    print(
        json.dumps(
            {
                "task": "calibrate_and_geocode",
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
