from __future__ import annotations

import numpy as np

from tasks.lib.sar import copy_raster, find_raster, read_raster, stats, write_float_raster
from runtime.task_runtime import task


@task("features", name='local_incidence_angle')
def main(ctx):
    source_path = find_raster(
        ctx.input_dir,
        "local_incidence_angle",
        "incidence_angle",
        "angle",
        required=False,
    )
    if source_path is not None:
        values, _ = read_raster(source_path)
        artifact_path = ctx.artifact_path("local_incidence_angle.tif")
        copy_raster(source_path, artifact_path)
    else:
        reference_path = find_raster(ctx.input_dir, "sigma0_vv")
        reference, profile = read_raster(reference_path)
        default_angle = float(
            ctx.spec["model"].get("parameters", {}).get("water_detection", {}).get("default_incidence_angle_deg", 35.0)
        )
        values = np.full(reference.shape, default_angle, dtype="float32")
        values[~np.isfinite(reference)] = np.nan
        artifact_path = ctx.artifact_path("local_incidence_angle.tif")
        write_float_raster(artifact_path, values, profile)

    summary_path = ctx.write_json(
        {
            "feature": "local_incidence_angle",
            "artifact": str(artifact_path),
            "stats": stats(values),
        },
        filename="local_incidence_angle.json",
    )
    return ctx.result(
        artifact=str(artifact_path),
        summary=str(summary_path),
        status="computed",
    )


if __name__ == "__main__":
    raise SystemExit(main())
