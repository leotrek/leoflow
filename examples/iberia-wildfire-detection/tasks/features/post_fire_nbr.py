from __future__ import annotations

from pathlib import Path

from runtime.task_runtime import task
from tasks.lib.eo import compute_nbr, read_raster, stats, workspace_band, write_float_raster


@task("features", name="post_fire_nbr")
def main(ctx):
    workspace_root = Path(ctx.input_dir)
    nir, profile = read_raster(workspace_band(workspace_root, "post_fire", "nir"))
    swir22, _ = read_raster(workspace_band(workspace_root, "post_fire", "swir22"))
    index = compute_nbr(nir, swir22)
    artifact_path = ctx.artifact_path(f"{ctx.name}.tif")
    write_float_raster(artifact_path, index, profile)
    payload = {
        "feature": ctx.name,
        "period": "post_fire",
        "artifact": str(artifact_path),
        "stats": stats(index),
        "status": "completed",
    }
    return ctx.report(
        payload,
        include=["stats"],
        artifact=str(artifact_path),
        status="completed",
    )


if __name__ == "__main__":
    raise SystemExit(main())
