from __future__ import annotations

from pathlib import Path

import numpy as np

from runtime.task_runtime import task
from tasks.lib.eo import (
    compute_nbr,
    feature_artifact,
    read_raster,
    stats,
    workspace_band,
    write_float_raster,
    write_grayscale_png,
)


@task("features", name="delta_nbr")
def main(ctx):
    feature_root = Path(ctx.output_dir)
    pre_fire_path = feature_root / "pre_fire_nbr.tif"
    post_fire_path = feature_root / "post_fire_nbr.tif"
    if not pre_fire_path.exists() or not post_fire_path.exists():
        workspace_root = Path(ctx.input_dir)
        pre_nir, profile = read_raster(workspace_band(workspace_root, "pre_fire", "nir"))
        pre_swir22, _ = read_raster(workspace_band(workspace_root, "pre_fire", "swir22"))
        post_nir, _ = read_raster(workspace_band(workspace_root, "post_fire", "nir"))
        post_swir22, _ = read_raster(workspace_band(workspace_root, "post_fire", "swir22"))
        pre_fire = compute_nbr(pre_nir, pre_swir22)
        post_fire = compute_nbr(post_nir, post_swir22)
    else:
        pre_fire, profile = read_raster(feature_artifact(feature_root, "pre_fire_nbr"))
        post_fire, _ = read_raster(feature_artifact(feature_root, "post_fire_nbr"))

    delta = (pre_fire - post_fire).astype("float32")
    delta[~np.isfinite(delta)] = np.nan

    artifact_path = ctx.artifact_path(f"{ctx.name}.tif")
    write_float_raster(artifact_path, delta, profile)
    preview_path = ctx.artifact_path("delta_nbr_preview.png")
    write_grayscale_png(preview_path, delta)
    payload = {
        "feature": ctx.name,
        "artifact": str(artifact_path),
        "preview": str(preview_path),
        "stats": stats(delta),
        "status": "completed",
    }
    return ctx.report(
        payload,
        include=["stats"],
        artifact=str(artifact_path),
        preview=str(preview_path),
        status="completed",
    )


if __name__ == "__main__":
    raise SystemExit(main())
