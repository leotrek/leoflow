from __future__ import annotations

import numpy as np

from runtime.task_runtime import task
from tasks.lib.eo import (
    feature_artifact,
    pixel_area_hectares,
    read_raster,
    stats,
    write_mask_overlay_png,
    write_uint8_raster,
)


@task("model", name="burned_area_mask", model_type="threshold")
def main(ctx):
    delta_nbr, profile = read_raster(feature_artifact(ctx.input_dir, "delta_nbr"))
    threshold = float(ctx.spec["model"].get("parameters", {}).get("threshold", 0.27))
    valid = np.isfinite(delta_nbr)
    mask = np.full(delta_nbr.shape, 255, dtype="uint8")
    mask[valid] = 0
    mask[(delta_nbr > threshold) & valid] = 1

    artifact_path = ctx.artifact_path(f"{ctx.name}.tif")
    preview_path = ctx.artifact_path(f"{ctx.name}_preview.png")
    write_uint8_raster(artifact_path, mask, profile, nodata=255)
    write_mask_overlay_png(preview_path, delta_nbr, mask)

    burned_pixels = int((mask == 1).sum())
    area_ha = burned_pixels * pixel_area_hectares(profile)
    payload = {
        "model": ctx.model_type,
        "artifact": str(artifact_path),
        "preview": str(preview_path),
        "threshold": threshold,
        "delta_nbr_stats": stats(delta_nbr),
        "burned_pixels": burned_pixels,
        "burned_area_ha": area_ha,
        "status": "executed",
    }
    return ctx.report(
        payload,
        include=["threshold", "burned_pixels", "burned_area_ha"],
        artifact=str(artifact_path),
        preview=str(preview_path),
        status="executed",
    )


if __name__ == "__main__":
    raise SystemExit(main())
