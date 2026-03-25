from __future__ import annotations

import numpy as np

from runtime.task_runtime import task
from tasks.lib.eo import (
    compute_index,
    find_band,
    latest_preprocess_dir,
    read_raster,
    stats,
    write_mask_overlay_png,
    write_uint8_raster,
)


@task("model", name="fire_mask", model_type="segmentation")
def main(ctx):
    ndvi, profile = read_raster(find_band(ctx.input_dir, "ndvi"))
    ndwi, _ = read_raster(find_band(ctx.input_dir, "ndwi"))
    preprocess_dir = latest_preprocess_dir(ctx.workflow_path, ctx.output_path.parent)
    b02_path = find_band(preprocess_dir, "B02", required=False)
    b03_path = find_band(preprocess_dir, "B03", required=False)
    b04_path = find_band(preprocess_dir, "B04", required=False)
    b08_path = find_band(preprocess_dir, "B08", required=False)
    b12_path = find_band(preprocess_dir, "B12", required=False)

    nbr = None
    if b08_path is not None and b12_path is not None:
        b08, _ = read_raster(b08_path)
        b12, _ = read_raster(b12_path)
        nbr = compute_index(b08, b12)

    valid = np.isfinite(ndvi) & np.isfinite(ndwi)
    fire_like = valid & (ndvi < 0.35) & (ndwi < 0.10)
    if nbr is not None:
        fire_like &= np.isfinite(nbr) & (nbr < 0.10)

    mask = np.full(ndvi.shape, 255, dtype="uint8")
    mask[valid] = 0
    mask[fire_like] = 1

    artifact_path = ctx.artifact_path(f"{ctx.name}.tif")
    write_uint8_raster(artifact_path, mask, profile, nodata=255)
    preview_path = None
    if b02_path is not None and b03_path is not None and b04_path is not None:
        b02, _ = read_raster(b02_path)
        b03, _ = read_raster(b03_path)
        b04, _ = read_raster(b04_path)
        preview_path = ctx.artifact_path("fire_mask_preview.png")
        write_mask_overlay_png(preview_path, b04, b03, b02, mask)
    summary_path = ctx.write_json(
        {
            "model": "heuristic burn-scar proxy",
            "artifact": str(artifact_path),
            "preview": str(preview_path) if preview_path is not None else None,
            "ndvi_stats": stats(ndvi),
            "ndwi_stats": stats(ndwi),
            "nbr_stats": stats(nbr),
            "detected_pixels": int((mask == 1).sum()),
            "valid_pixels": int(valid.sum()),
        },
    )
    return ctx.result(
        artifact=str(artifact_path),
        preview=str(preview_path) if preview_path is not None else None,
        summary=str(summary_path),
        feature_artifacts=[str(find_band(ctx.input_dir, "ndvi")), str(find_band(ctx.input_dir, "ndwi"))],
        status="executed",
    )


if __name__ == "__main__":
    raise SystemExit(main())
