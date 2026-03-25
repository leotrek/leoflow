from __future__ import annotations

import numpy as np
from rasterio.enums import Resampling

from runtime.task_runtime import task
from tasks.lib.eo import (
    CLOUD_CLASSES,
    find_band,
    first_scene_dir,
    read_aligned,
    reference_grid,
    stats,
    write_float_raster,
    write_rgb_png,
)


@task("preprocessing", name="cloud_mask", config="s2cloudless")
def main(ctx):
    scene_dir = first_scene_dir(ctx.input_dir)
    b04, profile = reference_grid(scene_dir, ctx.workflow_path)
    bands: dict[str, np.ndarray] = {"B04": b04}

    for band_name in ("B02", "B03", "B08"):
        bands[band_name] = read_aligned(find_band(scene_dir, band_name), profile, resampling=Resampling.bilinear)

    b12_path = find_band(scene_dir, "B12", required=False)
    if b12_path is not None:
        bands["B12"] = read_aligned(b12_path, profile, resampling=Resampling.bilinear)

    scl_path = find_band(scene_dir, "SCL", required=False)
    cloud_pixels = np.zeros_like(b04, dtype=bool)
    if scl_path is not None:
        scl = read_aligned(scl_path, profile, resampling=Resampling.nearest)
        cloud_pixels = np.isin(scl, list(CLOUD_CLASSES))
        bands["SCL"] = scl

    valid_pixels = np.isfinite(bands["B04"])
    cloud_mask = cloud_pixels | ~valid_pixels
    written_artifacts: list[str] = []
    band_stats: dict[str, dict[str, float | None] | None] = {}

    for band_name, values in bands.items():
        raster_values = values.astype("float32")
        if band_name != "SCL":
            raster_values[cloud_mask] = np.nan
        output_path = ctx.artifact_path(f"{band_name}.tif")
        write_float_raster(output_path, raster_values, profile)
        written_artifacts.append(str(output_path))
        band_stats[band_name] = stats(raster_values)

    preview_path = ctx.artifact_path("true_color.png")
    preview_red = bands["B04"].copy()
    preview_green = bands["B03"].copy()
    preview_blue = bands["B02"].copy()
    preview_red[cloud_mask] = np.nan
    preview_green[cloud_mask] = np.nan
    preview_blue[cloud_mask] = np.nan
    write_rgb_png(preview_path, preview_red, preview_green, preview_blue)
    written_artifacts.append(str(preview_path))

    manifest_path = ctx.write_json(
        {
            "task": "cloud_mask",
            "implementation": "scl-based cloud masking on a clipped AOI",
            "scene_dir": str(scene_dir),
            "masked_pixels": int(cloud_mask.sum()),
            "total_pixels": int(cloud_mask.size),
            "artifacts": written_artifacts,
            "preview": str(preview_path),
            "band_stats": band_stats,
        },
        filename="cloud_mask.json",
    )
    written_artifacts.append(str(manifest_path))
    return ctx.result(
        manifest=str(manifest_path),
        artifacts=written_artifacts,
    )


if __name__ == "__main__":
    raise SystemExit(main())
