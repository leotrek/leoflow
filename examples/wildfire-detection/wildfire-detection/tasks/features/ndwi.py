from __future__ import annotations

from runtime.task_runtime import task
from tasks.lib.eo import compute_index, find_band, read_raster, stats, write_float_raster


@task("features", name="ndwi")
def main(ctx):
    b03, profile = read_raster(find_band(ctx.input_dir, "B03"))
    b08, _ = read_raster(find_band(ctx.input_dir, "B08"))
    ndwi = compute_index(b03, b08)

    artifact_path = ctx.artifact_path("ndwi.tif")
    write_float_raster(artifact_path, ndwi, profile)
    summary_path = ctx.write_json(
        {
            "feature": "ndwi",
            "artifact": str(artifact_path),
            "stats": stats(ndwi),
        },
        filename="ndwi.json",
    )
    return ctx.result(
        artifact=str(artifact_path),
        summary=str(summary_path),
    )


if __name__ == "__main__":
    raise SystemExit(main())
