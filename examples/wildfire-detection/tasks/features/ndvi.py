from __future__ import annotations

from runtime.task_runtime import task
from tasks.lib.eo import compute_index, find_band, read_raster, stats, write_float_raster


@task("features", name="ndvi")
def main(ctx):
    b08, profile = read_raster(find_band(ctx.input_dir, "B08"))
    b04, _ = read_raster(find_band(ctx.input_dir, "B04"))
    ndvi = compute_index(b08, b04)

    artifact_path = ctx.artifact_path("ndvi.tif")
    write_float_raster(artifact_path, ndvi, profile)
    summary_path = ctx.write_json(
        {
            "feature": "ndvi",
            "artifact": str(artifact_path),
            "stats": stats(ndvi),
        },
        filename="ndvi.json",
    )
    return ctx.result(
        artifact=str(artifact_path),
        summary=str(summary_path),
    )


if __name__ == "__main__":
    raise SystemExit(main())
