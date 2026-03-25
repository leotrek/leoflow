from __future__ import annotations

from rasterio.enums import Resampling

from runtime.task_runtime import task
from tasks.lib.eo import (
    band_paths_for_period,
    load_region_geometries,
    mean_composite,
    stats,
    target_profile,
    write_float_raster,
)


@task("preprocessing", name="pre_fire_composite", config="mean")
def main(ctx):
    profile = target_profile(ctx.workflow_path)
    geometries = load_region_geometries(ctx.workflow_path, target_crs=profile["crs"])
    periods = ("pre_fire", "post_fire")
    bands = ("nir", "swir22")
    outputs: list[str] = []
    summary: dict[str, dict[str, object]] = {}

    for period in periods:
        summary[period] = {}
        for band_name in bands:
            source_paths = band_paths_for_period(ctx.input_dir, period, band_name)
            composite = mean_composite(
                source_paths,
                profile,
                geometries,
                resampling=Resampling.bilinear,
            )
            output_path = ctx.artifact_path(f"{period}/{band_name}.tif")
            write_float_raster(output_path, composite, profile)
            outputs.append(str(output_path))
            summary[period][band_name] = {
                "scene_count": len(source_paths),
                "artifact": str(output_path),
                "stats": stats(composite),
            }

    payload = {
        "task": ctx.name,
        "workflow": ctx.workflow_name,
        "config": ctx.config,
        "target_crs": str(profile["crs"]),
        "resolution": ctx.spec["data"]["resolution"],
        "periods": summary,
        "artifacts": outputs,
        "status": "completed",
    }
    return ctx.report(
        payload,
        path_field="manifest",
        include=["periods", "artifacts"],
        output_dir=str(ctx.output_dir),
        status="completed",
    )


if __name__ == "__main__":
    raise SystemExit(main())
