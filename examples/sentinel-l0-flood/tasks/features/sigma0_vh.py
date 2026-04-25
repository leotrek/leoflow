from __future__ import annotations

from tasks.lib.sar import copy_raster, find_raster, read_raster, stats
from runtime.task_runtime import task


@task("features", name='sigma0_vh')
def main(ctx):
    source_path = find_raster(ctx.input_dir, "sigma0_vh")
    values, _ = read_raster(source_path)
    artifact_path = ctx.artifact_path("sigma0_vh.tif")
    copy_raster(source_path, artifact_path)

    pre_event_path = find_raster(ctx.input_dir, "pre_event_sigma0_vh", required=False)
    pre_event_artifact = None
    if pre_event_path is not None:
        pre_event_artifact = ctx.artifact_path("pre_event_sigma0_vh.tif")
        copy_raster(pre_event_path, pre_event_artifact)

    summary_path = ctx.write_json(
        {
            "feature": "sigma0_vh",
            "artifact": str(artifact_path),
            "pre_event_artifact": str(pre_event_artifact) if pre_event_artifact is not None else None,
            "stats": stats(values),
        },
        filename="sigma0_vh.json",
    )
    return ctx.result(
        artifact=str(artifact_path),
        pre_event_artifact=str(pre_event_artifact) if pre_event_artifact is not None else None,
        summary=str(summary_path),
        status="computed",
    )


if __name__ == "__main__":
    raise SystemExit(main())
