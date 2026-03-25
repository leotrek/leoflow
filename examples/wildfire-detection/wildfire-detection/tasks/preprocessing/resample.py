from __future__ import annotations

from runtime.task_runtime import task
from runtime.task_support import copy_tree
from tasks.lib.eo import find_band, read_raster


@task("preprocessing", name="resample", config="10m")
def main(ctx):
    copied = copy_tree(ctx.input_dir, ctx.output_dir)

    b04_path = find_band(ctx.output_dir, "B04", required=False)
    pixel_size = None
    if b04_path is not None:
        _, profile = read_raster(b04_path)
        transform = profile["transform"]
        pixel_size = [float(abs(transform.a)), float(abs(transform.e))]

    manifest_path = ctx.write_json(
        {
            "task": "resample",
            "target_resolution": "10m",
            "input_dir": str(ctx.input_dir),
            "output_dir": str(ctx.output_dir),
            "copied_files": copied,
            "pixel_size": pixel_size,
        },
        filename="resample.json",
    )
    return ctx.result(
        manifest=str(manifest_path),
        artifacts=[str(manifest_path), *copied],
    )


if __name__ == "__main__":
    raise SystemExit(main())
