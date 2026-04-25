from __future__ import annotations

import numpy as np

from tasks.lib.sar import find_raster, read_raster, stats, write_float_raster
from runtime.task_runtime import task


@task("features", name='vv_vh_ratio')
def main(ctx):
    vv_path = find_raster(ctx.input_dir, "sigma0_vv")
    vh_path = find_raster(ctx.input_dir, "sigma0_vh")
    vv, profile = read_raster(vv_path)
    vh, _ = read_raster(vh_path)
    ratio = np.full(vv.shape, np.nan, dtype="float32")
    valid = np.isfinite(vv) & np.isfinite(vh)
    ratio[valid] = vv[valid] / np.maximum(vh[valid], 1.0e-6)

    artifact_path = ctx.artifact_path("vv_vh_ratio.tif")
    write_float_raster(artifact_path, ratio, profile)
    summary_path = ctx.write_json(
        {
            "feature": "vv_vh_ratio",
            "artifact": str(artifact_path),
            "stats": stats(ratio),
        },
        filename="vv_vh_ratio.json",
    )
    return ctx.result(
        artifact=str(artifact_path),
        summary=str(summary_path),
        status="computed",
    )


if __name__ == "__main__":
    raise SystemExit(main())
