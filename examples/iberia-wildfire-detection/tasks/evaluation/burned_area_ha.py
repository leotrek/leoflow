from __future__ import annotations

import numpy as np

from runtime.task_runtime import task
from tasks.lib.eo import pixel_area_hectares, read_raster


@task("evaluation", name="burned_area_ha")
def main(ctx):
    prediction, profile = read_raster(ctx.prediction_path)
    burned_pixels = int(((prediction == 1) & np.isfinite(prediction)).sum())
    area_ha = burned_pixels * pixel_area_hectares(profile)
    payload = {
        "metric": ctx.name,
        "workflow": ctx.workflow_name,
        "prediction_path": str(ctx.prediction_path),
        "burned_pixels": burned_pixels,
        "burned_area_ha": area_ha,
        "status": "completed",
    }
    return ctx.report(
        payload,
        path_field="artifact",
        include=["burned_pixels", "burned_area_ha"],
        status="completed",
    )


if __name__ == "__main__":
    raise SystemExit(main())
