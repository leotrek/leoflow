from __future__ import annotations

import numpy as np

from runtime.task_runtime import task
from tasks.lib.eo import read_raster


@task("evaluation", name="iou")
def main(ctx):
    reference_path = ctx.resource("reference-fire-mask.tif")
    if reference_path.exists():
        prediction, _ = read_raster(ctx.prediction_path)
        reference, _ = read_raster(reference_path)
        valid = np.isfinite(prediction) & np.isfinite(reference)
        pred_fire = (prediction == 1) & valid
        ref_fire = (reference == 1) & valid
        union = pred_fire | ref_fire
        score = float((pred_fire & ref_fire).sum() / union.sum()) if union.any() else None
        payload = {
            "metric": "iou",
            "workflow": ctx.workflow_name,
            "prediction_path": str(ctx.prediction_path),
            "reference_path": str(reference_path),
            "score": score,
            "status": "completed",
        }
    else:
        payload = {
            "metric": "iou",
            "workflow": ctx.workflow_name,
            "prediction_path": str(ctx.prediction_path),
            "reference_path": None,
            "score": None,
            "status": "unavailable",
            "reason": "resources/reference-fire-mask.tif is missing",
        }

    summary_path = ctx.write_json(payload)
    return ctx.result(
        artifact=str(summary_path),
        score=payload["score"],
        status=payload["status"],
    )


if __name__ == "__main__":
    raise SystemExit(main())
