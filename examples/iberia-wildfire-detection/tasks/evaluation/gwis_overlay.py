from __future__ import annotations

from pathlib import Path

import numpy as np
from rasterio.enums import Resampling

from runtime.task_runtime import task
from tasks.lib.eo import project_band, read_raster


@task("evaluation", name="gwis_overlay")
def main(ctx):
    prediction, profile = read_raster(ctx.prediction_path)
    reference_path = _reference_path(ctx)
    if reference_path is None:
        payload = {
            "metric": ctx.name,
            "workflow": ctx.workflow_name,
            "prediction_path": str(ctx.prediction_path),
            "reference_path": None,
            "status": "unavailable",
            "reason": "The EarthCODE example compares against the remote SeasFire/GWIS cube. Add a local reference raster under resources/ to enable overlay scoring.",
        }
    else:
        reference = project_band(reference_path, profile, resampling=Resampling.nearest)
        valid = np.isfinite(prediction) & np.isfinite(reference)
        predicted = (prediction == 1) & valid
        observed = (reference > 0) & valid
        intersection = int((predicted & observed).sum())
        union = int((predicted | observed).sum())
        iou = float(intersection / union) if union else None
        payload = {
            "metric": ctx.name,
            "workflow": ctx.workflow_name,
            "prediction_path": str(ctx.prediction_path),
            "reference_path": str(reference_path),
            "intersection_pixels": intersection,
            "union_pixels": union,
            "iou": iou,
            "status": "completed",
        }

    return ctx.report(
        payload,
        path_field="artifact",
        include=["intersection_pixels", "union_pixels", "iou"],
        score=payload.get("iou"),
        status=payload["status"],
    )


def _reference_path(ctx) -> Path | None:
    reference = ctx.spec.get("evaluation", {}).get("reference", {})
    candidates: list[Path] = []
    if isinstance(reference, dict) and reference.get("path"):
        candidates.append(ctx.project_root / str(reference["path"]))
    candidates.append(ctx.resource("reference-burned-area.tif"))
    candidates.append(ctx.resource("gwis-burned-area.tif"))
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


if __name__ == "__main__":
    raise SystemExit(main())
