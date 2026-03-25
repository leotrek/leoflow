from __future__ import annotations

from pathlib import Path

import rasterio

from runtime.task_runtime import task
from tasks.lib.eo import workspace_band


@task("preprocessing", name="reproject", config="EPSG:32629")
def main(ctx):
    workspace_root = Path(ctx.input_dir)
    expected_crs = str(ctx.spec["data"].get("crs", ctx.config))
    checked: list[str] = []
    for period in ("pre_fire", "post_fire"):
        for band_name in ("nir", "swir22"):
            band_path = workspace_band(workspace_root, period, band_name)
            with rasterio.open(band_path) as src:
                actual_crs = src.crs.to_string() if src.crs is not None else None
            if actual_crs != expected_crs:
                raise RuntimeError(f"{band_path} is in {actual_crs}, expected {expected_crs}")
            checked.append(str(band_path))

    payload = {
        "task": ctx.name,
        "workflow": ctx.workflow_name,
        "expected_crs": expected_crs,
        "validated_artifacts": checked,
        "note": "Composite generation already reprojected the imagery onto the target workflow grid.",
        "status": "completed",
    }
    return ctx.report(
        payload,
        path_field="manifest",
        include=["validated_artifacts"],
        output_dir=str(workspace_root),
        status="completed",
    )


if __name__ == "__main__":
    raise SystemExit(main())
