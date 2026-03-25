from __future__ import annotations

from pathlib import Path

import rasterio

from runtime.task_runtime import task
from tasks.lib.eo import resolution_value, workspace_band


@task("preprocessing", name="resample", config="10m")
def main(ctx):
    workspace_root = Path(ctx.input_dir)
    expected_resolution = resolution_value(ctx.spec["data"].get("resolution", ctx.config))
    checked: list[str] = []
    for period in ("pre_fire", "post_fire"):
        for band_name in ("nir", "swir22"):
            band_path = workspace_band(workspace_root, period, band_name)
            with rasterio.open(band_path) as src:
                xres = abs(float(src.transform.a))
                yres = abs(float(src.transform.e))
            if round(xres, 6) != round(expected_resolution, 6) or round(yres, 6) != round(expected_resolution, 6):
                raise RuntimeError(
                    f"{band_path} has resolution ({xres}, {yres}), expected {expected_resolution}"
                )
            checked.append(str(band_path))

    payload = {
        "task": ctx.name,
        "workflow": ctx.workflow_name,
        "expected_resolution": expected_resolution,
        "validated_artifacts": checked,
        "note": "Composite generation already resampled the imagery to the workflow resolution.",
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
