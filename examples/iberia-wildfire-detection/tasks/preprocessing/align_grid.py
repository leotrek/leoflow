from __future__ import annotations

from pathlib import Path

import rasterio

from runtime.task_runtime import task
from tasks.lib.eo import workspace_band


@task("preprocessing", name="align_grid", config="post_fire")
def main(ctx):
    workspace_root = Path(ctx.input_dir)
    reference_path = workspace_band(workspace_root, "post_fire", "nir")
    with rasterio.open(reference_path) as reference:
        reference_signature = (
            reference.crs.to_string() if reference.crs is not None else None,
            reference.width,
            reference.height,
            tuple(reference.transform),
        )

    checked = [str(reference_path)]
    for period in ("pre_fire", "post_fire"):
        for band_name in ("nir", "swir22"):
            band_path = workspace_band(workspace_root, period, band_name)
            with rasterio.open(band_path) as candidate:
                signature = (
                    candidate.crs.to_string() if candidate.crs is not None else None,
                    candidate.width,
                    candidate.height,
                    tuple(candidate.transform),
                )
            if signature != reference_signature:
                raise RuntimeError(f"{band_path} does not match the aligned post_fire grid")
            checked.append(str(band_path))

    payload = {
        "task": ctx.name,
        "workflow": ctx.workflow_name,
        "reference_grid": str(reference_path),
        "validated_artifacts": checked,
        "note": "Composite generation already aligned every period and band onto the shared target grid.",
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
