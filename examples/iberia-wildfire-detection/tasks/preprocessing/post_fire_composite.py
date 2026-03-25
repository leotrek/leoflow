from __future__ import annotations

from pathlib import Path

from runtime.task_runtime import task
from tasks.lib.eo import workspace_band


@task("preprocessing", name="post_fire_composite", config="mean")
def main(ctx):
    workspace_root = Path(ctx.input_dir)
    required = [
        workspace_band(workspace_root, "pre_fire", "nir"),
        workspace_band(workspace_root, "pre_fire", "swir22"),
        workspace_band(workspace_root, "post_fire", "nir"),
        workspace_band(workspace_root, "post_fire", "swir22"),
    ]
    payload = {
        "task": ctx.name,
        "workflow": ctx.workflow_name,
        "config": ctx.config,
        "workspace_dir": str(workspace_root),
        "validated_artifacts": [str(path) for path in required],
        "note": "Both pre-fire and post-fire composites were created by the upstream pre_fire_composite task.",
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
