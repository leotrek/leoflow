from __future__ import annotations

from runtime.task_runtime import task
from runtime.task_support import run_source_task


@task("data", name="stac_sentinel_2")
def main(ctx):
    return run_source_task(
        source=ctx.spec["data"]["source"],
        workflow_path=ctx.workflow_path,
        output_dir=ctx.output_dir,
        default_assets=[],
    )


if __name__ == "__main__":
    raise SystemExit(main())
