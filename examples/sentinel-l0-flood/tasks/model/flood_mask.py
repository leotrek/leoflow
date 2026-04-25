from __future__ import annotations

from runtime.task_runtime import task
from runtime.task_support import run_model_task


@task("model", name='flood_mask', model_type='water_detection')
def main(ctx):
    return run_model_task(
        output_name=ctx.name,
        model_type=ctx.model_type or "",
        workflow_path=ctx.workflow_path,
        input_dir=ctx.input_dir,
        output_path=ctx.output_path,
    )


if __name__ == "__main__":
    raise SystemExit(main())
