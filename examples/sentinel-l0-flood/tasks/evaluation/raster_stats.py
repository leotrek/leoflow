from __future__ import annotations

from runtime.task_runtime import task
from runtime.task_support import run_metric_task


@task("evaluation", name='raster_stats')
def main(ctx):
    return run_metric_task(
        metric_name=ctx.name,
        workflow_path=ctx.workflow_path,
        prediction_path=ctx.prediction_path,
        output_path=ctx.output_path,
    )


if __name__ == "__main__":
    raise SystemExit(main())
