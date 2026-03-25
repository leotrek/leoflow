from __future__ import annotations

from runtime.task_runtime import task


@task("evaluation", name="temporal_consistency")
def main(ctx):
    payload = {
        "metric": "temporal_consistency",
        "workflow": ctx.workflow_name,
        "prediction_path": str(ctx.prediction_path),
        "score": None,
        "status": "unavailable",
        "reason": (
            "temporal consistency needs multiple timestamps or reference history, "
            "but this runnable example uses a single scene"
        ),
    }
    summary_path = ctx.write_json(payload)
    return ctx.result(
        artifact=str(summary_path),
        score=None,
        status="unavailable",
    )


if __name__ == "__main__":
    raise SystemExit(main())
