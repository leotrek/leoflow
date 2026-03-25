from __future__ import annotations

from pathlib import Path

from runtime import WorkflowApp, load_workflow_spec

PROJECT_ROOT = Path(__file__).resolve().parent
WORKFLOW_PATH = PROJECT_ROOT / "workflow.yaml"

app = WorkflowApp(
    load_workflow_spec(WORKFLOW_PATH),
    runtime_name="{{RUNTIME_NAME}}",
    project_root=PROJECT_ROOT,
)

# Most users change:
# - workflow.yaml for configuration
# - resources/ for AOI and static inputs
# Usually leave runtime/ alone unless you are changing the template itself.

# Optional: override only the stages you care about.
# @app.step("preprocess")
# def preprocess(context: dict) -> dict:
#     return {
#         "task_graph": ["cloud_mask", "resample", "align_time", "custom-task"],
#         "status": "prepared",
#     }


if __name__ == "__main__":
    app.run()
