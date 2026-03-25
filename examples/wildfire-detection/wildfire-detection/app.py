from __future__ import annotations

from pathlib import Path

from runtime import WorkflowApp, load_workflow_spec

PROJECT_ROOT = Path(__file__).resolve().parent
WORKFLOW_PATH = PROJECT_ROOT / "workflow.yaml"

app = WorkflowApp(
    load_workflow_spec(WORKFLOW_PATH),
    runtime_name="python-minimal",
    project_root=PROJECT_ROOT,
)

# Most users change:
# - workflow.yaml for configuration
# - tasks/ for workflow logic and helper code
# - resources/ for AOI and static inputs
# - tests/ for workflow checks
# Usually leave runtime/ alone unless you are changing the template itself.

# Optional: override only the stages you care about.
# @app.step("run_model")
# def run_model(context: dict) -> dict:
#     return {
#         "engine": "custom-segmentation",
#         "artifact": str(app.artifacts_dir / "fire-mask.tif"),
#         "status": "executed",
#     }


if __name__ == "__main__":
    app.run()
