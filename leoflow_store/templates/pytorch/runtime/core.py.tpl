from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Callable

try:
    import yaml
except ImportError as exc:  # pragma: no cover - exercised only in broken envs
    raise RuntimeError("PyYAML is required. Install dependencies with `pip install -r requirements.txt`.") from exc

StageHandler = Callable[[dict[str, Any]], Any]
ALLOWED_STAGES = (
    "load_data",
    "preprocess",
    "extract_features",
    "run_model",
    "evaluate",
    "build_output",
)


def load_workflow_spec(path: str | Path) -> dict[str, Any]:
    workflow_path = Path(path)
    with workflow_path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"workflow spec must be a YAML mapping: {workflow_path}")
    return data


class WorkflowApp:
    def __init__(self, spec: dict[str, Any], runtime_name: str, project_root: str | Path) -> None:
        self.spec = spec
        self.runtime_name = runtime_name
        self.project_root = Path(project_root)
        self.slug = _workflow_slug(spec)
        self.artifacts_dir = self.project_root / "artifacts" / self.slug
        self._overrides: dict[str, StageHandler] = {}

    def step(self, name: str) -> Callable[[StageHandler], StageHandler]:
        if name not in ALLOWED_STAGES:
            raise ValueError(f"unknown workflow stage: {name}")

        def decorator(func: StageHandler) -> StageHandler:
            self._overrides[name] = func
            return func

        return decorator

    def run(self) -> dict[str, Any]:
        context: dict[str, Any] = {"spec": self.spec}
        context["data"] = self._call_stage("load_data", context)
        context["preprocessing"] = self._call_stage("preprocess", context)
        context["features"] = self._call_stage("extract_features", context)
        context["prediction"] = self._call_stage("run_model", context)
        context["evaluation"] = self._call_stage("evaluate", context)
        result = self._call_stage("build_output", context)

        self.artifacts_dir.mkdir(parents=True, exist_ok=True)
        report_path = self.artifacts_dir / "last-run.json"
        report_path.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(result, indent=2))
        return result

    def _call_stage(self, name: str, context: dict[str, Any]) -> Any:
        handler = self._overrides.get(name)
        if handler:
            return handler(context)
        return getattr(self, f"default_{name}")(context)

    def default_load_data(self, context: dict[str, Any]) -> dict[str, Any]:
        return {
            "source": self.spec["data"]["source"],
            "region": self.spec["data"]["region"],
            "time_range": self.spec["data"]["time"],
            "resolution": self.spec["data"]["resolution"],
            "status": "loaded",
        }

    def default_preprocess(self, context: dict[str, Any]) -> dict[str, Any]:
        steps = []
        for step in self.spec["preprocessing"]:
            name, value = next(iter(step.items()))
            steps.append(f"{name}:{value}")
        return {
            "steps": steps,
            "status": "prepared",
        }

    def default_extract_features(self, context: dict[str, Any]) -> dict[str, Any]:
        return {
            "names": list(self.spec["features"]),
            "tensor_count": len(self.spec["features"]),
            "status": "batched",
        }

    def default_run_model(self, context: dict[str, Any]) -> dict[str, Any]:
        return {
            "engine": "pytorch",
            "type": self.spec["model"]["type"],
            "input": self.spec["model"]["input"],
            "output": self.spec["model"]["output"],
            "checkpoint": str(self.artifacts_dir / "model.pt"),
            "artifact": str(self.artifacts_dir / "predictions.pt"),
            "status": "executed",
        }

    def default_evaluate(self, context: dict[str, Any]) -> dict[str, Any]:
        return {
            "metrics": list(self.spec["evaluation"]["metrics"]),
            "checkpoint": context["prediction"]["checkpoint"],
            "status": "evaluated",
        }

    def default_build_output(self, context: dict[str, Any]) -> dict[str, Any]:
        return {
            "workflow": self.spec["workflow"]["name"],
            "runtime": self.runtime_name,
            "status": "completed",
            "data": context["data"],
            "preprocessing": context["preprocessing"],
            "features": context["features"],
            "prediction": context["prediction"],
            "evaluation": context["evaluation"],
            "artifacts": {
                "run_report": str(self.artifacts_dir / "last-run.json"),
                "checkpoint": context["prediction"]["checkpoint"],
                "predictions": context["prediction"]["artifact"],
            },
        }


def _workflow_slug(spec: dict[str, Any]) -> str:
    explicit_slug = spec.get("workflow", {}).get("slug")
    if explicit_slug:
        return explicit_slug
    return re.sub(r"[^a-z0-9]+", "-", spec["workflow"]["name"].lower()).strip("-")
