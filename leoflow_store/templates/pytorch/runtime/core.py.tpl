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
        self.inputs_dir = self.artifacts_dir / "inputs"
        self.outputs_dir = self.artifacts_dir / "outputs"
        self.reports_dir = self.artifacts_dir / "reports"
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

        self.reports_dir.mkdir(parents=True, exist_ok=True)
        report_path = self.reports_dir / "last-run.json"
        io_manifest_path = self.reports_dir / "io-manifest.json"
        if isinstance(result, dict):
            result = self._finalize_result(result, report_path, io_manifest_path)
            io_manifest = self._build_io_manifest(result, io_manifest_path)
            io_manifest_path.write_text(json.dumps(io_manifest, indent=2) + "\n", encoding="utf-8")
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
            "checkpoint": str(self.outputs_dir / "model.pt"),
            "artifact": str(self.outputs_dir / "predictions.pt"),
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
                "artifact_root": str(self.artifacts_dir),
                "inputs_root": str(self.inputs_dir),
                "output_root": str(self.outputs_dir),
                "reports_root": str(self.reports_dir),
                "run_report": str(self.reports_dir / "last-run.json"),
                "checkpoint": context["prediction"]["checkpoint"],
                "predictions": context["prediction"]["artifact"],
            },
        }

    def _finalize_result(
        self,
        result: dict[str, Any],
        report_path: Path,
        io_manifest_path: Path,
    ) -> dict[str, Any]:
        finalized = dict(result)
        inputs = self._inputs_summary()
        outputs = self._outputs_summary()
        reports = self._reports_summary(report_path, io_manifest_path)
        artifacts = finalized.get("artifacts")
        if isinstance(artifacts, dict):
            artifacts.setdefault("artifact_root", str(self.artifacts_dir))
            artifacts.setdefault("inputs_root", str(self.inputs_dir))
            artifacts.setdefault("output_root", str(self.outputs_dir))
            artifacts.setdefault("reports_root", str(self.reports_dir))
            artifacts.setdefault("run_report", str(report_path))
            artifacts["io_manifest"] = str(io_manifest_path)
        else:
            finalized["artifacts"] = {
                "artifact_root": str(self.artifacts_dir),
                "inputs_root": str(self.inputs_dir),
                "output_root": str(self.outputs_dir),
                "reports_root": str(self.reports_dir),
                "run_report": str(report_path),
                "io_manifest": str(io_manifest_path),
            }
        finalized["inputs"] = inputs
        finalized["outputs"] = outputs
        finalized["reports"] = reports
        finalized["workflow_inputs"] = inputs
        finalized["output_files"] = outputs["files"]
        return finalized

    def _build_io_manifest(self, result: dict[str, Any], io_manifest_path: Path) -> dict[str, Any]:
        return {
            "workflow": self.spec["workflow"]["name"],
            "runtime": self.runtime_name,
            "manifest_path": str(io_manifest_path),
            "inputs": result["inputs"],
            "outputs": result["outputs"],
            "reports": result["reports"],
        }

    def _inputs_summary(self) -> dict[str, Any]:
        local_files = self._input_files()
        return {
            "input_root": str(self.inputs_dir.resolve()),
            "workflow_yaml": str((self.project_root / "workflow.yaml").resolve()),
            "data": {
                "source": self.spec["data"]["source"],
                "region": self.spec["data"]["region"],
                "time_range": self.spec["data"]["time"],
                "resolution": self.spec["data"]["resolution"],
            },
            "local_files": local_files,
            "raw_data_files": [],
            "model_files": [],
            "all_files": local_files,
        }

    def _input_files(self) -> list[str]:
        files = {str((self.project_root / "workflow.yaml").resolve())}
        stack: list[Any] = [self.spec]
        while stack:
            value = stack.pop()
            if isinstance(value, dict):
                stack.extend(value.values())
                continue
            if isinstance(value, list):
                stack.extend(value)
                continue
            if isinstance(value, str):
                resolved = self._resolve_local_input_file(value)
                if resolved:
                    files.add(str(resolved.resolve()))
        return sorted(files)

    def _resolve_local_input_file(self, value: str) -> Path | None:
        if not value or "://" in value or "{" in value or "}" in value or "\n" in value:
            return None
        candidate = Path(value).expanduser()
        if candidate.is_absolute():
            return candidate if candidate.exists() and candidate.is_file() else None
        resolved = self.project_root / candidate
        return resolved if resolved.exists() and resolved.is_file() else None

    def _outputs_summary(self) -> dict[str, Any]:
        files = {str(path.resolve()) for path in self.outputs_dir.rglob("*") if path.is_file()}
        primary_files = {
            *self._collect_existing_files(self.spec.get("prediction")),
        }
        return {
            "output_root": str(self.outputs_dir.resolve()),
            "primary_files": sorted(primary_files),
            "files": sorted(files),
        }

    def _reports_summary(self, report_path: Path, io_manifest_path: Path) -> dict[str, Any]:
        return {
            "report_root": str(self.reports_dir.resolve()),
            "files": sorted([str(report_path.resolve()), str(io_manifest_path.resolve())]),
        }

    def _collect_existing_files(self, value: Any) -> list[str]:
        files: set[str] = set()
        stack: list[Any] = [value]
        while stack:
            current = stack.pop()
            if isinstance(current, dict):
                stack.extend(current.values())
                continue
            if isinstance(current, (list, tuple, set)):
                stack.extend(current)
                continue
            if isinstance(current, Path):
                path = current
            elif isinstance(current, str):
                path = Path(current)
            else:
                continue
            if path.exists() and path.is_file():
                files.add(str(path.resolve()))
        return sorted(files)


def _workflow_slug(spec: dict[str, Any]) -> str:
    explicit_slug = spec.get("workflow", {}).get("slug")
    if explicit_slug:
        return explicit_slug
    return re.sub(r"[^a-z0-9]+", "-", spec["workflow"]["name"].lower()).strip("-")
