from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import urllib.parse
import urllib.request
from datetime import date, datetime, timezone
from pathlib import Path
from time import perf_counter
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
        started_at, started_perf = self._log_start("stage", name)
        handler = self._overrides.get(name)
        try:
            if handler:
                result = handler(context)
            else:
                result = getattr(self, f"default_{name}")(context)
        except Exception:
            self._log_failure("stage", name, started_perf)
            raise

        timing = self._log_finish("stage", name, started_at, started_perf)
        if isinstance(result, dict):
            result.setdefault("started_at", timing["started_at"])
            result.setdefault("finished_at", timing["finished_at"])
            result.setdefault("duration_seconds", timing["duration_seconds"])
        return result

    def default_load_data(self, context: dict[str, Any]) -> dict[str, Any]:
        source = self.spec["data"]["source"]
        if isinstance(source, str):
            task_name = _task_slug(source)
            if self._task_exists("data", task_name):
                return self._run_task_module(
                    "data",
                    task_name,
                    {
                        "--output-dir": self.artifacts_dir / "data" / "raw",
                    },
                )

        if isinstance(source, dict):
            task_name = _task_slug(source.get("name") or source.get("kind") or "data_source")
            if self._task_exists("data", task_name):
                return self._run_task_module(
                    "data",
                    task_name,
                    {
                        "--output-dir": self.artifacts_dir / "data" / "raw",
                    },
                )
            kind = source.get("kind", "").lower()
            if kind == "stac":
                return self._download_stac_source(source)
            if kind in {"url", "http", "https"}:
                return self._download_url_source(source)

        raise RuntimeError(
            f"no load_data implementation is available for source {source!r}. "
            "Provide an executable data.source mapping or replace the generated data task."
        )

    def default_preprocess(self, context: dict[str, Any]) -> dict[str, Any]:
        executed_commands: list[dict[str, Any]] = []
        executed_tasks: list[dict[str, Any]] = []
        declared_steps: list[str] = []
        current_input = context["data"].get("data_dir") or str(self.artifacts_dir / "data" / "raw")

        for step in self.spec["preprocessing"]:
            name, value = next(iter(step.items()))
            if name == "command":
                command_result = self._execute_command(value, context)
                if command_result:
                    executed_commands.append(command_result)
                    current_input = command_result.get("output") or current_input
                continue

            task_name = _task_slug(name)
            declared_steps.append(f"{name}:{value}")
            if self._task_exists("preprocessing", task_name):
                output_dir = self.artifacts_dir / "preprocessed" / task_name
                task_result = self._run_task_module(
                    "preprocessing",
                    task_name,
                    {
                        "--input-dir": current_input,
                        "--output-dir": output_dir,
                    },
                )
                executed_tasks.append(task_result)
                current_input = task_result.get("output_dir", str(output_dir))

        return {
            "steps": declared_steps or self._preprocessing_steps(),
            "commands": executed_commands,
            "tasks": executed_tasks,
            "input_source": context["data"]["source"],
            "output_dir": current_input or str(self.artifacts_dir / "preprocessed"),
            "status": "prepared",
        }

    def default_extract_features(self, context: dict[str, Any]) -> dict[str, Any]:
        input_dir = context["preprocessing"].get("output_dir") or context["data"].get("data_dir")
        output_dir = self.artifacts_dir / "features"
        executed_tasks: list[dict[str, Any]] = []
        output_files: list[str] = []

        for feature_name in self.spec["features"]:
            task_name = _task_slug(feature_name)
            if self._task_exists("features", task_name):
                task_result = self._run_task_module(
                    "features",
                    task_name,
                    {
                        "--input-dir": input_dir,
                        "--output-dir": output_dir,
                    },
                )
                executed_tasks.append(task_result)
                artifact = task_result.get("artifact")
                if artifact:
                    output_files.append(artifact)

        return {
            "names": list(self.spec["features"]),
            "count": len(self.spec["features"]),
            "input_dir": input_dir,
            "output_dir": str(output_dir),
            "artifacts": output_files,
            "tasks": executed_tasks,
            "status": "computed",
        }

    def default_run_model(self, context: dict[str, Any]) -> dict[str, Any]:
        model_source = self.spec["model"].get("source")
        model_path = self._resolve_model_source(model_source) if isinstance(model_source, dict) else None
        executor = self.spec["model"].get("executor")
        if executor:
            command_result = self._execute_command(executor, context, {"model_path": model_path})
            artifact = (
                command_result.get("artifact")
                if command_result
                else str(self.artifacts_dir / f"{self.spec['model']['output']}.tif")
            )
            return {
                "type": self.spec["model"]["type"],
                "input": self.spec["model"]["input"],
                "output": self.spec["model"]["output"],
                "model_path": model_path,
                "artifact": artifact,
                "executor": command_result,
                "status": "executed",
            }

        task_name = _task_slug(self.spec["model"]["output"])
        if self._task_exists("model", task_name):
            output_path = self.artifacts_dir / f"{task_name}.json"
            task_result = self._run_task_module(
                "model",
                task_name,
                {
                    "--input-dir": context["features"].get("output_dir") or context["preprocessing"].get("output_dir"),
                    "--output-path": output_path,
                },
            )
            return {
                "type": self.spec["model"]["type"],
                "input": self.spec["model"]["input"],
                "output": self.spec["model"]["output"],
                "model_path": model_path,
                "artifact": task_result.get("artifact", str(output_path)),
                "task": task_result,
                "status": "executed",
            }

        raise RuntimeError(
            "no model execution is configured for this workflow. "
            "Add model.source + model.executor to workflow.yaml or replace tasks/model/*.py."
        )

    def default_evaluate(self, context: dict[str, Any]) -> dict[str, Any]:
        executor = self.spec.get("evaluation", {}).get("executor")
        if executor:
            command_result = self._execute_command(executor, context)
            return {
                "metrics": list(self.spec["evaluation"]["metrics"]),
                "prediction": context["prediction"]["artifact"],
                "executor": command_result,
                "status": "evaluated",
            }

        task_results: list[dict[str, Any]] = []
        for metric_name in self.spec["evaluation"]["metrics"]:
            task_name = _task_slug(metric_name)
            if self._task_exists("evaluation", task_name):
                output_path = self.artifacts_dir / "evaluation" / f"{task_name}.json"
                task_results.append(
                    self._run_task_module(
                        "evaluation",
                        task_name,
                        {
                            "--prediction-path": context["prediction"]["artifact"],
                            "--output-path": output_path,
                        },
                    )
                )

        if not task_results:
            raise RuntimeError(
                "no evaluation execution is configured for this workflow. "
                "Add evaluation.executor to workflow.yaml or replace tasks/evaluation/*.py."
            )

        return {
            "metrics": list(self.spec["evaluation"]["metrics"]),
            "prediction": context["prediction"]["artifact"],
            "tasks": task_results,
            "status": "evaluated",
        }

    def default_build_output(self, context: dict[str, Any]) -> dict[str, Any]:
        artifacts = {
            "run_report": str(self.artifacts_dir / "last-run.json"),
            "prediction_artifact": context["prediction"].get("artifact"),
        }
        if context["evaluation"].get("tasks"):
            artifacts["evaluation_reports"] = [
                task["artifact"]
                for task in context["evaluation"]["tasks"]
                if task.get("artifact")
            ]
        return {
            "workflow": self.spec["workflow"]["name"],
            "runtime": self.runtime_name,
            "status": "completed",
            "data": context["data"],
            "preprocessing": context["preprocessing"],
            "features": context["features"],
            "prediction": context["prediction"],
            "evaluation": context["evaluation"],
            "artifacts": artifacts,
        }

    def _preprocessing_steps(self) -> list[str]:
        steps: list[str] = []
        for step in self.spec["preprocessing"]:
            name, value = next(iter(step.items()))
            steps.append(f"{name}:{value}")
        return steps

    def _task_exists(self, group: str, task_name: str) -> bool:
        return (self.project_root / "tasks" / group / f"{task_name}.py").exists()

    def _run_task_module(self, group: str, task_name: str, args: dict[str, str | Path]) -> dict[str, Any]:
        started_at, started_perf = self._log_start("task", f"{group}/{task_name}")
        command = [
            sys.executable,
            "-m",
            f"tasks.{group}.{task_name}",
            "--workflow",
            str(self.project_root / "workflow.yaml"),
        ]
        for key, value in args.items():
            command.extend([key, str(value)])
        try:
            result = subprocess.run(
                command,
                cwd=self.project_root,
                capture_output=True,
                text=True,
                check=True,
            )
        except subprocess.CalledProcessError as exc:
            duration = self._log_failure("task", f"{group}/{task_name}", started_perf)
            stderr = (exc.stderr or "").strip()
            stdout = (exc.stdout or "").strip()
            details = stderr or stdout or f"exit code {exc.returncode}"
            raise RuntimeError(
                f"task {group}/{task_name} failed after {duration:.3f}s: {details}"
            ) from exc
        stdout = result.stdout.strip()
        payload = json.loads(stdout) if stdout else {}
        payload.update(self._log_finish("task", f"{group}/{task_name}", started_at, started_perf))
        return payload

    def _download_stac_source(self, source: dict[str, Any]) -> dict[str, Any]:
        data_dir = self.artifacts_dir / "data"
        raw_dir = data_dir / "raw"
        raw_dir.mkdir(parents=True, exist_ok=True)

        if source.get("search_results_path"):
            search_result = self._load_json_path(source["search_results_path"])
        else:
            payload: dict[str, Any] = {
                "collections": [source["collection"]],
                "limit": source.get("limit", 1),
                "datetime": _normalize_datetime_interval(source.get("datetime") or self.spec["data"]["time"]),
            }
            if source.get("query"):
                payload["query"] = source["query"]

            intersects = source.get("intersects") or self._load_region_geometry(self.spec["data"]["region"])
            if intersects:
                payload["intersects"] = intersects
            elif source.get("bbox"):
                payload["bbox"] = source["bbox"]

            search_result = self._http_json(
                source["api_url"],
                method="POST",
                payload=payload,
                headers=source.get("headers"),
            )
        (data_dir / "search-results.json").write_text(json.dumps(search_result, indent=2) + "\n", encoding="utf-8")

        downloaded_assets: list[str] = []
        asset_names = source.get("assets") or []
        for item in search_result.get("features", []):
            item_id = item.get("id", "item")
            assets = item.get("assets", {})
            item_dir = raw_dir / item_id
            item_dir.mkdir(parents=True, exist_ok=True)
            for asset_name in asset_names:
                href = assets.get(asset_name, {}).get("href")
                if not href:
                    continue
                output_path = item_dir / self._asset_filename(asset_name, href)
                self._download_file(href, output_path, headers=source.get("asset_headers"))
                downloaded_assets.append(str(output_path))

        return {
            "source": source,
            "region": self.spec["data"]["region"],
            "time_range": self.spec["data"]["time"],
            "resolution": self.spec["data"]["resolution"],
            "items": len(search_result.get("features", [])),
            "downloaded_assets": downloaded_assets,
            "data_dir": str(raw_dir),
            "status": "loaded",
        }

    def _download_url_source(self, source: dict[str, Any]) -> dict[str, Any]:
        url = source["url"]
        data_dir = self.artifacts_dir / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        output_path = data_dir / self._asset_filename(source.get("filename", "source"), url)
        self._download_file(url, output_path, headers=source.get("headers"))
        return {
            "source": source,
            "region": self.spec["data"]["region"],
            "time_range": self.spec["data"]["time"],
            "resolution": self.spec["data"]["resolution"],
            "downloaded_assets": [str(output_path)],
            "data_dir": str(data_dir),
            "status": "loaded",
        }

    def _resolve_model_source(self, source: dict[str, Any]) -> str | None:
        kind = source.get("kind", "").lower()
        model_dir = self.artifacts_dir / "models"
        model_dir.mkdir(parents=True, exist_ok=True)

        if kind == "huggingface":
            repo_id = source["repo_id"]
            revision = source.get("revision", "main")
            filename = source["filename"]
            endpoint = source.get("endpoint", "https://huggingface.co").rstrip("/")
            url = f"{endpoint}/{repo_id}/resolve/{revision}/{filename}"
            output_path = model_dir / Path(filename).name
            token_env = source.get("token_env", "HF_TOKEN")
            headers = None
            token = os.environ.get(token_env)
            if token:
                headers = {"Authorization": f"Bearer {token}"}
            self._download_file(url, output_path, headers=headers)
            return str(output_path)

        if kind in {"url", "http", "https"}:
            url = source["url"]
            output_path = model_dir / self._asset_filename(source.get("filename", "model"), url)
            self._download_file(url, output_path, headers=source.get("headers"))
            return str(output_path)

        return None

    def _execute_command(
        self,
        command_spec: dict[str, Any] | str | None,
        context: dict[str, Any],
        extra_context: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        if not command_spec:
            return None

        if isinstance(command_spec, str):
            raw_command = command_spec
            output_template = None
            artifact_template = None
        else:
            raw_command = command_spec["run"]
            output_template = command_spec.get("output")
            artifact_template = command_spec.get("artifact")

        command_context = self._command_context(context, extra_context or {})
        command = raw_command.format_map(_SafeFormatMap(command_context))
        command_label = _command_label(command)
        started_at, started_perf = self._log_start("command", command_label)
        try:
            result = subprocess.run(
                command,
                shell=True,
                cwd=self.project_root,
                capture_output=True,
                text=True,
                check=True,
            )
        except subprocess.CalledProcessError as exc:
            duration = self._log_failure("command", command_label, started_perf)
            stderr = (exc.stderr or "").strip()
            stdout = (exc.stdout or "").strip()
            details = stderr or stdout or f"exit code {exc.returncode}"
            raise RuntimeError(
                f"command failed after {duration:.3f}s: {details}"
            ) from exc
        executed = {
            "command": command,
            "stdout": result.stdout.strip(),
            "stderr": result.stderr.strip(),
            "status": "executed",
        }
        executed.update(self._log_finish("command", command_label, started_at, started_perf))
        if output_template:
            executed["output"] = output_template.format_map(_SafeFormatMap(command_context))
        if artifact_template:
            executed["artifact"] = artifact_template.format_map(_SafeFormatMap(command_context))
        return executed

    def _log_start(self, kind: str, name: str) -> tuple[str, float]:
        started_at = _utc_timestamp()
        print(f"[start] {kind} {name} at {started_at}", file=sys.stderr, flush=True)
        return started_at, perf_counter()

    def _log_finish(self, kind: str, name: str, started_at: str, started_perf: float) -> dict[str, Any]:
        finished_at = _utc_timestamp()
        duration = round(perf_counter() - started_perf, 3)
        print(
            f"[finish] {kind} {name} at {finished_at} duration={duration:.3f}s",
            file=sys.stderr,
            flush=True,
        )
        return {
            "started_at": started_at,
            "finished_at": finished_at,
            "duration_seconds": duration,
        }

    def _log_failure(self, kind: str, name: str, started_perf: float) -> float:
        duration = perf_counter() - started_perf
        print(
            f"[failed] {kind} {name} at {_utc_timestamp()} duration={duration:.3f}s",
            file=sys.stderr,
            flush=True,
        )
        return duration

    def _command_context(self, context: dict[str, Any], extra_context: dict[str, Any]) -> dict[str, str]:
        data_dir = context.get("data", {}).get("data_dir") or str(self.artifacts_dir / "data")
        preprocess_dir = context.get("preprocessing", {}).get("output_dir") or str(self.artifacts_dir / "preprocessed")
        prediction_dir = str(self.artifacts_dir / "predictions")
        values = {
            "workflow_dir": str(self.project_root),
            "artifacts_dir": str(self.artifacts_dir),
            "data_dir": data_dir,
            "preprocess_dir": preprocess_dir,
            "prediction_dir": prediction_dir,
            "model_path": extra_context.get("model_path") or "",
            "workflow_name": self.spec["workflow"]["name"],
            "workflow_slug": self.slug,
        }
        for key, value in extra_context.items():
            if value is not None:
                values[key] = str(value)
        return values

    def _load_region_geometry(self, region: str) -> dict[str, Any] | None:
        region_path = Path(region)
        if not region_path.is_absolute():
            candidate = self.project_root / region
            if candidate.exists():
                region_path = candidate
        if region_path.exists() and region_path.suffix in {".geojson", ".json"}:
            with region_path.open("r", encoding="utf-8") as handle:
                data = json.load(handle)
            if data.get("type") == "Feature":
                return data.get("geometry")
            return data
        return None

    def _http_json(
        self,
        url: str,
        method: str,
        payload: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        request_headers = {"Content-Type": "application/json", **(headers or {})}
        data = json.dumps(payload).encode("utf-8") if payload is not None else None
        request = urllib.request.Request(url, data=data, headers=request_headers, method=method)
        with urllib.request.urlopen(request) as response:
            return json.loads(response.read().decode("utf-8"))

    def _load_json_path(self, path: str) -> dict[str, Any]:
        target = Path(path)
        if target.exists():
            return json.loads(target.read_text(encoding="utf-8"))
        with urllib.request.urlopen(path) as response:
            return json.loads(response.read().decode("utf-8"))

    def _download_file(self, url: str, output_path: Path, headers: dict[str, str] | None = None) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        request = urllib.request.Request(url, headers=headers or {})
        with urllib.request.urlopen(request) as response:
            output_path.write_bytes(response.read())

    def _asset_filename(self, fallback_name: str, url: str) -> str:
        parsed = urllib.parse.urlparse(url)
        filename = Path(parsed.path).name
        return filename or fallback_name


def _workflow_slug(spec: dict[str, Any]) -> str:
    explicit_slug = spec.get("workflow", {}).get("slug")
    if explicit_slug:
        return explicit_slug
    return re.sub(r"[^a-z0-9]+", "-", spec["workflow"]["name"].lower()).strip("-")


def _task_slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _command_label(command: str, *, limit: int = 96) -> str:
    single_line = " ".join(command.split())
    if len(single_line) <= limit:
        return single_line
    return single_line[: limit - 3] + "..."


class _SafeFormatMap(dict[str, str]):
    def __missing__(self, key: str) -> str:
        return "{" + key + "}"


def _normalize_datetime_interval(value: str) -> str:
    if "/" not in value:
        return _normalize_datetime_token(value, is_end=False)
    start, end = value.split("/", 1)
    return f"{_normalize_datetime_token(start, is_end=False)}/{_normalize_datetime_token(end, is_end=True)}"


def _normalize_datetime_token(value: str, is_end: bool) -> str:
    token = value.strip()
    if token in {"", ".."}:
        return ".."
    try:
        parsed = date.fromisoformat(token)
    except ValueError:
        return token
    suffix = "T23:59:59Z" if is_end else "T00:00:00Z"
    return f"{parsed.isoformat()}{suffix}"
