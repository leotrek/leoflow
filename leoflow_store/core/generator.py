from __future__ import annotations

import copy
import json
import re
import textwrap
from pathlib import Path
from typing import Any

from leoflow_store.core.parser import dump_workflow

DEFAULT_TEMPLATE = "python-minimal"


def template_names() -> list[str]:
    root = _templates_root()
    return sorted(path.name for path in root.iterdir() if path.is_dir())


def generate_project(
    spec: dict[str, Any],
    version: str,
    template_name: str,
    output_dir: str | Path,
    workflow_path: str | Path | None = None,
) -> Path:
    template_dir = _templates_root() / template_name
    if not template_dir.exists():
        raise ValueError(f"unknown template: {template_name}")

    destination = Path(output_dir)
    _ensure_empty_destination(destination)
    context = _build_context(spec, version, template_name)

    for source_path in sorted(template_dir.rglob("*")):
        if source_path.is_dir():
            continue
        if "__pycache__" in source_path.parts or source_path.suffix == ".pyc":
            continue
        relative_path = source_path.relative_to(template_dir)
        output_path = destination / _strip_template_suffix(relative_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        content = source_path.read_text(encoding="utf-8")
        output_path.write_text(_render_template(content, context), encoding="utf-8")

    dump_workflow(_generated_workflow_spec(spec, version), destination / "workflow.yaml")
    _write_region_fixture(spec, destination, workflow_path=workflow_path)
    _write_resources_notes(destination)
    if template_name == "python-minimal":
        _write_generated_tasks(spec, destination)
    return destination


def _templates_root() -> Path:
    return Path(__file__).resolve().parents[1] / "templates"


def _strip_template_suffix(path: Path) -> Path:
    return path.with_suffix("") if path.suffix == ".tpl" else path


def _render_template(content: str, context: dict[str, str]) -> str:
    rendered = content
    for key, value in context.items():
        rendered = rendered.replace(f"{{{{{key}}}}}", value)
    return rendered


def _ensure_empty_destination(path: Path) -> None:
    if path.exists():
        if any(path.iterdir()):
            raise FileExistsError(f"output directory must be empty: {path}")
        return
    path.mkdir(parents=True, exist_ok=True)


def _build_context(spec: dict[str, Any], version: str, template_name: str) -> dict[str, str]:
    preprocessing_steps = []
    for step in spec["preprocessing"]:
        name, value = next(iter(step.items()))
        preprocessing_steps.append(f"{name}: {value}")

    return {
        "WORKFLOW_NAME": spec["workflow"]["name"],
        "WORKFLOW_SLUG": spec["workflow"]["slug"],
        "WORKFLOW_VERSION": version,
        "DATA_SOURCE": _stringify_source(spec["data"]["source"]),
        "REGION": _generated_region_path(spec["data"]["region"]),
        "TIME_RANGE": spec["data"]["time"],
        "RESOLUTION": spec["data"]["resolution"],
        "FEATURES_CSV": ", ".join(spec["features"]),
        "FEATURES_JSON": json.dumps(spec["features"], indent=2),
        "PREPROCESSING_BULLETS": "\n".join(f"- {item}" for item in preprocessing_steps),
        "PREPROCESSING_JSON": json.dumps(preprocessing_steps, indent=2),
        "MODEL_TYPE": spec["model"]["type"],
        "MODEL_INPUT": spec["model"]["input"],
        "MODEL_OUTPUT": spec["model"]["output"],
        "METRICS_CSV": ", ".join(spec["evaluation"]["metrics"]),
        "METRICS_JSON": json.dumps(spec["evaluation"]["metrics"], indent=2),
        "RUNTIME_NAME": template_name,
    }


def _generated_workflow_spec(spec: dict[str, Any], version: str) -> dict[str, Any]:
    generated = copy.deepcopy(spec)
    generated["workflow"]["version"] = version
    generated["workflow"].pop("slug", None)
    generated["data"]["region"] = _generated_region_path(generated["data"]["region"])
    return generated


def _stringify_source(source: Any) -> str:
    if isinstance(source, str):
        return source
    if isinstance(source, dict):
        kind = str(source.get("kind", "")).lower()
        if kind == "stac":
            name = source.get("name")
            collection = source.get("collection")
            api_url = source.get("api_url")
            label = str(name or collection or "stac")
            if api_url:
                return f"stac:{label} @ {api_url}"
            return f"stac:{label}"
        if kind:
            return f"{kind}:{json.dumps(source, sort_keys=True)}"
        return json.dumps(source, sort_keys=True)
    return str(source)


def _write_region_fixture(
    spec: dict[str, Any],
    destination: Path,
    *,
    workflow_path: str | Path | None = None,
) -> None:
    region = spec["data"]["region"]
    if not isinstance(region, str):
        return
    region_path = Path(region)
    if region_path.is_absolute():
        return
    if region_path.suffix not in {".geojson", ".json"}:
        return

    target = destination / _generated_region_path(region)
    if target.exists():
        return

    target.parent.mkdir(parents=True, exist_ok=True)
    if workflow_path:
        source_root = Path(workflow_path).resolve().parent
        source_region = source_root / region_path
        if source_region.exists():
            target.write_text(source_region.read_text(encoding="utf-8"), encoding="utf-8")
            return

    target.write_text(
        textwrap.dedent(
            """\
            {
              "type": "Feature",
              "geometry": {
                "type": "Polygon",
                "coordinates": [
                  [
                    [-121.95, 38.40],
                    [-121.75, 38.40],
                    [-121.75, 38.55],
                    [-121.95, 38.55],
                    [-121.95, 38.40]
                  ]
                ]
              },
              "properties": {
                "name": "generated-default-region"
              }
            }
            """
        ),
        encoding="utf-8",
    )


def _write_resources_notes(destination: Path) -> None:
    resources_dir = destination / "resources"
    resources_dir.mkdir(parents=True, exist_ok=True)
    notes_path = resources_dir / "README.md"
    if notes_path.exists():
        return
    notes_path.write_text(
        textwrap.dedent(
            """\
            # Resources

            Put static workflow inputs here.

            Typical examples:

            - `polygon.geojson` or other AOI files
            - label maps
            - lookup tables
            - local fixture data used by tests

            If `workflow.yaml` references a relative `.geojson` file, the generator places it under `resources/`.
            """
        ),
        encoding="utf-8",
    )


def _write_generated_tasks(spec: dict[str, Any], destination: Path) -> None:
    task_root = destination / "tasks"
    runtime_root = destination / "runtime"
    for relative_path, content in _task_bootstrap_files().items():
        output_path = task_root / relative_path
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(content, encoding="utf-8")
    for relative_path, content in _runtime_support_files().items():
        output_path = runtime_root / relative_path
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(content, encoding="utf-8")

    data_task_name = _source_task_name(spec["data"]["source"])
    _write_task_file(task_root / "data" / f"{data_task_name}.py", _build_data_task_script(spec))

    for step in spec["preprocessing"]:
        name, value = next(iter(step.items()))
        if name == "command":
            continue
        _write_task_file(
            task_root / "preprocessing" / f"{_task_slug(name)}.py",
            _build_preprocess_task_script(name, value),
        )

    for feature_name in spec["features"]:
        _write_task_file(
            task_root / "features" / f"{_task_slug(feature_name)}.py",
            _build_feature_task_script(feature_name),
        )

    _write_task_file(
        task_root / "model" / f"{_task_slug(spec['model']['output'])}.py",
        _build_model_task_script(spec["model"]["output"], spec["model"]["type"]),
    )

    for metric_name in spec["evaluation"]["metrics"]:
        _write_task_file(
            task_root / "evaluation" / f"{_task_slug(metric_name)}.py",
            _build_metric_task_script(metric_name),
        )


def _write_task_file(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _task_bootstrap_files() -> dict[Path, str]:
    return {
        Path("__init__.py"): "",
        Path("README.md"): _tasks_readme(),
        Path("data/__init__.py"): "",
        Path("preprocessing/__init__.py"): "",
        Path("features/__init__.py"): "",
        Path("model/__init__.py"): "",
        Path("evaluation/__init__.py"): "",
        Path("lib/__init__.py"): "",
    }


def _runtime_support_files() -> dict[Path, str]:
    return {
        Path("task_support.py"): _common_task_module(),
        Path("task_runtime.py"): _task_runtime_module(),
    }


def _tasks_readme() -> str:
    return textwrap.dedent(
        """\
        # Tasks

        Edit files in this directory to change workflow behavior.

        Typical edits:

        - change task logic in `data/`, `preprocessing/`, `features/`, `model/`, and `evaluation/`
        - add reusable workflow helper code under `lib/`
        - add your own extra scripts anywhere under `tasks/`

        These files are intended to be user-editable.
        """
    )


def _build_data_task_script(spec: dict[str, Any]) -> str:
    return textwrap.dedent(
        f"""\
        from __future__ import annotations

        from runtime.task_runtime import task
        from runtime.task_support import run_source_task


        @task("data", name={_source_task_name(spec["data"]["source"])!r})
        def main(ctx):
            return run_source_task(
                source=ctx.spec["data"]["source"],
                workflow_path=ctx.workflow_path,
                output_dir=ctx.output_dir,
                default_assets=[],
            )


        if __name__ == "__main__":
            raise SystemExit(main())
        """
    )


def _build_preprocess_task_script(name: str, config: Any) -> str:
    return textwrap.dedent(
        f"""\
        from __future__ import annotations

        from runtime.task_runtime import task
        from runtime.task_support import run_preprocess_task


        @task("preprocessing", name={name!r}, config={config!r})
        def main(ctx):
            return run_preprocess_task(
                task_name=ctx.name,
                config=ctx.config,
                workflow_path=ctx.workflow_path,
                input_dir=ctx.input_dir,
                output_dir=ctx.output_dir,
            )


        if __name__ == "__main__":
            raise SystemExit(main())
        """
    )


def _build_feature_task_script(feature_name: str) -> str:
    return textwrap.dedent(
        f"""\
        from __future__ import annotations

        from runtime.task_runtime import task
        from runtime.task_support import run_feature_task


        @task("features", name={feature_name!r})
        def main(ctx):
            return run_feature_task(
                feature_name=ctx.name,
                workflow_path=ctx.workflow_path,
                input_dir=ctx.input_dir,
                output_dir=ctx.output_dir,
            )


        if __name__ == "__main__":
            raise SystemExit(main())
        """
    )


def _build_model_task_script(output_name: str, model_type: str) -> str:
    return textwrap.dedent(
        f"""\
        from __future__ import annotations

        from runtime.task_runtime import task
        from runtime.task_support import run_model_task


        @task("model", name={output_name!r}, model_type={model_type!r})
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
        """
    )


def _build_metric_task_script(metric_name: str) -> str:
    return textwrap.dedent(
        f"""\
        from __future__ import annotations

        from runtime.task_runtime import task
        from runtime.task_support import run_metric_task


        @task("evaluation", name={metric_name!r})
        def main(ctx):
            return run_metric_task(
                metric_name=ctx.name,
                workflow_path=ctx.workflow_path,
                prediction_path=ctx.prediction_path,
                output_path=ctx.output_path,
            )


        if __name__ == "__main__":
            raise SystemExit(main())
        """
    )


def _generated_region_path(region: Any) -> Any:
    if not isinstance(region, str):
        return region
    region_path = Path(region)
    if region_path.is_absolute():
        return region
    if region_path.suffix not in {".geojson", ".json"}:
        return region
    if region_path.parts and region_path.parts[0] == "resources":
        return region
    return str(Path("resources") / region_path)


def _source_task_name(source: Any) -> str:
    if isinstance(source, str):
        return _task_slug(source)
    if isinstance(source, dict):
        if source.get("name"):
            return _task_slug(str(source["name"]))
        if source.get("collection"):
            return _task_slug(f"{source.get('kind', 'source')}_{source['collection']}")
        if source.get("kind"):
            return _task_slug(source["kind"])
    return "data_source"


def _task_slug(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    return slug or "task"


def _task_runtime_module() -> str:
    return textwrap.dedent(
        """\
        from __future__ import annotations

        import argparse
        import json
        from dataclasses import dataclass
        from functools import wraps
        from pathlib import Path
        from typing import Any, Callable

        from runtime.task_support import load_workflow, write_json

        JsonDict = dict[str, Any]
        TaskHandler = Callable[["TaskContext"], JsonDict]


        @dataclass(slots=True)
        class TaskContext:
            group: str
            name: str
            workflow_path: Path
            spec: JsonDict
            args: argparse.Namespace
            config: Any = None
            model_type: str | None = None

            @property
            def project_root(self) -> Path:
                return self.workflow_path.resolve().parent

            @property
            def workflow_name(self) -> str:
                return str(self.spec["workflow"]["name"])

            @property
            def input_dir(self) -> Path:
                return Path(self.args.input_dir)

            @property
            def output_dir(self) -> Path:
                path = Path(self.args.output_dir)
                path.mkdir(parents=True, exist_ok=True)
                return path

            @property
            def output_path(self) -> Path:
                path = Path(self.args.output_path)
                path.parent.mkdir(parents=True, exist_ok=True)
                return path

            @property
            def prediction_path(self) -> Path:
                return Path(self.args.prediction_path)

            @property
            def output_root(self) -> Path:
                if hasattr(self.args, "output_dir"):
                    return self.output_dir
                if hasattr(self.args, "output_path"):
                    return self.output_path.parent
                raise AttributeError(f"task {self.group}/{self.name} does not define an output root")

            def resource(self, path: str | Path) -> Path:
                return self.project_root / "resources" / Path(path)

            def artifact_path(self, path: str | Path) -> Path:
                target = Path(path)
                if not target.is_absolute():
                    target = self.output_root / target
                target.parent.mkdir(parents=True, exist_ok=True)
                return target

            def json_path(self, filename: str | Path | None = None) -> Path:
                if filename is None and hasattr(self.args, "output_path"):
                    return self.output_path
                if filename is None:
                    filename = f"{self.name}.json"
                return self.artifact_path(filename)

            def write_json(self, payload: JsonDict, *, filename: str | Path | None = None, path: str | Path | None = None) -> Path:
                target = Path(path) if path is not None else self.json_path(filename)
                return write_json(target, _stringify_paths(payload))

            def result(self, *, status: str = "completed", **fields: Any) -> JsonDict:
                payload = self._base_result(status)
                payload.update(fields)
                return _stringify_paths(payload)

            def _base_result(self, status: str) -> JsonDict:
                payload: JsonDict = {"status": status}
                if self.group == "preprocessing":
                    payload.update(
                        {
                            "task": self.name,
                            "config": self.config,
                            "input_dir": str(self.input_dir),
                            "output_dir": str(self.output_dir),
                        }
                    )
                elif self.group == "features":
                    payload.update(
                        {
                            "name": self.name,
                            "input_dir": str(self.input_dir),
                        }
                    )
                elif self.group == "model":
                    payload["name"] = self.name
                elif self.group == "evaluation":
                    payload["name"] = self.name
                return payload


        def task(
            group: str,
            *,
            name: str | None = None,
            config: Any = None,
            model_type: str | None = None,
        ) -> Callable[[TaskHandler], Callable[..., int]]:
            def decorator(func: TaskHandler) -> Callable[..., int]:
                parser_spec = _PARSER_SPECS[group]
                task_name = name or func.__name__

                @wraps(func)
                def main(argv: list[str] | None = None) -> int:
                    parser = argparse.ArgumentParser(description=parser_spec["description"])
                    parser.add_argument("--workflow", required=True)
                    parser_spec["configure_parser"](parser)
                    args = parser.parse_args(argv)
                    return _execute(
                        func,
                        TaskContext(
                            group=group,
                            name=task_name,
                            workflow_path=Path(args.workflow),
                            spec=load_workflow(args.workflow),
                            args=args,
                            config=config,
                            model_type=model_type,
                        ),
                    )

                return main

            return decorator


        def _add_data_arguments(parser: argparse.ArgumentParser) -> None:
            parser.add_argument("--output-dir", required=True)


        def _add_input_output_arguments(parser: argparse.ArgumentParser) -> None:
            parser.add_argument("--input-dir", required=True)
            parser.add_argument("--output-dir", required=True)


        def _add_model_arguments(parser: argparse.ArgumentParser) -> None:
            parser.add_argument("--input-dir", required=True)
            parser.add_argument("--output-path", required=True)


        def _add_evaluation_arguments(parser: argparse.ArgumentParser) -> None:
            parser.add_argument("--prediction-path", required=True)
            parser.add_argument("--output-path", required=True)


        _PARSER_SPECS: dict[str, dict[str, Any]] = {
            "data": {
                "description": "Load workflow data source.",
                "configure_parser": _add_data_arguments,
            },
            "preprocessing": {
                "description": "Run preprocessing task.",
                "configure_parser": _add_input_output_arguments,
            },
            "features": {
                "description": "Run feature task.",
                "configure_parser": _add_input_output_arguments,
            },
            "model": {
                "description": "Run model task.",
                "configure_parser": _add_model_arguments,
            },
            "evaluation": {
                "description": "Run evaluation metric task.",
                "configure_parser": _add_evaluation_arguments,
            },
        }


        def _execute(func: TaskHandler, context: TaskContext) -> int:
            payload = func(context)
            if not isinstance(payload, dict):
                raise TypeError(f"task {context.group}/{context.name} must return a JSON object")
            print(json.dumps(_stringify_paths(payload)))
            return 0


        def _stringify_paths(value: Any) -> Any:
            if isinstance(value, Path):
                return str(value)
            if isinstance(value, dict):
                return {key: _stringify_paths(item) for key, item in value.items()}
            if isinstance(value, list):
                return [_stringify_paths(item) for item in value]
            if isinstance(value, tuple):
                return [_stringify_paths(item) for item in value]
            return value
        """
    )


def _common_task_module() -> str:
    return textwrap.dedent(
        """\
        from __future__ import annotations

        import json
        import shutil
        import time
        import urllib.request
        from datetime import date
        from pathlib import Path
        from typing import Any
        from urllib.error import HTTPError, URLError

        try:
            import yaml
        except ImportError as exc:  # pragma: no cover - exercised only in broken envs
            raise RuntimeError("PyYAML is required. Install dependencies with `pip install -r requirements.txt`.") from exc


        def project_root(workflow_path: str | Path) -> Path:
            return Path(workflow_path).resolve().parent


        def load_workflow(path: str | Path) -> dict[str, Any]:
            workflow_path = Path(path)
            with workflow_path.open("r", encoding="utf-8") as handle:
                data = yaml.safe_load(handle)
            if not isinstance(data, dict):
                raise ValueError(f"workflow spec must be a YAML mapping: {workflow_path}")
            return data


        def write_json(path: str | Path, payload: dict[str, Any]) -> Path:
            target = Path(path)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(json.dumps(payload, indent=2) + "\\n", encoding="utf-8")
            return target


        def copy_tree(input_dir: str | Path, output_dir: str | Path) -> list[str]:
            source = Path(input_dir)
            target = Path(output_dir)
            target.mkdir(parents=True, exist_ok=True)
            copied: list[str] = []
            for source_path in sorted(source.rglob("*")):
                if not source_path.is_file():
                    continue
                relative = source_path.relative_to(source)
                destination = target / relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source_path, destination)
                copied.append(str(destination))
            return copied


        def run_source_task(
            source: str | dict[str, Any],
            workflow_path: str | Path,
            output_dir: str | Path,
            default_assets: list[str],
        ) -> dict[str, Any]:
            spec = load_workflow(workflow_path)
            raw_dir = Path(output_dir)
            raw_dir.mkdir(parents=True, exist_ok=True)
            try:
                resolved_source = _resolve_source_config(source, spec, default_assets)
                if resolved_source["kind"] == "stac":
                    return _download_stac_source(
                        spec,
                        project_root(workflow_path),
                        raw_dir,
                        resolved_source,
                    )
            except Exception as exc:
                raise RuntimeError(
                    f"failed to download real data for source {_source_label(source)!r}. "
                    "Check workflow.yaml data.source and resources/, "
                    "or override tasks/data/*.py with your own loader."
                ) from exc
            raise RuntimeError(
                f"unsupported generated source task for {source!r}. "
                "Use an executable data.source mapping or replace the generated data task."
            )


        def run_preprocess_task(
            task_name: str,
            config: Any,
            workflow_path: str | Path,
            input_dir: str | Path,
            output_dir: str | Path,
        ) -> dict[str, Any]:
            spec = load_workflow(workflow_path)
            source_dir = Path(input_dir)
            target_dir = Path(output_dir)
            copied = _copy_tree(source_dir, target_dir)
            manifest_path = target_dir / f"{task_name}.json"
            payload = {
                "task": task_name,
                "config": config,
                "workflow": spec["workflow"]["name"],
                "input_dir": str(source_dir),
                "output_dir": str(target_dir),
                "copied_files": copied,
                "implementation": "generated-pass-through",
            }
            _write_json(manifest_path, payload)
            return {
                "task": task_name,
                "config": config,
                "input_dir": str(source_dir),
                "output_dir": str(target_dir),
                "manifest": str(manifest_path),
                "artifacts": [str(manifest_path), *copied],
                "status": "completed",
            }


        def run_feature_task(
            feature_name: str,
            workflow_path: str | Path,
            input_dir: str | Path,
            output_dir: str | Path,
        ) -> dict[str, Any]:
            spec = load_workflow(workflow_path)
            source_dir = Path(input_dir)
            target_dir = Path(output_dir)
            target_dir.mkdir(parents=True, exist_ok=True)
            input_files = _list_files(source_dir)
            artifact_path = target_dir / f"{feature_name}.json"
            payload = {
                "feature": feature_name,
                "workflow": spec["workflow"]["name"],
                "input_dir": str(source_dir),
                "input_files": input_files,
                "value": len(input_files),
                "implementation": "generated-summary",
            }
            _write_json(artifact_path, payload)
            return {
                "name": feature_name,
                "artifact": str(artifact_path),
                "input_dir": str(source_dir),
                "status": "completed",
            }


        def run_model_task(
            output_name: str,
            model_type: str,
            workflow_path: str | Path,
            input_dir: str | Path,
            output_path: str | Path,
        ) -> dict[str, Any]:
            _ = (output_name, model_type, workflow_path, input_dir, output_path)
            raise RuntimeError(
                "generated model tasks are scaffolds. "
                "Add model.source + model.executor to workflow.yaml or replace tasks/model/*.py "
                "with real inference code."
            )


        def run_metric_task(
            metric_name: str,
            workflow_path: str | Path,
            prediction_path: str | Path,
            output_path: str | Path,
        ) -> dict[str, Any]:
            _ = (metric_name, workflow_path, prediction_path, output_path)
            raise RuntimeError(
                "generated evaluation tasks are scaffolds. "
                "Add evaluation.executor to workflow.yaml or replace tasks/evaluation/*.py "
                "with real metric code."
            )


        def _download_stac_source(
            spec: dict[str, Any],
            project_root: Path,
            raw_dir: Path,
            source: dict[str, Any],
        ) -> dict[str, Any]:
            region_geometry = _load_region_geometry(project_root, spec["data"]["region"])
            if not region_geometry:
                raise RuntimeError(
                    "data.region must point to an existing GeoJSON file for generated STAC tasks"
                )

            request_payload = {
                "collections": _stac_collections(source),
                "limit": int(source.get("limit", 64)),
                "datetime": _normalize_datetime_interval(
                    str(source.get("datetime") or spec["data"]["time"])
                ),
                "intersects": region_geometry,
            }
            if source.get("query"):
                request_payload["query"] = source["query"]
            request = urllib.request.Request(
                str(source["api_url"]),
                data=json.dumps(request_payload).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(request, timeout=20) as response:
                search_result = json.loads(response.read().decode("utf-8"))

            (raw_dir.parent / "search-results.json").write_text(json.dumps(search_result, indent=2) + "\\n", encoding="utf-8")
            item_ids = {
                item.get("id", "item")
                for item in search_result.get("features", [])
            }
            _prune_stale_item_dirs(raw_dir, item_ids)
            downloaded_assets: list[str] = []
            requested_assets = [str(asset) for asset in source.get("assets") or []]
            for item in search_result.get("features", []):
                item_id = item.get("id", "item")
                item_dir = raw_dir / item_id
                item_dir.mkdir(parents=True, exist_ok=True)
                for asset_name in requested_assets:
                    href = item.get("assets", {}).get(asset_name, {}).get("href")
                    if not href:
                        continue
                    output_path = item_dir / _asset_filename(asset_name, href)
                    _download_file(href, output_path)
                    downloaded_assets.append(str(output_path))

            if not downloaded_assets:
                raise RuntimeError("no downloadable STAC assets returned")

            return {
                "source": _source_label(source),
                "region": spec["data"]["region"],
                "time_range": spec["data"]["time"],
                "resolution": spec["data"]["resolution"],
                "items": sorted(item_ids),
                "downloaded_assets": downloaded_assets,
                "data_dir": str(raw_dir),
                "strategy": "network",
                "status": "loaded",
            }


        def _resolve_source_config(
            source: str | dict[str, Any],
            spec: dict[str, Any],
            default_assets: list[str],
        ) -> dict[str, Any]:
            if source == "stac://sentinel-2":
                return {
                    "kind": "stac",
                    "name": "stac_sentinel_2",
                    "api_url": "https://earth-search.aws.element84.com/v1/search",
                    "collection": "sentinel-2-l2a",
                    "assets": _stac_assets(spec, default_assets, None),
                    "limit": 64,
                }
            if isinstance(source, dict):
                kind = str(source.get("kind", "")).lower()
                if kind != "stac":
                    raise RuntimeError(f"unsupported generated source kind: {kind or source!r}")
                resolved = dict(source)
                resolved["kind"] = "stac"
                resolved.setdefault("name", "stac_source")
                resolved.setdefault("api_url", "https://earth-search.aws.element84.com/v1/search")
                resolved.setdefault("collection", "sentinel-2-l2a")
                resolved["assets"] = _stac_assets(spec, default_assets, resolved.get("assets"))
                resolved.setdefault("limit", 64)
                return resolved
            raise RuntimeError(f"unsupported generated source task for {source!r}")


        def _stac_assets(
            spec: dict[str, Any],
            default_assets: list[str],
            explicit_assets: Any,
        ) -> list[str]:
            if explicit_assets:
                return [str(asset) for asset in explicit_assets]
            if default_assets:
                return [str(asset) for asset in default_assets]

            assets = {"red", "nir"}
            if "ndwi" in spec.get("features", []):
                assets.add("green")
            if any("cloud_mask" in step for step in spec.get("preprocessing", [])):
                assets.add("scl")
            return sorted(assets)


        def _stac_collections(source: dict[str, Any]) -> list[str]:
            collections = source.get("collections")
            if isinstance(collections, list) and collections:
                return [str(item) for item in collections]
            collection = source.get("collection")
            if collection:
                return [str(collection)]
            raise RuntimeError("generated STAC source requires collection or collections in data.source")


        def _source_label(source: str | dict[str, Any]) -> str:
            if source == "stac://sentinel-2":
                return source
            if isinstance(source, dict):
                kind = str(source.get("kind", "")).lower()
                if kind == "stac":
                    return f"stac:{source.get('name') or source.get('collection') or 'source'}"
            return str(source)


        def _copy_tree(source_dir: Path, target_dir: Path) -> list[str]:
            target_dir.mkdir(parents=True, exist_ok=True)
            copied: list[str] = []
            for source_path in _iter_files(source_dir):
                relative = source_path.relative_to(source_dir)
                destination_path = target_dir / relative
                destination_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source_path, destination_path)
                copied.append(str(destination_path))
            return copied


        def _iter_files(root: Path) -> list[Path]:
            if not root.exists():
                return []
            return sorted(path for path in root.rglob("*") if path.is_file())


        def _list_files(root: Path) -> list[str]:
            return [str(path) for path in _iter_files(root)]


        def _prune_stale_item_dirs(raw_dir: Path, current_item_ids: set[str]) -> None:
            for child in raw_dir.iterdir():
                if not child.is_dir():
                    continue
                if child.name not in current_item_ids:
                    shutil.rmtree(child)


        def _write_json(path: Path, payload: dict[str, Any]) -> None:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(payload, indent=2) + "\\n", encoding="utf-8")


        def _download_file(url: str, output_path: Path) -> None:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            if output_path.exists() and output_path.stat().st_size > 0:
                return

            temp_path = output_path.with_suffix(output_path.suffix + ".part")
            last_error: Exception | None = None
            for attempt in range(1, 5):
                try:
                    request = urllib.request.Request(url, headers={"User-Agent": "leoflow/0.1"})
                    with urllib.request.urlopen(request, timeout=60) as response:
                        with temp_path.open("wb") as handle:
                            while True:
                                chunk = response.read(1024 * 1024)
                                if not chunk:
                                    break
                                handle.write(chunk)
                    temp_path.replace(output_path)
                    return
                except HTTPError:
                    temp_path.unlink(missing_ok=True)
                    raise
                except (URLError, TimeoutError, ConnectionResetError) as exc:
                    temp_path.unlink(missing_ok=True)
                    last_error = exc
                    if attempt == 4:
                        break
                    time.sleep(2 ** (attempt - 1))

            if last_error is not None:
                raise RuntimeError(
                    f"could not download asset after 4 attempts: {url}"
                ) from last_error
            raise RuntimeError(f"could not download asset: {url}")


        def _asset_filename(fallback_name: str, url: str) -> str:
            name = Path(urllib.request.url2pathname(url.split("?")[0])).name
            return name or fallback_name


        def _load_region_geometry(project_root: Path, region: str) -> dict[str, Any] | None:
            region_path = Path(region)
            if not region_path.is_absolute():
                region_path = project_root / region_path
            if not region_path.exists():
                return None
            data = json.loads(region_path.read_text(encoding="utf-8"))
            if data.get("type") == "Feature":
                return data.get("geometry")
            if data.get("type") == "FeatureCollection":
                features = data.get("features") or []
                if len(features) == 1:
                    return features[0].get("geometry")
            return data


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
        """
    )
