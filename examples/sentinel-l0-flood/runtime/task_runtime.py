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

    def write_json(
        self,
        payload: JsonDict,
        *,
        filename: str | Path | None = None,
        path: str | Path | None = None,
    ) -> Path:
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
