from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

from leoflow_store.core.generator import DEFAULT_TEMPLATE, generate_project
from leoflow_store.core.parser import load_workflow
from leoflow_store.core.validator import resolve_version, validate_workflow_spec, workflow_slug

DEFAULT_EXAMPLE_TEMPLATE = "wildfire-detection"


def create_project(
    project_name: str,
    output_dir: str | Path,
    *,
    runtime_template: str = DEFAULT_TEMPLATE,
    workflow_template: str = DEFAULT_EXAMPLE_TEMPLATE,
    examples_root: str | Path | None = None,
) -> Path:
    template_info = load_example(workflow_template, examples_root=examples_root)
    spec = copy.deepcopy(template_info["spec"])
    spec["workflow"]["name"] = project_name
    spec = validate_workflow_spec(spec)
    version = resolve_version(spec)
    return generate_project(
        spec,
        version,
        runtime_template,
        Path(output_dir),
        workflow_path=template_info["workflow_path"],
    )


def example_template_names(*, examples_root: str | Path | None = None) -> list[str]:
    return sorted(path.parent.name for path in _examples_root(examples_root).glob("*/workflow.yaml"))


def list_examples(
    query: str | None = None,
    *,
    examples_root: str | Path | None = None,
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for workflow_path in sorted(_examples_root(examples_root).glob("*/workflow.yaml")):
        spec = validate_workflow_spec(load_workflow(workflow_path))
        item = {
            "starter": workflow_path.parent.name,
            "name": spec["workflow"]["name"],
            "version": spec["workflow"]["version"],
            "path": str(workflow_path),
            "data_source": _source_label(spec["data"]["source"]),
            "model_type": spec["model"]["type"],
            "features": list(spec["features"]),
            "metrics": list(spec["evaluation"]["metrics"]),
            "tags": _derive_tags(spec),
        }
        items.append(item)

    if not query:
        return items

    needle = query.lower()
    return [item for item in items if needle in _search_text(item)]


def load_example(
    name: str,
    *,
    examples_root: str | Path | None = None,
) -> dict[str, Any]:
    workflow_path = _examples_root(examples_root) / name / "workflow.yaml"
    if not workflow_path.exists():
        raise FileNotFoundError(f"workflow template not found: {name}")
    return {
        "workflow_path": workflow_path,
        "spec": validate_workflow_spec(load_workflow(workflow_path)),
    }


def _examples_root(root: str | Path | None = None) -> Path:
    return Path(root) if root else Path(__file__).resolve().parents[2] / "examples"


def _derive_tags(spec: dict[str, Any]) -> list[str]:
    source = spec["data"]["source"]
    tags = {
        workflow_slug(spec),
        _tagify(_source_label(source)),
        spec["model"]["type"],
        *spec["features"],
        *spec["evaluation"]["metrics"],
    }
    if isinstance(source, dict):
        if source.get("kind"):
            tags.add(_tagify(str(source["kind"])))
        if source.get("collection"):
            tags.add(_tagify(str(source["collection"])))
    return sorted(tags)


def _search_text(item: dict[str, Any]) -> str:
    return " ".join(
        [
            str(item["starter"]),
            str(item["name"]),
            str(item["version"]),
            str(item["path"]),
            str(item["data_source"]),
            str(item["model_type"]),
            " ".join(item["features"]),
            " ".join(item["metrics"]),
            " ".join(item["tags"]),
        ]
    ).lower()


def _source_label(source: Any) -> str:
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
            return f"{kind}:{source}"
    return str(source)


def _tagify(value: str) -> str:
    import re

    return re.sub(r"[^a-z0-9]+", "-", value.strip().lower()).strip("-")
