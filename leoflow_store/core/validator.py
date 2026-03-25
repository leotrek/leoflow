from __future__ import annotations

import copy
import re
from typing import Any

SEMVER_RE = re.compile(r"^\d+\.\d+\.\d+$")


def workflow_slug(spec: dict[str, Any]) -> str:
    name = spec["workflow"]["name"]
    slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    if not slug:
        raise ValueError("workflow name must contain at least one alphanumeric character")
    return slug


def resolve_version(spec: dict[str, Any], override: str | None = None) -> str:
    version = override or spec.get("workflow", {}).get("version") or "0.1.0"
    if not SEMVER_RE.match(version):
        raise ValueError(f"workflow version must look like 0.1.0, got: {version}")
    return version


def validate_workflow_spec(spec: dict[str, Any]) -> dict[str, Any]:
    errors: list[str] = []
    normalized = copy.deepcopy(spec)

    if not isinstance(normalized.get("workflow"), dict):
        errors.append("workflow must be a mapping")
    elif not normalized["workflow"].get("name"):
        errors.append("workflow.name is required")

    _require_mapping_keys(normalized, "data", ["source", "region", "time", "resolution"], errors)
    _require_mapping_keys(normalized, "model", ["type", "input", "output"], errors)

    features = normalized.get("features")
    if not isinstance(features, list) or not features:
        errors.append("features must be a non-empty list")

    preprocessing = normalized.get("preprocessing")
    if not isinstance(preprocessing, list) or not preprocessing:
        errors.append("preprocessing must be a non-empty list")
    else:
        for index, step in enumerate(preprocessing, start=1):
            if not isinstance(step, dict) or len(step) != 1:
                errors.append(f"preprocessing step {index} must be a single-key mapping")

    evaluation = normalized.get("evaluation")
    metrics = evaluation.get("metrics") if isinstance(evaluation, dict) else None
    if not isinstance(metrics, list) or not metrics:
        errors.append("evaluation.metrics must be a non-empty list")

    if errors:
        message = "invalid workflow spec:\n- " + "\n- ".join(errors)
        raise ValueError(message)

    normalized["workflow"]["slug"] = workflow_slug(normalized)
    normalized["workflow"]["version"] = resolve_version(normalized)
    return normalized


def _require_mapping_keys(
    spec: dict[str, Any],
    section_name: str,
    required_keys: list[str],
    errors: list[str],
) -> None:
    section = spec.get(section_name)
    if not isinstance(section, dict):
        errors.append(f"{section_name} must be a mapping")
        return

    for key in required_keys:
        if section.get(key) in (None, ""):
            errors.append(f"{section_name}.{key} is required")
