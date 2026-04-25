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

    _validate_workflow_section(normalized, errors)
    _require_mapping_keys(normalized, "data", ["source", "region", "time", "resolution"], errors)
    _require_mapping_keys(normalized, "model", ["type", "input", "output"], errors)
    _validate_features_section(normalized, errors)
    _validate_preprocessing_section(normalized, errors)
    _validate_model_section(normalized, errors)
    _validate_evaluation_section(normalized, errors)

    if errors:
        message = "invalid workflow spec:\n- " + "\n- ".join(errors)
        raise ValueError(message)

    normalized["workflow"]["slug"] = workflow_slug(normalized)
    normalized["workflow"]["version"] = resolve_version(normalized)
    return normalized


def _validate_workflow_section(spec: dict[str, Any], errors: list[str]) -> None:
    workflow = spec.get("workflow")
    if not isinstance(workflow, dict):
        errors.append("workflow must be a mapping")
        return
    if not workflow.get("name"):
        errors.append("workflow.name is required")


def _validate_features_section(spec: dict[str, Any], errors: list[str]) -> None:
    features = spec.get("features")
    if not isinstance(features, list) or not features:
        errors.append("features must be a non-empty list")


def _validate_preprocessing_section(spec: dict[str, Any], errors: list[str]) -> None:
    preprocessing = spec.get("preprocessing")
    if not isinstance(preprocessing, list) or not preprocessing:
        errors.append("preprocessing must be a non-empty list")
        return

    spec["preprocessing"] = _normalize_preprocessing(preprocessing, errors)
    for index, step in enumerate(spec["preprocessing"], start=1):
        if not isinstance(step, dict) or len(step) != 1:
            errors.append(f"preprocessing step {index} must be a single-key mapping")


def _validate_model_section(spec: dict[str, Any], errors: list[str]) -> None:
    model = spec.get("model")
    if isinstance(model, dict):
        _normalize_executor_field(model, "model", errors)


def _validate_evaluation_section(spec: dict[str, Any], errors: list[str]) -> None:
    evaluation = spec.get("evaluation")
    metrics = evaluation.get("metrics") if isinstance(evaluation, dict) else None
    if not isinstance(metrics, list) or not metrics:
        errors.append("evaluation.metrics must be a non-empty list")
        return
    _normalize_executor_field(evaluation, "evaluation", errors)


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


def _normalize_preprocessing(steps: list[Any], errors: list[str]) -> list[Any]:
    normalized_steps: list[Any] = []
    for index, step in enumerate(steps, start=1):
        if not isinstance(step, dict) or len(step) != 1:
            normalized_steps.append(step)
            continue

        name, value = next(iter(step.items()))
        if name != "command":
            normalized_steps.append(step)
            continue

        normalized_steps.append(
            {
                "command": _normalize_command_like(
                    value,
                    f"preprocessing step {index} command",
                    errors,
                )
            }
        )
    return normalized_steps


def _normalize_executor_field(section: dict[str, Any], section_name: str, errors: list[str]) -> None:
    executor = section.get("executor")
    if executor is None:
        return
    section["executor"] = _normalize_command_like(executor, f"{section_name}.executor", errors)


def _normalize_command_like(value: Any, label: str, errors: list[str]) -> Any:
    if isinstance(value, str):
        return {"name": value}

    if not isinstance(value, dict):
        errors.append(f"{label} must be a string or mapping")
        return value

    normalized = copy.deepcopy(value)
    name = normalized.get("name")
    run = normalized.get("run")

    if name not in (None, "") and not isinstance(name, str):
        errors.append(f"{label}.name must be a string")
    if run not in (None, "") and not isinstance(run, str):
        errors.append(f"{label}.run must be a string")
    if normalized.get("script") not in (None, "") and not isinstance(normalized.get("script"), str):
        errors.append(f"{label}.script must be a string")
    if normalized.get("output") not in (None, "") and not isinstance(normalized.get("output"), str):
        errors.append(f"{label}.output must be a string")
    if normalized.get("artifact") not in (None, "") and not isinstance(normalized.get("artifact"), str):
        errors.append(f"{label}.artifact must be a string")

    if run in (None, "") and name in (None, ""):
        errors.append(f"{label} must define either name or run")

    return normalized
