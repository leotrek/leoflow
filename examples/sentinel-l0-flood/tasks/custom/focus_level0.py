from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from runtime.task_support import load_workflow, project_root, write_json  # noqa: E402


def _iter_tiffs(root: str | Path) -> list[Path]:
    base = Path(root)
    if not base.exists():
        return []
    return sorted(path for path in base.rglob("*.tif") if path.is_file())


def _copy_raster(source_path: str | Path, destination_path: str | Path) -> Path:
    source = Path(source_path)
    destination = Path(destination_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if source.resolve() != destination.resolve():
        shutil.copy2(source, destination)
    return destination


def _match_candidates(
    roots: Iterable[Path],
    *aliases: str,
    exclude_parts: tuple[str, ...] = (),
) -> Path | None:
    expected = {alias.lower() for alias in aliases}
    excluded = set(exclude_parts)
    for root in roots:
        if not root.exists():
            continue
        for path in _iter_tiffs(root):
            if excluded and any(part in excluded for part in path.parts):
                continue
            stem = path.stem.lower()
            if stem in expected or any(stem.endswith(f"_{alias}") for alias in expected):
                return path
    return None


def _pair_paths(
    roots: Iterable[Path],
    *,
    prefix: str | None = None,
    exclude_parts: tuple[str, ...] = (),
) -> tuple[Path | None, Path | None]:
    vv_names = [
        "vv",
        "sigma0_vv",
        "post_event_vv" if prefix is None else f"{prefix}_vv",
        "post_event_sigma0_vv",
    ]
    vh_names = [
        "vh",
        "sigma0_vh",
        "post_event_vh" if prefix is None else f"{prefix}_vh",
        "post_event_sigma0_vh",
    ]
    vv_path = _match_candidates(roots, *vv_names, exclude_parts=exclude_parts)
    vh_path = _match_candidates(roots, *vh_names, exclude_parts=exclude_parts)
    return vv_path, vh_path


def _copy_pair(
    source_roots: Iterable[Path],
    output_dir: Path,
    *,
    prefix: str | None = None,
    exclude_parts: tuple[str, ...] = (),
) -> list[str]:
    vv_path, vh_path = _pair_paths(
        source_roots,
        prefix=prefix,
        exclude_parts=exclude_parts,
    )
    if vv_path is None or vh_path is None:
        return []

    copied: list[str] = []
    vv_target = output_dir / ("vv.tif" if prefix is None else f"{prefix}_vv.tif")
    vh_target = output_dir / ("vh.tif" if prefix is None else f"{prefix}_vh.tif")
    copied.append(str(_copy_raster(vv_path, vv_target)))
    copied.append(str(_copy_raster(vh_path, vh_target)))
    return copied


def _collect_output_pair(output_dir: Path, *, prefix: str | None = None) -> list[str]:
    vv_path, vh_path = _pair_paths([output_dir], prefix=prefix)
    if vv_path is None or vh_path is None:
        return []
    return [str(vv_path), str(vh_path)]


def _resolve_path(root: Path, value: str | None) -> Path | None:
    if not value:
        return None
    path = Path(value)
    if path.is_absolute():
        return path
    return root / path


def _find_archive_path(spec: dict[str, Any], input_dir: Path) -> Path | None:
    downloads_dir = input_dir / "downloads"
    configured_name = str(spec.get("data", {}).get("download", {}).get("filename", "")).strip()
    if configured_name:
        configured_path = downloads_dir / configured_name
        if configured_path.exists():
            return configured_path
    archives = sorted(path for path in downloads_dir.glob("*.zip") if path.is_file())
    if archives:
        return archives[0]
    return None


def _find_safe_dir(input_dir: Path) -> Path | None:
    extracted_dir = input_dir / "extracted"
    safe_dirs = sorted(path for path in extracted_dir.rglob("*.SAFE") if path.is_dir())
    if safe_dirs:
        return safe_dirs[0]
    return None


def _toolchain_context(spec: dict[str, Any], workflow_path: str, input_dir: Path, output_dir: Path) -> dict[str, str]:
    workflow_root = project_root(workflow_path)
    archive_path = _find_archive_path(spec, input_dir)
    safe_dir = _find_safe_dir(input_dir)
    region_path = _resolve_path(workflow_root, str(spec.get("data", {}).get("region", "")).strip() or None)
    values = {
        "python_executable": sys.executable,
        "workflow_path": str(Path(workflow_path).resolve()),
        "workflow_dir": str(workflow_root),
        "workflow_name": str(spec.get("workflow", {}).get("name", "")),
        "input_dir": str(input_dir),
        "output_dir": str(output_dir),
        "raw_dir": str(input_dir),
        "downloads_dir": str(input_dir / "downloads"),
        "extracted_dir": str(input_dir / "extracted"),
        "archive_path": str(archive_path) if archive_path is not None else "",
        "safe_dir": str(safe_dir) if safe_dir is not None else "",
        "region_path": str(region_path) if region_path is not None else "",
        "post_event_override_dir": str(input_dir / "overrides" / "post_event"),
        "pre_event_override_dir": str(input_dir / "overrides" / "pre_event"),
    }
    return values


def _format_template(template: str, values: dict[str, str], *, field_name: str) -> str:
    try:
        return template.format_map(values)
    except KeyError as exc:
        available = ", ".join(sorted(values))
        raise RuntimeError(
            f"data.toolchain.focus.{field_name} references unknown placeholder {exc.args[0]!r}. "
            f"Available placeholders: {available}."
        ) from exc


def _run_toolchain_focus(
    spec: dict[str, Any],
    workflow_path: str,
    input_dir: Path,
    output_dir: Path,
) -> dict[str, Any] | None:
    config = spec.get("data", {}).get("toolchain", {}).get("focus")
    if not isinstance(config, dict):
        return None

    command_template = str(config.get("command", "")).strip()
    if not command_template:
        return None

    values = _toolchain_context(spec, workflow_path, input_dir, output_dir)
    command = _format_template(command_template, values, field_name="command")

    cwd_template = str(config.get("cwd", "")).strip()
    if cwd_template:
        cwd = Path(_format_template(cwd_template, values, field_name="cwd"))
    else:
        cwd = project_root(workflow_path)

    env = os.environ.copy()
    env_config = config.get("env", {})
    if env_config is not None and not isinstance(env_config, dict):
        raise RuntimeError("data.toolchain.focus.env must be a mapping of environment variables.")
    for key, value in (env_config or {}).items():
        if value is None:
            continue
        env[str(key)] = _format_template(str(value), values, field_name=f"env.{key}")

    try:
        result = subprocess.run(
            command,
            shell=True,
            cwd=cwd,
            env=env,
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        details = (exc.stderr or "").strip() or (exc.stdout or "").strip() or f"exit code {exc.returncode}"
        raise RuntimeError(f"external SAR toolchain failed: {details}") from exc

    return {
        "command": command,
        "cwd": str(cwd),
        "stdout": result.stdout.strip(),
        "stderr": result.stderr.strip(),
        "env_keys": sorted(str(key) for key in (env_config or {}).keys()),
    }


def _missing_post_event_message(spec: dict[str, Any], toolchain_ran: bool) -> str:
    archive_type = str(spec.get("data", {}).get("acquisition", {}).get("archive_type", "")).lower()
    download_name = str(spec.get("data", {}).get("download", {}).get("filename", "")).lower()
    if toolchain_ran:
        return (
            "external SAR toolchain finished, but no post-event VV/VH rasters were found in the focus output. "
            "Ensure the command writes vv.tif and vh.tif into the --output directory."
        )
    if archive_type == "raw" or "_raw_" in download_name:
        return (
            "no raster VV/VH inputs were found. True Sentinel-1 RAW focusing requires an external SAR toolchain. "
            "Provide geocoded VV/VH rasters under data.overrides.raster_dir or configure data.toolchain.focus.command."
        )
    if archive_type == "grd" or "_grd_" in download_name:
        return (
            "no raster VV/VH inputs were found. True Sentinel-1 GRD orbit application, calibration, and terrain "
            "correction require an external SAR toolchain. Provide terrain-corrected VV/VH rasters under "
            "data.overrides.raster_dir or configure data.toolchain.focus.command."
        )
    return (
        "no raster VV/VH inputs were found for preprocessing. Provide geocoded VV/VH rasters or configure "
        "data.toolchain.focus.command."
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workflow", required=True)
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)

    spec = load_workflow(args.workflow)
    input_dir = Path(args.input)
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    post_event_roots = [input_dir]
    pre_event_roots = [input_dir / "overrides" / "pre_event"]

    post_event_artifacts = _copy_pair(post_event_roots, output_dir, exclude_parts=("pre_event",))
    pre_event_artifacts = _copy_pair(pre_event_roots, output_dir, prefix="pre_event")
    angle_artifacts: list[str] = []
    angle_source = _match_candidates(post_event_roots, "local_incidence_angle", "incidence_angle", "angle")
    if angle_source is not None:
        angle_artifacts.append(str(_copy_raster(angle_source, output_dir / "local_incidence_angle.tif")))

    toolchain_result = None
    if not post_event_artifacts:
        toolchain_result = _run_toolchain_focus(spec, args.workflow, input_dir, output_dir)
        if toolchain_result is not None:
            post_event_artifacts = _collect_output_pair(output_dir)
            if not angle_artifacts:
                generated_angle = _match_candidates([output_dir], "local_incidence_angle", "incidence_angle", "angle")
                if generated_angle is not None:
                    angle_artifacts.append(str(generated_angle))

    if not post_event_artifacts:
        raise RuntimeError(_missing_post_event_message(spec, toolchain_result is not None))

    artifacts = post_event_artifacts + pre_event_artifacts + angle_artifacts
    manifest_path = write_json(
        output_dir / "focus_level0.json",
        {
            "task": "focus_level0",
            "workflow": spec["workflow"]["name"],
            "input_dir": str(input_dir),
            "output_dir": str(output_dir),
            "copied_artifacts": artifacts,
            "toolchain": toolchain_result,
            "implementation": "copy available VV/VH rasters or invoke an external SAR focus command",
        },
    )
    print(
        json.dumps(
            {
                "task": "focus_level0",
                "input_dir": str(input_dir),
                "output_dir": str(output_dir),
                "manifest": str(manifest_path),
                "artifacts": artifacts + [str(manifest_path)],
                "status": "completed",
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
