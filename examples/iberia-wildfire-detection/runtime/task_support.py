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
    target.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
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


def http_json(
    url: str,
    *,
    payload: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    method: str = "GET",
    timeout: int = 30,
) -> dict[str, Any]:
    request_headers = {"User-Agent": "leoflow/0.1", **(headers or {})}
    body = json.dumps(payload).encode("utf-8") if payload is not None else None
    if body is not None:
        request_headers.setdefault("Content-Type", "application/json")
    request = urllib.request.Request(url, data=body, headers=request_headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        details = exc.read().decode("utf-8", errors="replace").strip()
        if details:
            raise RuntimeError(f"HTTP {exc.code} from {url}: {details}") from exc
        raise


def download_file(
    url: str,
    output_path: str | Path,
    *,
    headers: dict[str, str] | None = None,
    timeout: int = 60,
    retries: int = 4,
) -> Path:
    target = Path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists() and target.stat().st_size > 0:
        return target

    temp_path = target.with_suffix(target.suffix + ".part")
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            request = urllib.request.Request(url, headers={"User-Agent": "leoflow/0.1", **(headers or {})})
            with urllib.request.urlopen(request, timeout=timeout) as response:
                with temp_path.open("wb") as handle:
                    while True:
                        chunk = response.read(1024 * 1024)
                        if not chunk:
                            break
                        handle.write(chunk)
            temp_path.replace(target)
            return target
        except HTTPError:
            temp_path.unlink(missing_ok=True)
            raise
        except (URLError, TimeoutError, ConnectionResetError) as exc:
            temp_path.unlink(missing_ok=True)
            last_error = exc
            if attempt == retries:
                break
            time.sleep(2 ** (attempt - 1))

    if last_error is not None:
        raise RuntimeError(f"could not download asset after {retries} attempts: {url}") from last_error
    raise RuntimeError(f"could not download asset: {url}")


def normalize_datetime_interval(value: str) -> str:
    return _normalize_datetime_interval(value)


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

    (raw_dir.parent / "search-results.json").write_text(json.dumps(search_result, indent=2) + "\n", encoding="utf-8")
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
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


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
