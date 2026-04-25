from __future__ import annotations

import http.cookiejar
import json
import os
import re
import shutil
import tempfile
import urllib.parse
import urllib.request
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse

from runtime.task_runtime import task

ASF_SEARCH_URL = "https://api.daac.asf.alaska.edu/services/search/param"


def _resolve_path(root: Path, value: str | None) -> Path | None:
    if not value:
        return None
    path = Path(value)
    if path.is_absolute():
        return path
    return root / path


def _region_geometries(region_path: Path) -> list[dict[str, object]]:
    data = json.loads(region_path.read_text(encoding="utf-8"))
    if data.get("type") == "FeatureCollection":
        return [feature["geometry"] for feature in data.get("features", []) if feature.get("geometry")]
    if data.get("type") == "Feature":
        return [data["geometry"]]
    return [data]


def _region_bbox(region_path: Path) -> list[float]:
    geometries = _region_geometries(region_path)
    x_values = [coord for geometry in geometries for coord in _collect_coordinates(geometry, axis=0)]
    y_values = [coord for geometry in geometries for coord in _collect_coordinates(geometry, axis=1)]
    return [min(x_values), min(y_values), max(x_values), max(y_values)]


def _bbox_wkt(bbox: list[float]) -> str:
    left, bottom, right, top = bbox
    return f"POLYGON(({left} {bottom},{right} {bottom},{right} {top},{left} {top},{left} {bottom}))"


def _collect_coordinates(geometry: dict[str, object], *, axis: int) -> list[float]:
    geometry_type = geometry.get("type")
    if geometry_type == "GeometryCollection":
        values: list[float] = []
        for item in geometry.get("geometries", []):  # type: ignore[union-attr]
            values.extend(_collect_coordinates(item, axis=axis))  # type: ignore[arg-type]
        return values
    return _flatten_coords(geometry.get("coordinates", []), axis=axis)


def _flatten_coords(value: object, *, axis: int) -> list[float]:
    if isinstance(value, (list, tuple)):
        if value and isinstance(value[0], (int, float)):
            return [float(value[axis])]
        values: list[float] = []
        for item in value:
            values.extend(_flatten_coords(item, axis=axis))
        return values
    raise TypeError(f"unsupported coordinate payload: {value!r}")


def _copy_tree(source_dir: Path, target_dir: Path) -> list[str]:
    copied: list[str] = []
    if not source_dir.exists():
        return copied
    for source_path in sorted(source_dir.rglob("*")):
        if not source_path.is_file():
            continue
        destination = target_dir / source_path.relative_to(source_dir)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, destination)
        copied.append(str(destination))
    return copied


def _resolve_basic_auth(auth: dict[str, object]) -> tuple[str | None, str | None, str | None]:
    username = str(auth.get("username") or "").strip() or None

    inline_password = auth.get("password")
    if inline_password is not None and str(inline_password).strip():
        return username, str(inline_password), "inline"

    password_env = str(auth.get("password_env") or "").strip()
    if password_env:
        env_password = os.environ.get(password_env)
        if env_password:
            return username, env_password, password_env

    return username, None, password_env or None


def _build_authenticated_opener(
    username: str | None,
    password: str | None,
) -> urllib.request.OpenerDirector:
    handlers: list[urllib.request.BaseHandler] = [
        urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()),
    ]
    if username and password:
        password_manager = urllib.request.HTTPPasswordMgrWithDefaultRealm()
        for auth_url in ("https://urs.earthdata.nasa.gov", "https://urs.earthdata.nasa.gov/"):
            password_manager.add_password(None, auth_url, username, password)
        handlers.append(urllib.request.HTTPBasicAuthHandler(password_manager))
    return urllib.request.build_opener(*handlers)


def _download_with_auth(url: str, output_path: Path, username: str | None, password: str | None) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists() and output_path.stat().st_size > 0:
        return output_path

    parsed = urlparse(url)
    if parsed.scheme in {"", "file"}:
        source_path = Path(parsed.path if parsed.scheme == "file" else url)
        if not source_path.exists():
            raise FileNotFoundError(f"download source not found: {url}")
        shutil.copy2(source_path, output_path)
        return output_path

    opener = _build_authenticated_opener(username, password)
    request = urllib.request.Request(url, headers={"User-Agent": "leoflow-s1-flood/0.1"})
    partial_path = output_path.with_suffix(f"{output_path.suffix}.part")
    try:
        with opener.open(request, timeout=120) as response, partial_path.open("wb") as handle:
            shutil.copyfileobj(response, handle, length=1024 * 1024)
        partial_path.replace(output_path)
    except HTTPError as exc:
        partial_path.unlink(missing_ok=True)
        if exc.code in {401, 403}:
            raise RuntimeError(
                f"authenticated download failed with HTTP {exc.code} for {url}. "
                "Check Earthdata username/password and confirm the account can access ASF datapool downloads."
            ) from exc
        raise RuntimeError(f"download failed with HTTP {exc.code} for {url}: {exc.reason}") from exc
    except URLError as exc:
        partial_path.unlink(missing_ok=True)
        raise RuntimeError(f"download failed for {url}: {exc.reason}") from exc
    return output_path


def _extract_archive(archive_path: Path, extracted_root: Path) -> list[str]:
    if archive_path.suffix.lower() != ".zip":
        return []
    target_dir = extracted_root / archive_path.stem
    manifest_path = target_dir / ".extracted"
    if manifest_path.exists():
        return [str(path) for path in sorted(target_dir.rglob("*")) if path.is_file() and path != manifest_path]
    target_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive_path) as archive:
        archive.extractall(target_dir)
    manifest_path.write_text("ok\n", encoding="utf-8")
    return [str(path) for path in sorted(target_dir.rglob("*")) if path.is_file() and path != manifest_path]


def _parse_time_range(value: str) -> tuple[datetime, datetime]:
    start_text, end_text = value.split("/", 1)
    return _parse_timestamp(start_text), _parse_timestamp(end_text)


def _parse_timestamp(value: str) -> datetime:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    return datetime.fromisoformat(text).astimezone(timezone.utc)


def _format_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _flatten_search_results(payload: object) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    stack: list[object] = [payload]
    while stack:
        current = stack.pop()
        if isinstance(current, dict):
            if "granuleName" in current or "downloadUrl" in current:
                items.append(current)
            else:
                stack.extend(current.values())
        elif isinstance(current, list):
            stack.extend(current)
    return items


def _search_asf(params: dict[str, str], username: str | None, password: str | None) -> list[dict[str, Any]]:
    query = urllib.parse.urlencode(params)
    opener = _build_authenticated_opener(username, password)
    request = urllib.request.Request(
        f"{ASF_SEARCH_URL}?{query}",
        headers={"User-Agent": "leoflow-s1-flood/0.1"},
    )
    try:
        with opener.open(request, timeout=120) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        raise RuntimeError(f"ASF OPERA RTC search failed with HTTP {exc.code}: {exc.reason}") from exc
    except URLError as exc:
        raise RuntimeError(f"ASF OPERA RTC search failed: {exc.reason}") from exc
    return _flatten_search_results(payload)


def _extract_absolute_orbit(granule_id: str) -> int | None:
    match = re.search(r"_(\d{6})_", granule_id)
    if match is None:
        return None
    return int(match.group(1))


def _filter_opera_items(
    items: list[dict[str, Any]],
    *,
    platform: str | None = None,
    absolute_orbit: int | None = None,
) -> list[dict[str, Any]]:
    filtered = items
    if platform:
        platform_filtered = [item for item in filtered if str(item.get("platform") or "").strip() == platform]
        if platform_filtered:
            filtered = platform_filtered
    if absolute_orbit is not None:
        orbit_filtered = [item for item in filtered if int(item.get("absoluteOrbit") or -1) == absolute_orbit]
        if orbit_filtered:
            filtered = orbit_filtered
    return sorted(filtered, key=_item_start_time)


def _item_start_time(item: dict[str, Any]) -> datetime:
    return _parse_timestamp(str(item.get("startTime") or "1970-01-01T00:00:00Z"))


def _item_value(item: dict[str, Any], key: str) -> str:
    return str(item.get(key) or "").strip()


def _prefer_matching_items(
    items: list[dict[str, Any]],
    key: str,
    expected: str,
) -> list[dict[str, Any]]:
    if not expected:
        return items
    matches = [item for item in items if _item_value(item, key) == expected]
    return matches or items


def _select_previous_opera_items(
    items: list[dict[str, Any]],
    reference_items: list[dict[str, Any]],
    *,
    before: datetime,
) -> list[dict[str, Any]]:
    if not reference_items:
        return []

    reference = reference_items[0]
    candidates = [item for item in items if _item_start_time(item) < before]
    candidates = _prefer_matching_items(candidates, "platform", _item_value(reference, "platform"))
    candidates = _prefer_matching_items(
        candidates,
        "relativeOrbit",
        _item_value(reference, "relativeOrbit"),
    )
    candidates = _prefer_matching_items(candidates, "track", _item_value(reference, "track"))
    candidates = _prefer_matching_items(
        candidates,
        "flightDirection",
        _item_value(reference, "flightDirection"),
    )
    if not candidates:
        return []

    latest = max(candidates, key=_item_start_time)
    selected_orbit = int(latest.get("absoluteOrbit") or -1)
    selected = [item for item in candidates if int(item.get("absoluteOrbit") or -2) == selected_orbit]
    return sorted(selected, key=_item_start_time)


def _opera_asset_urls(item: dict[str, Any]) -> dict[str, str]:
    urls: dict[str, str] = {}
    for url in (item.get("opera") or {}).get("additionalUrls", []):
        value = str(url)
        upper = value.upper()
        if upper.endswith("_VV.TIF"):
            urls["vv"] = value
        elif upper.endswith("_VH.TIF"):
            urls["vh"] = value
        elif upper.endswith("_MASK.TIF"):
            urls["mask"] = value
    return urls


def _mosaic_geotiffs(source_paths: list[Path], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if len(source_paths) == 1:
        shutil.copy2(source_paths[0], output_path)
        return output_path

    import rasterio
    from rasterio.merge import merge

    datasets = [rasterio.open(path) for path in source_paths]
    try:
        mosaic, transform = merge(datasets)
        profile = datasets[0].profile.copy()
        profile.update(
            transform=transform,
            height=mosaic.shape[1],
            width=mosaic.shape[2],
            count=mosaic.shape[0],
            compress="lzw",
        )
        with rasterio.open(output_path, "w", **profile) as dst:
            dst.write(mosaic)
    finally:
        for dataset in datasets:
            dataset.close()
    return output_path


def _download_opera_pair(
    items: list[dict[str, Any]],
    destination_dir: Path,
    *,
    prefix: str | None,
    username: str | None,
    password: str | None,
) -> list[str]:
    if not items:
        return []

    vv_label = "vv" if prefix is None else f"{prefix}_vv"
    vh_label = "vh" if prefix is None else f"{prefix}_vh"

    with tempfile.TemporaryDirectory(prefix="leoflow-opera-rtc-") as temp_dir:
        stage_dir = Path(temp_dir)
        vv_tiles: list[Path] = []
        vh_tiles: list[Path] = []
        for index, item in enumerate(items, start=1):
            urls = _opera_asset_urls(item)
            granule = str(item.get("granuleName") or f"opera-item-{index}")
            if "vv" in urls:
                vv_tiles.append(
                    _download_with_auth(urls["vv"], stage_dir / f"{index:02d}-{granule}-vv.tif", username, password)
                )
            if "vh" in urls:
                vh_tiles.append(
                    _download_with_auth(urls["vh"], stage_dir / f"{index:02d}-{granule}-vh.tif", username, password)
                )

        if not vv_tiles or not vh_tiles:
            return []

        vv_output = _mosaic_geotiffs(vv_tiles, destination_dir / f"{vv_label}.tif")
        vh_output = _mosaic_geotiffs(vh_tiles, destination_dir / f"{vh_label}.tif")
        return [str(vv_output), str(vh_output)]


def _toolchain_focus_configured(spec: dict[str, Any]) -> bool:
    focus = spec.get("data", {}).get("toolchain", {}).get("focus")
    if not isinstance(focus, dict):
        return False
    return bool(str(focus.get("command") or "").strip())


def _has_existing_override_pair(root: Path, *, prefix: str | None = None) -> bool:
    names = {
        "vv" if prefix is None else f"{prefix}_vv",
        "sigma0_vv" if prefix is None else f"{prefix}_sigma0_vv",
        "post_event_vv" if prefix is None else f"post_event_{prefix}_vv",
    }
    vv_found = False
    vh_found = False
    for path in sorted(root.rglob("*.tif")):
        stem = path.stem.lower()
        if stem in names or stem.endswith("_vv"):
            vv_found = True
        if stem in {name.replace("vv", "vh") for name in names} or stem.endswith("_vh"):
            vh_found = True
    return vv_found and vh_found


def _download_opera_rtc_fallback(
    spec: dict[str, Any],
    *,
    region_bbox: list[float],
    overrides_dir: Path,
    username: str | None,
    password: str | None,
) -> tuple[list[str], dict[str, Any] | None]:
    if _toolchain_focus_configured(spec):
        return [], None

    post_event_dir = overrides_dir / "post_event"
    if _has_existing_override_pair(post_event_dir):
        return [], None

    platform = str(spec.get("data", {}).get("acquisition", {}).get("platform") or "").strip()
    granule_id = str(spec.get("data", {}).get("acquisition", {}).get("granule_id") or "").strip()
    absolute_orbit = _extract_absolute_orbit(granule_id) if granule_id else None
    start_time, end_time = _parse_time_range(str(spec["data"]["time"]))

    query = {
        "dataset": "OPERA-S1",
        "processingLevel": "RTC",
        "intersectsWith": _bbox_wkt(region_bbox),
        "start": _format_timestamp(start_time),
        "end": _format_timestamp(end_time),
        "maxResults": "200",
        "output": "json",
    }
    if platform:
        query["platform"] = platform
    post_items = _filter_opera_items(
        _search_asf(query, username, password),
        platform=platform or None,
        absolute_orbit=absolute_orbit,
    )
    if not post_items:
        return [], None

    downloaded_assets: list[str] = []
    downloaded_assets.extend(
        _download_opera_pair(
            post_items,
            post_event_dir,
            prefix=None,
            username=username,
            password=password,
        )
    )

    pre_items: list[dict[str, Any]] = []
    pre_query = {
        "dataset": "OPERA-S1",
        "processingLevel": "RTC",
        "intersectsWith": _bbox_wkt(region_bbox),
        "start": _format_timestamp(start_time - timedelta(days=48)),
        "end": _format_timestamp(start_time - timedelta(seconds=1)),
        "maxResults": "400",
        "output": "json",
    }
    if platform:
        pre_query["platform"] = platform
    pre_candidates = _search_asf(pre_query, username, password)
    pre_items = _select_previous_opera_items(pre_candidates, post_items, before=start_time)
    if pre_items:
        downloaded_assets.extend(
            _download_opera_pair(
                pre_items,
                overrides_dir / "pre_event",
                prefix="pre_event",
                username=username,
                password=password,
            )
        )

    return downloaded_assets, {
        "kind": "opera_rtc_fallback",
        "platform": platform,
        "absolute_orbit": absolute_orbit,
        "post_event_items": [str(item.get("granuleName") or "") for item in post_items],
        "pre_event_items": [str(item.get("granuleName") or "") for item in pre_items],
    }


@task("data", name="sentinel_1_asf_datapool_raw")
def main(ctx):
    spec = ctx.spec
    output_dir = ctx.output_dir
    project_dir = ctx.project_root
    downloads_dir = output_dir / "downloads"
    extracted_dir = output_dir / "extracted"
    overrides_dir = output_dir / "overrides"
    downloaded_assets: list[str] = []

    region_path = _resolve_path(project_dir, spec["data"]["region"])
    if region_path is None or not region_path.exists():
        raise FileNotFoundError(f"region resource not found: {spec['data']['region']}")
    bbox = _region_bbox(region_path)

    provider = spec["data"].get("provider", {})
    auth = provider.get("auth", {})
    username, password, password_source = _resolve_basic_auth(auth)

    download = spec["data"].get("download", {})
    url = str(download.get("url", "")).strip()
    if url:
        if str(download.get("auth_type", "")).lower() == "earthdata_basic":
            if not username:
                raise RuntimeError(
                    "download.auth_type is earthdata_basic but no username is configured. "
                    "Set data.provider.auth.username in workflow.yaml."
                )
            if not password:
                if password_source:
                    raise RuntimeError(
                        f"download.auth_type is earthdata_basic but no password was found. "
                        f"Set the {password_source} environment variable or put "
                        "data.provider.auth.password in workflow.yaml."
                    )
                raise RuntimeError(
                    "download.auth_type is earthdata_basic but no password is configured. "
                    "Set data.provider.auth.password_env and export that variable, "
                    "or put data.provider.auth.password in workflow.yaml."
                )
        filename = str(
            download.get("filename")
            or Path(urlparse(url).path).name
            or "sentinel-1-product.zip"
        )
        archive_path = _download_with_auth(url, downloads_dir / filename, username, password)
        downloaded_assets.append(str(archive_path))
        downloaded_assets.extend(_extract_archive(archive_path, extracted_dir))

    overrides = spec["data"].get("overrides", {})
    override_raster_dir = _resolve_path(project_dir, overrides.get("raster_dir"))
    override_pre_event_dir = _resolve_path(project_dir, overrides.get("pre_event_dir"))
    if override_raster_dir is not None:
        downloaded_assets.extend(_copy_tree(override_raster_dir, overrides_dir / "post_event"))
    if override_pre_event_dir is not None:
        downloaded_assets.extend(_copy_tree(override_pre_event_dir, overrides_dir / "pre_event"))

    fallback_assets, fallback = _download_opera_rtc_fallback(
        spec,
        region_bbox=bbox,
        overrides_dir=overrides_dir,
        username=username,
        password=password,
    )
    downloaded_assets.extend(fallback_assets)

    manifest_path = ctx.write_json(
        {
            "task": "sentinel_1_asf_datapool_raw",
            "provider": provider,
            "acquisition": spec["data"].get("acquisition", {}),
            "download": download,
            "auth": {
                "username": username,
                "password_source": password_source,
                "has_password": bool(password),
            },
            "region": str(region_path),
            "bbox": bbox,
            "fallback": fallback,
            "downloaded_assets": downloaded_assets,
            "data_dir": str(output_dir),
        },
        filename="query-result.json",
    )
    return ctx.result(
        source=spec["data"]["source"],
        region=spec["data"]["region"],
        time_range=spec["data"]["time"],
        resolution=spec["data"]["resolution"],
        manifest=str(manifest_path),
        downloaded_assets=downloaded_assets,
        data_dir=str(output_dir),
        status="loaded",
    )


if __name__ == "__main__":
    raise SystemExit(main())
