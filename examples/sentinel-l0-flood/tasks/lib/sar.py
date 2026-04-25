from __future__ import annotations

import json
import math
import shutil
from collections import deque
from pathlib import Path
from typing import Any

import numpy as np
import rasterio
from rasterio.crs import CRS
from rasterio.enums import Resampling
from rasterio.features import geometry_mask, rasterize, shapes
from rasterio.transform import from_origin
from rasterio.warp import reproject, transform_geom

from runtime.task_support import load_workflow, project_root, write_json


def resolve_path(workflow_path: str | Path, value: str | Path | None) -> Path | None:
    if not value:
        return None
    path = Path(value)
    if path.is_absolute():
        return path
    return project_root(workflow_path) / path


def load_region_geometries(
    workflow_path: str | Path,
    *,
    target_crs: Any | None = None,
) -> list[dict[str, Any]]:
    spec = load_workflow(workflow_path)
    region_path = resolve_path(workflow_path, spec["data"]["region"])
    if region_path is None or not region_path.exists():
        raise FileNotFoundError(f"region geometry not found: {spec['data']['region']}")
    data = json.loads(region_path.read_text(encoding="utf-8"))
    if data.get("type") == "FeatureCollection":
        geometries = [feature["geometry"] for feature in data.get("features", []) if feature.get("geometry")]
    elif data.get("type") == "Feature":
        geometries = [data["geometry"]]
    else:
        geometries = [data]

    if target_crs is not None:
        target = target_crs.to_string() if hasattr(target_crs, "to_string") else str(target_crs)
        geometries = [transform_geom("EPSG:4326", target, geometry) for geometry in geometries]
    return geometries


def target_profile(workflow_path: str | Path) -> dict[str, Any]:
    spec = load_workflow(workflow_path)
    target_crs = CRS.from_user_input(spec["data"].get("crs", "EPSG:4326"))
    resolution = resolution_value(spec["data"].get("resolution", 10))
    geometries = load_region_geometries(workflow_path, target_crs=target_crs)
    left, bottom, right, top = _geometry_bounds(geometries)
    width = max(1, int(math.ceil((right - left) / resolution)))
    height = max(1, int(math.ceil((top - bottom) / resolution)))
    transform = from_origin(left, top, resolution, resolution)
    return {
        "driver": "GTiff",
        "count": 1,
        "dtype": "float32",
        "crs": target_crs,
        "transform": transform,
        "width": width,
        "height": height,
        "compress": "lzw",
        "nodata": np.nan,
    }


def resolution_value(value: str | int | float) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().lower()
    if text.endswith("m"):
        text = text[:-1]
    return float(text)


def iter_tiffs(root: str | Path) -> list[Path]:
    base = Path(root)
    if not base.exists():
        return []
    return sorted(path for path in base.rglob("*.tif") if path.is_file())


def find_raster(root: str | Path, *aliases: str, required: bool = True) -> Path | None:
    expected = {alias.lower() for alias in aliases}
    for path in iter_tiffs(root):
        stem = path.stem.lower()
        if stem in expected:
            return path
        if any(stem.endswith(f"_{alias}") for alias in expected):
            return path
    if required:
        raise FileNotFoundError(f"could not find any of {aliases!r} under {root}")
    return None


def read_raster(path: str | Path) -> tuple[np.ndarray, dict[str, Any]]:
    with rasterio.open(path) as src:
        data = src.read(1, masked=True)
        profile = src.profile.copy()
    return masked_to_float(data), profile


def reproject_raster(
    source_path: str | Path,
    profile: dict[str, Any],
    *,
    resampling: Resampling = Resampling.bilinear,
) -> np.ndarray:
    destination = np.full((int(profile["height"]), int(profile["width"])), np.nan, dtype="float32")
    with rasterio.open(source_path) as src:
        reproject(
            source=rasterio.band(src, 1),
            destination=destination,
            src_transform=src.transform,
            src_crs=src.crs,
            src_nodata=src.nodata,
            dst_transform=profile["transform"],
            dst_crs=profile["crs"],
            dst_nodata=np.nan,
            resampling=resampling,
            init_dest_nodata=True,
        )
    return destination.astype("float32")


def apply_aoi_mask(
    values: np.ndarray,
    profile: dict[str, Any],
    geometries: list[dict[str, Any]],
) -> np.ndarray:
    inside = geometry_mask(
        geometries,
        out_shape=(int(profile["height"]), int(profile["width"])),
        transform=profile["transform"],
        invert=True,
    )
    masked = values.astype("float32", copy=True)
    masked[~inside] = np.nan
    return masked


def write_float_raster(path: str | Path, data: np.ndarray, profile: dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    raster_profile = profile.copy()
    raster_profile.update(driver="GTiff", count=1, dtype="float32", compress="lzw", nodata=np.nan)
    with rasterio.open(target, "w", **raster_profile) as dst:
        dst.write(data.astype("float32"), 1)


def write_uint8_raster(path: str | Path, data: np.ndarray, profile: dict[str, Any], *, nodata: int = 255) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    raster_profile = profile.copy()
    raster_profile.update(driver="GTiff", count=1, dtype="uint8", compress="lzw", nodata=nodata)
    with rasterio.open(target, "w", **raster_profile) as dst:
        dst.write(data.astype("uint8"), 1)


def write_geojson(path: str | Path, payload: dict[str, Any]) -> Path:
    return write_json(path, payload)


def copy_raster(source_path: str | Path, destination_path: str | Path) -> Path:
    source = Path(source_path)
    destination = Path(destination_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)
    return destination


def masked_to_float(values: np.ndarray | np.ma.MaskedArray[Any]) -> np.ndarray:
    array = np.asarray(values, dtype="float32")
    if np.ma.isMaskedArray(values):
        array = np.where(np.ma.getmaskarray(values), np.nan, array)
    return array.astype("float32")


def stats(values: np.ndarray | None) -> dict[str, float | None] | None:
    if values is None:
        return None
    valid = values[np.isfinite(values)]
    if valid.size == 0:
        return {"min": None, "max": None, "mean": None}
    return {
        "min": float(valid.min()),
        "max": float(valid.max()),
        "mean": float(valid.mean()),
    }


def to_linear_backscatter(values: np.ndarray) -> np.ndarray:
    valid = values[np.isfinite(values)]
    if valid.size == 0:
        return values.astype("float32")
    converted = values.astype("float32", copy=True)
    p95 = float(np.nanpercentile(valid, 95))
    p05 = float(np.nanpercentile(valid, 5))
    if p95 <= 5.0 and p05 < 0.0:
        converted[np.isfinite(converted)] = np.power(10.0, converted[np.isfinite(converted)] / 10.0)
    converted[converted < 0.0] = 0.0
    return converted


def linear_to_db(values: np.ndarray, *, floor: float = 1.0e-6) -> np.ndarray:
    result = np.full(values.shape, np.nan, dtype="float32")
    valid = np.isfinite(values) & (values > 0.0)
    if not np.any(valid):
        return result
    result[valid] = 10.0 * np.log10(np.maximum(values[valid], floor))
    return result.astype("float32")


def mean_filter(values: np.ndarray, *, kernel_size: int = 3) -> np.ndarray:
    if kernel_size % 2 == 0:
        raise ValueError("kernel_size must be odd")
    pad = kernel_size // 2
    finite = np.isfinite(values)
    if not np.any(finite):
        return np.full(values.shape, np.nan, dtype="float32")

    padded_values = np.pad(np.where(finite, values, 0.0), pad, mode="edge")
    padded_counts = np.pad(finite.astype("float32"), pad, mode="edge")
    total = np.zeros(values.shape, dtype="float32")
    counts = np.zeros(values.shape, dtype="float32")
    for row in range(kernel_size):
        for col in range(kernel_size):
            total += padded_values[row: row + values.shape[0], col: col + values.shape[1]]
            counts += padded_counts[row: row + values.shape[0], col: col + values.shape[1]]
    filtered = np.full(values.shape, np.nan, dtype="float32")
    valid = counts > 0.0
    filtered[valid] = total[valid] / counts[valid]
    return filtered


_NEIGHBOR_OFFSETS = (
    (-1, -1),
    (-1, 0),
    (-1, 1),
    (0, -1),
    (0, 1),
    (1, -1),
    (1, 0),
    (1, 1),
)


def _neighbor_pixels(row: int, col: int, height: int, width: int) -> list[tuple[int, int]]:
    neighbors: list[tuple[int, int]] = []
    for d_row, d_col in _NEIGHBOR_OFFSETS:
        next_row = row + d_row
        next_col = col + d_col
        if next_row < 0 or next_col < 0 or next_row >= height or next_col >= width:
            continue
        neighbors.append((next_row, next_col))
    return neighbors


def _collect_component(
    source: np.ndarray,
    visited: np.ndarray,
    row: int,
    col: int,
) -> list[tuple[int, int]]:
    height, width = source.shape
    component: list[tuple[int, int]] = []
    queue: deque[tuple[int, int]] = deque([(row, col)])
    visited[row, col] = True

    while queue:
        current_row, current_col = queue.popleft()
        component.append((current_row, current_col))
        for next_row, next_col in _neighbor_pixels(current_row, current_col, height, width):
            if visited[next_row, next_col] or not source[next_row, next_col]:
                continue
            visited[next_row, next_col] = True
            queue.append((next_row, next_col))

    return component


def remove_small_regions(mask: np.ndarray, min_pixels: int) -> np.ndarray:
    if min_pixels <= 1:
        return mask.astype(bool, copy=True)

    source = mask.astype(bool, copy=False)
    visited = np.zeros(source.shape, dtype=bool)
    kept = np.zeros(source.shape, dtype=bool)
    height, width = source.shape

    for row in range(height):
        for col in range(width):
            if visited[row, col] or not source[row, col]:
                continue
            component = _collect_component(source, visited, row, col)
            if len(component) >= min_pixels:
                for current_row, current_col in component:
                    kept[current_row, current_col] = True
    return kept


def rasterize_resource(
    workflow_path: str | Path,
    resource_value: str | Path | None,
    profile: dict[str, Any],
) -> np.ndarray | None:
    resource_path = resolve_path(workflow_path, resource_value)
    if resource_path is None or not resource_path.exists():
        return None
    data = json.loads(resource_path.read_text(encoding="utf-8"))
    if data.get("type") == "FeatureCollection":
        geometries = [feature["geometry"] for feature in data.get("features", []) if feature.get("geometry")]
    elif data.get("type") == "Feature":
        geometries = [data["geometry"]]
    else:
        geometries = [data]
    target_crs = profile["crs"]
    target = target_crs.to_string() if hasattr(target_crs, "to_string") else str(target_crs)
    transformed = [transform_geom("EPSG:4326", target, geometry) for geometry in geometries]
    return rasterize_geometries(transformed, profile)


def rasterize_geometries(geometries: list[dict[str, Any]], profile: dict[str, Any]) -> np.ndarray:
    return rasterize(
        [(geometry, 1) for geometry in geometries],
        out_shape=(int(profile["height"]), int(profile["width"])),
        transform=profile["transform"],
        fill=0,
        all_touched=True,
        dtype="uint8",
    ).astype(bool)


def pixel_area_km2(profile: dict[str, Any]) -> float:
    transform = profile["transform"]
    return abs(float(transform.a) * float(transform.e)) / 1_000_000.0


def mask_to_feature_collection(mask: np.ndarray, profile: dict[str, Any]) -> dict[str, Any]:
    features: list[dict[str, Any]] = []
    pixel_area = pixel_area_km2(profile)
    source_crs = profile.get("crs")
    source_crs_text = source_crs.to_string() if hasattr(source_crs, "to_string") else str(source_crs or "")
    for index, (geometry, value) in enumerate(
        shapes(mask.astype("uint8"), mask=mask.astype(bool), transform=profile["transform"]),
        start=1,
    ):
        if int(value) != 1:
            continue
        geom_mask = rasterize(
            [(geometry, 1)],
            out_shape=mask.shape,
            transform=profile["transform"],
            fill=0,
            all_touched=False,
            dtype="uint8",
        ).astype(bool)
        pixel_count = int(np.logical_and(mask, geom_mask).sum())
        output_geometry = geometry
        if source_crs_text and source_crs_text != "EPSG:4326":
            output_geometry = transform_geom(source_crs_text, "EPSG:4326", geometry)
        features.append(
            {
                "type": "Feature",
                "geometry": output_geometry,
                "properties": {
                    "id": index,
                    "pixel_count": pixel_count,
                    "area_km2": pixel_count * pixel_area,
                },
            }
        )
    return {"type": "FeatureCollection", "features": features}


def _geometry_bounds(geometries: list[dict[str, Any]]) -> tuple[float, float, float, float]:
    x_coords = [coord for geometry in geometries for coord in _collect_coordinates(geometry, axis=0)]
    y_coords = [coord for geometry in geometries for coord in _collect_coordinates(geometry, axis=1)]
    return min(x_coords), min(y_coords), max(x_coords), max(y_coords)


def _collect_coordinates(geometry: dict[str, Any], *, axis: int) -> list[float]:
    geometry_type = geometry.get("type")
    if geometry_type == "GeometryCollection":
        values: list[float] = []
        for item in geometry.get("geometries", []):
            values.extend(_collect_coordinates(item, axis=axis))
        return values
    return _flatten_coords(geometry.get("coordinates", []), axis=axis)


def _flatten_coords(value: Any, *, axis: int) -> list[float]:
    if isinstance(value, (list, tuple)):
        if value and isinstance(value[0], (int, float)):
            return [float(value[axis])]
        values: list[float] = []
        for item in value:
            values.extend(_flatten_coords(item, axis=axis))
        return values
    raise TypeError(f"unsupported coordinate payload: {value!r}")
