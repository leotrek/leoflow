from __future__ import annotations

import json
import math
import warnings
from pathlib import Path
from typing import Any

import numpy as np
import rasterio
from rasterio.crs import CRS
from rasterio.enums import Resampling
from rasterio.features import geometry_mask
from rasterio.transform import from_origin
from rasterio.warp import reproject, transform_geom

from runtime.task_support import load_workflow, project_root


def load_region_geometries(
    workflow_path: str | Path,
    *,
    target_crs: Any | None = None,
) -> list[dict[str, Any]]:
    region_path = _region_path(workflow_path)
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


def load_intersects_geometry(workflow_path: str | Path) -> dict[str, Any]:
    geometries = load_region_geometries(workflow_path)
    if len(geometries) == 1:
        return geometries[0]
    return {"type": "GeometryCollection", "geometries": geometries}


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


def scene_dirs(raw_root: str | Path, period: str) -> list[Path]:
    root = Path(raw_root) / period
    if not root.exists():
        return []
    return sorted(path for path in root.iterdir() if path.is_dir())


def band_paths_for_period(raw_root: str | Path, period: str, band_name: str) -> list[Path]:
    paths: list[Path] = []
    for scene_dir in scene_dirs(raw_root, period):
        band_path = find_band(scene_dir, band_name, required=False)
        if band_path is not None:
            paths.append(band_path)
    return paths


def workspace_band(workspace_root: str | Path, period: str, band_name: str) -> Path:
    path = Path(workspace_root) / period / f"{band_name}.tif"
    if not path.exists():
        raise FileNotFoundError(f"expected {path} to exist")
    return path


def feature_artifact(output_dir: str | Path, feature_name: str) -> Path:
    path = Path(output_dir) / f"{feature_name}.tif"
    if not path.exists():
        raise FileNotFoundError(f"expected {path} to exist")
    return path


def project_band(
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


def mean_composite(
    source_paths: list[Path],
    profile: dict[str, Any],
    geometries: list[dict[str, Any]],
    *,
    resampling: Resampling = Resampling.bilinear,
) -> np.ndarray:
    if not source_paths:
        raise FileNotFoundError("no source scenes were provided for compositing")
    stack = [apply_aoi_mask(project_band(path, profile, resampling=resampling), profile, geometries) for path in source_paths]
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=RuntimeWarning)
        composite = np.nanmean(np.stack(stack, axis=0), axis=0)
    return composite.astype("float32")


def read_raster(path: str | Path) -> tuple[np.ndarray, dict[str, Any]]:
    with rasterio.open(path) as src:
        data = src.read(1, masked=True)
        profile = src.profile.copy()
    return _masked_to_float(data), profile


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


def compute_nbr(nir: np.ndarray, swir22: np.ndarray) -> np.ndarray:
    with np.errstate(divide="ignore", invalid="ignore"):
        result = (nir - swir22) / (nir + swir22)
    result = result.astype("float32")
    result[~np.isfinite(result)] = np.nan
    return result


def pixel_area_hectares(profile: dict[str, Any]) -> float:
    transform = profile["transform"]
    return abs(float(transform.a) * float(transform.e)) / 10000.0


def find_band(root: str | Path, band_name: str, *, required: bool = True) -> Path | None:
    expected = band_name.lower()
    for path in sorted(Path(root).rglob("*.tif")):
        stem = path.stem.lower()
        if stem == expected or stem.endswith(f"_{expected}"):
            return path
    if required:
        raise FileNotFoundError(f"band {band_name} not found under {root}")
    return None


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


def stretch_to_uint8(values: np.ndarray, *, low: float = 2.0, high: float = 98.0) -> np.ndarray:
    valid = values[np.isfinite(values)]
    stretched = np.full(values.shape, 255, dtype="uint8")
    if valid.size == 0:
        return stretched
    lo = float(np.nanpercentile(valid, low))
    hi = float(np.nanpercentile(valid, high))
    if hi <= lo:
        stretched[np.isfinite(values)] = 127
        return stretched
    scaled = np.clip((values - lo) / (hi - lo), 0.0, 1.0)
    stretched[np.isfinite(values)] = np.round(scaled[np.isfinite(values)] * 255.0).astype("uint8")
    return stretched


def write_grayscale_png(path: str | Path, values: np.ndarray) -> None:
    image = stretch_to_uint8(values)
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(
        target,
        "w",
        driver="PNG",
        width=image.shape[1],
        height=image.shape[0],
        count=1,
        dtype="uint8",
    ) as dst:
        dst.write(image, 1)


def write_mask_overlay_png(path: str | Path, base: np.ndarray, mask_values: np.ndarray) -> None:
    gray = stretch_to_uint8(base)
    rgb = np.stack([gray, gray, gray], axis=0)
    burned = mask_values == 1
    background = mask_values == 0
    rgb[:, ~background & ~burned] = 255
    rgb[0, burned] = 255
    rgb[1, burned] = 96
    rgb[2, burned] = 0
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(
        target,
        "w",
        driver="PNG",
        width=rgb.shape[2],
        height=rgb.shape[1],
        count=3,
        dtype="uint8",
    ) as dst:
        dst.write(rgb)


def _region_path(workflow_path: str | Path) -> Path:
    spec = load_workflow(workflow_path)
    region_path = Path(spec["data"]["region"])
    if region_path.is_absolute():
        return region_path
    return project_root(workflow_path) / region_path


def _geometry_bounds(geometries: list[dict[str, Any]]) -> tuple[float, float, float, float]:
    x_coords = [coord for geometry in geometries for coord in _collect_coordinates(geometry, axis=0)]
    y_coords = [coord for geometry in geometries for coord in _collect_coordinates(geometry, axis=1)]
    min_x = min(x_coords)
    min_y = min(y_coords)
    max_x = max(x_coords)
    max_y = max(y_coords)
    return min_x, min_y, max_x, max_y


def _collect_coordinates(geometry: dict[str, Any], *, axis: int) -> list[float]:
    geometry_type = geometry.get("type")
    if geometry_type == "GeometryCollection":
        coords: list[float] = []
        for item in geometry.get("geometries", []):
            coords.extend(_collect_coordinates(item, axis=axis))
        return coords
    return _flatten_coords(geometry.get("coordinates", []), axis=axis)


def _flatten_coords(value: Any, *, axis: int) -> list[float]:
    if isinstance(value, (list, tuple)):
        if value and isinstance(value[0], (int, float)):
            return [float(value[axis])]
        coords: list[float] = []
        for item in value:
            coords.extend(_flatten_coords(item, axis=axis))
        return coords
    raise TypeError(f"unsupported coordinate value: {value!r}")


def _masked_to_float(values: np.ndarray | np.ma.MaskedArray[Any]) -> np.ndarray:
    array = np.asarray(values, dtype="float32")
    if np.ma.isMaskedArray(values):
        array = np.where(np.ma.getmaskarray(values), np.nan, array)
    return array.astype("float32")
