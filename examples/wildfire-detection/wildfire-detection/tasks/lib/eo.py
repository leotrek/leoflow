from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import numpy as np
import rasterio
from rasterio.enums import Resampling
from rasterio.mask import mask
from rasterio.vrt import WarpedVRT
from rasterio.warp import transform_geom

from runtime.task_support import load_workflow, project_root


CLOUD_CLASSES = {0, 1, 3, 8, 9, 10, 11}


def load_region_geometries(
    workflow_path: str | Path,
    *,
    target_crs: Any | None = None,
) -> list[dict[str, Any]]:
    spec = load_workflow(workflow_path)
    region_path = Path(spec["data"]["region"])
    if not region_path.is_absolute():
        region_path = project_root(workflow_path) / region_path
    data = json.loads(region_path.read_text(encoding="utf-8"))
    if data.get("type") == "FeatureCollection":
        geometries = [feature["geometry"] for feature in data.get("features", []) if feature.get("geometry")]
    elif data.get("type") == "Feature":
        geometries = [data["geometry"]]
    else:
        geometries = [data]

    if target_crs:
        target = target_crs.to_string() if hasattr(target_crs, "to_string") else str(target_crs)
        geometries = [transform_geom("EPSG:4326", target, geometry) for geometry in geometries]
    return geometries


def first_scene_dir(input_dir: str | Path) -> Path:
    root = Path(input_dir)
    candidates = sorted(path for path in root.iterdir() if path.is_dir()) if root.exists() else []
    for candidate in candidates:
        if any(candidate.glob("*.tif")):
            return candidate
    tiffs = sorted(root.rglob("*.tif")) if root.exists() else []
    if tiffs:
        return tiffs[0].parent
    raise FileNotFoundError(f"no GeoTIFF assets found under {root}")


def find_band(input_dir: str | Path, band_name: str, required: bool = True) -> Path | None:
    root = Path(input_dir)
    expected = band_name.upper()
    for path in sorted(root.rglob("*.tif")):
        stem = path.stem.upper()
        if stem == expected or stem.endswith(f"_{expected}"):
            return path
    if required:
        raise FileNotFoundError(f"band {band_name} not found under {root}")
    return None


def reference_grid(scene_dir: str | Path, workflow_path: str | Path) -> tuple[np.ndarray, dict[str, Any]]:
    ref_path = find_band(scene_dir, "B04")
    with rasterio.open(ref_path) as src:
        shapes = load_region_geometries(workflow_path, target_crs=src.crs)
        clipped, transform = mask(src, shapes, crop=True, filled=False)
        array = _masked_to_float(clipped[0])
        profile = src.profile.copy()
    profile.update(
        driver="GTiff",
        count=1,
        dtype="float32",
        height=array.shape[0],
        width=array.shape[1],
        transform=transform,
        compress="lzw",
        nodata=np.nan,
    )
    return array, profile


def read_aligned(path: str | Path, profile: dict[str, Any], *, resampling: Resampling) -> np.ndarray:
    with rasterio.open(path) as src:
        with WarpedVRT(
            src,
            crs=profile["crs"],
            transform=profile["transform"],
            width=profile["width"],
            height=profile["height"],
            resampling=resampling,
        ) as vrt:
            data = vrt.read(1, masked=True)
    return _masked_to_float(data)


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


def compute_index(numerator: np.ndarray, denominator: np.ndarray) -> np.ndarray:
    with np.errstate(divide="ignore", invalid="ignore"):
        result = (numerator - denominator) / (numerator + denominator)
    result = result.astype("float32")
    result[~np.isfinite(result)] = np.nan
    return result


def stretch_to_uint8(values: np.ndarray, low: float = 2.0, high: float = 98.0) -> np.ndarray:
    valid = values[np.isfinite(values)]
    output = np.full(values.shape, 255, dtype="uint8")
    if valid.size == 0:
        return output
    lo = float(np.nanpercentile(valid, low))
    hi = float(np.nanpercentile(valid, high))
    if hi <= lo:
        output[np.isfinite(values)] = 127
        return output
    scaled = (values - lo) / (hi - lo)
    scaled = np.clip(scaled, 0.0, 1.0)
    output[np.isfinite(values)] = np.round(scaled[np.isfinite(values)] * 255.0).astype("uint8")
    return output


def write_rgb_png(path: str | Path, red: np.ndarray, green: np.ndarray, blue: np.ndarray) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    rgb = np.stack(
        [
            stretch_to_uint8(red),
            stretch_to_uint8(green),
            stretch_to_uint8(blue),
        ],
        axis=0,
    )
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


def write_mask_overlay_png(
    path: str | Path,
    red: np.ndarray,
    green: np.ndarray,
    blue: np.ndarray,
    mask_values: np.ndarray,
) -> None:
    base = np.stack(
        [
            stretch_to_uint8(red),
            stretch_to_uint8(green),
            stretch_to_uint8(blue),
        ],
        axis=0,
    )
    overlay = base.copy()
    detected = mask_values == 1
    background = mask_values == 0
    overlay[0, detected] = 255
    overlay[1, detected] = 80
    overlay[2, detected] = 0
    overlay[:, background] = np.maximum(overlay[:, background], 32)
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(
        target,
        "w",
        driver="PNG",
        width=overlay.shape[2],
        height=overlay.shape[1],
        count=3,
        dtype="uint8",
    ) as dst:
        dst.write(overlay)


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


def latest_preprocess_dir(workflow_path: str | Path, artifacts_dir: str | Path) -> Path:
    spec = load_workflow(workflow_path)
    last_named_step = None
    for step in spec["preprocessing"]:
        name = next(iter(step.keys()))
        if name != "command":
            last_named_step = name
    root = Path(artifacts_dir)
    if last_named_step is None:
        return root / "preprocessed"
    return root / "preprocessed" / task_slug(last_named_step)


def task_slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def _masked_to_float(values: np.ndarray | np.ma.MaskedArray[Any]) -> np.ndarray:
    array = np.asarray(values, dtype="float32")
    if np.ma.isMaskedArray(values):
        array = np.where(np.ma.getmaskarray(values), np.nan, array)
    return array.astype("float32")
