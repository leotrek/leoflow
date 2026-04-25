from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from runtime.task_support import load_workflow, write_json
from tasks.lib.sar import (
    find_raster,
    linear_to_db,
    mask_to_feature_collection,
    pixel_area_km2,
    rasterize_resource,
    read_raster,
    remove_small_regions,
    write_geojson,
    write_uint8_raster,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workflow", required=True)
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    spec = load_workflow(args.workflow)
    input_dir = Path(args.input)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    parameters = spec["model"].get("parameters", {})
    detection = parameters.get("water_detection", {})
    postprocess = parameters.get("postprocess", {})
    resources = parameters.get("resources", {})

    vv, profile = read_raster(find_raster(input_dir, "sigma0_vv"))
    vh, _ = read_raster(find_raster(input_dir, "sigma0_vh"))
    ratio, _ = read_raster(find_raster(input_dir, "vv_vh_ratio"))

    valid = np.isfinite(vv) & np.isfinite(vh) & np.isfinite(ratio)
    flood = (
        valid
        & (vv <= float(detection.get("vv_threshold", 0.06)))
        & (vh <= float(detection.get("vh_threshold", 0.02)))
        & (ratio >= float(detection.get("ratio_threshold", 1.25)))
    )

    pre_event_vv_path = find_raster(input_dir, "pre_event_sigma0_vv", required=False)
    used_pre_event = False
    if bool(detection.get("use_pre_event_change", True)) and pre_event_vv_path is not None:
        pre_event_vv, _ = read_raster(pre_event_vv_path)
        change_db = linear_to_db(vv) - linear_to_db(pre_event_vv)
        flood &= np.isfinite(change_db) & (change_db <= float(detection.get("change_threshold_db", -1.5)))
        used_pre_event = True

    flood = remove_small_regions(flood, int(postprocess.get("min_region_pixels", 25)))

    permanent_water_mask = rasterize_resource(
        args.workflow,
        resources.get("permanent_water", "resources/permanent-water.geojson"),
        profile,
    )
    if permanent_water_mask is not None:
        flood &= ~permanent_water_mask

    flood = remove_small_regions(flood, int(postprocess.get("min_region_pixels", 25)))

    mask = np.full(vv.shape, 255, dtype="uint8")
    mask[valid] = 0
    mask[flood] = 1
    write_uint8_raster(output_path, mask, profile, nodata=255)

    polygons = mask_to_feature_collection(flood, profile)
    polygon_path = output_path.with_name("flood_polygons.geojson")
    write_geojson(polygon_path, polygons)

    overlay_stats: dict[str, dict[str, object]] = {}
    for name, default_path in (
        ("roads", "resources/roads.geojson"),
        ("settlements", "resources/settlements.geojson"),
        ("agriculture", "resources/agriculture.geojson"),
    ):
        overlay_mask = rasterize_resource(args.workflow, resources.get(name, default_path), profile)
        if overlay_mask is None:
            overlay_stats[name] = {"present": False, "flooded_pixels": 0, "flooded_area_km2": 0.0}
            continue
        flooded_pixels = int(np.logical_and(flood, overlay_mask).sum())
        overlay_stats[name] = {
            "present": True,
            "flooded_pixels": flooded_pixels,
            "flooded_area_km2": flooded_pixels * pixel_area_km2(profile),
        }

    summary_path = output_path.with_name("flood_summary.json")
    summary = {
        "workflow": spec["workflow"]["name"],
        "artifact": str(output_path),
        "vector_polygons": str(polygon_path),
        "flooded_pixels": int(flood.sum()),
        "affected_area_km2": float(flood.sum()) * pixel_area_km2(profile),
        "polygon_count": len(polygons["features"]),
        "used_pre_event_change": used_pre_event,
        "thresholds": detection,
        "postprocess": postprocess,
        "overlay_stats": overlay_stats,
    }
    write_json(summary_path, summary)

    print(
        json.dumps(
            {
                "artifact": str(output_path),
                "vector_polygons": str(polygon_path),
                "summary": str(summary_path),
                "status": "executed",
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
