from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from runtime.task_support import load_workflow, write_json  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workflow", required=True)
    parser.add_argument("--prediction", required=True)
    parser.add_argument("--report", required=True)
    args = parser.parse_args()

    spec = load_workflow(args.workflow)
    prediction_path = Path(args.prediction)
    report_path = Path(args.report)
    summary_path = prediction_path.with_name("flood_summary.json")
    polygon_path = prediction_path.with_name("flood_polygons.geojson")

    if not summary_path.exists():
        raise FileNotFoundError(f"expected flood summary to exist at {summary_path}")

    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    report = {
        "workflow": spec["workflow"]["name"],
        "prediction": str(prediction_path),
        "flood_summary": str(summary_path),
        "vector_polygons": str(polygon_path) if polygon_path.exists() else None,
        "affected_area_km2": summary.get("affected_area_km2"),
        "polygon_count": summary.get("polygon_count"),
        "overlay_stats": summary.get("overlay_stats"),
        "status": "evaluated",
    }
    write_json(report_path, report)
    print(json.dumps({"artifact": str(report_path), "status": "evaluated"}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
