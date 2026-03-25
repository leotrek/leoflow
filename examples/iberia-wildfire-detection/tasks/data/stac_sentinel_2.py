from __future__ import annotations

from pathlib import Path

from runtime.task_runtime import task
from runtime.task_support import download_file, http_json, normalize_datetime_interval, write_json
from tasks.lib.eo import load_intersects_geometry


@task("data", name="stac_sentinel_2")
def main(ctx):
    source = ctx.spec["data"]["source"]
    if not isinstance(source, dict) or source.get("kind") != "stac":
        raise RuntimeError("workflow data.source must be a STAC mapping for this task")

    windows = ctx.spec["data"].get("windows") or {"scene": ctx.spec["data"]["time"]}
    intersects = load_intersects_geometry(ctx.workflow_path)
    asset_names = [str(asset) for asset in source.get("assets") or []]
    raw_root = Path(ctx.output_dir)
    metadata_root = raw_root.parent

    downloaded_assets: list[str] = []
    period_results: dict[str, dict[str, object]] = {}

    for period_name, period_window in windows.items():
        payload = {
            "collections": [source["collection"]],
            "limit": int(source.get("limit", 100)),
            "datetime": normalize_datetime_interval(str(period_window)),
            "intersects": intersects,
        }
        if source.get("query"):
            payload["query"] = source["query"]

        search_result = http_json(
            str(source["api_url"]),
            payload=payload,
            method="POST",
            headers=source.get("headers"),
            timeout=60,
        )
        search_results_path = metadata_root / f"search-results-{period_name}.json"
        write_json(search_results_path, search_result)

        period_assets: list[str] = []
        item_count = 0
        for item in search_result.get("features", []):
            item_assets = item.get("assets", {})
            item_hrefs = {
                asset_name: item_assets.get(asset_name, {}).get("href")
                for asset_name in asset_names
            }
            if not all(item_hrefs.values()):
                continue

            item_id = str(item.get("id", "item"))
            item_dir = raw_root / period_name / item_id
            item_dir.mkdir(parents=True, exist_ok=True)
            item_count += 1

            for asset_name, href in item_hrefs.items():
                output_path = item_dir / f"{asset_name}.tif"
                download_file(
                    str(href),
                    output_path,
                    headers=source.get("asset_headers"),
                    timeout=120,
                )
                downloaded_assets.append(str(output_path))
                period_assets.append(str(output_path))

        if item_count == 0:
            raise RuntimeError(
                f"no STAC items with assets {asset_names} were returned for {period_name} ({period_window})"
            )

        period_results[period_name] = {
            "window": str(period_window),
            "items": item_count,
            "search_results": str(search_results_path),
            "downloaded_assets": period_assets,
        }

    payload = {
        "source": source,
        "region": ctx.spec["data"]["region"],
        "time_range": ctx.spec["data"]["time"],
        "windows": {key: str(value) for key, value in windows.items()},
        "resolution": ctx.spec["data"]["resolution"],
        "periods": period_results,
        "downloaded_assets": downloaded_assets,
        "data_dir": str(raw_root),
        "status": "loaded",
    }
    return ctx.report(
        payload,
        path_field="summary",
        merge=True,
        status="loaded",
    )


if __name__ == "__main__":
    raise SystemExit(main())
