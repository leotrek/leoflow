from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _load_module():
    module_path = ROOT / "tasks" / "data" / "sentinel_1_asf_datapool_raw.py"
    spec = importlib.util.spec_from_file_location("sentinel_1_asf_datapool_raw", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(ROOT))
    try:
        spec.loader.exec_module(module)
    finally:
        sys.path.pop(0)
    return module


class Sentinel1AsfDatapoolRawTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.module = _load_module()

    def test_flatten_search_results_collects_nested_granules(self) -> None:
        payload = [[{"granuleName": "one"}, {"granuleName": "two"}], {"ignored": [{"granuleName": "three"}]}]

        items = self.module._flatten_search_results(payload)

        self.assertEqual(
            [item["granuleName"] for item in items],
            ["three", "two", "one"],
        )

    def test_filter_opera_items_prefers_platform_and_orbit(self) -> None:
        items = [
            {"platform": "Sentinel-1A", "absoluteOrbit": 7000, "startTime": "2026-04-25T05:09:13Z"},
            {"platform": "Sentinel-1C", "absoluteOrbit": 7368, "startTime": "2026-04-25T05:09:14Z"},
            {"platform": "Sentinel-1C", "absoluteOrbit": 7368, "startTime": "2026-04-25T05:09:12Z"},
        ]

        filtered = self.module._filter_opera_items(items, platform="Sentinel-1C", absolute_orbit=7368)

        self.assertEqual([item["startTime"] for item in filtered], ["2026-04-25T05:09:12Z", "2026-04-25T05:09:14Z"])

    def test_select_previous_opera_items_uses_latest_matching_orbit_cycle(self) -> None:
        reference_items = [
            {
                "platform": "Sentinel-1C",
                "relativeOrbit": 22,
                "track": 22,
                "flightDirection": "DESCENDING",
                "absoluteOrbit": 7368,
                "startTime": "2026-04-25T05:09:14Z",
            }
        ]
        items = [
            {
                "platform": "Sentinel-1C",
                "relativeOrbit": 22,
                "track": 22,
                "flightDirection": "DESCENDING",
                "absoluteOrbit": 7346,
                "startTime": "2026-04-23T16:34:02Z",
            },
            {
                "platform": "Sentinel-1C",
                "relativeOrbit": 22,
                "track": 22,
                "flightDirection": "DESCENDING",
                "absoluteOrbit": 7346,
                "startTime": "2026-04-23T16:34:05Z",
            },
            {
                "platform": "Sentinel-1C",
                "relativeOrbit": 22,
                "track": 22,
                "flightDirection": "DESCENDING",
                "absoluteOrbit": 7324,
                "startTime": "2026-04-20T16:34:02Z",
            },
            {
                "platform": "Sentinel-1A",
                "relativeOrbit": 22,
                "track": 22,
                "flightDirection": "DESCENDING",
                "absoluteOrbit": 9000,
                "startTime": "2026-04-24T16:34:02Z",
            },
        ]

        selected = self.module._select_previous_opera_items(
            items,
            reference_items,
            before=self.module._parse_timestamp("2026-04-25T05:09:14Z"),
        )

        self.assertEqual([item["absoluteOrbit"] for item in selected], [7346, 7346])
        self.assertEqual(
            [item["startTime"] for item in selected],
            ["2026-04-23T16:34:02Z", "2026-04-23T16:34:05Z"],
        )

    def test_opera_asset_urls_extracts_vv_and_vh(self) -> None:
        item = {
            "opera": {
                "additionalUrls": [
                    "https://example.com/product_VH.tif",
                    "https://example.com/product.iso.xml",
                    "https://example.com/product_VV.tif",
                    "https://example.com/product_mask.tif",
                ]
            }
        }

        urls = self.module._opera_asset_urls(item)

        self.assertEqual(urls["vv"], "https://example.com/product_VV.tif")
        self.assertEqual(urls["vh"], "https://example.com/product_VH.tif")
        self.assertEqual(urls["mask"], "https://example.com/product_mask.tif")


if __name__ == "__main__":
    unittest.main()
