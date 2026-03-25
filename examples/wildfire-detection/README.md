# wildfire-detection

Runnable real-data wildfire example for the `python-minimal` EOFlowSpec template.

Version: `0.1.0`
Data source: `stac:stac_sentinel_2 @ https://earth-search.aws.element84.com/v1/search`
Region: `resources/polygon.geojson`
Time range: `2025-06-01/2025-08-01`
Resolution: `10m`
Features: `ndvi, ndwi`
Model output: `fire_mask`

What to edit:

- `workflow.yaml`: workflow configuration, dates, AOI, providers.
- `tasks/`: workflow business logic and helper code.
- `resources/`: AOI and other local static inputs.
- `tests/`: workflow checks.

Usually leave these alone:

- `runtime/`: generated runtime internals.
- `app.py`: small runnable entrypoint unless you need stage overrides.

Run:

```bash
pip install -r requirements.txt
python3 app.py
```

This example downloads real Sentinel-2 assets, runs the generated preprocessing and feature steps, and writes outputs under `artifacts/wildfire-detection/`.
Task files are intentionally thin: the single `@task(...)` decorator in `runtime/task_runtime.py` supplies `workflow.yaml`, path arguments, result serialization, and generic helpers like `ctx.artifact_path(...)` and `ctx.write_json(...)` so each task file focuses on EO logic. Reusable task helpers live under `tasks/lib/`. The STAC task reads provider settings from `workflow.yaml`, and the AOI for the query comes from `resources/polygon.geojson`.

Useful outputs:

- `artifacts/wildfire-detection/preprocessed/cloud_mask/true_color.png`
- `artifacts/wildfire-detection/fire_mask_preview.png`
- `artifacts/wildfire-detection/fire_mask.tif`
- `artifacts/wildfire-detection/last-run.json`

Notes:

- The default `fire_mask` task is a heuristic burn-scar proxy, not a downloaded ML checkpoint.
- `iou` stays unavailable until you add `resources/reference-fire-mask.tif`.
- The built-in STAC task picks a best single Sentinel-2 tile for the AOI. It does not mosaic all of Greece.

Test:

```bash
python3 -m unittest discover -s tests -p 'test_*.py'
```
