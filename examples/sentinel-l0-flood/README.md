# sentinel-l0-flood

Sentinel-1 flood mapping example for LeoFlow. This example is configured around one Sentinel-1C acquisition over eastern Austria on April 25, 2026 and produces raster, vector, and JSON flood outputs.

The runnable workflow is [workflow.yaml](/Users/kenia/workspace/leoflow/examples/sentinel-l0-flood/workflow.yaml:1). The main outputs are:

- `artifacts/sentinel-l0-flood/outputs/predictions/flood_mask.tif`
- `artifacts/sentinel-l0-flood/outputs/predictions/flood_polygons.geojson`
- `artifacts/sentinel-l0-flood/outputs/predictions/flood_summary.json`
- `artifacts/sentinel-l0-flood/outputs/evaluation/flood-summary.json`

The example catalog descriptor at [examples/sentinel-l0-flood.yaml](/Users/kenia/workspace/leoflow/examples/sentinel-l0-flood.yaml:1) now mirrors the same source URLs and high-level workflow description.

## What This Workflow Does

1. Reads the area of interest from `resources/austria-east.geojson`.
2. Downloads the configured Sentinel-1C RAW archive from ASF DAAC with Earthdata authentication.
3. Copies any local override rasters from `resources/processed-inputs/` and `resources/pre-event/`.
4. If no local override rasters exist and no external focus command is configured, queries ASF for OPERA RTC-S1 products covering the same AOI and time window, downloads the VV/VH GeoTIFF tiles, and mosaics them into override rasters.
5. Runs preprocessing:
   - `focus_level0.py`: reuses override rasters or calls an external SAR toolchain
   - `calibrate_and_geocode.py`: clips to the AOI grid, reprojects, and converts to linear sigma0-style rasters
   - `speckle_filter.py`: applies a 3x3 mean filter
6. Derives `sigma0_vv`, `sigma0_vh`, `vv_vh_ratio`, and `local_incidence_angle`.
7. Runs threshold-based flood detection with optional pre-event change filtering.
8. Writes flood raster, polygon GeoJSON, overlay statistics, and an evaluation summary.

## Data Sources And URLs

This example uses the following external sources:

- Configured RAW archive from ASF Datapool:
  `https://datapool.asf.alaska.edu/RAW/SC/S1C_IW_RAW__0SDV_20260425T050914_20260425T050947_007368_00EF26_F2C8.zip`
- ASF Search API endpoint used for OPERA RTC fallback:
  `https://api.daac.asf.alaska.edu/services/search/param`
- ASF Search API documentation:
  `https://docs.asf.alaska.edu/api/basics/`
- ASF download and authentication documentation:
  `https://docs.asf.alaska.edu/asf_search/downloading/`
- JPL OPERA RTC-S1 product page:
  `https://www.jpl.nasa.gov/go/opera/products/rtc-product/`
- ASF OPERA RTC-S1 product guide:
  `https://hyp3-docs.asf.alaska.edu/guides/opera_rtc_product_guide/`
- OPERA RTC notebook used as the example reference notebook:
  `https://github.com/OPERA-Cal-Val/OPERA_Applications/blob/main/RTC/notebooks/RTC_notebook.ipynb`

Local inputs in this example:

- `resources/austria-east.geojson`: AOI polygon
- `resources/processed-inputs/`: optional post-event override rasters
- `resources/pre-event/`: optional pre-event override rasters
- `resources/permanent-water.geojson`: optional permanent-water mask
- `resources/roads.geojson`: optional road overlay
- `resources/settlements.geojson`: optional settlement overlay
- `resources/agriculture.geojson`: optional agriculture overlay

Implementation entrypoints:

- data loader: [sentinel_1_asf_datapool_raw.py](/Users/kenia/workspace/leoflow/examples/sentinel-l0-flood/tasks/data/sentinel_1_asf_datapool_raw.py:1)
- preprocessing hook: [focus_level0.py](/Users/kenia/workspace/leoflow/examples/sentinel-l0-flood/tasks/custom/focus_level0.py:1)
- AOI reprojection and sigma0 normalization: [calibrate_and_geocode.py](/Users/kenia/workspace/leoflow/examples/sentinel-l0-flood/tasks/custom/calibrate_and_geocode.py:1)
- smoothing: [speckle_filter.py](/Users/kenia/workspace/leoflow/examples/sentinel-l0-flood/tasks/custom/speckle_filter.py:1)
- flood detection: [detect_flood.py](/Users/kenia/workspace/leoflow/examples/sentinel-l0-flood/tasks/custom/detect_flood.py:1)
- evaluation summary: [summarize_flood.py](/Users/kenia/workspace/leoflow/examples/sentinel-l0-flood/tasks/custom/summarize_flood.py:1)

## RAW vs OPERA RTC Fallback

There are two valid ways to get the post-event VV/VH rasters used by the flood model:

- External SAR toolchain path: configure `data.toolchain.focus.command` so `tasks/custom/focus_level0.py` can call SNAP, pyroSAR, ISCE2, GAMMA, or another SAR processor that writes `vv.tif` and `vh.tif`.
- Automatic fallback path: if no toolchain command is configured and no override rasters are present, `tasks/data/sentinel_1_asf_datapool_raw.py` searches ASF for OPERA RTC-S1 products for the same AOI and acquisition window, mosaics the returned VV/VH tiles, and continues with those rasters.

The fallback is intentionally pragmatic rather than physically identical to focusing the downloaded RAW archive. It uses analysis-ready OPERA RTC-S1 products discovered by AOI, time window, platform, and orbit metadata instead of performing full SAR focusing inside LeoFlow.

## Expected Resource Layout

Suggested filenames:

- post-event: `vv.tif`, `vh.tif`
- pre-event: `pre_event_vv.tif`, `pre_event_vh.tif`
- accepted alternatives: `sigma0_vv.tif`, `sigma0_vh.tif`, `pre_event_sigma0_vv.tif`, `pre_event_sigma0_vh.tif`
- optional angle raster: `local_incidence_angle.tif`

## External Focus Hook

Configure `data.toolchain.focus.command` in [workflow.yaml](/Users/kenia/workspace/leoflow/examples/sentinel-l0-flood/workflow.yaml:1) when you want LeoFlow to call a real SAR processor from `tasks/custom/focus_level0.py`.

Available placeholders:

- `{python_executable}`
- `{workflow_path}`
- `{workflow_dir}`
- `{workflow_name}`
- `{input_dir}`
- `{output_dir}`
- `{raw_dir}`
- `{downloads_dir}`
- `{extracted_dir}`
- `{archive_path}`
- `{safe_dir}`
- `{region_path}`
- `{post_event_override_dir}`
- `{pre_event_override_dir}`

The external command must create `vv.tif` and `vh.tif` in `{output_dir}`. If it also creates `local_incidence_angle.tif`, the later steps reuse it automatically.

Example:

```yaml
data:
  toolchain:
    focus:
      command: '"{python_executable}" scripts/run_snap_raw_focus.py --safe "{safe_dir}" --archive "{archive_path}" --region "{region_path}" --output "{output_dir}"'
      env:
        SNAP_GPT: /Applications/snap/bin/gpt
        SNAP_RAW_GRAPH: /absolute/path/to/raw-to-vv-vh.xml
```

## Run

```bash
export EARTHDATA_PASSWORD='your-password'
lf run . --setup
```

You can also set `data.provider.auth.password` directly in `workflow.yaml`, but `EARTHDATA_PASSWORD` is safer.

If you already have terrain-corrected rasters and do not want LeoFlow to download anything, replace the data task or point `data.download.url` at a local file and supply overrides in the resource folders.
