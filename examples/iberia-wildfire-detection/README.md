# iberia-wildfire-detection

Runnable EOFlowSpec app derived from [examples/real-world-wildfire](/Users/kenia/workspace/leoflow/examples/real-world-wildfire/README.md#L1).

This bundle reproduces the Iberia dNBR workflow from the EarthCODE example:

- download pre-fire and post-fire Sentinel-2 scenes from STAC
- build mean `nir` and `swir22` composites for each period
- compute `pre_fire_nbr`, `post_fire_nbr`, and `delta_nbr`
- threshold `delta_nbr` into `burned_area_mask`
- report burned area in hectares
- optionally compare against a local GWIS reference raster

Main files:

- `workflow.yaml`: workflow configuration and provider settings
- `tasks/`: workflow business logic you are expected to edit
- `tasks/lib/`: workflow-specific EO helper code
- `resources/`: AOI and optional local reference inputs
- `tests/`: workflow tests
- `runtime/`: generated execution plumbing; usually leave this alone

Editable vs generated:

- change `workflow.yaml` if you need a different AOI, dates, STAC query, or threshold
- change `tasks/` if you need different business logic
- change `resources/` when you want a different AOI or add `reference-burned-area.tif`
- avoid changing `runtime/` unless you are updating the template/runtime itself

Run:

```bash
pip install -r requirements.txt
python3 app.py
```

Important outputs:

- `artifacts/iberia-wildfire-detection/data/`: STAC search results and raw downloaded scenes
- `artifacts/iberia-wildfire-detection/preprocessed/pre_fire_composite/`: aligned period composites
- `artifacts/iberia-wildfire-detection/features/`: `pre_fire_nbr.tif`, `post_fire_nbr.tif`, `delta_nbr.tif`
- `artifacts/iberia-wildfire-detection/burned_area_mask.tif`: thresholded burned-area raster
- `artifacts/iberia-wildfire-detection/burned_area_mask_preview.png`: quick-look preview
- `artifacts/iberia-wildfire-detection/evaluation/`: evaluation JSON reports
- `artifacts/iberia-wildfire-detection/last-run.json`: full workflow run report

Notes:

- the workflow uses a custom `tasks/data/stac_sentinel_2.py` task even though `data.source` is a mapping; the runtime now prefers a named data task when `data.source.name` is present
- `gwis_overlay` is only fully available if you add a local raster such as `resources/reference-burned-area.tif`; the original notebook uses the remote SeasFire/GWIS cube, which is not bundled here

Tests:

```bash
python3 -m unittest discover -s tests -p 'test_*.py'
```
