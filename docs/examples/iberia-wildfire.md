# Example: Iberia Wildfire Workflow

This example uses the more detailed `iberia-wildfire-detection` workflow template.

It reproduces the EarthCODE-style dNBR flow:

1. download pre-fire and post-fire Sentinel-2 scenes
2. build composites
3. compute `pre_fire_nbr`, `post_fire_nbr`, and `delta_nbr`
4. threshold `delta_nbr` into `burned_area_mask`
5. compute burned area in hectares

## 1. Create a project from the Iberia example

```bash
lf create iberia-demo ./iberia-demo --template iberia-wildfire-detection
```

## 2. Inspect the workflow fields

This example includes extra fields beyond the minimal contract:

- `workflow.title`
- `workflow.description`
- `workflow.notebook`
- `data.windows`
- `data.reference_event_date`
- `data.crs`
- `data.groupby`
- `data.source.query`
- `model.parameters`
- `evaluation.reference`
- `publication`

That is a good reference when you want a richer workflow spec.

## 3. Run the workflow

```bash
lf run ./iberia-demo
```

If you want LeoFlow to create a local virtualenv and install dependencies first:

```bash
lf run ./iberia-demo --setup
```

## 4. Inspect outputs

Look under:

- `artifacts/iberia-demo/inputs/data/`
- `artifacts/iberia-demo/outputs/preprocessed/`
- `artifacts/iberia-demo/outputs/features/`
- `artifacts/iberia-demo/outputs/evaluation/`
- `artifacts/iberia-demo/reports/last-run.json`

Typical interesting files:

- `burned_area_mask.tif`
- `burned_area_mask_preview.png`
- `features/delta_nbr.tif`
- evaluation JSON reports

## 5. Customize the workflow

Typical changes:

- change `data.windows` for different pre-fire or post-fire periods
- narrow `data.source.limit` so less data is downloaded
- change `model.parameters.threshold`
- add a local reference raster under `resources/` for richer evaluation
