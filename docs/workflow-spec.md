# Workflow Spec Reference

This page documents the workflow fields LeoFlow uses today.

## Minimal Valid Spec

This is the minimum shape required by the validator:

```yaml
workflow:
  name: wildfire-detection
  version: 0.1.0

data:
  source: stac://sentinel-2
  region: resources/polygon.geojson
  time: 2025-06-01/2025-08-01
  resolution: 10m

preprocessing:
  - cloud_mask: s2cloudless

features:
  - ndvi

model:
  type: segmentation
  input: patches(256x256)
  output: fire_mask

evaluation:
  metrics:
    - iou
```

## Validation Rules

LeoFlow currently enforces these required fields:

- `workflow.name`
- `data.source`
- `data.region`
- `data.time`
- `data.resolution`
- `preprocessing`
- `features`
- `model.type`
- `model.input`
- `model.output`
- `evaluation.metrics`

Additional fields are allowed. LeoFlow preserves them and your runtime or tasks can use them.

## Top-Level Sections

Common top-level sections:

- `workflow`
- `data`
- `preprocessing`
- `features`
- `model`
- `evaluation`
- `publication`

Only `publication` is entirely optional from LeoFlow's perspective today.

## `workflow`

Workflow metadata.

| Field | Required | Type | Meaning |
| --- | --- | --- | --- |
| `workflow.name` | yes | string | human-facing workflow name and source for the derived slug |
| `workflow.version` | optional | string | semantic version like `0.1.0`; defaults to `0.1.0` |
| `workflow.title` | optional | string | longer display title |
| `workflow.description` | optional | string | longer description |
| `workflow.notebook` | optional | string | reference notebook or source URL |

Notes:

- the slug is derived from `workflow.name`
- you generally do not write `workflow.slug` yourself

## `data`

Data acquisition and spatial or temporal scope.

| Field | Required | Type | Meaning |
| --- | --- | --- | --- |
| `data.source` | yes | string or mapping | data provider configuration |
| `data.region` | yes | string | AOI file path, usually a relative GeoJSON path |
| `data.time` | yes | string | main time interval |
| `data.resolution` | yes | string | target spatial resolution label such as `10m` |
| `data.windows` | optional | mapping | named time windows such as `pre_fire` and `post_fire` |
| `data.reference_event_date` | optional | string | reference event date used by workflow logic |
| `data.crs` | optional | string | target CRS, for example `EPSG:32629` |
| `data.groupby` | optional | string | grouping key used by a workflow |

### `data.source`

The simplest form is a shorthand string:

```yaml
data:
  source: stac://sentinel-2
```

The more explicit and preferred form is a mapping:

```yaml
data:
  source:
    kind: stac
    name: stac_sentinel_2
    api_url: https://earth-search.aws.element84.com/v1/search
    collection: sentinel-2-l2a
    assets: [blue, green, red, nir, swir22, scl]
    limit: 64
    query:
      eo:cloud_cover:
        lt: 20
```

Common `data.source` fields:

| Field | Required | Type | Meaning |
| --- | --- | --- | --- |
| `kind` | optional but recommended | string | provider type such as `stac` |
| `name` | optional but recommended | string | task name to bind to under `tasks/data/` |
| `api_url` | optional | string | STAC search endpoint |
| `collection` | optional | string | STAC collection name |
| `assets` | optional | list | asset aliases or band names to download |
| `limit` | optional | integer | maximum number of scenes to fetch |
| `query` | optional | mapping | provider-specific search filter |

Notes:

- if `data.source.name` is present, LeoFlow uses that name for the data task file
- for relative AOI files, LeoFlow copies them into `resources/` in the generated project
- the STAC task derives the spatial filter from the GeoJSON referenced by `data.region`

## `preprocessing`

An ordered list of preprocessing stages.

Each item must be a one-key mapping:

```yaml
preprocessing:
  - cloud_mask: s2cloudless
  - resample: 10m
  - align_time: 5d
```

How LeoFlow uses it:

- the key becomes the task name
- the key is turned into a file name under `tasks/preprocessing/`
- the value becomes task configuration exposed as `ctx.config`

Example:

```yaml
- cloud_mask: s2cloudless
```

generates:

```text
tasks/preprocessing/cloud_mask.py
```

### Special `command` step

In more executable specs, a preprocessing step may use a `command` mapping:

```yaml
- command:
    run: python3 tasks/custom/preprocess.py --input "{data_dir}" --output "{preprocess_dir}"
    output: "{preprocess_dir}"
```

That pattern is for command-driven runtimes. LeoFlow does not generate a `tasks/preprocessing/command.py` file for it.

LeoFlow also supports a shorthand form for command-driven preprocessing:

```yaml
preprocessing:
  - command: focus_level0
  - command:
      name: calibrate_and_geocode
      output: "{outputs_dir}/preprocessed/rtc"
```

For command-driven runtimes such as `python-minimal`, LeoFlow expands that shorthand using these defaults:

- script path: `tasks/custom/<name>.py`
- workflow argument: `--workflow "{workflow_dir}/workflow.yaml"`
- input argument: `--input "{input_dir}"`
- output argument: `--output "{outputs_dir}/preprocessed/<name>"`

Users can still override any part of the command by setting explicit `run`, `script`, or `output` fields.

## `features`

An ordered list of feature names:

```yaml
features:
  - ndvi
  - ndwi
```

How LeoFlow uses it:

- each feature becomes a task file under `tasks/features/`
- the feature name is exposed to the task as `ctx.name`

Example:

```text
tasks/features/ndvi.py
tasks/features/ndwi.py
```

## `model`

Model or decision stage settings.

| Field | Required | Type | Meaning |
| --- | --- | --- | --- |
| `model.type` | yes | string | model category such as `segmentation` or `threshold` |
| `model.input` | yes | string | declared model input |
| `model.output` | yes | string | output name and source for `tasks/model/<name>.py` |
| `model.parameters` | optional | mapping | model-specific parameters |
| `model.source` | optional | mapping | model download source such as Hugging Face |
| `model.executor` | optional | mapping | command-based execution settings |

### `model.source`

Common executable form:

```yaml
model:
  source:
    kind: huggingface
    repo_id: your-org/wildfire-segmentation
    revision: main
    filename: model.onnx
```

### `model.executor`

Command-driven model execution:

```yaml
model:
  executor:
    run: python3 tasks/custom/infer.py --model "{model_path}" --input "{data_dir}" --output "{prediction_dir}"
    artifact: "{prediction_dir}/fire_mask.tif"
```

LeoFlow also supports a shorthand executor:

```yaml
model:
  executor: package_rtc_product
```

or:

```yaml
model:
  executor:
    name: package_rtc_product
    artifact: "{prediction_dir}/rtc_backscatter.tif"
```

For command-driven runtimes such as `python-minimal`, LeoFlow expands that shorthand using these defaults:

- script path: `tasks/custom/<name>.py`
- workflow argument: `--workflow "{workflow_dir}/workflow.yaml"`
- input argument: `--input "{input_dir}"`
- output argument: `--output "{prediction_dir}/<model.output>.tif"`

Users can still override the generated behavior by supplying explicit `run`, `script`, or `artifact` fields.

## `evaluation`

Evaluation configuration.

| Field | Required | Type | Meaning |
| --- | --- | --- | --- |
| `evaluation.metrics` | yes | list | metrics to run |
| `evaluation.reference` | optional | mapping | reference dataset configuration |
| `evaluation.executor` | optional | mapping | command-driven evaluation |

Each metric name becomes a file under `tasks/evaluation/`.

Example:

```yaml
evaluation:
  metrics:
    - iou
    - temporal_consistency
```

generates:

```text
tasks/evaluation/iou.py
tasks/evaluation/temporal_consistency.py
```

### `evaluation.reference`

Example from the Iberia workflow:

```yaml
evaluation:
  reference:
    source: gwis
    dataset: burned_area
    comparison: overlay
```

### `evaluation.executor`

Command-driven evaluation:

```yaml
evaluation:
  executor:
    run: python3 tasks/custom/evaluate.py --predictions "{prediction_dir}" --report "{outputs_dir}/evaluation/evaluation.json"
    artifact: "{outputs_dir}/evaluation/evaluation.json"
```

`evaluation.executor` supports the same shorthand:

```yaml
evaluation:
  metrics:
    - raster_stats
  executor: evaluate_rtc_product
```

For command-driven runtimes such as `python-minimal`, LeoFlow expands that shorthand using these defaults:

- script path: `tasks/custom/<name>.py`
- workflow argument: `--workflow "{workflow_dir}/workflow.yaml"`
- prediction argument: `--prediction "{prediction_path}"`
- report argument: `--report "{outputs_dir}/evaluation/<name>.json"`
- artifact path: `"{outputs_dir}/evaluation/<name>.json"`

## `publication`

Optional publication metadata.

Example:

```yaml
publication:
  dataset_id: dnbr_dataset.zarr
  collection_id: pangeo-test
  access_link: s3://pangeo-test-fires
```

LeoFlow currently preserves this section but does not validate or execute publication logic by itself.

## How Names Turn Into Files

LeoFlow maps workflow names to task files like this:

- `data.source.name: stac_sentinel_2` -> `tasks/data/stac_sentinel_2.py`
- `cloud_mask` preprocessing step -> `tasks/preprocessing/cloud_mask.py`
- `ndvi` feature -> `tasks/features/ndvi.py`
- `model.output: fire_mask` -> `tasks/model/fire_mask.py`
- `iou` metric -> `tasks/evaluation/iou.py`

## Example: simple wildfire spec

```yaml
workflow:
  name: wildfire-detection
  version: 0.1.0
data:
  source:
    kind: stac
    name: stac_sentinel_2
    api_url: https://earth-search.aws.element84.com/v1/search
    collection: sentinel-2-l2a
    assets: [blue, green, red, nir, swir22, scl]
    limit: 64
  region: resources/polygon.geojson
  time: 2025-06-01/2025-08-01
  resolution: 10m
preprocessing:
  - cloud_mask: s2cloudless
  - resample: 10m
  - align_time: 5d
features:
  - ndvi
  - ndwi
model:
  type: segmentation
  input: patches(256x256)
  output: fire_mask
evaluation:
  metrics:
    - iou
    - temporal_consistency
```

## Example: Iberia wildfire spec

The Iberia example adds optional fields beyond the validator minimum:

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
