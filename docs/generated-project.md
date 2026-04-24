# Generated Project Layout

This page explains what LeoFlow generates and what you are expected to edit.

## Typical Layout

```text
my-workflow/
  workflow.yaml
  app.py
  README.md
  requirements.txt
  runtime/
    core.py
    task_runtime.py
    task_support.py
  tasks/
    README.md
    data/
    preprocessing/
    features/
    model/
    evaluation/
    lib/
  resources/
    README.md
    polygon.geojson
  tests/
    test_workflow.py
  artifacts/
    my-workflow/
      inputs/
      outputs/
      reports/
```

The exact contents vary by runtime template and workflow, but this is the stable shape for `python-minimal`.

## Files You Should Usually Edit

### `workflow.yaml`

Edit this when you need to change:

- workflow name or metadata
- AOI path
- dates
- data provider settings
- preprocessing stages
- features
- model parameters
- evaluation settings

### `tasks/`

This is where your business logic lives.

Subdirectories:

- `tasks/data/`: data acquisition tasks
- `tasks/preprocessing/`: preprocessing tasks
- `tasks/features/`: feature computation tasks
- `tasks/model/`: model or threshold tasks
- `tasks/evaluation/`: evaluation tasks
- `tasks/lib/`: helper code shared across tasks

These files use the `@task(...)` decorator and a runtime-provided `TaskContext` so you do not need to hand-write CLI parsing and JSON serialization in every task.

### `resources/`

Put static workflow inputs here:

- AOI GeoJSON files
- lookup tables
- reference rasters
- local fixtures for tests

### `tests/`

Generated tests live here. Extend them when you add custom logic.

## Files You Should Usually Not Edit

### `runtime/`

This directory contains generated runtime plumbing:

- stage execution
- task invocation
- path and artifact helpers
- task context helpers

You normally edit this only when working on LeoFlow itself or when designing a new runtime template.

### `app.py`

`app.py` is the small workflow entrypoint.

For most generated projects, you run:

```bash
lf run .
```

If you want LeoFlow to create or refresh a project-local virtualenv first:

```bash
lf run . --setup
```

You usually leave `app.py` alone unless you need custom stage overrides or entrypoint behavior.

## How LeoFlow Names Generated Task Files

LeoFlow derives task file names from the workflow spec.

Examples:

- `data.source.name: stac_sentinel_2` -> `tasks/data/stac_sentinel_2.py`
- `- cloud_mask: s2cloudless` -> `tasks/preprocessing/cloud_mask.py`
- `- ndvi` -> `tasks/features/ndvi.py`
- `model.output: fire_mask` -> `tasks/model/fire_mask.py`
- `evaluation.metrics: [iou]` -> `tasks/evaluation/iou.py`

## Runtime Helper Surface

The `python-minimal` runtime gives each task a `TaskContext`.

Common helpers include:

- `ctx.workflow_path`
- `ctx.spec`
- `ctx.project_root`
- `ctx.resource(...)`
- `ctx.output_dir`
- `ctx.output_path`
- `ctx.prediction_path`
- `ctx.artifact_path(...)`
- `ctx.json_path(...)`
- `ctx.write_json(...)`
- `ctx.result(...)`
- `ctx.report(...)`

The purpose of these helpers is to keep task files focused on workflow logic instead of boilerplate.

## Outputs And Artifacts

Workflow runs write artifacts under `artifacts/<workflow-slug>/`.

Typical layout:

- `inputs/`: downloaded raw data, downloaded model files, and other runtime materialized inputs
- `outputs/`: preprocessed rasters, derived features, predictions, and evaluation reports
- `reports/`: `last-run.json` and `io-manifest.json`

Example wildfire outputs:

- `artifacts/wildfire-detection/outputs/preprocessed/cloud_mask/true_color.png`
- `artifacts/wildfire-detection/outputs/predictions/fire_mask.tif`
- `artifacts/wildfire-detection/outputs/predictions/fire_mask_preview.png`
- `artifacts/wildfire-detection/reports/last-run.json`

The `reports/last-run.json` report includes:

- `inputs`: declared workflow inputs, local config/resources, and materialized runtime inputs such as downloaded raw data
- `outputs`: produced files under `artifacts/<workflow-slug>/outputs/`, separated from runtime inputs so you can reason about downlink candidates
- `reports`: generated report files under `artifacts/<workflow-slug>/reports/`

LeoFlow also writes `artifacts/<workflow-slug>/reports/io-manifest.json` with the same input/output summary.

## Editable Boundary Summary

Edit:

- `workflow.yaml`
- `tasks/`
- `resources/`
- `tests/`

Usually do not edit:

- `runtime/`
- `app.py`

If you feel the need to edit `runtime/` often, that usually means the abstraction should move into LeoFlow's runtime or generator instead of being repeated in each workflow.
