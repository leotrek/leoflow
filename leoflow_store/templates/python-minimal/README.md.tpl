# {{WORKFLOW_NAME}}

Generated from the `python-minimal` EOFlowSpec template.

Version: `{{WORKFLOW_VERSION}}`
Data source: `{{DATA_SOURCE}}`
Region: `{{REGION}}`
Time range: `{{TIME_RANGE}}`
Resolution: `{{RESOLUTION}}`
Features: `{{FEATURES_CSV}}`
Model: `{{MODEL_TYPE}}`
Model input: `{{MODEL_INPUT}}`
Model output: `{{MODEL_OUTPUT}}`
Metrics: `{{METRICS_CSV}}`

Preprocessing:
{{PREPROCESSING_BULLETS}}

Files:

- `workflow.yaml`: user-editable workflow configuration.
- `tasks/`: user-editable business logic and helper code.
- `resources/`: user-editable AOI files and static inputs.
- `tests/`: user-editable workflow checks.
- `app.py`: usually leave this small entrypoint alone unless you want stage-level overrides.
- `runtime/`: generated engine internals; usually do not edit this.

Run:

```bash
pip install -r requirements.txt
python3 app.py
```

This bundle always writes its run report to `artifacts/{{WORKFLOW_SLUG}}/last-run.json`.
Whether it executes real EO work depends on the task implementations and workflow config:

- generated preprocessing and feature tasks are lightweight scaffolds
- generated model and evaluation tasks must be replaced, or `workflow.yaml` must provide explicit executors
- data loading does not silently create fake raster files; if the configured source cannot be resolved, the app fails clearly

Run the generated test:

```bash
python3 -m unittest discover -s tests -p 'test_*.py'
```

Recommended edit points:

- change `workflow.yaml` when you need a different data source, model, or execution command
- change `tasks/` when you need different preprocessing, feature extraction, inference, evaluation logic, or helper functions
- change `resources/` when you need different polygons or local static files
- avoid changing `runtime/` unless you are modifying the template itself

Override only the stages you need in `app.py`:

- `load_data`
- `preprocess`
- `extract_features`
- `run_model`
- `evaluate`
- `build_output`

Generated task names follow the workflow:

- `tasks/data/stac_sentinel_2.py`
- `tasks/preprocessing/cloud_mask.py`
- `tasks/preprocessing/resample.py`
- `tasks/preprocessing/align_time.py`
- `tasks/features/ndvi.py`
- `tasks/features/ndwi.py`
- `tasks/model/fire_mask.py`
- `tasks/evaluation/iou.py`
- `tasks/evaluation/temporal_consistency.py`

You can keep the generated defaults or replace them with real implementations. If you need provider-specific behavior, either edit these generated task files or add your own scripts under `tasks/` and reference them from `workflow.yaml`.

The task runtime also provides generic helper methods such as `ctx.artifact_path(...)` and `ctx.write_json(...)` so tasks can avoid repeating path and manifest boilerplate.
