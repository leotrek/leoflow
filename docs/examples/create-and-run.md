# Example: Create And Run A Workflow

This example starts from the built-in `wildfire-detection` example workflow template.

## 1. List available example templates

```bash
lf list
```

Expected output:

```text
iberia-wildfire-detection
wildfire-detection
```

## 2. Create a new workflow project

```bash
lf create wildfire-demo ./wildfire-demo --template wildfire-detection
```

This generates a runnable project in `./wildfire-demo`.

## 3. Inspect the generated project

Important files:

- `wildfire-demo/workflow.yaml`
- `wildfire-demo/tasks/`
- `wildfire-demo/resources/polygon.geojson`
- `wildfire-demo/tests/test_workflow.py`

## 4. Run the workflow

```bash
lf run ./wildfire-demo
```

If you want LeoFlow to create a local virtualenv and install dependencies first:

```bash
lf run ./wildfire-demo --setup
```

## 5. Inspect outputs

Look under:

- `artifacts/wildfire-demo/`

Typical outputs include:

- `outputs/preprocessed/`
- `outputs/features/`
- `outputs/predictions/`
- `reports/last-run.json`

Inside `last-run.json`, LeoFlow now writes:

- `inputs` for the workflow config, local resources, and runtime input data
- `outputs` for the files created by the run
- `reports` for generated report files

LeoFlow also writes `artifacts/<workflow-slug>/reports/io-manifest.json`.

## 6. Customize the workflow

Common edits:

- edit `workflow.yaml` to change time range, AOI, or data source settings
- edit `tasks/` to change the business logic
- edit `resources/polygon.geojson` to change the AOI

## 7. Run the generated tests

```bash
lf test ./wildfire-demo
```
