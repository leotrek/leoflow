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

## 4. Install dependencies

```bash
cd wildfire-demo
pip install -r requirements.txt
```

## 5. Run the workflow

```bash
python app.py
```

## 6. Inspect outputs

Look under:

- `artifacts/wildfire-demo/`

Typical outputs include:

- `last-run.json`
- previews under preprocessing artifacts
- feature rasters
- model output artifacts

## 7. Customize the workflow

Common edits:

- edit `workflow.yaml` to change time range, AOI, or data source settings
- edit `tasks/` to change the business logic
- edit `resources/polygon.geojson` to change the AOI

## 8. Run the generated tests

```bash
python -m unittest discover -s tests -p 'test_*.py'
```
