# {{WORKFLOW_NAME}}

Generated from the `eolearn` EOFlowSpec template.

Use this bundle when the generated workflow app will become an EO-Learn task graph.

Version: `{{WORKFLOW_VERSION}}`
Data source: `{{DATA_SOURCE}}`
Features: `{{FEATURES_CSV}}`
Metrics: `{{METRICS_CSV}}`

Files:

- `workflow.yaml`: edit this to change workflow configuration.
- `resources/`: put GeoJSON, labels, and other static inputs here.
- `app.py`: edit this when you want stage-level overrides.
- `runtime/`: generated template boilerplate; usually do not edit this.
- `tests/`: edit or extend these tests to validate the workflow.

Run:

```bash
pip install -r requirements.txt
python3 app.py
```

Run the generated test:

```bash
python3 -m unittest discover -s tests -p 'test_*.py'
```

Relative GeoJSON paths from the input spec are placed under `resources/` in the generated bundle.
