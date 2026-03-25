# Registry

LeoFlow includes a local filesystem-backed registry for generated workflow bundles.

## Registry Layout

```text
registry/
  <workflow-slug>/
    <version>/
      workflow.yaml
      metadata.json
      template.zip
```

Example:

```text
registry/
  wildfire-detection/
    0.1.0/
      workflow.yaml
      metadata.json
      template.zip
```

## What Gets Published

When a workflow is published, LeoFlow stores:

- `workflow.yaml`: the published workflow spec
- `metadata.json`: searchable metadata
- `template.zip`: the generated runnable project

LeoFlow excludes:

- `artifacts/`
- `__pycache__/`
- `.pyc` files
- `.DS_Store`

## `metadata.json` Fields

The registry metadata currently contains these fields:

| Field | Meaning |
| --- | --- |
| `name` | workflow name |
| `slug` | slug derived from `workflow.name` |
| `version` | semantic version |
| `template` | runtime template used to generate the project |
| `data_source` | normalized data source label |
| `model_type` | `model.type` from the workflow |
| `features` | list of feature names |
| `metrics` | list of evaluation metric names |
| `tags` | derived searchable tags |
| `published_at` | UTC timestamp |
| `bundle` | path to `template.zip` |

## Search Behavior

Registry search is free-text over:

- `name`
- `slug`
- `version`
- `template`
- `data_source`
- `model_type`
- `features`
- `metrics`
- `tags`

## CLI Registry Operations

The `lf` CLI currently exposes:

- `lf list --registry`
- `lf delete --registry`

Examples:

```bash
lf list --registry
lf list wildfire --registry
lf delete wildfire-detection --registry --yes
```

## Python Module Registry Operations

Publishing, searching, and downloading are available through the module entrypoints.

### Publish

```bash
python -m leoflow_store.api.publish examples/wildfire-detection/workflow.yaml --template python-minimal
```

Options:

- `--template`: runtime template to package
- `--version`: override version
- `--registry-root`: custom registry path

### Search

```bash
python -m leoflow_store.api.search
python -m leoflow_store.api.search wildfire
python -m leoflow_store.api.search wildfire --json
```

### Download

```bash
python -m leoflow_store.api.download wildfire-detection --output downloads/wildfire-detection
python -m leoflow_store.api.download wildfire-detection --version 0.1.0 --output downloads/wildfire-detection
```

## Version Rules

- versions follow semantic versioning like `0.1.0`
- if no version is supplied for download, LeoFlow resolves the latest available version
- latest version is chosen by numeric semver ordering

## When To Use The Registry

Use the local registry when you want:

- a reusable bundle for teammates
- a stable versioned workflow artifact
- a workflow catalog on your machine or CI worker
- a clean download path for users who should start from a generated app instead of raw template code
