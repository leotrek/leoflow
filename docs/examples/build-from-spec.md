# Example: Build From An Existing Workflow Spec

This example uses a workflow that already exists in the repo.

## 1. Build from the workflow directory

```bash
lf build examples/wildfire-detection --output build/wildfire-detection
```

You can also point directly at the YAML file:

```bash
lf build examples/wildfire-detection/workflow.yaml --output build/wildfire-detection
```

## 2. Inspect the generated build

Important files:

- `build/wildfire-detection/workflow.yaml`
- `build/wildfire-detection/app.py`
- `build/wildfire-detection/runtime/`
- `build/wildfire-detection/tasks/`
- `build/wildfire-detection/resources/`
- `build/wildfire-detection/tests/`

## 3. Run tests

```bash
lf test build/wildfire-detection
```

## 4. Run the app

```bash
lf run build/wildfire-detection
```

Or let LeoFlow create a local virtualenv for the generated build:

```bash
lf run build/wildfire-detection --setup
```

## 5. Rebuild after changing the source workflow

If you edit the source workflow spec and want a clean rebuild:

```bash
lf delete build/wildfire-detection --yes
lf build examples/wildfire-detection --output build/wildfire-detection
```

## When To Use `lf build`

Use `lf build` when:

- you already have a `workflow.yaml`
- you want to regenerate code after changing the spec
- you want to pick a different runtime template
