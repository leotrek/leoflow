# Example: Publish, Search, And Download

This example shows the local store flow.

## 1. Build a runnable project

```bash
lf build examples/wildfire-detection --output build/wildfire-detection
```

## 2. Publish it to the local registry

```bash
python -m leoflow_store.api.publish examples/wildfire-detection/workflow.yaml --template python-minimal
```

If you want a custom registry path:

```bash
python -m leoflow_store.api.publish \
  examples/wildfire-detection/workflow.yaml \
  --template python-minimal \
  --registry-root ./registry
```

## 3. Search the registry

Plain text:

```bash
python -m leoflow_store.api.search wildfire
```

JSON:

```bash
python -m leoflow_store.api.search wildfire --json
```

Through the main CLI:

```bash
lf list --registry
lf list wildfire --registry
```

## 4. Download the published bundle

```bash
python -m leoflow_store.api.download wildfire-detection --output downloads/wildfire-detection
```

Or download a specific version:

```bash
python -m leoflow_store.api.download wildfire-detection --version 0.1.0 --output downloads/wildfire-detection
```

## 5. Run the downloaded bundle

```bash
cd downloads/wildfire-detection
pip install -r requirements.txt
python app.py
```

## 6. Delete a registry entry

```bash
lf delete wildfire-detection --registry --yes
```
