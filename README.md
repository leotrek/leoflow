[![Docs](https://readthedocs.org/projects/leoflow-store/badge/?version=latest)](https://leoflow-store.readthedocs.io)

# LeoFlow Store

LeoFlow Store is a local workflow store and project generator for Earth observation workflows.

It lets you:

- create a workflow project from an example template
- build runnable code from a `workflow.yaml`
- test generated workflows
- publish and download versioned bundles from a local registry

Detailed documentation lives under [docs/index.md](/Users/kenia/workspace/leoflow/docs/index.md#L1). A browsable docs site is configured in [mkdocs.yml](/Users/kenia/workspace/leoflow/mkdocs.yml#L1).

## Install

Install the CLI locally:

```bash
python -m pip install -e .
lf help
```

Build a package:

```bash
python -m pip install build
python -m build
```

Uninstall:

```bash
python -m pip uninstall leoflow-store
```

## Quick Start

List available example workflow templates:

```bash
lf list
```

Create a project from an example workflow:

```bash
lf create wildfire-demo ./wildfire-demo --template wildfire-detection
```

Run the generated project:

```bash
cd wildfire-demo
pip install -r requirements.txt
python app.py
```

Run the example project:

```bash
cd examples/wildfire-detection

python3 -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt
python app.py
```


Build from an existing workflow spec:

```bash
lf build examples/wildfire-detection --output build/wildfire-detection
```

Run tests:

```bash
lf test build/wildfire-detection
```

## Main Commands

- `lf create`: create a new project from an example workflow template
- `lf list`: list example workflow templates, or registry entries with `--registry`
- `lf build`: generate a runnable project from a workflow spec
- `lf test`: run tests for a generated project or generate-and-test from a workflow spec
- `lf delete`: delete a generated project directory or a registry entry
- `lf help`: show command help

## Main Paths

- [examples/](/Users/kenia/workspace/leoflow/examples): example workflow templates
- [leoflow_store/](/Users/kenia/workspace/leoflow/leoflow_store): CLI, generator, registry, and runtime template code
- [registry/](/Users/kenia/workspace/leoflow/registry): local published bundles
- [docs/](/Users/kenia/workspace/leoflow/docs): full documentation

## Read The Docs Style Docs

Serve the docs locally with MkDocs:

```bash
python -m pip install mkdocs
mkdocs serve
```

Then open the local URL shown by MkDocs.

## Where To Read More

- [docs/concepts.md](/Users/kenia/workspace/leoflow/docs/concepts.md#L1): templates, generated projects, registry bundles
- [docs/cli.md](/Users/kenia/workspace/leoflow/docs/cli.md#L1): full CLI reference
- [docs/workflow-spec.md](/Users/kenia/workspace/leoflow/docs/workflow-spec.md#L1): every workflow field LeoFlow uses
- [docs/generated-project.md](/Users/kenia/workspace/leoflow/docs/generated-project.md#L1): generated layout and editable files
- [docs/registry.md](/Users/kenia/workspace/leoflow/docs/registry.md#L1): publish, search, and download
- [docs/examples/create-and-run.md](/Users/kenia/workspace/leoflow/docs/examples/create-and-run.md#L1): step-by-step example
