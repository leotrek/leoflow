# LeoFlow Store

LeoFlow Store is a local workflow store and project generator for Earth observation workflows.

It has four main jobs:

1. define a workflow in YAML
2. generate a runnable project from that workflow
3. package generated projects into a local registry
4. download and reuse published workflow bundles

## What LeoFlow manages

LeoFlow has a few distinct layers:

- example workflow templates: the reusable workflows under `examples/`
- runtime templates: the code-generation backends such as `python-minimal`, `pytorch`, and `eolearn`
- workflow spec: the `workflow.yaml` file that describes data, preprocessing, features, model, and evaluation
- generated project: the runnable app created by `lf create` or `lf build`
- registry bundle: a versioned zip plus metadata under `registry/`

If you remember only one distinction, remember this one:

- `lf list` shows example workflow templates from `examples/`
- `lf create --template ...` chooses one of those example workflows
- `--runtime-template ...` chooses the generator backend that turns the workflow into code

## Quick Start

Install the CLI:

```bash
python -m pip install -e .
```

List the available example workflow templates:

```bash
lf list
```

Create a new project from an example template:

```bash
lf create wildfire-demo ./wildfire-demo --template wildfire-detection
```

Run the generated project:

```bash
cd wildfire-demo
pip install -r requirements.txt
python app.py
```

Build code from an existing `workflow.yaml`:

```bash
lf build examples/wildfire-detection --output build/wildfire-detection
```

Run tests for a generated project:

```bash
lf test build/wildfire-detection
```

## Documentation Map

- [Concepts](concepts.md): store model and object lifecycle
- [CLI](cli.md): every `lf` command and its flags
- [Workflow Spec](workflow-spec.md): every workflow field used by LeoFlow today
- [Generated Project](generated-project.md): generated file layout and what to edit
- [Registry](registry.md): registry structure, metadata, publish, search, and download
- [Examples](examples/create-and-run.md): step-by-step walkthroughs

## Serve These Docs Locally

This repo includes an `mkdocs.yml` configured with the Read the Docs theme.

If you want a browsable docs site locally:

```bash
python -m pip install mkdocs
mkdocs serve
```

Then open the local MkDocs server shown in the terminal.
