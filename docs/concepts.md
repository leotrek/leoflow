# Concepts

This page explains the objects LeoFlow uses and how they relate to each other.

## Example Workflow Template

An example workflow template is a complete workflow project stored under `examples/`.

Current examples:

- `wildfire-detection`
- `iberia-wildfire-detection`

Each example contains at least:

- `workflow.yaml`
- `README.md`
- `tasks/`
- `resources/`
- `tests/`

These are the names returned by `lf list`.

## Runtime Template

A runtime template is the code-generation backend used to turn a workflow spec into runnable code.

Current runtime templates:

- `python-minimal`
- `pytorch`
- `eolearn`

These are used by:

- `lf create --runtime-template ...`
- `lf build --template ...`
- `lf test --template ...` when testing from a raw workflow spec
- `python -m leoflow_store.api.generate --template ...`

## Workflow Spec

A workflow spec is a YAML file, usually named `workflow.yaml`, that describes:

- workflow metadata
- data source and spatial or temporal scope
- preprocessing stages
- features to compute
- model output
- evaluation metrics

LeoFlow validates a minimal required contract and preserves additional fields for tasks and runtimes to use.

That means:

- some fields are enforced by the validator
- other fields are optional but still useful because the runtime or your tasks read them

## Generated Project

A generated project is the runnable app created from a workflow spec.

It usually contains:

- `workflow.yaml`
- `app.py`
- `runtime/`
- `tasks/`
- `resources/`
- `tests/`
- `README.md`
- `requirements.txt`

This is what you run with `python app.py`.

## Registry Bundle

A registry bundle is a published, versioned copy of a generated project.

Registry layout:

```text
registry/
  <workflow-slug>/
    <version>/
      workflow.yaml
      metadata.json
      template.zip
```

The zip contains the generated project without local artifacts or cache files.

## Normal Lifecycle

Most users follow one of these flows.

### Flow 1: start from an example

1. run `lf list`
2. choose an example workflow template
3. run `lf create <name> <output> --template <example-name>`
4. edit `workflow.yaml`, `tasks/`, and `resources/`
5. run `python app.py`

### Flow 2: start from your own workflow spec

1. write `workflow.yaml`
2. run `lf build <workflow-dir-or-yaml> --output <build-dir>`
3. inspect the generated project
4. run `lf test <build-dir>`
5. run `python <build-dir>/app.py`

### Flow 3: publish and reuse a bundle

1. build or generate a project
2. publish it into the local registry
3. search the registry
4. download the bundle into a fresh directory
5. run the downloaded project

## What LeoFlow Abstracts

LeoFlow is designed to abstract:

- project layout
- CLI plumbing for tasks
- runtime path handling
- task result serialization
- local registry packaging
- generated tests

LeoFlow does not automatically invent domain logic for you. Business logic still belongs in:

- `tasks/`
- `tasks/lib/`
- optional provider-specific or model-specific scripts you add

## What To Edit And What Not To Edit

Usually edit these:

- `workflow.yaml`
- `tasks/`
- `resources/`
- `tests/`

Usually do not edit these unless you are changing the template system itself:

- `runtime/`
- `leoflow_store/templates/`
- `leoflow_store/core/generator.py`
