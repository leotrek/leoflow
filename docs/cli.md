# CLI Reference

The main command is `lf`.

```bash
lf help
```

Most users only need these run commands:

```bash
lf run examples/wildfire-detection --setup
lf run ./my-workflow --setup
lf run ./my-workflow
lf run examples/wildfire-detection/workflow.yaml
```

## Top-Level Commands

- `lf create`: create a new project from an example workflow template
- `lf list`: list example workflow templates or registry entries
- `lf build`: build a runnable project from a workflow spec
- `lf run`: run a generated project directly or generate-and-run from a workflow spec
- `lf test`: run tests for a generated project or generate-and-test from a workflow spec
- `lf delete`: delete a generated project directory or a registry entry
- `lf help`: show help for the CLI or a specific command

## `lf create`

Create a project directory from one of the example workflow templates.

```bash
lf create <name> <output> [--template <example-name>] [--runtime-template <runtime-template>]
```

Arguments:

- `name`: workflow name to write into the generated `workflow.yaml`
- `output`: target directory; it must be new or empty

Options:

- `--template`: example workflow template name from `examples/`
- `--runtime-template`: code-generation backend, default `python-minimal`

Example:

```bash
lf create wildfire-demo ./wildfire-demo --template wildfire-detection
```

This command:

1. loads `examples/wildfire-detection/workflow.yaml`
2. replaces `workflow.name` with `wildfire-demo`
3. generates a new runnable project in `./wildfire-demo`

## `lf list`

List example workflow templates by default.

```bash
lf list [query] [--json]
```

Examples:

```bash
lf list
lf list wildfire
lf list --json
```

Use `--registry` to list published registry entries instead:

```bash
lf list [query] --registry [--registry-root <path>] [--json]
```

Examples:

```bash
lf list --registry
lf list wildfire --registry
lf list --registry --json
```

## `lf build`

Generate runnable code from a workflow spec.

```bash
lf build [target] [--template <runtime-template>] [--version <semver>] [--output <dir>]
```

Arguments:

- `target`: workflow directory or `workflow.yaml`; default `.` 

Options:

- `--template`: runtime template to use for code generation
- `--version`: override `workflow.version`
- `--output`: build destination, default `build/<workflow-slug>`

Examples:

```bash
lf build examples/wildfire-detection
lf build examples/wildfire-detection --output build/wildfire-detection
lf build examples/iberia-wildfire-detection --template python-minimal
```

## `lf test`

Run tests for a generated project, or generate a temporary build and test that.

```bash
lf test [target] [--template <runtime-template>] [--version <semver>] [--keep-build]
```

Behavior:

- if `target` is already a generated project, LeoFlow runs `python -m unittest discover -s tests -p 'test_*.py'`
- if `target` is a workflow directory or a `workflow.yaml`, LeoFlow generates a temporary project first and tests that

Examples:

```bash
lf test build/wildfire-detection
lf test examples/wildfire-detection
lf test examples/wildfire-detection --keep-build
```

## `lf run`

Run a generated project directly, or generate a temporary build and run that.

```bash
lf run [target] [--template <runtime-template>] [--version <semver>] [--keep-build] [--setup] [--venv-dir <dir>]
```

Behavior:

- if `target` is already a runnable project, LeoFlow runs `app.py` in that directory
- if `target` is a workflow directory or a `workflow.yaml`, LeoFlow generates a project first and runs that
- if `--setup` is set, LeoFlow creates or reuses a project-local virtualenv and runs `python -m pip install -r requirements.txt` before execution
- if a virtualenv already exists under `--venv-dir`, LeoFlow uses it automatically even without `--setup`

Examples:

```bash
lf run build/wildfire-detection
lf run examples/wildfire-detection --setup
lf run examples/wildfire-detection/workflow.yaml --keep-build
```

## `lf delete`

Delete either a local generated project or a registry entry.

Delete a local path:

```bash
lf delete <path> [--yes]
```

Delete a registry entry:

```bash
lf delete <name> --registry [--version <semver>] [--registry-root <path>] [--yes]
```

Examples:

```bash
lf delete build/wildfire-detection --yes
lf delete wildfire-detection --registry --yes
lf delete wildfire-detection --registry --version 0.1.0 --yes
```

## `lf help`

Show help for the whole CLI or for a specific command.

Examples:

```bash
lf help
lf help create
lf help build
```

## Important Distinction

There are two different kinds of template names in LeoFlow:

- example workflow templates, used by `lf create --template`
- runtime templates, used by `lf create --runtime-template`, `lf build --template`, and `lf test --template`

Examples:

- example workflow template: `wildfire-detection`
- runtime template: `python-minimal`
