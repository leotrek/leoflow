from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from leoflow_store.core.generator import DEFAULT_TEMPLATE, generate_project, template_names
from leoflow_store.core.parser import load_workflow
from leoflow_store.core.registry import WorkflowRegistry
from leoflow_store.core.scaffold import (
    DEFAULT_EXAMPLE_TEMPLATE,
    create_project,
    example_template_names,
    list_examples,
)
from leoflow_store.core.validator import resolve_version, validate_workflow_spec, workflow_slug


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="lf", description="Local workflow CLI.")
    subparsers = parser.add_subparsers(dest="command")

    create_parser = subparsers.add_parser(
        "create",
        help="Create a workflow project.",
        description="Create a workflow project in the given output directory from an example workflow template.",
    )
    create_parser.add_argument("name", help="Workflow name.")
    create_parser.add_argument("output", help="Directory where the workflow project should be created.")
    create_parser.add_argument(
        "--template",
        default=DEFAULT_EXAMPLE_TEMPLATE,
        choices=example_template_names(),
        help="Example workflow template to create from.",
    )
    create_parser.add_argument(
        "--runtime-template",
        default=DEFAULT_TEMPLATE,
        choices=template_names(),
        help="Code generation runtime template.",
    )
    create_parser.set_defaults(handler=_handle_create)

    list_parser = subparsers.add_parser(
        "list",
        help="List example workflows or registry entries.",
        description="List example workflows from examples/ by default, or registry entries with --registry.",
    )
    list_parser.add_argument("query", nargs="?", help="Optional search query.")
    list_parser.add_argument("--registry", action="store_true", help="List workflows from the local registry instead of examples/.")
    list_parser.add_argument("--registry-root", help="Registry directory. Defaults to ./registry.")
    list_parser.add_argument("--json", action="store_true", help="Emit JSON.")
    list_parser.set_defaults(handler=_handle_list)

    build_parser = subparsers.add_parser(
        "build",
        help="Generate runnable workflow code from a workflow spec.",
        description="Generate runnable workflow code from a workflow spec.",
    )
    build_parser.add_argument("target", nargs="?", default=".", help="Workflow directory or workflow.yaml path.")
    build_parser.add_argument(
        "--template",
        default=DEFAULT_TEMPLATE,
        choices=template_names(),
        help="Template to generate from.",
    )
    build_parser.add_argument("--version", help="Override the workflow version.")
    build_parser.add_argument("--output", help="Directory to write the generated app into.")
    build_parser.set_defaults(handler=_handle_build)

    test_parser = subparsers.add_parser(
        "test",
        help="Run workflow tests.",
        description="Run workflow tests.",
    )
    test_parser.add_argument("target", nargs="?", default=".", help="Generated app directory, workflow directory, or workflow.yaml path.")
    test_parser.add_argument(
        "--template",
        default=DEFAULT_TEMPLATE,
        choices=template_names(),
        help="Template to use when testing from a workflow spec.",
    )
    test_parser.add_argument("--version", help="Override the workflow version when testing from a workflow spec.")
    test_parser.add_argument("--keep-build", action="store_true", help="Keep the temporary generated build when testing from a workflow spec.")
    test_parser.set_defaults(handler=_handle_test)

    delete_parser = subparsers.add_parser(
        "delete",
        help="Delete a generated workflow directory or a registry entry.",
        description="Delete a generated workflow directory or a registry entry.",
    )
    delete_parser.add_argument("target", help="Filesystem path or registry workflow name.")
    delete_parser.add_argument("--version", help="Registry version to delete.")
    delete_parser.add_argument("--registry", action="store_true", help="Delete from the local registry instead of the filesystem.")
    delete_parser.add_argument("--registry-root", help="Registry directory. Defaults to ./registry.")
    delete_parser.add_argument("--yes", action="store_true", help="Delete without prompting.")
    delete_parser.set_defaults(handler=_handle_delete)

    help_parser = subparsers.add_parser(
        "help",
        help="Show CLI help.",
        description="Show CLI help.",
    )
    help_parser.add_argument("topic", nargs="?", help="Optional command name.")
    help_parser.set_defaults(handler=_handle_help, parser=parser)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not getattr(args, "command", None):
        parser.print_help()
        return 0
    return args.handler(args, parser)


def _handle_create(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    created = create_project(
        args.name,
        Path(args.output),
        workflow_template=args.template,
        runtime_template=args.runtime_template,
    )
    print(f"created workflow project in {created}")
    return 0


def _handle_list(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    if args.registry:
        items = WorkflowRegistry(args.registry_root).search(args.query)
    else:
        items = list_examples(args.query)
    if args.json:
        print(json.dumps(items, indent=2))
        return 0
    if not items:
        print("no workflows found")
        return 0
    names = sorted({str(item["name"]) for item in items})
    for name in names:
        print(name)
    return 0


def _handle_build(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    workflow_path = _resolve_workflow_path(args.target)
    spec = validate_workflow_spec(load_workflow(workflow_path))
    version = resolve_version(spec, args.version)
    output_dir = Path(args.output) if args.output else Path("build") / workflow_slug(spec)
    generate_project(spec, version, args.template, output_dir, workflow_path=workflow_path)
    print(f"built {spec['workflow']['name']} {version} into {output_dir}")
    return 0


def _handle_test(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    target = Path(args.target).resolve()
    if _is_generated_bundle_dir(target):
        _run_tests_in_dir(target)
        print(f"tests passed in {target}")
        return 0

    workflow_path = _resolve_workflow_path(target)
    spec = validate_workflow_spec(load_workflow(workflow_path))
    version = resolve_version(spec, args.version)

    if args.keep_build:
        build_dir = Path("build") / workflow_slug(spec)
        generate_project(spec, version, args.template, build_dir, workflow_path=workflow_path)
        _run_tests_in_dir(build_dir)
        print(f"generated and tested {build_dir}")
        return 0

    with tempfile.TemporaryDirectory(prefix="lf-test-") as temp_dir:
        build_dir = Path(temp_dir) / workflow_slug(spec)
        generate_project(spec, version, args.template, build_dir, workflow_path=workflow_path)
        _run_tests_in_dir(build_dir)
    print(f"generated and tested {spec['workflow']['name']}")
    return 0


def _handle_delete(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    if args.registry:
        registry = WorkflowRegistry(args.registry_root)
        label = f"{args.target}@{args.version}" if args.version else args.target
        _confirm_or_exit(args.yes, f"delete registry entry {label}?")
        removed = registry.delete(args.target, args.version)
        for path in removed:
            print(f"deleted {path}")
        return 0

    target = Path(args.target)
    if not target.exists():
        raise FileNotFoundError(f"path not found: {target}")
    _confirm_or_exit(args.yes, f"delete {target}?")
    if target.is_dir():
        shutil.rmtree(target)
    else:
        target.unlink()
    print(f"deleted {target}")
    return 0


def _handle_help(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    if not args.topic:
        parser.print_help()
        return 0

    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):  # type: ignore[attr-defined]
            if args.topic in action.choices:
                action.choices[args.topic].print_help()
                return 0
    raise ValueError(f"unknown help topic: {args.topic}")


def _resolve_workflow_path(target: str | Path) -> Path:
    path = Path(target)
    if path.is_dir():
        workflow_path = path / "workflow.yaml"
        if workflow_path.exists():
            return workflow_path
    if path.is_file():
        return path
    raise FileNotFoundError(f"workflow target not found: {path}")


def _is_generated_bundle_dir(path: Path) -> bool:
    return path.is_dir() and (path / "app.py").exists() and (path / "tests").is_dir()


def _run_tests_in_dir(path: Path) -> None:
    subprocess.run(
        [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-p", "test_*.py"],
        cwd=path,
        check=True,
    )


def _confirm_or_exit(assume_yes: bool, prompt: str) -> None:
    if assume_yes:
        return
    reply = input(f"{prompt} [y/N] ").strip().lower()
    if reply not in {"y", "yes"}:
        raise SystemExit(1)


if __name__ == "__main__":
    raise SystemExit(main())
