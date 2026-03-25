from __future__ import annotations

import argparse
import tempfile
from pathlib import Path

from leoflow_store.core.generator import DEFAULT_TEMPLATE, generate_project, template_names
from leoflow_store.core.parser import load_workflow
from leoflow_store.core.registry import WorkflowRegistry
from leoflow_store.core.validator import resolve_version, validate_workflow_spec


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Publish a generated workflow bundle into the local registry.")
    parser.add_argument("workflow_path", help="Path to the workflow YAML file.")
    parser.add_argument(
        "--template",
        default=DEFAULT_TEMPLATE,
        choices=template_names(),
        help="Template to package into the registry.",
    )
    parser.add_argument("--version", help="Override the workflow version.")
    parser.add_argument("--registry-root", help="Registry directory. Defaults to ./registry.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    spec = validate_workflow_spec(load_workflow(args.workflow_path))
    version = resolve_version(spec, args.version)
    registry = WorkflowRegistry(args.registry_root)

    with tempfile.TemporaryDirectory(prefix="eoflowspec-build-") as temp_dir:
        build_dir = Path(temp_dir) / "bundle"
        generate_project(spec, version, args.template, build_dir, workflow_path=args.workflow_path)
        published = registry.publish(spec, version, args.template, build_dir)

    print(f"published {published['name']} {published['version']} to {published['registry_path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
