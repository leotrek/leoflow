from __future__ import annotations

import argparse
from pathlib import Path

from leoflow_store.core.generator import DEFAULT_TEMPLATE, generate_project, template_names
from leoflow_store.core.parser import load_workflow
from leoflow_store.core.validator import resolve_version, validate_workflow_spec, workflow_slug


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate editable workflow code from a workflow spec.")
    parser.add_argument("workflow_path", help="Path to the workflow YAML file.")
    parser.add_argument(
        "--template",
        default=DEFAULT_TEMPLATE,
        choices=template_names(),
        help="Template to generate from.",
    )
    parser.add_argument("--version", help="Override the workflow version.")
    parser.add_argument(
        "--output",
        help="Directory to write the generated source into. Defaults to build/<slug>.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    spec = validate_workflow_spec(load_workflow(args.workflow_path))
    version = resolve_version(spec, args.version)
    output_dir = Path(args.output) if args.output else Path("build") / workflow_slug(spec)
    generate_project(spec, version, args.template, output_dir, workflow_path=args.workflow_path)
    print(f"generated {spec['workflow']['name']} {version} into {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
