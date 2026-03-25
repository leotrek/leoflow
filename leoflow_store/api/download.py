from __future__ import annotations

import argparse
from pathlib import Path

from leoflow_store.core.registry import WorkflowRegistry


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Download a workflow bundle from the local registry.")
    parser.add_argument("name", help="Workflow name or slug.")
    parser.add_argument("--version", help="Version to download. Defaults to the latest available version.")
    parser.add_argument("--output", required=True, help="Directory to extract the bundle into.")
    parser.add_argument("--registry-root", help="Registry directory. Defaults to ./registry.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    registry = WorkflowRegistry(args.registry_root)
    downloaded_to = registry.download(args.name, args.version, Path(args.output))
    print(f"downloaded {args.name} to {downloaded_to}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
