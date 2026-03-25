from __future__ import annotations

import argparse
import json

from leoflow_store.core.registry import WorkflowRegistry


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Search the local workflow registry.")
    parser.add_argument("query", nargs="?", help="Optional free-text query.")
    parser.add_argument("--registry-root", help="Registry directory. Defaults to ./registry.")
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of plain text.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    registry = WorkflowRegistry(args.registry_root)
    matches = registry.search(args.query)

    if args.json:
        print(json.dumps(matches, indent=2))
        return 0

    if not matches:
        print("no workflows found")
        return 0

    for item in matches:
        print(
            f"{item['name']}@{item['version']} template={item['template']} "
            f"source={item['data_source']} tags={','.join(item['tags'])}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
