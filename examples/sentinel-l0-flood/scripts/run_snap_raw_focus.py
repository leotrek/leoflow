from __future__ import annotations

import argparse
import os
import subprocess
from pathlib import Path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Thin wrapper for running a user-provided SNAP GPT graph that converts a Sentinel-1 RAW product "
            "into vv.tif and vh.tif inside the requested output directory."
        )
    )
    parser.add_argument("--safe", required=True, help="Path to the Sentinel-1 .SAFE directory.")
    parser.add_argument("--output", required=True, help="Directory where the graph should write vv.tif/vh.tif.")
    parser.add_argument("--archive", default="", help="Optional archive path for graphs that need the original ZIP.")
    parser.add_argument("--region", default="", help="Optional AOI path to forward into the graph.")
    parser.add_argument("--gpt", default=os.environ.get("SNAP_GPT", "gpt"), help="SNAP GPT executable.")
    parser.add_argument(
        "--graph",
        default=os.environ.get("SNAP_RAW_GRAPH", ""),
        help="SNAP graph XML path. Can also be provided with SNAP_RAW_GRAPH.",
    )
    parser.add_argument(
        "--extra-arg",
        action="append",
        default=[],
        help="Additional raw GPT argument. Repeat for multiple arguments.",
    )
    args = parser.parse_args(argv)

    if not args.graph:
        raise RuntimeError(
            "no SNAP graph was configured. Set SNAP_RAW_GRAPH or pass --graph with a graph XML that writes "
            "vv.tif and vh.tif into the requested --output directory."
        )

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    command = [
        args.gpt,
        args.graph,
        f"-Psafe_dir={Path(args.safe).resolve()}",
        f"-Poutput_dir={output_dir.resolve()}",
    ]
    if args.archive:
        command.append(f"-Parchive_path={Path(args.archive).resolve()}")
    if args.region:
        command.append(f"-Pregion_path={Path(args.region).resolve()}")
    command.extend(args.extra_arg)

    try:
        subprocess.run(command, check=True)
    except FileNotFoundError as exc:
        raise RuntimeError(
            f"SNAP GPT executable not found: {args.gpt!r}. Set SNAP_GPT or pass --gpt with the full path."
        ) from exc
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
