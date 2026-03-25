from __future__ import annotations

from runtime.task_runtime import task
from runtime.task_support import copy_tree


@task("preprocessing", name="align_time", config="5d")
def main(ctx):
    copied = copy_tree(ctx.input_dir, ctx.output_dir)
    manifest_path = ctx.write_json(
        {
            "task": "align_time",
            "window": "5d",
            "implementation": "single-scene passthrough",
            "input_dir": str(ctx.input_dir),
            "output_dir": str(ctx.output_dir),
            "copied_files": copied,
        },
        filename="align_time.json",
    )
    return ctx.result(
        manifest=str(manifest_path),
        artifacts=[str(manifest_path), *copied],
    )


if __name__ == "__main__":
    raise SystemExit(main())
