from __future__ import annotations

import json
import shutil
import re
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from leoflow_store.core.parser import dump_workflow
from leoflow_store.core.validator import workflow_slug


class WorkflowRegistry:
    def __init__(self, root: str | Path | None = None) -> None:
        self.root = Path(root) if root else _default_registry_root()
        self.root.mkdir(parents=True, exist_ok=True)

    def publish(
        self,
        spec: dict[str, Any],
        version: str,
        template_name: str,
        generated_dir: str | Path,
    ) -> dict[str, str]:
        slug = workflow_slug(spec)
        target_dir = self.root / slug / version
        target_dir.mkdir(parents=True, exist_ok=True)

        dump_workflow(spec, target_dir / "workflow.yaml")
        metadata = self._build_metadata(spec, version, template_name, target_dir)
        (target_dir / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
        self._zip_directory(Path(generated_dir), target_dir / "template.zip")

        return {
            "name": metadata["name"],
            "version": metadata["version"],
            "registry_path": str(target_dir),
        }

    def search(self, query: str | None = None) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for metadata_path in self.root.glob("*/*/metadata.json"):
            item = json.loads(metadata_path.read_text(encoding="utf-8"))
            items.append(item)

        items.sort(key=lambda item: (item["name"], _version_key(item["version"])), reverse=False)
        if not query:
            return items

        needle = query.lower()
        return [item for item in items if needle in _search_text(item)]

    def download(self, name: str, version: str | None, output_dir: str | Path) -> Path:
        slug = _resolve_name_to_slug(name)
        resolved_version = version or self.latest_version(slug)
        if not resolved_version:
            raise FileNotFoundError(f"no versions found for workflow: {name}")

        zip_path = self.root / slug / resolved_version / "template.zip"
        if not zip_path.exists():
            raise FileNotFoundError(f"workflow bundle not found: {zip_path}")

        destination = Path(output_dir)
        _ensure_empty_destination(destination)
        with zipfile.ZipFile(zip_path, "r") as archive:
            archive.extractall(destination)
        return destination

    def latest_version(self, name: str) -> str | None:
        slug = _resolve_name_to_slug(name)
        versions_root = self.root / slug
        if not versions_root.exists():
            return None

        versions = [path.name for path in versions_root.iterdir() if path.is_dir()]
        if not versions:
            return None

        return sorted(versions, key=_version_key)[-1]

    def delete(self, name: str, version: str | None = None) -> list[Path]:
        slug = _resolve_name_to_slug(name)
        removed: list[Path] = []
        if version:
            target = self.root / slug / version
            if not target.exists():
                raise FileNotFoundError(f"workflow version not found: {name}@{version}")
            shutil.rmtree(target)
            removed.append(target)
            parent = target.parent
            if parent.exists() and not any(parent.iterdir()):
                parent.rmdir()
        else:
            target = self.root / slug
            if not target.exists():
                raise FileNotFoundError(f"workflow not found: {name}")
            shutil.rmtree(target)
            removed.append(target)
        return removed

    def _build_metadata(
        self,
        spec: dict[str, Any],
        version: str,
        template_name: str,
        target_dir: Path,
    ) -> dict[str, Any]:
        data_source = _source_label(spec["data"]["source"])
        return {
            "name": spec["workflow"]["name"],
            "slug": workflow_slug(spec),
            "version": version,
            "template": template_name,
            "data_source": data_source,
            "model_type": spec["model"]["type"],
            "features": spec["features"],
            "metrics": spec["evaluation"]["metrics"],
            "tags": _derive_tags(spec),
            "published_at": datetime.now(timezone.utc).isoformat(),
            "bundle": str(target_dir / "template.zip"),
        }

    def _zip_directory(self, source_dir: Path, output_zip: Path) -> None:
        with zipfile.ZipFile(output_zip, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for file_path in sorted(source_dir.rglob("*")):
                if file_path.is_dir():
                    continue
                if _should_package(file_path, source_dir):
                    archive.write(file_path, file_path.relative_to(source_dir))


def _derive_tags(spec: dict[str, Any]) -> list[str]:
    source = spec["data"]["source"]
    tags = {
        workflow_slug(spec),
        _tagify(_source_label(source)),
        spec["model"]["type"],
        *spec["features"],
        *spec["evaluation"]["metrics"],
    }
    if isinstance(source, dict):
        if source.get("kind"):
            tags.add(_tagify(str(source["kind"])))
        if source.get("collection"):
            tags.add(_tagify(str(source["collection"])))
    return sorted(tags)


def _default_registry_root() -> Path:
    return Path(__file__).resolve().parents[2] / "registry"


def _resolve_name_to_slug(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", name.strip().lower()).strip("-")


def _search_text(item: dict[str, Any]) -> str:
    return " ".join(
        [
            str(item["name"]),
            str(item["slug"]),
            str(item["version"]),
            str(item["template"]),
            str(item["data_source"]),
            str(item["model_type"]),
            " ".join(item["features"]),
            " ".join(item["metrics"]),
            " ".join(item["tags"]),
        ]
    ).lower()


def _version_key(version: str) -> tuple[int, int, int]:
    major, minor, patch = version.split(".")
    return int(major), int(minor), int(patch)


def _ensure_empty_destination(path: Path) -> None:
    if path.exists():
        if any(path.iterdir()):
            raise FileExistsError(f"output directory must be empty: {path}")
        return
    path.mkdir(parents=True, exist_ok=True)


def _should_package(file_path: Path, source_dir: Path) -> bool:
    relative = file_path.relative_to(source_dir)
    if any(part == "__pycache__" for part in relative.parts):
        return False
    if relative.suffix == ".pyc":
        return False
    if any(part == "artifacts" for part in relative.parts):
        return False
    if file_path.name == ".DS_Store":
        return False
    return True


def _source_label(source: Any) -> str:
    if isinstance(source, str):
        return source
    if isinstance(source, dict):
        kind = str(source.get("kind", "")).lower()
        if kind == "stac":
            name = source.get("name")
            collection = source.get("collection")
            api_url = source.get("api_url")
            label = str(name or collection or "stac")
            if api_url:
                return f"stac:{label} @ {api_url}"
            return f"stac:{label}"
        if kind:
            return f"{kind}:{json.dumps(source, sort_keys=True)}"
        return json.dumps(source, sort_keys=True)
    return str(source)


def _tagify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.strip().lower()).strip("-")
