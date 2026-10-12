"""Metadata for card-specific Spine cut-ins, without exporting asset payloads."""

import json
import logging
import re
from typing import Any, Dict, Optional

from pandas import DataFrame, Series

CUTIN_PATH = re.compile(
    r"^assets/resourcesassetbundle/duel/timeline/duel/monstercutin/"
    r"(?P<region>common|tcg|ocg)/(?:monster/)?p(?P<card_id>\d+)/(?P<relative>.+)$",
    re.IGNORECASE,
)
SHARED_PATH = re.compile(
    r"^assets/resourcesassetbundle/duel/timeline/duel/monstercutin/"
    r"(?P<region>common)/(?P<relative>shader/.+|[^/]+\.anim)$",
    re.IGNORECASE,
)
STRING_COLUMNS = (
    "game_region",
    "source",
    "bundle",
    "asset_path",
    "card_id",
    "display_name",
    "asset_region",
    "quality",
    "scale",
    "role",
    "object_type",
    "path_id",
    "serialized_file",
    "asset_name",
    "spine_version",
    "animation_names",
    "atlas_pages",
    "external_references",
)
INTEGER_COLUMNS = ("width", "height")


def cutin_identity(asset_path: str) -> Optional[Dict[str, str]]:
    """Match card cut-ins only; universal summon effects are a different feature."""
    match = CUTIN_PATH.fullmatch(asset_path) or SHARED_PATH.fullmatch(asset_path)
    if not match:
        return None
    parts = match["relative"].split("/")
    card_id = match.groupdict().get("card_id")
    quality = parts[0].lower() if card_id and len(parts) > 1 else None
    scale = (
        parts[1]
        if card_id and len(parts) > 2 and re.fullmatch(r"\d+(?:\.\d+)?", parts[1])
        else None
    )
    filename = parts[-1].lower()
    if filename.endswith(".atlas.txt"):
        role = "atlas"
    elif filename.endswith(".json"):
        role = "skeleton_json"
    elif filename.endswith(".png"):
        role = "texture"
    elif filename.endswith(".mat"):
        role = "material"
    elif filename.endswith(".prefab"):
        role = "prefab"
    elif filename.endswith(".anim"):
        role = "animation_clip"
    elif filename.endswith(".playable"):
        role = "timeline"
    elif filename.endswith("_skeletondata.asset"):
        role = "skeleton_data"
    elif filename.endswith("_atlas.asset"):
        role = "atlas_asset"
    elif re.fullmatch(r"(?:p\d+)?animation(?:_.*)?\.asset", filename):
        role = "animation"
    else:
        role = "other"
    return {
        "card_id": card_id,
        "asset_region": match["region"].lower(),
        "quality": quality,
        "scale": scale,
        "role": role,
    }


def atlas_pages(script: str) -> list:
    """Read page headers (first line of each blank-line-separated atlas page)."""
    return [
        block.splitlines()[0].strip()
        for block in re.split(r"\n\s*\n", script.replace("\r\n", "\n").strip())
        if block.strip()
    ]


def extract_cutin_metadata(
    env: Any, bundle: str, is_streaming: bool, ocg_only: bool
) -> Dict[str, Dict[str, Any]]:
    """Keep container object identity and component metadata for each bundle."""
    records = {}
    logger = logging.getLogger(__name__)
    for asset_path, pointer in env.container.items():
        identity = cutin_identity(asset_path)
        if identity is None or identity["asset_region"] not in (
            "common",
            "ocg" if ocg_only else "tcg",
        ):
            continue
        record = {
            **identity,
            "game_region": "ocg" if ocg_only else "global",
            "source": "streaming" if is_streaming else "local",
            "bundle": bundle,
            "asset_path": asset_path,
        }
        # Resolve the actual container object, never the first object in a bundle.
        try:
            obj = pointer.deref()
            record.update(
                {
                    "object_type": obj.type.name,
                    "path_id": str(obj.path_id),
                    "serialized_file": obj.assets_file.name,
                    "external_references": json.dumps(
                        [external.path for external in obj.assets_file.externals]
                    ),
                }
            )
            data = obj.read()
            record["asset_name"] = getattr(data, "m_Name", None)
            if obj.type.name == "Texture2D":
                record.update(width=data.m_Width, height=data.m_Height)
            elif obj.type.name == "TextAsset":
                if identity["role"] == "atlas":
                    record["atlas_pages"] = json.dumps(atlas_pages(data.m_Script))
                elif identity["role"] == "skeleton_json":
                    skeleton = json.loads(data.m_Script)
                    record["spine_version"] = skeleton.get("skeleton", {}).get("spine")
                    record["animation_names"] = json.dumps(
                        sorted(skeleton.get("animations", {}))
                    )
        except (ValueError, TypeError, AttributeError, KeyError) as error:
            # Retain the reference so future formats remain discoverable.
            logger.warning("Incomplete cut-in metadata for %s: %s", asset_path, error)
        key = json.dumps([record["game_region"], record["source"], bundle, asset_path])
        records[key] = record
    return records


def cutin_dataframe(data: Dict[str, Any]) -> DataFrame:
    """Export stable, typed columns even for older intermediates or empty scans.

    Path IDs are decimal strings: Unity IDs can exceed JavaScript's safe integer
    range. JSON string columns avoid nested Arrow/SQLite schema dependencies.
    """
    records = []
    names = data.get("summon_card_names", {})
    for record in data.get("summon_animation", {}).values():
        record = dict(record)
        record["display_name"] = names.get(record["card_id"])
        records.append(record)
    records.sort(
        key=lambda row: (
            row["game_region"],
            row["source"],
            row["bundle"],
            row["asset_path"],
        )
    )
    frame = DataFrame(records, columns=STRING_COLUMNS + INTEGER_COLUMNS)
    for column in STRING_COLUMNS:
        frame[column] = Series(frame[column], dtype="string")
    for column in INTEGER_COLUMNS:
        frame[column] = Series(frame[column], dtype="Int64")
    return frame
