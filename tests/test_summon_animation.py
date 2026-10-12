"""Tests for summon cut-in discovery, object identity, and Parquet exports."""

# pylint: disable=missing-function-docstring

import json
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from services.data_service import DataService
from services.game_service import GameService
from services.summon_animation import (
    INTEGER_COLUMNS,
    STRING_COLUMNS,
    atlas_pages,
    cutin_dataframe,
    cutin_identity,
    extract_cutin_metadata,
)
from util import IdsData

PREFIX = "assets/resourcesassetbundle/duel/timeline/duel/monstercutin/"


def _pointer(kind, path_id, **values):
    obj = SimpleNamespace(
        type=SimpleNamespace(name=kind),
        path_id=path_id,
        assets_file=SimpleNamespace(
            name="CAB-example",
            externals=[SimpleNamespace(path="archive:/CAB-other/CAB-other")],
        ),
        read=MagicMock(return_value=SimpleNamespace(**values)),
    )
    return SimpleNamespace(deref=lambda: obj), obj


@pytest.mark.parametrize(
    "relative,role,quality,scale",
    [
        ("sd/0.5/p8258.png", "texture", "sd", "0.5"),
        ("highend_hd/1/p8258_2.png", "texture", "highend_hd", "1"),
        ("sd/p8258js.json", "skeleton_json", "sd", None),
        ("sd/0.5/p8258.atlas.txt", "atlas", "sd", "0.5"),
        ("sd/0.5/p8258_atlas.asset", "atlas_asset", "sd", "0.5"),
        ("sd/p8258js_skeletondata.asset", "skeleton_data", "sd", None),
        ("sd/p8258.prefab", "prefab", "sd", None),
        ("p8258timeline.playable", "timeline", None, None),
        ("animation_kde.asset", "animation", None, None),
        ("unknown.asset", "other", None, None),
        ("sd/referenceassets/p8258animation.asset", "animation", "sd", None),
        ("p8258.anim", "animation_clip", None, None),
    ],
)
def test_component_paths(relative, role, quality, scale):
    assert cutin_identity(PREFIX + "TCG/P8258/" + relative) == {
        "card_id": "8258",
        "asset_region": "tcg",
        "role": role,
        "quality": quality,
        "scale": scale,
    }


@pytest.mark.parametrize(
    "asset_path",
    [
        "assets/resourcesassetbundle/duel/timeline/duel/universal/summon/p8258.png",
        "card/images/illust/tcg/8258.png",
        PREFIX + "tcg/pbad/sd/file.png",
        PREFIX + "tcg/p8258/",
    ],
)
def test_does_not_classify_unrelated_assets(asset_path):
    assert cutin_identity(asset_path) is None


def test_scan_preserves_components_and_does_not_read_unrelated_objects():
    skeleton, _ = _pointer(
        "TextAsset",
        6979588375885976112,
        m_Name="P8258JS",
        m_Script=json.dumps(
            {
                "skeleton": {"spine": "4.2.22"},
                "animations": {"intro": {}, "animation": {}},
            }
        ),
    )
    texture, obj = _pointer(
        "Texture2D", -4137136694945423547, m_Name="P8258", m_Width=2048, m_Height=2048
    )
    unrelated = MagicMock()
    env = SimpleNamespace(
        container={
            PREFIX + "tcg/p8258/sd/p8258js.json": skeleton,
            PREFIX + "tcg/p8258/sd/0.5/p8258.png": texture,
            "other/file.png": unrelated,
            PREFIX + "ocg/p8258/sd/file.png": unrelated,
        }
    )
    records = list(extract_cutin_metadata(env, "0016672c", True, False).values())
    assert len(records) == 2
    assert records[0]["spine_version"] == "4.2.22"
    assert json.loads(records[0]["animation_names"]) == ["animation", "intro"]
    assert records[1]["path_id"] == "-4137136694945423547"
    assert records[1]["serialized_file"] == "CAB-example"
    assert json.loads(records[1]["external_references"]) == [
        "archive:/CAB-other/CAB-other"
    ]
    assert records[1]["width"] == 2048
    assert records[1]["source"] == "streaming"
    obj.read.assert_called_once()
    unrelated.deref.assert_not_called()


def test_multi_page_atlas_ignores_region_names():
    assert atlas_pages(
        "\r\nP1.png\r\nsize:2048,2048\r\nBody\r\nbounds:1,2,3,4\r\n"
        "\r\nP2.png\r\nsize:1024,1024\r\nFace\r\nbounds:1,2,3,4\r\n"
    ) == ["P1.png", "P2.png"]


def test_bad_json_keeps_reference_and_logs_warning(caplog):
    pointer, _ = _pointer("TextAsset", 123, m_Name="Broken", m_Script="bad")
    asset_path = PREFIX + "tcg/p8258/sd/p8258js.json"
    rows = extract_cutin_metadata(
        SimpleNamespace(container={asset_path: pointer}), "12345678", False, False
    )
    assert next(iter(rows.values()))["path_id"] == "123"
    assert "Incomplete cut-in metadata" in caplog.text


def test_pipeline_routes_ocg_and_common_cutins():
    pointer, _ = _pointer("Texture2D", 123, m_Name="P8258", m_Width=1, m_Height=1)
    env = SimpleNamespace(
        container={
            PREFIX + region + "/p8258/sd/file.png": pointer
            for region in ("tcg", "ocg", "common")
        }
    )
    with (
        patch("services.game_service.os.walk", return_value=[("", [], ["12345678"])]),
        patch("services.game_service.UnityPy.load", return_value=env),
    ):
        ids = GameService().get_dir_data("12", False, "path", True)
    assert {r["asset_region"] for r in ids.summon_animation.values()} == {
        "ocg",
        "common",
    }
    assert {r["game_region"] for r in ids.summon_animation.values()} == {"ocg"}


def test_merge_keeps_multiple_bundles_and_origins():
    pointer, _ = _pointer("Texture2D", 123, m_Name="P8258", m_Width=1, m_Height=1)
    env = SimpleNamespace(container={PREFIX + "tcg/p8258/sd/file.png": pointer})
    service = DataService()
    ids = IdsData()
    for bundle, streaming in [
        ("11111111", False),
        ("22222222", False),
        ("11111111", True),
    ]:
        service.merge_data(
            ids,
            IdsData(
                summon_animation=extract_cutin_metadata(env, bundle, streaming, False)
            ),
        )
    service.merge_data(ids, ids)
    assert len(ids.summon_animation) == 3


def test_parquet_roundtrip_and_names(tmp_path):
    pointer, _ = _pointer(
        "Texture2D", 6979588375885976112, m_Name="P8258", m_Width=2048, m_Height=2048
    )
    env = SimpleNamespace(container={PREFIX + "tcg/p8258/sd/file.png": pointer})
    data = {
        "summon_animation": extract_cutin_metadata(env, "12345678", False, False),
        "summon_card_names": {"8258": "Card name"},
    }
    frame = cutin_dataframe(data)
    file_path = tmp_path / "summon.parquet"
    frame.to_parquet(file_path, index=False)
    loaded = pd.read_parquet(file_path)
    assert loaded.loc[0, "path_id"] == "6979588375885976112"
    assert loaded.loc[0, "display_name"] == "Card name"
    assert loaded.loc[0, "width"] == 2048
    assert list(loaded.columns) == list(STRING_COLUMNS + INTEGER_COLUMNS)
    assert pd.isna(loaded.loc[0, "spine_version"])


def test_empty_and_old_intermediates_have_typed_schema(tmp_path):
    frame = cutin_dataframe({})
    frame.to_parquet(tmp_path / "empty.parquet", index=False)
    loaded = pd.read_parquet(tmp_path / "empty.parquet")
    assert loaded.empty
    assert list(loaded.columns) == list(STRING_COLUMNS + INTEGER_COLUMNS)
    assert str(frame.dtypes["path_id"]) == "string"
    assert str(loaded.dtypes["width"]) == "Int64"


def test_decoded_card_ids_join_through_clean_and_write(tmp_path, monkeypatch):
    """Names join by card ID even when the data index is a different number."""
    monkeypatch.chdir(tmp_path)
    temp = tmp_path / "etl" / "services" / "temp"
    temp.mkdir(parents=True)
    (tmp_path / "data").mkdir()
    pointer, _ = _pointer(
        "TextAsset",
        6979588375885976112,
        m_Name="P8258JS",
        m_Script=json.dumps(
            {"skeleton": {"spine": "4.2.22"}, "animations": {"animation": {}}}
        ),
    )
    env = SimpleNamespace(container={PREFIX + "tcg/p8258/sd/p8258js.json": pointer})
    ids = IdsData(
        card_id={"8258": "12345678"},
        summon_animation=extract_cutin_metadata(env, "12345678", False, False),
    )
    inputs = {
        "ids.json": asdict(ids),
        "card_name.bytes.dec.json": ["Test card"],
        "card_prop.bytes.Card_IDs.dec.json": [8258],
        "card_desc.bytes.dec.json": ["Test description"],
    }
    for filename, data in inputs.items():
        (temp / filename).write_text(json.dumps(data), encoding="utf8")
    service = DataService()
    service.get_card_data()
    service.clean_data(sort_fields=False)
    service.write_data()
    exported = pd.read_parquet(tmp_path / "data" / "summon_animations.parquet")
    assert exported.loc[0, "card_id"] == "8258"
    assert exported.loc[0, "display_name"] == "Test card"
    assert exported.loc[0, "spine_version"] == "4.2.22"
    assert (
        pd.read_parquet(tmp_path / "data" / "cards.parquet").loc[0, "data_index"] == 0
    )


def test_committed_summon_metadata_integrity():
    """Validate the extracted snapshot's identities, variants, and JSON metadata."""
    frame = pd.read_parquet(
        Path(__file__).resolve().parents[1] / "data" / "summon_animations.parquet"
    )
    assert not frame.empty
    assert list(frame.columns) == list(STRING_COLUMNS + INTEGER_COLUMNS)
    keys = ["game_region", "source", "bundle", "asset_path"]
    assert not frame[keys].isna().any().any()
    assert not frame.duplicated(keys).any()
    assert frame.bundle.str.fullmatch(r"[0-9a-f]{8}").all()
    assert frame.card_id.dropna().str.fullmatch(r"\d+").all()
    assert frame.path_id.dropna().str.fullmatch(r"-?\d+").all()
    assert frame.display_name.notna().any()
    assert {"sd", "highend_hd"}.issubset(set(frame.quality.dropna()))
    assert {"texture", "skeleton_json", "atlas", "timeline"}.issubset(set(frame.role))
    for row in frame.to_dict("records"):
        identity = cutin_identity(row["asset_path"])
        assert identity is not None
        assert identity["card_id"] == row["card_id"]
        assert identity["role"] == row["role"]
        for column in ("animation_names", "atlas_pages", "external_references"):
            if pd.notna(row[column]):
                values = json.loads(row[column])
                assert isinstance(values, list)
                assert all(isinstance(value, str) for value in values)
    textures = frame[frame.object_type == "Texture2D"]
    assert (textures[["width", "height"]] > 0).all().all()


def test_common_monster_and_shared_dependencies():
    assert cutin_identity(PREFIX + "common/monster/p23487/sd/p23487_tree.png") == {
        "card_id": "23487",
        "asset_region": "common",
        "quality": "sd",
        "scale": None,
        "role": "texture",
    }
    for filename, role in [
        ("shader/sozai.png", "texture"),
        ("shader/add_sm_ukiyoe.mat", "material"),
        ("sm_fire4_tras_common.anim", "animation_clip"),
    ]:
        identity = cutin_identity(PREFIX + "common/" + filename)
        assert identity["card_id"] is None
        assert identity["quality"] is None
        assert identity["role"] == role
