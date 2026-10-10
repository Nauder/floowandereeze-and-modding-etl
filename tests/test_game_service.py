"""Tests for regional asset extraction in GameService."""

# pylint: disable=missing-function-docstring

from unittest.mock import MagicMock, patch
from types import SimpleNamespace
import zlib

import msgpack
import pytest

from services.game_service import GameService
from services.decode_service import DecodeService
from util import CARD_FACE_SIZE, IdsData


ASSET_KEYS = {
    "card/images/illust/common/card": None,
    "card/images/illust/tcg/card": None,
    "card/images/illust/ocg/card": None,
    "images/profileicon/icon": None,
    "assets/resourcesassetbundle/protector/common/icon": None,
    "assets/resourcesassetbundle/protector/tcg/icon": None,
    "assets/resourcesassetbundle/protector/ocg/icon": None,
}


def _run_extraction(ocg_only):
    service = GameService()
    env = MagicMock()
    env.container = ASSET_KEYS

    parsers = {
        name: patch.object(service, name)
        for name in (
            "_parse_card",
            "_parse_ocg_card",
            "_parse_icon",
            "_parse_sleeve",
            "_parse_ocg_sleeve",
        )
    }
    mocks = {name: parser.start() for name, parser in parsers.items()}
    try:
        with (
            patch("services.game_service.os.walk", return_value=[("", [], ["bundle"])]),
            patch("services.game_service.UnityPy.load", return_value=env),
        ):
            service.get_dir_data("ab", False, "game_path", ocg_only)
    finally:
        for parser in parsers.values():
            parser.stop()

    return mocks


def test_global_scan_excludes_ocg_assets():
    parsers = _run_extraction(ocg_only=False)

    assert parsers["_parse_card"].call_count == 2
    assert parsers["_parse_sleeve"].call_count == 2
    parsers["_parse_icon"].assert_called_once()
    parsers["_parse_ocg_card"].assert_not_called()
    parsers["_parse_ocg_sleeve"].assert_not_called()


def test_ocg_scan_extracts_only_ocg_assets():
    parsers = _run_extraction(ocg_only=True)

    assert parsers["_parse_ocg_card"].call_count == 2
    assert parsers["_parse_ocg_sleeve"].call_count == 2
    parsers["_parse_card"].assert_not_called()
    parsers["_parse_sleeve"].assert_not_called()
    parsers["_parse_icon"].assert_not_called()


def _asset(asset_type, **data):
    obj = MagicMock()
    obj.type.name = asset_type
    obj.read.return_value = SimpleNamespace(**data)
    return obj


@pytest.mark.parametrize("ocg", [False, True])
def test_sleeve_item_id_ignores_texture_variant(ocg):
    service = GameService()
    ids = IdsData()
    env = SimpleNamespace(
        objects=[_asset("Texture2D", m_Name="ProtectorIcon1072008_2")]
    )
    parser = service._parse_ocg_sleeve if ocg else service._parse_sleeve
    parser(ids, env, "bundle")
    key = "ocg_sleeve" if ocg else "sleeve"
    assert getattr(ids, key) == ["bundle"]
    assert getattr(ids, key + "_ids") == {"bundle": "1072008"}


def test_extracts_compressed_english_item_names_through_scan():
    names = {"_GRP_": "ITEM", "_LNG_": "en-US", "abcdef12": "Sleeve name"}
    script = b"YDLZ\x01\x00\x00\x00" + zlib.compress(msgpack.packb(names))
    env = SimpleNamespace(
        container={"assets/resourcesassetbundle/texts/en-us/ids_item.bytes": None},
        objects=[
            _asset(
                "TextAsset",
                m_Name="IDS_ITEM",
                m_Script=script.decode("utf-8", "surrogateescape"),
            )
        ],
    )
    with (
        patch("services.game_service.os.walk", return_value=[("", [], ["bundle"])]),
        patch("services.game_service.UnityPy.load", return_value=env),
    ):
        ids = GameService().get_dir_data("ab", False, "game_path")
    assert ids.item_names == {"abcdef12": "Sleeve name"}


def test_item_names_reject_non_dictionary():
    with pytest.raises(ValueError, match="item name dictionary"):
        DecodeService.decode_item_names(msgpack.packb(["bad"]))


def test_field_item_id_comes_from_base_texture():
    ids = IdsData()
    env = SimpleNamespace(
        objects=[
            _asset("Texture2D", m_Name="fxt_sand_001"),
            _asset("Texture2D", m_Name="Mat_011_01_BaseColor_near"),
        ]
    )
    GameService()._parse_field(ids, env, "bundle")
    assert ids.field == ["bundle"]
    assert ids.field_ids == {"bundle": "1090011"}


def test_unity3d_extracts_metadata_without_reading_unrelated_objects():
    unrelated = _asset("MonoBehaviour")
    unrelated.read.side_effect = AssertionError("Unrelated object was read")
    face = _asset(
        "Texture2D", m_Name="card_frame00", m_CompleteImageSize=CARD_FACE_SIZE
    )
    face.path_id = 123
    other_texture = _asset("Texture2D", m_Name="card_frame01", m_CompleteImageSize=1)
    other_atlas = _asset("SpriteAtlas", m_Name="OtherAtlas")
    rect = SimpleNamespace(x=12, y=34, width=56, height=78)
    atlas = _asset(
        "SpriteAtlas",
        m_Name="CardSpriteAtlas",
        m_RenderDataMap=[("sprite", SimpleNamespace(textureRect=rect))],
        m_PackedSpriteNamesToIndex=["attribute_icon"],
    )
    env = SimpleNamespace(objects=[unrelated, face, other_texture, other_atlas, atlas])

    with patch("services.game_service.UnityPy.load", return_value=env):
        ids = GameService().get_unity3d_data()

    assert ids == IdsData(
        face={"Normal": 123},
        card_icon={"attribute_icon": {"x": 12, "y": 912, "width": 56, "height": 78}},
    )
    unrelated.read.assert_not_called()
    for obj in (face, other_texture, other_atlas, atlas):
        obj.read.assert_called_once_with()


@pytest.mark.parametrize("asset_type", ["Texture2D", "SpriteAtlas"])
def test_unity3d_skips_unreadable_objects_and_continues(asset_type):
    unreadable = _asset(asset_type)
    unreadable.read.side_effect = ValueError("Cannot read object")
    face = _asset(
        "Texture2D", m_Name="card_frame18", m_CompleteImageSize=CARD_FACE_SIZE
    )
    face.path_id = 456

    with patch(
        "services.game_service.UnityPy.load",
        return_value=SimpleNamespace(objects=[unreadable, face]),
    ):
        ids = GameService().get_unity3d_data()

    assert ids == IdsData(face={"Link": 456})
