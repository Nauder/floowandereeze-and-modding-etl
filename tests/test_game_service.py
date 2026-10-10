"""Tests for regional asset extraction in GameService."""

# pylint: disable=missing-function-docstring

from unittest.mock import MagicMock, patch
from types import SimpleNamespace
import zlib

import msgpack
import pytest

from services.game_service import GameService
from services.decode_service import DecodeService
from util import IdsData


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
