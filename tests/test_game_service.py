"""Tests for regional asset extraction in GameService."""

# pylint: disable=missing-function-docstring

from unittest.mock import MagicMock, patch

from services.game_service import GameService


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
