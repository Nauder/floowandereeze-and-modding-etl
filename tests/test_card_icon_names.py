"""Tests for readable atlas symbol labels."""

import pytest
from services.card_icon_names import get_card_icon_name


@pytest.mark.parametrize(
    "sprite,expected",
    [
        ("GUI_T_Icon1_Attr02", "DARK"),
        ("GUI_T_Icon1_Attr02_Ruby", "DARK (Ruby)"),
        ("GUI_T_Icon1_Type13", "Fish"),
        ("GUI_T_Icon1_Type26", "Illusion"),
        ("GUI_T_Icon1_Icon05", "Quick-Play"),
        ("GUI_T_Icon1_Link_Num_6", "Link-6"),
        ("GUI_Icon_Rarity2", "Super Rare rarity"),
        ("GUI_Icon_Rarity_Deck3", "Ultra Rare rarity (Deck)"),
        ("GUI_CardPicture_MarkerLinkOff_UL", "Link Arrow Up-Left (Inactive)"),
        ("GUI_T_Icon1_Limit00", "Forbidden"),
        ("GUI_T_Icon1_Type99", None),
        ("GUI_T_Icon1_Attr02_unknown", None),
        ("unrecognized", None),
    ],
)
def test_labels_known_symbols_only(sprite, expected):
    """Preserve variants and directions without guessing unknown symbols."""
    assert get_card_icon_name(sprite) == expected
