"""Tests for readable atlas symbol labels."""

import pytest
from services.card_icon_names import get_card_icon_name


@pytest.mark.parametrize(
    "sprite,expected",
    [
        ("GUI_T_Icon1_Attr02", "DARK"),
        ("GUI_T_Icon1_Attr02_Ruby", "DARK (Ruby)"),
        ("GUI_T_Icon1_Type01", "Dragon"),
        ("GUI_T_Icon1_Type02", "Zombie"),
        ("GUI_T_Icon1_Type02_Ruby", "Zombie (Ruby)"),
        ("GUI_T_Icon1_Type08", "Fish"),
        ("GUI_T_Icon1_Type13", "Plant"),
        ("GUI_T_Icon1_Type18", "Spellcaster"),
        ("GUI_T_Icon1_Type22", "Wyrm"),
        ("GUI_T_Icon1_Type23", "Cyberse"),
        ("GUI_T_Icon1_Type24", "Divine-Beast"),
        ("GUI_T_Icon1_Type25", "Illusion"),
        ("GUI_T_Icon1_Type26", "Creator God"),
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
