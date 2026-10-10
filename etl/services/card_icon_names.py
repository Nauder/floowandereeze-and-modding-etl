"""Readable labels derived from CardSpriteAtlas sprite keys, rather than item text."""

import re
from typing import Optional


# Numeric suffixes follow the game's card attribute, type and icon enums.
ATTRIBUTES = {
    1: "LIGHT",
    2: "DARK",
    3: "WATER",
    4: "FIRE",
    5: "EARTH",
    6: "WIND",
    7: "DIVINE",
    8: "Spell",
    9: "Trap",
}
MONSTER_TYPES = dict(
    enumerate(
        [
            "Dragon",
            "Spellcaster",
            "Zombie",
            "Warrior",
            "Beast-Warrior",
            "Beast",
            "Winged Beast",
            "Fiend",
            "Fairy",
            "Insect",
            "Dinosaur",
            "Reptile",
            "Fish",
            "Sea Serpent",
            "Machine",
            "Thunder",
            "Aqua",
            "Pyro",
            "Rock",
            "Plant",
            "Psychic",
            "Divine-Beast",
            "Creator God",
            "Wyrm",
            "Cyberse",
            "Illusion",
        ],
        start=1,
    )
)
SPELL_TRAP_ICONS = {
    0: "Normal",
    1: "Counter",
    2: "Field",
    3: "Equip",
    4: "Continuous",
    5: "Quick-Play",
    6: "Ritual",
    7: "Ritual (Rush Duel)",
}
RARITIES = {0: "Normal", 1: "Rare", 2: "Super Rare", 3: "Ultra Rare"}
DIRECTIONS = {
    "U": "Up",
    "D": "Down",
    "L": "Left",
    "R": "Right",
    "UL": "Up-Left",
    "UR": "Up-Right",
    "DL": "Down-Left",
    "DR": "Down-Right",
}


def get_card_icon_name(sprite: str) -> Optional[str]:
    """Label recognized symbols; leave unfamiliar sprite keys unresolved."""
    match = re.fullmatch(r"GUI_T_Icon1_(Attr|Type|Icon)(\d+)(_Ruby)?", sprite)
    if match:
        category, number, ruby = match.groups()
        labels = {"Attr": ATTRIBUTES, "Type": MONSTER_TYPES, "Icon": SPELL_TRAP_ICONS}
        label = labels[category].get(int(number))
        return f"{label} (Ruby)" if label and ruby else label
    match = re.fullmatch(r"GUI_Icon_Rarity(_Deck)?([0-3])", sprite)
    if match:
        deck, number = match.groups()
        return f"{RARITIES[int(number)]} rarity" + (" (Deck)" if deck else "")
    match = re.fullmatch(r"GUI_T_Icon1_Link_Num_([1-8])", sprite)
    if match:
        return f"Link-{match.group(1)}"
    match = re.fullmatch(
        r"GUI_CardPicture_MarkerLink(On|Off)_(U|D|L|R|UL|UR|DL|DR)", sprite
    )
    if match:
        state, direction = match.groups()
        return f"Link Arrow {DIRECTIONS[direction]} ({'Active' if state == 'On' else 'Inactive'})"
    return {
        "GUI_T_Icon1_Limit00": "Forbidden",
        "GUI_T_Icon1_Limit01": "Limited",
        "GUI_T_Icon1_Limit02": "Semi-Limited",
        "GUI_T_Icon1_LimitNone": "Unlimited",
        "GUI_T_Icon1_Mask_Level": "Level mask",
        "GUI_Icon_TurnCounter": "Turn counter",
    }.get(sprite)
