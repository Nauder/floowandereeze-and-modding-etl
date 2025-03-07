import os
import re

import UnityPy

from services.unity_service import UnityService
from util import STREAMING_PATH, GAME_PATH


class GameService:
    face_names = {
        'card_frame00': 'Normal',
        'card_frame01': 'Effect',
        'card_frame02': 'Ritual',
        'card_frame03': 'Fusion',
        'card_frame07': 'Spell',
        'card_frame08': 'Trap',
        'card_frame09': 'Token',
        'card_frame10': 'Synchro',
        'card_frame12': 'Xyz',
        'card_frame13': 'Normal Pendulum',
        'card_frame14': 'Effect Pendulum',
        'card_frame15': 'Xyz Pendulum',
        'card_frame16': 'Synchro Pendulum',
        'card_frame17': 'Fusion Pendulum',
        'card_frame18': 'Link',
        'card_frame19': 'Ritual Pendulum'
    }

    def __init__(self):
        self.unity_servie = UnityService()

    def get_dir_data(self, data_dir: str, is_streaming: bool) -> dict:

        ids = {'card_id': {}, 'sleeve': [], 'icon': {}, 'deck_box': {}, 'field': [], 'wallpaper': {}}
        for _, _, files in os.walk(os.path.join(STREAMING_PATH if is_streaming else GAME_PATH, data_dir)):
            for bundle in files:
                env = UnityPy.load(self.unity_servie.prepare_environment(is_streaming, bundle))
                for key in env.container.keys():
                    if data_dir.lower() == "c7":
                        pass
                    if "card/images/illust/common/" in key or "card/images/illust/tcg/" in key:
                        self._parse_card(ids, env, bundle)
                    elif "images/profileicon/" in key:
                        self._parse_icon(ids, env, bundle)
                    elif "assets/resourcesassetbundle/protector/common/" in key or "assets/resourcesassetbundle/protector/tcg/" in key:
                        self._parse_sleeve(ids, env, bundle)
                    elif "assets/resourcesassetbundle/images/deckcase" in key:
                        self._parse_deck_box(ids, env, bundle)
                    elif re.search(re.compile(r"mat_0\d\d_near"), key.lower()):
                        self._parse_field(ids, env, bundle)
                    elif "card/data" in key and "en-us/card_" in key:
                        self._parse_card_data_part(env, key.split("/")[-1])
                    elif "assets/resourcesassetbundle/wallpaper/wallpaper" in key and (
                        "wallpapericon" in key or re.search(re.compile(r"tcg/wallpaper\d\d\d\d_\d"), key.lower())
                    ):
                        self._parse_wallpaper(ids, env, bundle, re.search(r'\d{4}', key).group(0))

        return ids

    def get_unity3d_data(self) -> dict:
        ids = {'card_id': {}, 'face': {}}
        env = UnityPy.load(self.unity_servie.prepare_unity3d_environment())

        for obj in env.objects:
            try:
                data = obj.read()

                if hasattr(data, 'm_Name') and hasattr(data, 'm_CompleteImageSize') and 'card_frame' in data.m_Name and data.m_CompleteImageSize == 720896:
                    ids['face'][self.face_names[data.m_Name]] = obj.path_id

            # Ignore assets that UnityPy can't read
            except ValueError:
                pass

        return ids

    def _parse_card(self, ids: dict, env, bundle: str):
        for obj in env.objects:
            if obj.type.name == "Texture2D":
                obj_data = obj.read()
                ids["card_id"][obj_data.m_Name] = bundle

    def _parse_icon(self, ids: dict, env, bundle: str):
        for obj in env.objects:
            if obj.type.name == "Texture2D":
                obj_data = obj.read()
                ids["icon"].setdefault(obj_data.m_Name[11:18], []).append(bundle)

    def _parse_sleeve(self, ids: dict, env, bundle: str):
        for obj in env.objects:
            obj_data = obj.read()
            if obj.type.name == "Texture2D" and "ProtectorIcon" in obj_data.m_Name:
                ids["sleeve"].append(bundle)

    def _parse_deck_box(self, ids: dict, env, bundle: str):
        for obj in env.objects:
            obj_data = obj.read()
            if "DeckCase" in obj_data.m_Name and obj.type.name == "Texture2D":
                deck_id = int(''.join(ch for ch in obj_data.m_Name if ch.isdigit()))
                match [''.join(ch for ch in obj_data.m_Name if not ch.isdigit()),
                       (obj_data.m_Width, obj_data.m_Height)]:
                    case ["DeckCase", (256, 256)]:
                        image_type = "small"
                    case ["DeckCase_L", (256, 256)]:
                        image_type = "medium"
                    case ["DeckCase_L", (512, 512)]:
                        image_type = "large"
                    case ["DeckCase_L_reverse", (256, 256)]:
                        image_type = "r_medium"
                    case ["DeckCase_L_reverse", (512, 512)]:
                        image_type = "r_large"
                    case ["DeckCase_Open_L", (256, 256)]:
                        image_type = "o_medium"
                    case ["DeckCase_Open_L", (512, 512)]:
                        image_type = "o_large"
                    case _:
                        image_type = ""
                if deck_id not in ids["deck_box"].keys():
                    ids["deck_box"][deck_id] = {}
                ids["deck_box"][deck_id][image_type] = bundle

    def _parse_field(self, ids: dict, env, bundle: str):
        for obj in env.objects:
            obj_data = obj.read()
            if (hasattr(obj_data, 'm_Name')
                    and re.search(re.compile(r"mat_0\d\d_01_basecolor_near"), obj_data.m_Name.lower())
                    and obj.type.name == "Texture2D"):
                ids["field"].append(bundle)

    def _parse_wallpaper(self, ids, env, bundle: str, wallpaper: str):
        for obj in env.objects:
            obj_data = obj.read()
            if obj.type.name == "Texture2D":
                if wallpaper not in ids["wallpaper"].keys():
                    ids["wallpaper"][wallpaper] = {}
                if "Icon" in obj_data.m_Name:
                    ids["wallpaper"][wallpaper]['icon'] = bundle
                elif "_1" in obj_data.m_Name:
                    ids["wallpaper"][wallpaper]['front'] = bundle
                elif "_2" in obj_data.m_Name:
                    ids["wallpaper"][wallpaper]['back'] = bundle

    def _parse_card_data_part(self, env, part: str):
        for obj in env.objects:
            data = obj.read()
            if obj.type.name == "TextAsset":
                with open(f"./etl/services/temp/{part}", "wb") as f:
                    f.write(data.m_Script.encode("utf-8", "surrogateescape"))
