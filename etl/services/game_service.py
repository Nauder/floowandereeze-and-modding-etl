"""Service for handling game data extraction and processing."""

import os
import re
import logging
from typing import Any, Dict

import UnityPy

from util import STREAMING_PATH, GAME_PATH, get_data_wrapper

from .unity_service import UnityService


class GameService:
    """Service class for handling game data operations."""

    face_names: Dict[str, str] = {
        "card_frame00": "Normal",
        "card_frame01": "Effect",
        "card_frame02": "Ritual",
        "card_frame03": "Fusion",
        "card_frame07": "Spell",
        "card_frame08": "Trap",
        "card_frame09": "Token",
        "card_frame10": "Synchro",
        "card_frame12": "Xyz",
        "card_frame13": "Normal Pendulum",
        "card_frame14": "Effect Pendulum",
        "card_frame15": "Xyz Pendulum",
        "card_frame16": "Synchro Pendulum",
        "card_frame17": "Fusion Pendulum",
        "card_frame18": "Link",
        "card_frame19": "Ritual Pendulum",
    }

    def __init__(self) -> None:
        """Initialize the GameService with a UnityService instance."""
        self.logger = logging.getLogger("GameService")
        self.unity_service = UnityService()

    def get_dir_data(self, data_dir: str, is_streaming: bool) -> Dict[str, Any]:
        """Get data from a directory in the game files.

        Args:
            data_dir: Directory to extract data from.
            is_streaming: Whether to use streaming assets path.

        Returns:
            Dictionary containing extracted data.
        """
        ids = get_data_wrapper()
        for _, _, files in os.walk(
            os.path.join(STREAMING_PATH if is_streaming else GAME_PATH, data_dir)
        ):
            for bundle in files:
                env = UnityPy.load(
                    self.unity_service.prepare_environment(is_streaming, bundle)
                )
                for key in env.container.keys():
                    if data_dir.lower() == "c7":
                        pass
                    if (
                        "card/images/illust/common/" in key
                        or "card/images/illust/tcg/" in key
                    ):
                        self._parse_card(ids, env, bundle)
                    elif "images/profileicon/" in key:
                        self._parse_icon(ids, env, bundle)
                    elif (
                        "assets/resourcesassetbundle/protector/common/" in key
                        or "assets/resourcesassetbundle/protector/tcg/" in key
                    ):
                        self._parse_sleeve(ids, env, bundle)
                    elif "assets/resourcesassetbundle/images/deckcase" in key:
                        self._parse_deck_box(ids, env, bundle)
                    elif re.search(re.compile(r"mat_0\d\d_near"), key.lower()):
                        self._parse_field(ids, env, bundle)
                    elif "card/data" in key and "en-us/card_" in key:
                        self._parse_card_data_part(env, key.split("/")[-1], ids, bundle)
                    elif "assets/resourcesassetbundle/wallpaper/wallpaper" in key and (
                        "wallpapericon" in key
                        or re.search(
                            re.compile(r"tcg/wallpaper\d\d\d\d_\d"), key.lower()
                        )
                    ):
                        self._parse_wallpaper(
                            ids, env, bundle, re.search(r"\d{4}", key).group(0)
                        )
                    elif re.search(re.compile(r"coin\d\dtex"), key.lower()) or (
                        "cointoss" in key.lower() and "icon" not in key.lower()
                    ):
                        self._parse_coin(ids, env, bundle)

        return ids

    def get_unity3d_data(self) -> Dict[str, Any]:
        """Get data from Unity3D files.

        Returns:
            Dictionary containing Unity3D data.
        """
        ids = {"card_id": {}, "face": {}}
        env = UnityPy.load(self.unity_service.prepare_unity3d_environment())

        self.logger.info("Got env...")

        for obj in env.objects:
            try:
                data = obj.read()

                if (
                    hasattr(data, "m_Name")
                    and hasattr(data, "m_CompleteImageSize")
                    and "card_frame" in data.m_Name
                    and data.m_CompleteImageSize == 720896
                ):
                    ids["face"][self.face_names[data.m_Name]] = obj.path_id

            # Some objects can't be read, so skip them
            except ValueError:
                pass

        return ids

    def _parse_card(self, ids: Dict[str, Any], env: Any, bundle: str) -> None:
        """Parse card data from Unity environment.

        Args:
            ids: Dictionary to store parsed data.
            env: Unity environment.
            bundle: Bundle name.
        """
        for obj in env.objects:
            if obj.type.name == "Texture2D":
                obj_data = obj.read()
                ids["card_id"][obj_data.m_Name] = bundle

    def _parse_icon(self, ids: Dict[str, Any], env: Any, bundle: str) -> None:
        """Parse icon data from Unity environment.

        Args:
            ids: Dictionary to store parsed data.
            env: Unity environment.
            bundle: Bundle name.
        """
        for obj in env.objects:
            if obj.type.name == "Texture2D":
                obj_data = obj.read()
                ids["icon"].setdefault(obj_data.m_Name[11:18], []).append(bundle)

    def _parse_sleeve(self, ids: Dict[str, Any], env: Any, bundle: str) -> None:
        """Parse sleeve data from Unity environment.

        Args:
            ids: Dictionary to store parsed data.
            env: Unity environment.
            bundle: Bundle name.
        """
        for obj in env.objects:
            obj_data = obj.read()
            if obj.type.name == "Texture2D" and "ProtectorIcon" in obj_data.m_Name:
                ids["sleeve"].append(bundle)

    def _parse_deck_box(self, ids: Dict[str, Any], env: Any, bundle: str) -> None:
        """Parse deck box data from Unity environment.

        Args:
            ids: Dictionary to store parsed data.
            env: Unity environment.
            bundle: Bundle name.
        """
        for obj in env.objects:
            obj_data = obj.read()
            if "DeckCase" in obj_data.m_Name and obj.type.name == "Texture2D":
                deck_id = int("".join(ch for ch in obj_data.m_Name if ch.isdigit()))
                match [
                    "".join(ch for ch in obj_data.m_Name if not ch.isdigit()),
                    (obj_data.m_Width, obj_data.m_Height),
                ]:
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
                if deck_id not in ids["deck_box"]:
                    ids["deck_box"][deck_id] = {}
                ids["deck_box"][deck_id][image_type] = bundle

    def _parse_field(self, ids: Dict[str, Any], env: Any, bundle: str) -> None:
        """Parse field data from Unity environment.

        Args:
            ids: Dictionary to store parsed data.
            env: Unity environment.
            bundle: Bundle name.
        """
        for obj in env.objects:
            obj_data = obj.read()
            if (
                hasattr(obj_data, "m_Name")
                and re.search(
                    re.compile(r"mat_0\d\d_01_basecolor_near"), obj_data.m_Name.lower()
                )
                and obj.type.name == "Texture2D"
            ):
                ids["field"].append(bundle)

    def _parse_wallpaper(
        self, ids: Dict[str, Any], env: Any, bundle: str, wallpaper: str
    ) -> None:
        """Parse wallpaper data from Unity environment.

        Args:
            ids: Dictionary to store parsed data.
            env: Unity environment.
            bundle: Bundle name.
            wallpaper: Wallpaper identifier.
        """
        for obj in env.objects:
            obj_data = obj.read()
            if obj.type.name == "Texture2D":
                if wallpaper not in ids["wallpaper"]:
                    ids["wallpaper"][wallpaper] = {}
                if "Icon" in obj_data.m_Name:
                    ids["wallpaper"][wallpaper]["icon"] = bundle
                elif "_1" in obj_data.m_Name:
                    ids["wallpaper"][wallpaper]["front"] = bundle
                elif "_2" in obj_data.m_Name:
                    ids["wallpaper"][wallpaper]["back"] = bundle

    def _parse_card_data_part(
        self, env: Any, part: str, ids: Dict[str, Any], bundle: str
    ) -> None:
        """Parse card data part from Unity environment.

        Args:
            env: Unity environment.
            part: Part identifier.
            ids: Dictionary to store parsed data.
            bundle: Bundle name.
        """
        for obj in env.objects:
            data = obj.read()
            if obj.type.name == "TextAsset":
                with open(f"./etl/services/temp/{part}", "wb") as f:
                    f.write(data.m_Script.encode("utf-8", "surrogateescape"))
                ids["card_data"][part] = bundle

    def _parse_coin(self, ids: Dict[str, Any], env: Any, bundle: str) -> None:
        """Parse coin data from Unity environment.

        Args:
            ids: Dictionary to store parsed data.
            env: Unity environment.
            bundle: Bundle name.
        """
        for obj in env.objects:
            obj_data = obj.read()
            if obj.type.name == "Texture2D" and "coin" in obj_data.m_Name.lower():
                ids["coin"].append(bundle)
