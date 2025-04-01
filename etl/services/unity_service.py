"""Service for handling Unity asset operations."""

import re
from os.path import join
from typing import Dict, List

from PIL import Image
from UnityPy import load as unity_load

from util import GAME_PATH, STREAMING_PATH


class UnityService:
    """Service class for handling Unity asset operations."""

    def prepare_environment(self, miss: bool, bundle: str) -> str:
        """Prepare the UnityPy environment path for a given bundle.

        Args:
            miss: Whether to use streaming assets path.
            bundle: Name of the asset bundle.

        Returns:
            Path to the Unity asset bundle.
        """
        return (
            join(
                STREAMING_PATH,
                bundle[:2],
                bundle,
            )
            if miss
            else join(GAME_PATH, bundle[:2], bundle)
        )

    def prepare_unity3d_environment(self) -> str:
        """Prepare the path to the Unity3D data file.

        Returns:
            Path to the Unity3D data file.
        """
        return join(GAME_PATH[:-23], "masterduel_Data", "data.unity3d")

    def fetch_image(
        self, bundle: str, img_type: str, miss: bool = False
    ) -> Image.Image:
        """Fetch an image from a Unity asset bundle.

        Args:
            bundle: Name of the asset bundle.
            type: Type of image to fetch.
            miss: Whether a previous fetch attempt failed.

        Returns:
            PIL Image object representing the fetched image.
        """
        env_path = self.prepare_environment(miss, bundle)
        env = unity_load(env_path)

        found: bool = False

        for obj in env.objects:
            if obj.type.name == "Texture2D":
                data = obj.read()

                if img_type == "fld":
                    found = (
                        hasattr(data, "m_Name")
                        and re.search(
                            re.compile(r"mat_0\d\d_01_basecolor_near"),
                            data.m_Name.lower(),
                        )
                        and obj.type.name == "Texture2D"
                    )
                else:
                    found = True

                if found:
                    img = data.image
                    img.convert("RGB")
                    img.name = "image.jpg"
                    return img

        return self.fetch_image(bundle, True)

    def sort_sprite_list(self, sprite_list: List[str]) -> Dict[str, str]:
        """Sort a list of sprites by image size.

        Args:
            sprite_list: List of sprite names to sort.

        Returns:
            Dictionary mapping size categories to sprite names.
        """
        sorted_sprites: Dict[str, str] = {}

        for sprite in sprite_list:
            sprite_art = self.fetch_image(sprite, "spt")

            if sprite_art.width == 128:
                sorted_sprites["small"] = sprite
            elif sprite_art.width == 256:
                sorted_sprites["medium"] = sprite
            elif sprite_art.width == 512:
                sorted_sprites["large"] = sprite
            else:
                print(f"Could not sort {sprite} of width {sprite_art.width}")

        if len(sorted_sprites) == 3:
            return sorted_sprites

        print(f"Failed to sort sprites: {sprite_list} => {sorted_sprites}")
        return {}

    def sort_icon_sizes(self, icons: List[List[str]]) -> List[Dict[str, str]]:
        """Sort multiple lists of icons by size.

        Args:
            icons: List of icon lists to sort.

        Returns:
            List of dictionaries mapping size categories to icon names.
        """
        return [self.sort_sprite_list(icon) for icon in icons]
