import re
from os.path import join

from PIL import Image
from UnityPy import load as unity_load

from util import GAME_PATH, STREAMING_PATH


class UnityService:

    def prepare_environment(self, miss: bool, bundle: str) -> str:
        """returns the UnityPy environment path related to the bundle and game path given"""

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

        return (
            join(
                GAME_PATH[:-23],
                "masterduel_Data",
                "data.unity3d"
            )
        )


    def fetch_image(self, bundle: str, type: str, miss=False) -> Image.Image:
        """
        Fetches an image from a Unity asset bundle.

        :param bundle: A string representing the name of the asset bundle.
        :param type: A string representing the type of the image to fetch.
        :param miss: A bool indicating whether a previous fetch attempt failed (default: False).
        (default: (0, 0)).

        :return: An instance of PIL Image.Image representing the fetched image.
        """
        env_path = self.prepare_environment(miss, bundle)

        env = unity_load(env_path)

        found: bool = False

        for obj in env.objects:
            if obj.type.name == "Texture2D":
                data = obj.read()

                if type == "fld":
                    found = (hasattr(data, 'm_Name')
                        and re.search(re.compile(r"mat_0\d\d_01_basecolor_near"), data.m_Name.lower())
                        and obj.type.name == "Texture2D")
                else:
                    found = True

                if found:
                    img = data.image
                    img.convert("RGB")
                    img.name = "image.jpg"
                    return img

        return self.fetch_image(bundle, True)

    def sort_sprite_list(self, sprite_list: list) -> dict:
        """Sorts a given sprite list by image size"""

        sorted_sprites = {}

        for sprite in sprite_list:
            sprite_art = self.fetch_image(sprite, "spt")

            if sprite_art.width == 128:
                sorted_sprites['small'] = sprite
            elif sprite_art.width == 256:
                sorted_sprites['medium'] = sprite
            elif sprite_art.width == 512:
                sorted_sprites['large'] = sprite
            else:
                print(f"Could not sort {sprite} of width {sprite_art.width}")

        if len(sorted_sprites.keys()) == 3:
            return sorted_sprites
        else:
            print(f'Failed to sort sprites: {sprite_list} => {sorted_sprites}')

    def sort_icon_sizes(self, icons: list) -> list:
        sorted_icons = []

        for icon in icons:
            sorted_icons.append(self.sort_sprite_list(icon))

        return sorted_icons