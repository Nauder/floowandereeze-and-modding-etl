from os.path import join

from PIL import Image
from UnityPy import load as unity_load

# Master Duel game path, up to the 0x00000000 directory
GAME_PATH = 'C:\\Program Files (x86)\\Steam\\steamapps\\common\\Yu-Gi-Oh!  Master Duel\\LocalData\\0x'


def prepare_environment(miss: bool, bundle: str) -> str:
    """returns the UnityPy environment path related to the bundle and game path given"""

    return (
        join(
            GAME_PATH[:-18],
            "masterduel_Data",
            "StreamingAssets",
            "AssetBundle",
            bundle[:2],
            bundle,
        )
        if miss
        else join(GAME_PATH, "0000", bundle[:2], bundle)
    )

def fetch_image(bundle: str, aspect: str, miss=False, simple_aspect=(0, 0)) -> Image.Image:
    """
    Fetches an image from a Unity asset bundle.

    :param bundle: A string representing the name of the asset bundle.
    :param aspect: A string representing the aspect of the image to fetch.
    :param miss: A bool indicating whether a previous fetch attempt failed (default: False).
    :param simple_aspect: A tuple representing the dimensions of the image to fetch when aspect is "smp"
    (default: (0, 0)).

    :return: An instance of PIL Image.Image representing the fetched image.
    """
    env_path = prepare_environment(miss, bundle)

    env = unity_load(env_path)

    for obj in env.objects:
        if obj.type.name == "Texture2D":
            data = obj.read()

            img = data.image
            img.convert("RGB")
            img.name = "image.jpg"
            return img

    return fetch_image(bundle, aspect, True, simple_aspect)