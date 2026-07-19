"""Utility module containing helper functions and classes for the ETL process."""

import json
import os
import shutil
from dataclasses import dataclass, field as dataclass_field
from os.path import join
from typing import Any, Dict, List, NamedTuple


def load_config(config_path: str = "config.json") -> Dict[str, Any]:
    """Load configuration from a JSON file.

    Args:
        config_path: Path to the configuration file.

    Returns:
        Dictionary containing the configuration.
    """
    with open(config_path, "r", encoding="utf-8") as file:
        return json.load(file)


config = load_config()

GAME_PATH = config["game_path"]
EXCLUDED_SLEEVES = config["excluded_sleeves"]
NUM_THREADS = config["num_threads"]

STREAMING_PATH = join(
    GAME_PATH[:-23], "masterduel_Data", "StreamingAssets", "AssetBundle"
)

CARD_FACE_SIZE = 720896


@dataclass
class IdsData:
    """Collected asset-bundle references found while extracting game data."""

    card_id: Dict[str, str] = dataclass_field(default_factory=dict)
    sleeve: List[str] = dataclass_field(default_factory=list)
    icon: Dict[str, List[str]] = dataclass_field(default_factory=dict)
    deck_box: Dict[int, Dict[str, str]] = dataclass_field(default_factory=dict)
    field: List[str] = dataclass_field(default_factory=list)
    wallpaper: Dict[str, Dict[str, str]] = dataclass_field(default_factory=dict)
    card_data: Dict[str, str] = dataclass_field(default_factory=dict)
    face: Dict[str, int] = dataclass_field(default_factory=dict)
    coin: Dict[str, List[str]] = dataclass_field(default_factory=dict)
    card_icon: Dict[str, Dict[str, float]] = dataclass_field(default_factory=dict)


def merge_nested_dict_lists(ids: IdsData, result: IdsData) -> None:
    """Merge icon and coin references into an ID collection, removing duplicates.

    Args:
        ids: ID collection to merge into.
        result: ID collection to merge from.
    """
    for asset_type in ("icon", "coin"):
        destination = getattr(ids, asset_type)
        source = getattr(result, asset_type)
        for key, value in source.items():
            if key in destination:
                destination[key].extend(value)
                destination[key] = list(dict.fromkeys(destination[key]))
            else:
                destination[key] = value


def merge_nested_dicts(dict1: Dict[str, Any], dict2: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively merge two nested dictionaries.

    For overlapping keys, the values from dict2 are added to dict1.

    Args:
        dict1: First dictionary to merge into.
        dict2: Second dictionary to merge from.

    Returns:
        Merged dictionary.
    """
    for key, value in dict2.items():
        if key in dict1:
            if isinstance(value, dict) and isinstance(dict1[key], dict):
                merge_nested_dicts(dict1[key], value)
            else:
                dict1[key] = value
        else:
            dict1[key] = value
    return dict1


def chunkify(lst: List[Any], n: int) -> List[List[Any]]:
    """Split a list into n nearly equal parts.

    Args:
        lst: List to split.
        n: Number of parts to split into.

    Returns:
        List of n sublists.
    """
    k, m = divmod(len(lst), n)
    return [lst[i * k + min(i, m) : (i + 1) * k + min(i + 1, m)] for i in range(n)]


class BColors:  # pylint: disable=too-few-public-methods
    """ANSI color codes for terminal output."""

    HEADER = "\033[95m"
    OKCYAN = "\033[96m"
    OKGREEN = "\033[92m"
    WARNING = "\033[93m"
    ENDC = "\033[0m"


def print_splash() -> None:
    """Print the splash screen from a text file."""
    with open("./etl/res/splash.txt", "r", encoding="utf-8") as f:
        print(BColors.HEADER + f.read() + BColors.ENDC)


def clear_directory(directory_path: str) -> None:
    """Delete all contents of a given directory.

    Args:
        directory_path: Path to the directory to clear.

    Raises:
        ValueError: If the directory_path is not a valid directory.
    """
    if not os.path.isdir(directory_path):
        raise ValueError(f"The provided path '{directory_path}' is not a directory.")

    for entry in os.listdir(directory_path):
        entry_path = os.path.join(directory_path, entry)
        if os.path.isfile(entry_path) or os.path.islink(entry_path):
            os.remove(entry_path)
        elif os.path.isdir(entry_path):
            shutil.rmtree(entry_path)


def get_data_wrapper() -> IdsData:
    """Create an empty ID collection for the ETL extraction process."""
    return IdsData()


class SortSizes(NamedTuple):
    """Tuple containing image sizes for sorting."""

    small: int
    medium: int
    large: int


ICON_SIZES = SortSizes(128, 256, 512)
COIN_SIZES = SortSizes(64, 128, 512)
