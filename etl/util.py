"""Utility module containing helper functions and classes for the ETL process."""

import json
import os
import shutil
from os.path import join
from typing import Any, Dict, List


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


def merge_nested_dict_lists(dict1: Dict[str, Any], dict2: Dict[str, Any]) -> None:
    """Merge nested dictionary lists, handling duplicate values.

    Args:
        dict1: First dictionary to merge into.
        dict2: Second dictionary to merge from.
    """
    for key, value in dict2["icon"].items():
        if key in dict1["icon"]:
            dict1["icon"][key].extend(value)
            dict1["icon"][key] = list(dict.fromkeys(dict1["icon"][key]))
        else:
            dict1["icon"][key] = value


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


def get_data_wrapper() -> Dict[str, Any]:
    """
    Get a data wrapper for the ETL process.

    Returns:
        A dictionary with the following keys:
        - card_id: A dictionary for card IDs.
        - sleeve: A list of sleeve IDs.
        - icon: A dictionary for icons.
        - deck_box: A dictionary for deck boxes.
        - field: A list of field IDs.
        - wallpaper: A dictionary for wallpapers.
        - card_data: A dictionary for card data.
        - face: A dictionary for face data.
    """
    return {
        "card_id": {},
        "sleeve": [],
        "icon": {},
        "deck_box": {},
        "field": [],
        "wallpaper": {},
        "card_data": {},
        "face": {},
        "coin": [],
        "card_icon": {},
    }
