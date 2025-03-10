import os, sys, shutil
from os.path import join
import json


def load_config(config_path="config.json"):
    with open(config_path, "r", encoding="utf-8") as file:
        return json.load(file)

config = load_config()

GAME_PATH = config["game_path"]
EXCLUDED_SLEEVES = config["excluded_sleeves"]
NUM_THREADS = config["num_threads"]

STREAMING_PATH = join(
    GAME_PATH[:-23],
    "masterduel_Data",
    "StreamingAssets",
    "AssetBundle"
)

def merge_nested_dict_lists(dict1, dict2):
    for key, value in dict2['icon'].items():
        if key in dict1['icon']:
            dict1['icon'][key].extend(value)
            dict1['icon'][key] = list(dict.fromkeys(dict1['icon'][key]))
        else:
            dict1['icon'][key] = value


def merge_nested_dicts(dict1, dict2):
    """
    Recursively merge two nested dictionaries.
    For overlapping keys, the values from dict2 are added to dict1.
    """
    for key, value in dict2.items():
        if key in dict1:
            # If the value is a dictionary, recursively merge
            if isinstance(value, dict) and isinstance(dict1[key], dict):
                merge_nested_dicts(dict1[key], value)
            else:
                # Otherwise, overwrite dict1's value with dict2's value
                dict1[key] = value
        else:
            # If key is not in dict1, simply add it
            dict1[key] = value
    return dict1


def chunkify(lst, n):
    """Splits a list into n nearly equal parts."""
    k, m = divmod(len(lst), n)
    return [lst[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n)]


class HiddenPrints:
    def __enter__(self):
        self._original_stdout = sys.stdout
        sys.stdout = open(os.devnull, 'w')

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout.close()
        sys.stdout = self._original_stdout


class BColors:
    HEADER = '\033[95m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    ENDC = '\033[0m'


def print_splash():
    with open('./etl/res/splash.txt', 'r') as f:
        print(BColors.HEADER + f.read() + BColors.ENDC)


def clear_directory(directory_path):
    """
    Deletes all contents of a given directory.

    :param directory_path: Path to the directory to clear.
    :raises ValueError: If the directory_path is not a valid directory.
    """
    if not os.path.isdir(directory_path):
        raise ValueError(f"The provided path '{directory_path}' is not a directory.")

    for entry in os.listdir(directory_path):
        entry_path = os.path.join(directory_path, entry)
        if os.path.isfile(entry_path) or os.path.islink(entry_path):  # Handle files and symlinks
            os.remove(entry_path)
        elif os.path.isdir(entry_path):  # Handle directories
            shutil.rmtree(entry_path)