"""Service for handling data processing and management."""

import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from os.path import isfile
from typing import Any, Dict, List, Union
from datetime import datetime

from pandas import DataFrame, Series

from util import (
    EXCLUDED_SLEEVES,
    GAME_PATH,
    get_data_wrapper,
    merge_nested_dict_lists,
    merge_nested_dicts,
    chunkify,
    NUM_THREADS,
    STREAMING_PATH,
)

from .game_service import GameService


class DataService:
    """Service class for handling data operations."""

    def __init__(self) -> None:
        """Initialize the DataService with a GameService instance."""
        self.game_service = GameService()
        self.logger = logging.getLogger("DataService")
        self.processed = 0

    def clean_data(self, sort_fields: bool = True) -> None:
        """Clean and validate the extracted data.

        Args:
            sort_fields: Whether to sort fields (requires user input). Defaults to True.
        """
        with open(
            "./etl/services/temp/data_dirty.json", "r", encoding="utf-8"
        ) as data_file:
            data = json.load(data_file)

            self.logger.info("Removing bad icons")
            to_remove = []

            for key, value in data["icon"].items():
                if len(value) != 3 or not key.isdigit():
                    to_remove.append(key)
                else:
                    art_list = self.game_service.unity_service.sort_sprite_list(value)
                    if not art_list or len(art_list) != 3:
                        to_remove.append(key)

            for key in to_remove:
                del data["icon"][key]

            self.logger.info("Removing bad deck boxes")
            to_remove = []

            for key, value in data["deck_box"].items():
                if len(value) != 7 or not key.isdigit():
                    to_remove.append(key)
                if (
                    not {
                        "large",
                        "o_large",
                        "r_large",
                        "o_medium",
                        "r_medium",
                        "medium",
                        "small",
                    }.issubset(value.keys())
                    and key not in to_remove
                ):
                    to_remove.append(key)

            for key in to_remove:
                del data["deck_box"][key]

            self.logger.info("Removing bad sleeves")
            to_remove = []

            for key, value in enumerate(data["sleeve"]):
                if value in EXCLUDED_SLEEVES:
                    to_remove.append(key)

            for key in sorted(to_remove, reverse=True):
                del data["sleeve"][key]

            self.logger.info("Removing bad wallpapers")
            to_remove = []

            for key in data["wallpaper"]:
                if len(data["wallpaper"][key]) != 3:
                    to_remove.append(key)

            for key in to_remove:
                del data["wallpaper"][key]

            self.logger.info("Sorting fields")
            fields = {}
            field_position_prompt = """
                Was the field center:
                [1] On the top
                [2] On the bottom
                [3] Neither
                > 
                """
            field_flip_prompt = (
                "Was the field top:\n[1] On the top\n[2] On the bottom\n[3] Neither\n> "
            )

            if sort_fields:
                for field in sorted(data["field"]):
                    field_data = {}
                    field_image = self.game_service.unity_service.fetch_image(
                        field, "fld"
                    )
                    field_image.show()

                    field_type = 0
                    while field_type not in ["1", "2", "3"]:
                        field_type = input(field_position_prompt)
                        match field_type:
                            case "1":
                                field_data["bottom"] = False
                            case "2":
                                field_data["bottom"] = True
                            case "3":
                                pass  # Skip unsupported fields

                    if field_type in ["1", "2"]:
                        field_type = 0
                        while field_type not in ["1", "2"]:
                            field_type = input(field_flip_prompt)

                            match field_type:
                                case "1":
                                    field_data["flipped"] = False
                                case "2":
                                    field_data["flipped"] = True

                        fields[field] = field_data

                    print("\n")
            else:
                self.logger.info("Skipping field sorting")
                if isfile("./etl/services/temp/data.json"):
                    with open(
                        "./etl/services/temp/data.json", "r", encoding="utf-8"
                    ) as clean_file:
                        clean_file_data = json.load(clean_file)
                        fields = clean_file_data["field"]

            data["field"] = fields

            with open(
                "./etl/services/temp/data.json", "w", encoding="utf-8"
            ) as clean_file:
                json.dump(data, clean_file)

    def get_ids(self) -> None:
        """Extract and process game IDs from asset bundles."""
        ids = get_data_wrapper()

        self.logger.info("Getting AssetBundles data...")

        # self.game_service.get_dir_data("c7", True)

        all_dirs = [
            [data_dir, is_streaming]
            for is_streaming, path in [(False, GAME_PATH), (True, STREAMING_PATH)]
            for _, dirs, _ in os.walk(path)
            for data_dir in dirs
        ]

        dir_chunks = chunkify(all_dirs, NUM_THREADS)

        with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
            results = list(executor.map(self.process_dirs, dir_chunks))

        for result in results:
            self.merge_data(ids, result)

        self.logger.info("Getting unity3d data...")

        unity3d_data = self.game_service.get_unity3d_data()

        ids["card_icon"].update(unity3d_data["card_icon"])

        self.logger.info("Saving ids...")

        with open("./etl/services/temp/ids.json", "w", encoding="utf-8") as outfile:
            json.dump(ids, outfile)

    def process_dirs(
        self, dir_list: List[List[Union[str, bool]]]
    ) -> Dict[str, Union[Dict[str, Any], List[Any]]]:
        """Process a list of directories to extract game data.

        Args:
            dir_list: List of [directory_name, is_streaming] pairs.

        Returns:
            Dictionary containing extracted data.
        """
        local_ids = get_data_wrapper()
        for data_dir, is_streaming in dir_list:
            if data_dir != "root":
                dir_ids = self.game_service.get_dir_data(data_dir, is_streaming)
                self.merge_data(local_ids, dir_ids)
                self.processed += 1

        return local_ids

    def merge_data(self, ids: Dict[str, Any], result: Dict[str, Any]) -> None:
        """Merge extracted data into the main data structure.

        Args:
            ids: Main data structure to merge into.
            result: Data to merge.
        """
        ids["card_id"].update(result["card_id"])
        ids["sleeve"].extend(result["sleeve"])
        merge_nested_dict_lists(ids, result)
        merge_nested_dicts(ids["deck_box"], result["deck_box"])
        merge_nested_dicts(ids["wallpaper"], result["wallpaper"])
        ids["field"].extend(result["field"])
        ids["card_data"].update(result["card_data"])
        ids["face"].update(result["face"])
        ids["coin"].extend(result["coin"])

    def add_suffix(self, names: List[str]) -> List[str]:
        """Add suffixes to duplicate names.

        Args:
            names: List of names to process.

        Returns:
            List of names with suffixes added for duplicates.
        """
        # Reverse the list to process from last to first
        reversed_names = list(reversed(names))
        name_counts: Dict[str, int] = {}
        updated_names = []

        for name in reversed_names:
            # Increment the count for this name
            if name in name_counts:
                name_counts[name] += 1
                updated_names.append(f"{name} (alt {name_counts[name] - 1})")
            else:
                name_counts[name] = 1
                updated_names.append(name)

        # Reverse the list back to the original order
        return list(reversed(updated_names))

    def remove_extra_suffix(self, cards: Dict[str, Any]) -> Dict[str, Any]:
        """Remove extra suffixes from card names.

        Args:
            cards: Dictionary of card data.

        Returns:
            Dictionary with cleaned card names.
        """
        # Create a new dictionary to store the updated keys
        updated_dict: Dict[str, Any] = {}

        # Iterate through the original dictionary
        for key, value in cards.items():
            # Check if the key contains '(alt 1)'
            if "(alt 1)" in key:
                # Remove '(alt 1)' from the key
                new_key = key.replace(" (alt 1)", "")

                # Check if the new key already exists in the updated dictionary
                if new_key not in cards:
                    # If it doesn't exist, add the new key with the original value
                    updated_dict[new_key] = value
                else:
                    # If it does exist, keep the original key and value
                    updated_dict[key] = value
            else:
                updated_dict[key] = value

        return updated_dict

    def get_card_data(self) -> None:
        """Extract and process card data from JSON files."""
        # Add alt art
        with open(
            "./etl/services/temp/card_name.bytes.dec.json", "r", encoding="utf-8"
        ) as names_json:
            names = self.add_suffix(json.load(names_json))

        with (
            open(
                "./etl/services/temp/card_prop.bytes.Card_IDs.dec.json",
                "r",
                encoding="utf-8",
            ) as props_json,
            open(
                "./etl/services/temp/card_desc.bytes.dec.json", "r", encoding="utf-8"
            ) as desc_json,
        ):
            id_names = {
                key: [value_b, value_c, value_d]
                for key, value_b, value_c, value_d in zip(
                    json.load(props_json),
                    json.load(desc_json),
                    names,
                    range(len(names)),
                )
            }

            # Cards in this range seem to be irrelevant duplicates of exising ones
            to_remove = [key for key in id_names if key in range(30000, 30100)]
            for key in to_remove:
                del id_names[key]

        with open("./etl/services/temp/ids.json", "r", encoding="utf-8") as ids_json:
            ids = json.load(ids_json)
            updated_card_id = self.remove_extra_suffix(
                {
                    id_names.get(int(key), key)[1]: [
                        value,
                        id_names.get(int(key), key)[0],
                        id_names.get(int(key), key)[2],
                    ]
                    for key, value, in ids["card_id"].items()
                }
            )

            ids["card_names"] = updated_card_id
            ids["legacy"] = {
                name: value[0] for name, value in ids["card_names"].items()
            }

            with open(
                "./etl/services/temp/data_dirty.json", "w", encoding="utf-8"
            ) as data_file:
                json.dump(ids, data_file)

    def write_data(self) -> None:
        """Write processed data to Parquet files and update version information."""
        with open("./etl/services/temp/data.json", "r", encoding="utf-8") as data_file:
            data = json.load(data_file)

            self.logger.info("Writing Sleeves...")
            sleeves = DataFrame()
            sleeves.insert(0, "bundle", data["sleeve"])
            sleeves.to_parquet("./data/sleeves.parquet")

            self.logger.info("Writing Cards...")
            cards = DataFrame()
            cards.insert(0, "name", data["card_names"].keys())
            cards.insert(
                0, "bundle", Series([value[0] for value in data["card_names"].values()])
            )
            cards.insert(
                0,
                "description",
                Series([value[1] for value in data["card_names"].values()]),
            )
            cards.insert(
                0,
                "data_index",
                Series([value[2] for value in data["card_names"].values()]),
            )
            cards.to_parquet("./data/cards.parquet")

            self.logger.info("Writing Fields...")
            fields = DataFrame()
            fields.insert(0, "bundle", data["field"].keys())
            fields.insert(
                0,
                "flipped",
                Series([field["flipped"] for field in data["field"].values()]),
            )
            fields.insert(
                0,
                "bottom",
                Series([field["bottom"] for field in data["field"].values()]),
            )
            fields.to_parquet("./data/fields.parquet")

            self.logger.info("Writing Wallpapers...")
            wallpapers = DataFrame()
            wallpapers.insert(0, "name", data["wallpaper"].keys())
            wallpapers.insert(
                0,
                "icon",
                Series([wallpaper["icon"] for wallpaper in data["wallpaper"].values()]),
            )
            wallpapers.insert(
                0,
                "back",
                Series([wallpaper["back"] for wallpaper in data["wallpaper"].values()]),
            )
            wallpapers.insert(
                0,
                "front",
                Series(
                    [wallpaper["front"] for wallpaper in data["wallpaper"].values()]
                ),
            )
            wallpapers.to_parquet("./data/wallpapers.parquet")

            self.logger.info("Writing Card Faces...")
            faces = DataFrame()
            faces.insert(
                0, "bundle", Series([f["bundle"] for f in data["face"].values()])
            )
            faces.insert(0, "key", Series([f["key"] for f in data["face"].values()]))
            faces.insert(0, "name", data["face"].keys())
            faces.to_parquet("./data/faces.parquet")

            self.logger.info("Writing Deck Boxes...")
            boxes = DataFrame()
            boxes.insert(0, "name", data["deck_box"].keys())
            boxes.insert(
                0,
                "r_large",
                Series([deck_box["r_large"] for deck_box in data["deck_box"].values()]),
            )
            boxes.insert(
                0,
                "o_large",
                Series([deck_box["o_large"] for deck_box in data["deck_box"].values()]),
            )
            boxes.insert(
                0,
                "large",
                Series([deck_box["large"] for deck_box in data["deck_box"].values()]),
            )
            boxes.insert(
                0,
                "r_medium",
                Series(
                    [deck_box["r_medium"] for deck_box in data["deck_box"].values()]
                ),
            )
            boxes.insert(
                0,
                "o_medium",
                Series(
                    [deck_box["o_medium"] for deck_box in data["deck_box"].values()]
                ),
            )
            boxes.insert(
                0,
                "medium",
                Series([deck_box["medium"] for deck_box in data["deck_box"].values()]),
            )
            boxes.insert(
                0,
                "small",
                Series([deck_box["small"] for deck_box in data["deck_box"].values()]),
            )
            boxes.to_parquet("./data/deck_boxes.parquet")

            self.logger.info("Writing Icons...")
            icons = DataFrame()
            sorted_icons = self.game_service.unity_service.sort_icon_sizes(
                data["icon"].values()
            )
            icons.insert(0, "small", Series([icon["small"] for icon in sorted_icons]))
            icons.insert(0, "medium", Series([icon["medium"] for icon in sorted_icons]))
            icons.insert(0, "large", Series([icon["large"] for icon in sorted_icons]))
            icons.insert(0, "name", data["icon"].keys())
            icons.to_parquet("./data/icons.parquet")

            self.logger.info("Writing Card Metadata...")
            metadata = DataFrame()
            metadata.insert(0, "name", data["card_data"].keys())
            metadata.insert(0, "bundle", data["card_data"].values())
            metadata.to_parquet("./data/metadata.parquet")

            self.logger.info("Writing Coins...")
            coins = DataFrame()
            coins.insert(0, "bundle", data["coin"])
            coins.to_parquet("./data/coins.parquet")

            self.logger.info("Writing Card Icons...")
            card_icons = DataFrame()
            card_icons.insert(
                0,
                "height",
                Series([icon["height"] for icon in data["card_icon"].values()]),
            )
            card_icons.insert(
                0,
                "width",
                Series([icon["width"] for icon in data["card_icon"].values()]),
            )
            card_icons.insert(
                0, "y", Series([icon["y"] for icon in data["card_icon"].values()])
            )
            card_icons.insert(
                0, "x", Series([icon["x"] for icon in data["card_icon"].values()])
            )
            card_icons.insert(0, "name", data["card_icon"].keys())
            card_icons.to_parquet("./data/card_icons.parquet")

            self.logger.info("Updating Version...")
            with open("./data/version.txt", "w", encoding="utf-8") as file:
                file.write(datetime.today().strftime("%Y-%m-%d"))
