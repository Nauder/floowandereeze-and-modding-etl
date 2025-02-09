import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor

from dateutil.utils import today
from pandas import DataFrame

from services.game_service import GameService
from util import EXCLUDED_SLEEVES, GAME_PATH, merge_nested_dict_lists, merge_nested_dicts, chunkify, NUM_THREADS


class DataService:

    def __init__(self):
        self.game_service = GameService(GAME_PATH)
        self.logger = logging.getLogger('DataService')
        self.processed = 0

    def clean_data(self):
        with open('./etl/services/temp/data_dirty.json', "r", encoding='utf-8') as data_file:
            data = json.load(data_file)

            self.logger.info('Removing bad icons')
            to_remove = []

            for key, value in data['icon'].items():
                if len(value) != 3 or not key.isdigit():
                    to_remove.append(key)

            for key in to_remove:
                del data['icon'][key]

            self.logger.info('Removing bad deck boxes')
            to_remove = []

            for key, value in data['deck_box'].items():
                if len(value) != 7 or not key.isdigit():
                    to_remove.append(key)
                if not {'large', 'o_large', 'r_large', 'o_medium', 'r_medium', 'medium', 'small'}.issubset(value.keys()) and key not in to_remove:
                    to_remove.append(key)

            for key in to_remove:
                del data['deck_box'][key]

            self.logger.info('Removing bad sleeves')
            to_remove = []

            for key, value in enumerate(data['sleeve']):
                if value in EXCLUDED_SLEEVES:
                    to_remove.append(key)

            for key in sorted(to_remove, reverse=True):
                del data['sleeve'][key]

            self.logger.info('Removing bad wallpapers')
            to_remove = []

            for key in data['wallpaper']:
                if len(data['wallpaper'][key]) != 3:
                    to_remove.append(key)

            for key in to_remove:
                del data['wallpaper'][key]

            self.logger.info('Sorting fields')
            fields = {}
            field_position_prompt = 'Was the field center:\n[1] On the top\n[2] On the bottom\n[3] Neither\n> '
            field_flip_prompt = 'Was the field top:\n[1] On the top\n[2] On the bottom\n[3] Neither\n> '

            # for field in sorted(data['field']):
            #     field_data = {}
            #     field_image = self.game_service.unity_servie.fetch_image(field, 'fld')
            #     field_image.show()
            #
            #     field_type = 0
            #     while field_type not in ['1', '2', '3']:
            #         field_type = input(field_position_prompt)
            #         match field_type:
            #             case '1':
            #                 field_data['bottom'] = False
            #             case '2':
            #                 field_data['bottom'] = True
            #             case '3':
            #                 pass # Skip unsupported fields
            #
            #     if field_type in ['1', '2']:
            #         field_type = 0
            #         while field_type not in ['1', '2']:
            #             field_type = input(field_flip_prompt)
            #
            #             match field_type:
            #                 case '1':
            #                     field_data['flipped'] = False
            #                 case '2':
            #                     field_data['flipped'] = True
            #
            #         fields[field] = field_data
            #
            #     print('\n')
            #
            # data['field'] = fields
            with open('./etl/services/temp/data.json', "w", encoding='utf-8') as clean_file:
                json.dump(data, clean_file)

    def get_ids(self):

        ids = {'card_id': {}, 'sleeve': [], 'icon': {}, 'deck_box': {}, 'field': [], 'face': {}, 'wallpaper': {}}

        self.logger.info('Getting AssetBundles data...')

        all_dirs = []
        for _, dirs, _ in os.walk(GAME_PATH):
            all_dirs.extend(dirs)

        def process_dirs(dir_list):
            local_ids = {'card_id': {}, 'sleeve': [], 'icon': {}, 'deck_box': {}, 'field': [], 'wallpaper': {}}
            for data_dir in dir_list:
                if data_dir != 'root':
                    dir_ids = self.game_service.get_dir_data(data_dir)
                    local_ids['card_id'].update(dir_ids['card_id'])
                    local_ids['sleeve'].extend(dir_ids['sleeve'])
                    merge_nested_dict_lists(local_ids, dir_ids)
                    merge_nested_dicts(local_ids['deck_box'], dir_ids['deck_box'])
                    merge_nested_dicts(local_ids['wallpaper'], dir_ids['wallpaper'])
                    local_ids['field'].extend(dir_ids['field'])

                    self.processed += 1

            return local_ids

        dir_chunks = chunkify(all_dirs, NUM_THREADS)

        with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
            results = list(executor.map(process_dirs, dir_chunks))


        for result in results:
            ids['card_id'].update(result['card_id'])
            ids['sleeve'].extend(result['sleeve'])
            merge_nested_dict_lists(ids, result)
            merge_nested_dicts(ids['deck_box'], result['deck_box'])
            merge_nested_dicts(ids['wallpaper'], result['wallpaper'])
            ids['field'].extend(result['field'])

        self.logger.info('Getting Unity3D data...')
        unity3d_data = self.game_service.get_unity3d_data()

        ids['face'].update(unity3d_data['face'])

        with open("./etl/services/temp/ids.json", "w") as outfile:
            outfile.write(json.dumps(ids))

    def add_suffix(self, names) -> list:
        # Reverse the list to process from last to first
        reversed_names = list(reversed(names))
        name_counts = {}
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

    def remove_extra_suffix(self, cards: dict):
        # Create a new dictionary to store the updated keys
        updated_dict = {}

        # Iterate through the original dictionary
        for key, value in cards.items():
            # Check if the key contains '(alt 1)'
            if '(alt 1)' in key:
                # Remove '(alt 1)' from the key
                new_key = key.replace(' (alt 1)', '')

                # Check if the new key already exists in the updated dictionary
                if new_key not in cards:
                    # If it doesn't exist, add the new key with the original value
                    updated_dict[new_key] = value
                else:
                    # If it does exist, keep the original key and value
                    updated_dict[key] = value
            else:
                # If the key doesn't contain '(alt 1)', add it as is
                updated_dict[key] = value

        return updated_dict

    def get_card_names(self):
        # Add alt art
        with open('./etl/services/temp/card_name.bytes.dec.json', 'r', encoding='utf-8') as names_json:
            names = self.add_suffix(json.load(names_json))

        with open('./etl/services/temp/card_prop.bytes.Card_IDs.dec.json', 'r', encoding='utf-8') as props_json:
            id_names = dict(zip(json.load(props_json), names))
            to_remove = [key for key in id_names if key in range(30000, 30100)]

            for key in to_remove:
                del id_names[key]

        with open('./etl/services/temp/ids.json', 'r', encoding='utf-8') as ids_json:
            ids = json.load(ids_json)
            updated_card_id = self.remove_extra_suffix({
                id_names.get(int(key), key): value for key, value in ids["card_id"].items()
            })

            ids["card_names"] = updated_card_id

            with open('./etl/services/temp/data_dirty.json', "w", encoding='utf-8') as data_file:
                json.dump(ids, data_file)

    def write_data(self):
        with open('./etl/services/temp/data.json', "r", encoding='utf-8') as data_file:
            data = json.load(data_file)

            self.logger.info('Writing Sleeves...')
            sleeves = DataFrame()
            sleeves.insert(0, 'bundle', data['sleeve'])
            sleeves.to_parquet('./data/sleeves.parquet')

            self.logger.info('Writing Cards...')
            cards = DataFrame()
            cards.insert(0, 'name', data['card_names'].keys())
            cards.insert(0, 'bundle', data['card_names'].values())
            cards.to_parquet('./data/cards.parquet')

            self.logger.info('Writing Fields...')
            fields = DataFrame()
            fields.insert(0, 'bundle', data['field'].keys())
            fields.insert(0, 'flipped', [field['flipped'] for field in data['field'].values()])
            fields.insert(0, 'bottom', [field['bottom'] for field in data['field'].values()])
            fields.to_parquet('./data/fields.parquet')

            self.logger.info('Writing Wallpapers...')
            wallpapers = DataFrame()
            wallpapers.insert(0, 'name', data['wallpaper'].keys())
            wallpapers.insert(0, 'icon', [wallpaper['icon'] for wallpaper in data['wallpaper'].values()])
            wallpapers.insert(0, 'back', [wallpaper['back'] for wallpaper in data['wallpaper'].values()])
            wallpapers.insert(0, 'front', [wallpaper['front'] for wallpaper in data['wallpaper'].values()])
            wallpapers.to_parquet('./data/wallpapers.parquet')

            self.logger.info('Writing Card Faces...')
            faces = DataFrame()
            faces.insert(0, 'key', data['face'].values())
            faces.insert(0, 'name', data['face'].keys())
            faces.to_parquet('./data/faces.parquet')

            self.logger.info('Writing Deck Boxes...')
            boxes = DataFrame()
            boxes.insert(0, 'name', data['deck_box'].keys())
            boxes.insert(0, 'r_large', [deck_box['r_large'] for deck_box in data['deck_box'].values()])
            boxes.insert(0, 'o_large', [deck_box['o_large'] for deck_box in data['deck_box'].values()])
            boxes.insert(0, 'large', [deck_box['large'] for deck_box in data['deck_box'].values()])
            boxes.insert(0, 'r_medium', [deck_box['r_medium'] for deck_box in data['deck_box'].values()])
            boxes.insert(0, 'o_medium', [deck_box['o_medium'] for deck_box in data['deck_box'].values()])
            boxes.insert(0, 'medium', [deck_box['medium'] for deck_box in data['deck_box'].values()])
            boxes.insert(0, 'small', [deck_box['small'] for deck_box in data['deck_box'].values()])
            boxes.to_parquet('./data/deck_boxes.parquet')

            self.logger.info('Writing Icons...')
            icons = DataFrame()
            sorted_icons = self.game_service.unity_servie.sort_icon_sizes(data['icon'].values())
            icons.insert(0, 'small', [icon['small'] for icon in sorted_icons])
            icons.insert(0, 'medium', [icon['medium'] for icon in sorted_icons])
            icons.insert(0, 'large', [icon['large'] for icon in sorted_icons])
            icons.insert(0, 'name', data['icon'].keys())
            icons.to_parquet('./data/icons.parquet')

            self.logger.info('Updating Version...')
            with open('./data/version.txt', 'w') as file:
                file.write(today().strftime('%Y-%m-%d'))

# "field": {"1621b0a7": {"bottom": false, "flipped": false}, "18415e72": {"bottom": true, "flipped": false}, "19bdd8e7": {"bottom": false, "flipped": false}, "25d61281": {"bottom": true, "flipped": false}, "2e6959e7": {"bottom": false, "flipped": true}, "509865b2": {"bottom": false, "flipped": true}, "5547c001": {"bottom": false, "flipped": true}, "5adba841": {"bottom": false, "flipped": true}, "5f040df2": {"bottom": false, "flipped": true}, "674ce4b2": {"bottom": false, "flipped": true}, "692c0a67": {"bottom": false, "flipped": true}, "81b56fbe": {"bottom": true, "flipped": false}, "846aca0d": {"bottom": false, "flipped": true}, "8bf6a24d": {"bottom": true, "flipped": false}, "b242cd98": {"bottom": true, "flipped": false}, "b79d682b": {"bottom": false, "flipped": true}, "b9fd86fe": {"bottom": false, "flipped": false}, "c2d31f18": {"bottom": false, "flipped": true}, "c32f998d": {"bottom": false, "flipped": true}, "c6f03c3e": {"bottom": false, "flipped": true}, "cd4f7758": {"bottom": false, "flipped": true}, "feb8d57e": {"bottom": false, "flipped": true}}