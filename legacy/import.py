import json

from pandas import DataFrame

from legacy_service import fetch_image


def sort_sprite_list(sprite_list: list) -> dict:
    """Sorts a given sprite list by image size"""

    sorted_sprites = {}

    for sprite in sprite_list:
        print(sprite)
        sprite_art = fetch_image(sprite, "spt")

        if sprite_art.width == 128:
            sorted_sprites['small'] = sprite
        elif sprite_art.width == 256:
            sorted_sprites['medium'] = sprite
        elif sprite_art.width == 512:
            sorted_sprites['large'] = sprite

    return sorted_sprites


def sort_icon_sizes(icons: list) -> list:
    sorted_icons = []

    for icon in icons:
        print(icon)
        sorted_icons.append(sort_sprite_list(icon))

    return sorted_icons


def import_from_legacy():

    with open('./legacy/data.json') as json_file:
        data_legacy = json.load(json_file)

        # Sleeves
        sleeves = DataFrame()
        sleeves.insert(0, 'bundle', data_legacy['adress'])
        sleeves.to_parquet('./data/sleeves.parquet')

        # Cards
        cards = DataFrame()
        cards.insert(0, 'name', data_legacy['name'].keys())
        cards.insert(0, 'bundle', data_legacy['name'].values())
        cards.to_parquet('./data/cards.parquet')

        # Fields
        fields = DataFrame()
        fields.insert(0, 'bundle', data_legacy['field'])
        fields.to_parquet('./data/fields.parquet')

        # Wallpapers
        wallpapers = DataFrame()
        wallpapers.insert(0, 'bundle_front', [data[0] for data in data_legacy['wallpaper']])
        wallpapers.insert(0, 'bundle_back', [data[1] for data in data_legacy['wallpaper']])
        wallpapers.insert(0, 'name', data_legacy['wallpaper_names'])
        wallpapers.to_parquet('./data/wallpapers.parquet')

        # Card Faces
        faces = DataFrame()
        faces.insert(0, 'key', data_legacy['types'].values())
        faces.insert(0, 'name', data_legacy['types'].keys())
        faces.to_parquet('./data/faces.parquet')

        # Icons
        icons = DataFrame()
        sorted_icons = sort_icon_sizes(data_legacy['icons'].values())
        icons.insert(0, 'small', [data['small'] for data in sorted_icons])
        icons.insert(0, 'medium', [data['medium'] for data in sorted_icons])
        icons.insert(0, 'large', [data['large'] for data in sorted_icons])
        icons.insert(0, 'name', data_legacy['icons'].keys())
        icons.to_parquet('./data/icons.parquet')


if __name__ == '__main__':
    import_from_legacy()