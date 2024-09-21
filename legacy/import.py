import json

from pandas import DataFrame


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
        icons.insert(0, 'bundle_1', [data[0] for data in data_legacy['icons'].values()])
        icons.insert(0, 'bundle_2', [data[1] for data in data_legacy['icons'].values()])
        icons.insert(0, 'bundle_3', [data[2] for data in data_legacy['icons'].values()])
        icons.insert(0, 'name', data_legacy['icons'].keys())
        icons.to_parquet('./data/icons.parquet')


if __name__ == '__main__':
    import_from_legacy()