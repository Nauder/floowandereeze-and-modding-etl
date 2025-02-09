# Floowandereeze & Modding ETL

## Overview
This project is an **Extract, Transform, Load (ETL) pipeline** designed to extract data from the Yu-Gi-Oh! Master Duel 
game via **reverse engineering**, transform it into a structured format, and store the results in **Parquet files**,
mainly to be used by the Floowandereeze & Modding tool.

## Features
- **Extracts** game data by reverse engineering Unity game files.
- **Transforms** raw game data into a structured format.
- **Loads** the processed data into Parquet files for efficient storage and querying.

## Table of Contents
- [Technologies Used](#technologies-used)
- [Installation](#installation)
- [Usage](#usage)
- [Important Files Structure](#important-files-structure)
- [Configuration](#configuration)
- [License](#license)
- [Credits](#credits)

## Technologies Used
- **Python** for scripting and data processing
- **UnityPy** for reverse engineering Unity assets
- **pandas** for data manipulation
- **PIL** for image manipulation

## Installation
### Prerequisites
Ensure you have the following installed:
- Python 3.8+
- pip package manager
- Yu-Gi-Oh! Master Duel, with the in-game download complete

### Steps
```sh
# Clone the repository
git clone https://github.com/Nauder/floowandereeze-and-modding-etl.git
cd floowandereeze-and-modding-etl

# Install dependencies
pip install -r requirements.txt
```

## Usage
### ETL from Game Data
Replace the empty string in `etl/util.py` with your game path.

Run the extraction script to pull data from Unity game files:
```sh
python .\etl\main.py
```
After extracting the data, the script will prompt about the layout of the game fields it found, as determining the type
of each field has not been automated yet.



### ETL from Legacy Data
If converting from the format use by the first Floowandereze & Modding major version, run the conversion script:
```sh
python .\legacy\import.py
```

## Important Files Structure
```
├── data/                 # Final Parquet files
├── etl/
│   ├── decode/           # Decoding logic
│   ├── services/         # Pipeline logic
│   ├── main.py           # Main script
│   ├── util.py           # Utility functions and configurable parameters
├── legacy/
│   ├── import.py         # Legacy data import script
└── README.md             # This
```

## Configuration
Modify the constants in `etl/util.py` to specify the game path, filtering rules, and other settings.

## License
This project is licensed under the [GNU General Public License](LICENSE).

## Credits
- [Akintos](https://gist.github.com/akintos/04e2494c62184d2d4384078b0511673b) 
and [Timelic](https://github.com/timelic/master-duel-chinese-translation-switch) for the decoding scripts.
