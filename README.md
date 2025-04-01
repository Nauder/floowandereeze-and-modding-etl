# Floowandereeze & Modding ETL

[![Pylint](https://github.com/Nauder/floowandereeze-and-modding-etl/actions/workflows/pylint.yml/badge.svg)](https://github.com/Nauder/floowandereeze-and-modding-etl/actions/workflows/pylint.yml)
[![Black](https://github.com/Nauder/floowandereeze-and-modding-etl/actions/workflows/black.yml/badge.svg)](https://github.com/Nauder/floowandereeze-and-modding-etl/actions/workflows/black.yml)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)

![Python](https://img.shields.io/badge/python-3.8+-blue?logo=python&logoColor=white)
![UnityPy](https://img.shields.io/badge/UnityPy-1.20-orange?logo=unity&logoColor=white)
![pandas](https://img.shields.io/badge/pandas-latest-6554A8?logo=pandas&logoColor=white)

## Overview

This project is an **Extract, Transform, Load (ETL) pipeline** designed to extract metadata from the global version of
the Yu-Gi-Oh! Master Duel game via **reverse engineering**, transform it into a structured format, and store the results
in **Parquet files**, mainly to be used by the Floowandereeze & Modding tool.

This project **does not** extract the assets themselves (e.g. png files containing art of cards), it only gets the
references to the bundles containing the data, which can be used for modding.

## Features

- **Extracts** game metadata by reverse engineering Unity game files. The data currently extracted includes:
  - Card Arts.
  - Card Descriptions and Names (en-us versions).
  - Card Faces.
  - CARD_* Files (en-us versions).
  - Deck Boxes.
  - Fields.
  - Player Icons.
  - Sleeves.
- **Transforms** raw game data into a structured format, and cleans bad data.
- **Loads** the processed data into Parquet files for efficient storage and querying.

## Table of Contents

- [Technologies Used](#technologies-used)
- [Installation](#installation)
- [Usage](#usage)
- [Important Files Structure](#important-files-structure)
- [Configuration](#configuration)
- [CI/CD](#cicd)
- [License](#license)
- [Credits](#credits)

## Technologies Used

- [**Python**](https://www.python.org/) for scripting and data processing
- [**UnityPy**](https://github.com/K0lb3/UnityPy) for reverse engineering Unity assets
- [**pandas**](https://pandas.pydata.org/) for data manipulation
- [**PIL**](https://pypi.org/project/pillow/) for image manipulation

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

Replace the empty string in `config.json` with your game path.

Run the extraction script to pull data from Unity game files:

```sh
python .\etl\main.py
```

After extracting the data, the script will prompt about the layout of the game fields it found, as determining the type
of each field has not been automated yet.

Finally, the data will be available as Parquet files inside the `data/` folder, as well as a `version.txt` file
containing the date of the last script run.

## Important Files Structure

```txt
├── data/                 # Final Parquet files
├── etl/
│   ├── decode/           # Decoding logic
│   ├── services/         # Pipeline logic
│   ├── main.py           # Main script
│   └── util.py           # Utility functions
├── config.json           # Configurable parameters
└── README.md             # This
```

The `data/` folder in the repository contains the game metadata as of it's last push, so if there was no game update
after the last push it should already be up to date.

## Configuration

Modify the values in `config.json`, the configurations are:

- **game_path** path to your Master Duel installation's user data, up to the 0000 folder.
- **num_threads** amount of threads to use when extracting data, performance varies by hardware.
- **excluded_sleeves** sleeve assets to be ignored when building the list of sleeves. The game names sleeve materials
the same way as animated sleeve frames, so they are removed manually.

The only mandatory configuration is the **game_path**, the rest come with default values that should be appropriate for
most cases.

## CI/CD

This project uses GitHub Actions for continuous integration and code quality checks:

- **Pylint**: Runs on every push and pull request to the main branch to ensure code quality and catch potential errors.
The check requires a minimum score of 9/10.
- **Black**: Runs on every push and pull request to the main branch to enforce consistent code formatting across the
project.

The `etl/decode` folders code is excluded from checks, as it is external code and not maintained
in this repository.

## License

This project is licensed under the [GNU General Public License](LICENSE).

## Credits

- [Akintos](https://gist.github.com/akintos/04e2494c62184d2d4384078b0511673b)
and [Timelic](https://github.com/timelic/master-duel-chinese-translation-switch) for the decoding scripts.
