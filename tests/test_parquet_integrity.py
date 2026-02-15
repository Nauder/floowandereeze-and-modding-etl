"""Data integrity tests for Parquet output files in data/."""

# pylint: disable=missing-class-docstring,missing-function-docstring,redefined-outer-name

import re
from pathlib import Path

import pandas as pd
import pytest

DATA_DIR = Path(__file__).parent.parent / "data"
BUNDLE_HEX_RE = re.compile(r"^[0-9a-f]{8}$")


def _load(filename):
    return pd.read_parquet(DATA_DIR / filename)


def _is_bundle(val):
    return bool(BUNDLE_HEX_RE.match(str(val)))


@pytest.fixture(scope="module")
def cards():
    return _load("cards.parquet")


@pytest.fixture(scope="module")
def icons():
    return _load("icons.parquet")


@pytest.fixture(scope="module")
def deck_boxes():
    return _load("deck_boxes.parquet")


@pytest.fixture(scope="module")
def sleeves():
    return _load("sleeves.parquet")


@pytest.fixture(scope="module")
def wallpapers():
    return _load("wallpapers.parquet")


@pytest.fixture(scope="module")
def faces():
    return _load("faces.parquet")


@pytest.fixture(scope="module")
def coins():
    return _load("coins.parquet")


@pytest.fixture(scope="module")
def card_icons():
    return _load("card_icons.parquet")


@pytest.fixture(scope="module")
def metadata():
    return _load("metadata.parquet")


@pytest.fixture(scope="module")
def fields():
    return _load("fields.parquet")


class TestCards:
    def test_not_empty(self, cards):
        assert len(cards) > 0

    def test_columns(self, cards):
        assert set(cards.columns) == {"data_index", "description", "bundle", "name"}

    def test_no_nulls(self, cards):
        assert not cards.isna().any().any()

    def test_unique_names(self, cards):
        assert not cards["name"].duplicated().any()

    def test_bundle_format(self, cards):
        assert cards["bundle"].apply(_is_bundle).all()

    def test_data_index_non_negative(self, cards):
        assert (cards["data_index"] >= 0).all()


class TestIcons:
    BUNDLE_COLS = ["large", "medium", "small"]

    def test_not_empty(self, icons):
        assert len(icons) > 0

    def test_columns(self, icons):
        assert set(icons.columns) == {"name", "large", "medium", "small"}

    def test_no_nulls(self, icons):
        assert not icons.isna().any().any()

    def test_name_is_numeric(self, icons):
        assert icons["name"].apply(lambda x: str(x).isdigit()).all()

    def test_bundle_format(self, icons):
        for col in self.BUNDLE_COLS:
            assert (
                icons[col].apply(_is_bundle).all()
            ), f"Invalid bundle in column '{col}'"

    def test_unique_names(self, icons):
        assert not icons["name"].duplicated().any()


class TestDeckBoxes:
    SIZE_COLS = [
        "small",
        "medium",
        "o_medium",
        "r_medium",
        "large",
        "o_large",
        "r_large",
    ]

    def test_not_empty(self, deck_boxes):
        assert len(deck_boxes) > 0

    def test_columns(self, deck_boxes):
        assert set(deck_boxes.columns) == set(self.SIZE_COLS) | {"name"}

    def test_no_nulls(self, deck_boxes):
        assert not deck_boxes.isna().any().any()

    def test_all_size_bundles_valid(self, deck_boxes):
        for col in self.SIZE_COLS:
            assert (
                deck_boxes[col].apply(_is_bundle).all()
            ), f"Invalid bundle in column '{col}'"

    def test_name_is_numeric(self, deck_boxes):
        assert deck_boxes["name"].apply(lambda x: str(x).isdigit()).all()

    def test_unique_names(self, deck_boxes):
        assert not deck_boxes["name"].duplicated().any()


class TestSleeves:
    def test_not_empty(self, sleeves):
        assert len(sleeves) > 0

    def test_columns(self, sleeves):
        assert list(sleeves.columns) == ["bundle"]

    def test_no_nulls(self, sleeves):
        assert not sleeves.isna().any().any()

    def test_bundle_format(self, sleeves):
        assert sleeves["bundle"].apply(_is_bundle).all()

    def test_no_duplicates(self, sleeves):
        assert not sleeves["bundle"].duplicated().any()


class TestWallpapers:
    BUNDLE_COLS = ["front", "back", "icon"]

    def test_not_empty(self, wallpapers):
        assert len(wallpapers) > 0

    def test_columns(self, wallpapers):
        assert set(wallpapers.columns) == {"front", "back", "icon", "name"}

    def test_no_nulls(self, wallpapers):
        assert not wallpapers.isna().any().any()

    def test_bundle_format(self, wallpapers):
        for col in self.BUNDLE_COLS:
            assert (
                wallpapers[col].apply(_is_bundle).all()
            ), f"Invalid bundle in column '{col}'"

    def test_unique_names(self, wallpapers):
        assert not wallpapers["name"].duplicated().any()


class TestFaces:
    def test_not_empty(self, faces):
        assert len(faces) > 0

    def test_columns(self, faces):
        assert set(faces.columns) == {"name", "key", "bundle"}

    def test_no_nulls(self, faces):
        assert not faces.isna().any().any()

    def test_bundle_format(self, faces):
        assert faces["bundle"].apply(_is_bundle).all()

    def test_unique_names(self, faces):
        assert not faces["name"].duplicated().any()


class TestCoins:
    def test_not_empty(self, coins):
        assert len(coins) > 0

    def test_columns(self, coins):
        assert list(coins.columns) == ["bundle"]

    def test_no_nulls(self, coins):
        assert not coins.isna().any().any()

    def test_bundle_format(self, coins):
        assert coins["bundle"].apply(_is_bundle).all()

    def test_no_duplicates(self, coins):
        assert not coins["bundle"].duplicated().any()


class TestCardIcons:
    def test_not_empty(self, card_icons):
        assert len(card_icons) > 0

    def test_columns(self, card_icons):
        assert set(card_icons.columns) == {"name", "x", "y", "width", "height"}

    def test_no_nulls(self, card_icons):
        assert not card_icons.isna().any().any()

    def test_non_negative_coordinates(self, card_icons):
        assert (card_icons["x"] >= 0).all()
        assert (card_icons["y"] >= 0).all()

    def test_positive_dimensions(self, card_icons):
        assert (card_icons["width"] > 0).all()
        assert (card_icons["height"] > 0).all()

    def test_unique_names(self, card_icons):
        assert not card_icons["name"].duplicated().any()


class TestMetadata:
    REQUIRED_FILES = {
        "card_name.bytes",
        "card_desc.bytes",
        "card_prop.bytes",
        "card_same.bytes",
        "card_indx.bytes",
        "card_intid.bytes",
    }

    def test_not_empty(self, metadata):
        assert len(metadata) > 0

    def test_columns(self, metadata):
        assert set(metadata.columns) == {"bundle", "name"}

    def test_no_nulls(self, metadata):
        assert not metadata.isna().any().any()

    def test_bundle_format(self, metadata):
        assert metadata["bundle"].apply(_is_bundle).all()

    def test_unique_names(self, metadata):
        assert not metadata["name"].duplicated().any()

    def test_required_files_present(self, metadata):
        assert self.REQUIRED_FILES <= set(metadata["name"])


class TestFields:  # pylint: disable=too-few-public-methods
    def test_columns(self, fields):
        assert set(fields.columns) == {"bottom", "flipped", "bundle"}
