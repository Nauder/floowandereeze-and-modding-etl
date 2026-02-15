"""Tests for DataService methods."""

# pylint: disable=missing-class-docstring,missing-function-docstring,redefined-outer-name,duplicate-code

from unittest.mock import patch

import pytest

from services.data_service import DataService


@pytest.fixture
def data_service():
    """DataService instance with GameService dependency mocked out."""
    with patch("services.data_service.GameService"):
        svc = DataService()
    # Configure sort_sprite_list to return a valid 3-size mapping by default
    svc.game_service.unity_service.sort_sprite_list.return_value = {
        "small": "s",
        "medium": "m",
        "large": "l",
    }
    return svc


class TestAddSuffix:
    def test_no_duplicates_unchanged(self, data_service):
        assert data_service.add_suffix(["Alpha", "Beta", "Gamma"]) == [
            "Alpha",
            "Beta",
            "Gamma",
        ]

    def test_single_duplicate_last_keeps_base_name(self, data_service):
        # Processing happens in reverse: last occurrence retains base name
        result = data_service.add_suffix(["Dark Magician", "Dark Magician"])
        assert result == ["Dark Magician (alt 1)", "Dark Magician"]

    def test_triple_duplicate_numbered_in_reverse(self, data_service):
        result = data_service.add_suffix(["Blue-Eyes", "Blue-Eyes", "Blue-Eyes"])
        assert result == ["Blue-Eyes (alt 2)", "Blue-Eyes (alt 1)", "Blue-Eyes"]

    def test_empty_list(self, data_service):
        assert data_service.add_suffix([]) == []

    def test_preserves_length(self, data_service):
        names = ["A", "B", "A", "C", "B"]
        assert len(data_service.add_suffix(names)) == 5

    def test_interleaved_duplicates(self, data_service):
        # Only the last occurrence of each name gets the clean name
        result = data_service.add_suffix(["X", "Y", "X"])
        assert result == ["X (alt 1)", "Y", "X"]

    def test_suffixed_names_do_not_collide(self, data_service):
        names = ["Card", "Card", "Card"]
        result = data_service.add_suffix(names)
        assert len(set(result)) == 3


class TestRemoveExtraSuffix:
    def test_removes_alt1_when_base_absent(self, data_service):
        result = data_service.remove_extra_suffix({"Dark Magician (alt 1)": "bundle_a"})
        assert "Dark Magician" in result
        assert "Dark Magician (alt 1)" not in result

    def test_keeps_alt1_when_base_present(self, data_service):
        cards = {"Dark Magician": "bundle_a", "Dark Magician (alt 1)": "bundle_b"}
        result = data_service.remove_extra_suffix(cards)
        assert "Dark Magician (alt 1)" in result
        assert "Dark Magician" in result

    def test_no_alt1_unchanged(self, data_service):
        cards = {"Alpha": "bundle1", "Beta": "bundle2"}
        assert data_service.remove_extra_suffix(cards) == cards

    def test_empty_dict(self, data_service):
        assert data_service.remove_extra_suffix({}) == {}

    def test_alt2_and_higher_preserved(self, data_service):
        cards = {"X": "a", "X (alt 2)": "b"}
        result = data_service.remove_extra_suffix(cards)
        assert "X (alt 2)" in result

    def test_value_preserved_on_rename(self, data_service):
        cards = {"Card (alt 1)": ["bundle_id", "desc", 42]}
        result = data_service.remove_extra_suffix(cards)
        assert result["Card"] == ["bundle_id", "desc", 42]


class TestMergeData:
    def _wrapper(self, **overrides):
        base = {
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
        base.update(overrides)
        return base

    def test_merges_card_ids(self, data_service):
        ids = self._wrapper(card_id={"a": 1})
        data_service.merge_data(ids, self._wrapper(card_id={"b": 2}))
        assert ids["card_id"] == {"a": 1, "b": 2}

    def test_extends_sleeve_list(self, data_service):
        ids = self._wrapper(sleeve=["s1"])
        data_service.merge_data(ids, self._wrapper(sleeve=["s2"]))
        assert ids["sleeve"] == ["s1", "s2"]

    def test_extends_field_list(self, data_service):
        ids = self._wrapper(field=["f1"])
        data_service.merge_data(ids, self._wrapper(field=["f2"]))
        assert ids["field"] == ["f1", "f2"]

    def test_extends_coin_list(self, data_service):
        ids = self._wrapper(coin=["c1"])
        data_service.merge_data(ids, self._wrapper(coin=["c2"]))
        assert ids["coin"] == ["c1", "c2"]

    def test_merges_card_data(self, data_service):
        ids = self._wrapper(card_data={"part_a": "bundle_1"})
        data_service.merge_data(ids, self._wrapper(card_data={"part_b": "bundle_2"}))
        assert ids["card_data"] == {"part_a": "bundle_1", "part_b": "bundle_2"}

    def test_merges_face_data(self, data_service):
        ids = self._wrapper(face={"Normal": {"key": 0, "bundle": "b1"}})
        data_service.merge_data(
            ids, self._wrapper(face={"Effect": {"key": 1, "bundle": "b2"}})
        )
        assert "Normal" in ids["face"]
        assert "Effect" in ids["face"]

    def test_icon_deduplication_via_nested_dict_lists(self, data_service):
        ids = self._wrapper(icon={"100": ["bundle_a"]})
        data_service.merge_data(
            ids, self._wrapper(icon={"100": ["bundle_a", "bundle_b"]})
        )
        assert ids["icon"]["100"].count("bundle_a") == 1
        assert "bundle_b" in ids["icon"]["100"]


class TestCleanData:
    """Tests for data validation rules applied in clean_data()."""

    def _make_dirty_data(self, **overrides):
        base = {
            "icon": {},
            "deck_box": {},
            "sleeve": [],
            "wallpaper": {},
            "field": [],
        }
        base.update(overrides)
        return base

    def _run_clean(self, data_service, dirty_data):
        """Run clean_data with mocked file I/O, return the saved data dict."""
        with (
            patch("builtins.open"),
            patch("json.load", return_value=dirty_data),
            patch("json.dump") as mock_dump,
            patch("services.data_service.isfile", return_value=False),
        ):
            data_service.clean_data(sort_fields=False)
        return mock_dump.call_args[0][0]

    def test_removes_icon_with_fewer_than_3_bundles(self, data_service):
        dirty = self._make_dirty_data(icon={"123": ["a", "b"], "456": ["a", "b", "c"]})
        result = self._run_clean(data_service, dirty)
        assert "123" not in result["icon"]
        assert "456" in result["icon"]

    def test_removes_icon_with_non_numeric_id(self, data_service):
        dirty = self._make_dirty_data(
            icon={"abc": ["a", "b", "c"], "789": ["a", "b", "c"]}
        )
        result = self._run_clean(data_service, dirty)
        assert "abc" not in result["icon"]
        assert "789" in result["icon"]

    def test_removes_icon_when_sort_returns_wrong_size(self, data_service):
        # Simulate sort_sprite_list returning incomplete mapping
        data_service.game_service.unity_service.sort_sprite_list.return_value = {
            "small": "s",
            "medium": "m",
        }
        dirty = self._make_dirty_data(icon={"100": ["a", "b", "c"]})
        result = self._run_clean(data_service, dirty)
        assert "100" not in result["icon"]

    def test_removes_deck_box_with_missing_size_keys(self, data_service):
        valid_keys = {
            "large",
            "o_large",
            "r_large",
            "o_medium",
            "r_medium",
            "medium",
            "small",
        }
        dirty = self._make_dirty_data(
            deck_box={
                "1": {k: f"b_{k}" for k in valid_keys},
                "2": {"large": "b", "small": "b"},  # missing keys
            }
        )
        result = self._run_clean(data_service, dirty)
        assert "1" in result["deck_box"]
        assert "2" not in result["deck_box"]

    def test_removes_deck_box_with_non_numeric_id(self, data_service):
        valid_keys = {
            "large",
            "o_large",
            "r_large",
            "o_medium",
            "r_medium",
            "medium",
            "small",
        }
        dirty = self._make_dirty_data(deck_box={"xyz": {k: "b" for k in valid_keys}})
        result = self._run_clean(data_service, dirty)
        assert "xyz" not in result["deck_box"]

    def test_removes_excluded_sleeves(self, data_service):
        with patch("services.data_service.EXCLUDED_SLEEVES", ["bad_sleeve"]):
            dirty = self._make_dirty_data(sleeve=["good_sleeve", "bad_sleeve"])
            result = self._run_clean(data_service, dirty)
        assert "good_sleeve" in result["sleeve"]
        assert "bad_sleeve" not in result["sleeve"]

    def test_removes_wallpaper_without_3_files(self, data_service):
        dirty = self._make_dirty_data(
            wallpaper={
                "wp1": {"icon": "i", "front": "f", "back": "b"},  # valid
                "wp2": {"icon": "i", "front": "f"},  # missing back
            }
        )
        result = self._run_clean(data_service, dirty)
        assert "wp1" in result["wallpaper"]
        assert "wp2" not in result["wallpaper"]

    def test_field_data_preserved_from_existing_file(self, data_service):
        existing_clean = {"field": {"bundle_x": {"bottom": True, "flipped": False}}}
        dirty = self._make_dirty_data()

        with (
            patch("builtins.open"),
            patch("json.load", side_effect=[dirty, existing_clean]),
            patch("json.dump") as mock_dump,
            patch("services.data_service.isfile", return_value=True),
        ):
            data_service.clean_data(sort_fields=False)

        result = mock_dump.call_args[0][0]
        assert result["field"] == {"bundle_x": {"bottom": True, "flipped": False}}
