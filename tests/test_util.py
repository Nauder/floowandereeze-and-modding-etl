"""Tests for utility functions in etl/util.py."""

# pylint: disable=missing-class-docstring,missing-function-docstring,use-implicit-booleaness-not-comparison,duplicate-code

from util import chunkify, get_data_wrapper, merge_nested_dict_lists, merge_nested_dicts


class TestChunkify:
    def test_even_split(self):
        assert chunkify([1, 2, 3, 4], 2) == [[1, 2], [3, 4]]

    def test_uneven_split_distributes_remainder(self):
        result = chunkify([1, 2, 3, 4, 5], 3)
        assert len(result) == 3
        assert [item for chunk in result for item in chunk] == [1, 2, 3, 4, 5]

    def test_empty_list_returns_empty_chunks(self):
        result = chunkify([], 3)
        assert all(chunk == [] for chunk in result)

    def test_more_chunks_than_items(self):
        result = chunkify([1, 2], 5)
        assert [item for chunk in result for item in chunk] == [1, 2]

    def test_single_chunk(self):
        assert chunkify([1, 2, 3], 1) == [[1, 2, 3]]

    def test_single_item(self):
        assert chunkify([42], 1) == [[42]]

    def test_preserves_order(self):
        items = list(range(10))
        result = chunkify(items, 4)
        assert [item for chunk in result for item in chunk] == items


class TestMergeNestedDicts:
    def test_adds_new_key(self):
        d1 = {"a": 1}
        merge_nested_dicts(d1, {"b": 2})
        assert d1 == {"a": 1, "b": 2}

    def test_overwrites_scalar_value(self):
        d1 = {"a": 1}
        merge_nested_dicts(d1, {"a": 99})
        assert d1["a"] == 99

    def test_recursive_merge(self):
        d1 = {"nested": {"x": 1}}
        merge_nested_dicts(d1, {"nested": {"y": 2}})
        assert d1["nested"] == {"x": 1, "y": 2}

    def test_deep_recursive_merge(self):
        d1 = {"a": {"b": {"c": 1}}}
        merge_nested_dicts(d1, {"a": {"b": {"d": 2}}})
        assert d1 == {"a": {"b": {"c": 1, "d": 2}}}

    def test_empty_dicts(self):
        assert merge_nested_dicts({}, {}) == {}

    def test_returns_dict1_reference(self):
        d1 = {"a": 1}
        result = merge_nested_dicts(d1, {"b": 2})
        assert result is d1

    def test_does_not_modify_dict2(self):
        d2 = {"x": {"y": 1}}
        merge_nested_dicts({}, d2)
        assert d2 == {"x": {"y": 1}}


class TestMergeNestedDictLists:
    def test_adds_new_icon_key(self):
        d1 = {"icon": {}}
        merge_nested_dict_lists(d1, {"icon": {"id1": ["bundle_a"]}})
        assert d1["icon"]["id1"] == ["bundle_a"]

    def test_extends_existing_list(self):
        d1 = {"icon": {"id1": ["bundle_a"]}}
        merge_nested_dict_lists(d1, {"icon": {"id1": ["bundle_b"]}})
        assert set(d1["icon"]["id1"]) == {"bundle_a", "bundle_b"}

    def test_deduplicates_on_extend(self):
        d1 = {"icon": {"id1": ["bundle_a", "bundle_b"]}}
        merge_nested_dict_lists(d1, {"icon": {"id1": ["bundle_a", "bundle_c"]}})
        assert d1["icon"]["id1"].count("bundle_a") == 1
        assert "bundle_b" in d1["icon"]["id1"]
        assert "bundle_c" in d1["icon"]["id1"]

    def test_multiple_keys_merged(self):
        d1 = {"icon": {"1": ["a"]}}
        merge_nested_dict_lists(d1, {"icon": {"2": ["b"], "3": ["c"]}})
        assert set(d1["icon"].keys()) == {"1", "2", "3"}


class TestGetDataWrapper:
    def test_has_all_required_keys(self):
        expected = {
            "card_id",
            "sleeve",
            "icon",
            "deck_box",
            "field",
            "wallpaper",
            "card_data",
            "face",
            "coin",
            "card_icon",
        }
        assert set(get_data_wrapper().keys()) == expected

    def test_dict_values_are_empty(self):
        wrapper = get_data_wrapper()
        for key in (
            "card_id",
            "icon",
            "deck_box",
            "wallpaper",
            "card_data",
            "face",
            "card_icon",
        ):
            assert wrapper[key] == {}, f"Expected empty dict for '{key}'"

    def test_list_values_are_empty(self):
        wrapper = get_data_wrapper()
        for key in ("sleeve", "field", "coin"):
            assert wrapper[key] == [], f"Expected empty list for '{key}'"

    def test_returns_independent_instances(self):
        w1 = get_data_wrapper()
        w2 = get_data_wrapper()
        w1["sleeve"].append("sentinel")
        assert w2["sleeve"] == []
