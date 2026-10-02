from unittest.mock import Mock

import pytest

from arcus import ArcusSet


def cached_set():
    client = Mock()
    client.sop_get.return_value.get_result.return_value = {"old"}
    return ArcusSet(client, "key", cache_time=60), client


def test_set_add_updates_local_cache_only_after_remote_success():
    values, client = cached_set()

    def finish():
        assert values.cache == {"old"}
        return True

    client.sop_insert.return_value.get_result.side_effect = finish
    assert values.add("new") is True
    assert values.cache == {"old", "new"}
    client.sop_insert.assert_called_once_with("key", "new")


def test_set_add_rejection_keeps_local_cache_unchanged():
    values, client = cached_set()
    client.sop_insert.return_value.get_result.return_value = False
    assert values.add("new") is False
    assert values.cache == {"old"}


def test_set_add_exception_keeps_local_cache_unchanged():
    values, client = cached_set()
    client.sop_insert.return_value.get_result.side_effect = TimeoutError("failed")
    with pytest.raises(TimeoutError, match="failed"):
        values.add("new")
    assert values.cache == {"old"}


def test_set_add_without_local_cache_preserves_remote_result():
    client = Mock()
    client.sop_insert.return_value.get_result.return_value = True
    values = ArcusSet(client, "key")
    assert values.add("new") is True
    assert values.cache is None
    client.sop_get.assert_not_called()
    client.sop_insert.assert_called_once_with("key", "new")
