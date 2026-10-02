import queue
from types import SimpleNamespace
from unittest.mock import Mock, patch

from arcus import Arcus, ArcusTranscoder
from arcus_mc_node import ArcusMCNode


def test_cas_forwards_the_expiration_and_token():
    locator = Mock()
    client = Arcus(locator)

    operation = client.cas("key", "value", b"123", exptime=60)

    locator.get_node.assert_called_once_with("key")
    locator.get_node.return_value.cas.assert_called_once_with(
        "key", "value", b"123", 60
    )
    assert operation is locator.get_node.return_value.cas.return_value


def test_cas_encodes_a_token_returned_by_gets():
    allocator = SimpleNamespace(shutdown=False, worker=SimpleNamespace(q=queue.Queue()))
    with patch("arcus.protocol.node.Connection"):
        node = ArcusMCNode("127.0.0.1:11211", "test", ArcusTranscoder(), allocator)

    for token in (123, b"123"):
        operation = node.cas("key", "value", token, exptime=60)
        assert operation.request == b"cas key 0 60 5 123\r\nvalue"
        assert operation is allocator.worker.q.get_nowait()
        assert not operation.has_result()


def test_btree_decrement_dispatches_to_decrement():
    locator = Mock()
    client = Arcus(locator)

    operation = client.bop_decr("key", 42, 3, noreply=True, pipe=True)

    locator.get_node.assert_called_once_with("key")
    locator.get_node.return_value.bop_decr.assert_called_once_with(
        "key", 42, 3, True, True
    )
    locator.get_node.return_value.bop_incr.assert_not_called()
    assert operation is locator.get_node.return_value.bop_decr.return_value
