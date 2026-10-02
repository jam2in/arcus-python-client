import errno
import socket
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from arcus import (
    ArcusNodeConnectionException,
    ArcusProtocolException,
    ArcusTranscoder,
)
from arcus_mc_node import ArcusMCNode, ArcusMCPoll, Connection


class ConnectionTestCase(unittest.TestCase):
    def make_connection(self, *chunks):
        sock = Mock(spec=socket.socket)
        # Unexpected reads fail immediately, even if a regression would loop on EOF.
        sock.recv.side_effect = [*chunks, AssertionError("unexpected socket read")]
        with patch("arcus.protocol.connection.socket.socket", return_value=sock):
            connection = Connection("127.0.0.1:11211")
        return connection, sock

    def make_node(self, *chunks):
        connection, sock = self.make_connection(*chunks)
        node = ArcusMCNode.__new__(ArcusMCNode)
        node.handle = connection
        node.transcoder = ArcusTranscoder()
        node.lock = threading.Lock()
        node._io_lock = threading.RLock()
        node._generation = 0
        node._pending = set()
        node.node_allocator = SimpleNamespace(worker=Mock())
        node.ops = []
        node.addr = "127.0.0.1:11211"
        return node, sock


class ConnectionTests(ConnectionTestCase):
    def test_reconnect_closes_previous_socket_and_discards_buffer(self):
        connection, original = self.make_connection()
        connection.buffer = b"stale response\r\n"
        replacement = Mock(spec=socket.socket)
        with patch("arcus.protocol.connection.socket.socket", return_value=replacement):
            self.assertIs(connection.connect(), replacement)
        original.close.assert_called_once_with()
        replacement.connect.assert_called_once_with(("127.0.0.1", 11211))
        self.assertEqual(connection.buffer, b"")

    def test_failed_reconnect_leaves_connection_disconnected(self):
        connection, original = self.make_connection()
        replacement = Mock(spec=socket.socket)
        replacement.connect.side_effect = OSError("connection refused")
        with patch("arcus.protocol.connection.socket.socket", return_value=replacement):
            self.assertIsNone(connection.connect())
        self.assertTrue(connection.disconnected())
        original.close.assert_called_once_with()
        replacement.close.assert_called_once_with()

    def test_disconnect_is_idempotent_and_discards_buffer(self):
        connection, sock = self.make_connection()
        connection.buffer = b"stale response\r\n"
        connection.disconnect()
        connection.disconnect()
        self.assertTrue(connection.disconnected())
        self.assertEqual(connection.buffer, b"")
        sock.close.assert_called_once_with()

    def test_readline_combines_partial_header_and_keeps_following_data(self):
        connection, sock = self.make_connection(
            b"VAL", b"UE key 0 3\r", b"\nabc\r\nEND\r\n"
        )
        self.assertEqual(connection.readline(), b"VALUE key 0 3")
        self.assertEqual(connection.recv(5), b"abc\r\n")
        self.assertEqual(connection.readline(), b"END")
        self.assertEqual(connection.buffer, b"")
        self.assertEqual(sock.recv.call_count, 3)

    def test_readline_uses_buffer_without_socket_read(self):
        connection, sock = self.make_connection(b"first\r\nsecond\r\n")
        self.assertEqual(connection.readline(), b"first")
        self.assertTrue(connection.hasline())
        self.assertEqual(connection.readline(), b"second")
        self.assertFalse(connection.hasline())
        sock.recv.assert_called_once_with(4096)

    def test_recv_combines_partial_payload_and_keeps_next_response(self):
        connection, sock = self.make_connection(b"cd", b"ef\r\nEND\r\n")
        connection.buffer = b"ab"
        self.assertEqual(connection.recv(8), b"abcdef\r\n")
        self.assertEqual(connection.readline(), b"END")
        self.assertEqual(sock.recv.call_count, 2)

    def test_zero_length_recv_preserves_buffer(self):
        connection, sock = self.make_connection()
        connection.buffer = b"next response\r\n"
        self.assertEqual(connection.recv(0), b"")
        self.assertEqual(connection.buffer, b"next response\r\n")
        sock.recv.assert_not_called()

    def test_readline_eof_discards_partial_header_and_disconnects(self):
        connection, sock = self.make_connection(b"VALUE key", b"")
        with self.assertRaises(ArcusNodeConnectionException):
            connection.readline()
        self.assertTrue(connection.disconnected())
        self.assertEqual(connection.buffer, b"")
        sock.close.assert_called_once_with()

    def test_recv_eof_discards_partial_payload_and_disconnects(self):
        connection, sock = self.make_connection(b"ab", b"")
        with self.assertRaises(ArcusNodeConnectionException):
            connection.recv(5)
        self.assertTrue(connection.disconnected())
        self.assertEqual(connection.buffer, b"")
        sock.close.assert_called_once_with()

    def test_disconnected_io_raises_connection_error(self):
        connection, sock = self.make_connection()
        connection.disconnect()
        for operation in (
            connection.readline,
            lambda: connection.recv(1),
            lambda: connection.send_request(b"get key"),
        ):
            with self.subTest(operation=operation):
                with self.assertRaises(ArcusNodeConnectionException):
                    operation()
        sock.recv.assert_not_called()
        sock.sendall.assert_not_called()

    def test_negative_recv_length_is_rejected(self):
        connection, sock = self.make_connection()
        with self.assertRaises(ArcusProtocolException):
            connection.recv(-1)
        sock.recv.assert_not_called()


class ValueResponseTests(ConnectionTestCase):
    def test_get_and_gets_preserve_normal_miss(self):
        for method in ("_recv_value", "_recv_cas_value"):
            with self.subTest(method=method):
                node, sock = self.make_node(b"END\r\n")
                self.assertIsNone(getattr(node, method)())
                self.assertFalse(node.handle.disconnected())
                sock.close.assert_not_called()

    def test_get_decodes_partial_payload_with_embedded_crlf(self):
        node, sock = self.make_node(b"VALUE key 0 4\r\na\r", b"\nb\r", b"\nEND\r\n")
        self.assertEqual(node._recv_value(), "a\r\nb")
        self.assertEqual(node.handle.buffer, b"")
        self.assertEqual(sock.recv.call_count, 3)

    def test_gets_preserves_cas_token_and_following_response(self):
        node, sock = self.make_node(b"VALUE key 0 3 42\r\nabc\r\nEND\r\nEND\r\n")
        self.assertEqual(node._recv_cas_value(), ("abc", b"42"))
        self.assertIsNone(node._recv_value())
        self.assertEqual(sock.recv.call_count, 1)

    def test_empty_value_is_distinct_from_miss(self):
        node, _ = self.make_node(b"VALUE key 0 0\r\n\r\nEND\r\n")
        self.assertEqual(node._recv_value(), "")

    def test_unexpected_response_is_not_a_miss(self):
        for response in (b"ERROR", b"SERVER_ERROR unavailable", b"END extra", b""):
            for method in ("_recv_value", "_recv_cas_value"):
                with self.subTest(response=response, method=method):
                    node, _ = self.make_node(response + b"\r\n")
                    with self.assertRaises(ArcusProtocolException):
                        getattr(node, method)()

    def test_malformed_get_headers_raise_protocol_error(self):
        for header in (
            b"VALUE key 0",
            b"VALUE key 0 3 extra",
            b"VALUEextra key 0 3",
            b"VALUE key invalid 3",
            b"VALUE key 0 invalid",
            b"VALUE key -1 3",
            b"VALUE key 0 -1",
        ):
            with self.subTest(header=header):
                node, _ = self.make_node(header + b"\r\n")
                with self.assertRaises(ArcusProtocolException):
                    node._recv_value()

    def test_malformed_gets_headers_raise_protocol_error(self):
        for header in (
            b"VALUE key 0 3",
            b"VALUE key 0 3 invalid",
            b"VALUE key 0 3 -1",
        ):
            with self.subTest(header=header):
                node, _ = self.make_node(header + b"\r\n")
                with self.assertRaises(ArcusProtocolException):
                    node._recv_cas_value()

    def test_wrong_payload_terminator_is_rejected(self):
        node, _ = self.make_node(b"VALUE key 0 3\r\nabcXXEND\r\n")
        with self.assertRaises(ArcusProtocolException):
            node._recv_value()

    def test_incorrect_payload_length_is_rejected(self):
        for length in (2, 4):
            with self.subTest(length=length):
                node, _ = self.make_node(b"VALUE key 0 %d\r\nabc\r\nEND\r\n" % length)
                with self.assertRaises(ArcusProtocolException):
                    node._recv_value()

    def test_wrong_response_terminator_is_rejected(self):
        node, _ = self.make_node(b"VALUE key 0 3\r\nabc\r\nSTORED\r\n")
        with self.assertRaises(ArcusProtocolException):
            node._recv_value()

    def test_payload_eof_raises_connection_error(self):
        node, sock = self.make_node(b"VALUE key 0 3\r\nab", b"")
        with self.assertRaises(ArcusNodeConnectionException):
            node._recv_value()
        self.assertTrue(node.handle.disconnected())
        sock.close.assert_called_once_with()

    def test_decode_failure_does_not_leave_end_for_the_next_operation(self):
        node, _ = self.make_node(b"VALUE key 0 1\r\n\xff\r\nEND\r\nEND\r\n")
        with self.assertRaises(UnicodeDecodeError):
            node._recv_value()
        self.assertIsNone(node._recv_value())

    def test_buffered_responses_resolve_the_matching_operations(self):
        node, sock = self.make_node(
            b"VALUE first 0 3\r\none\r\nEND\r\nVALUE second 0 3\r\ntwo\r\nEND\r\n"
        )
        first = Mock(callback=node._recv_value, deadline=time.monotonic() + 5)
        second = Mock(callback=node._recv_value, deadline=time.monotonic() + 5)
        node.ops = [first, second]
        node.do_op()
        first.set_result.assert_called_once_with("one")
        second.set_result.assert_called_once_with("two")
        self.assertEqual(node.ops, [])
        self.assertEqual(sock.recv.call_count, 1)

    def test_protocol_failure_invalidates_pending_operations(self):
        node, sock = self.make_node(b"VALUE key 0 -1\r\nEND\r\n")
        first = Mock(callback=node._recv_value, deadline=time.monotonic() + 5)
        second = Mock(callback=node._recv_value, deadline=time.monotonic() + 5)
        node.ops = [first, second]
        node.do_op()
        first.set_result.assert_called_once()
        self.assertIsInstance(
            first.set_result.call_args.args[0], ArcusProtocolException
        )
        second.set_invalid.assert_called_once_with()
        second.set_result.assert_not_called()
        self.assertEqual(node.ops, [])
        self.assertTrue(node.handle.disconnected())
        sock.close.assert_called_once_with()

    def test_eof_invalidates_pending_operations(self):
        node, sock = self.make_node(b"")
        first = Mock(callback=node._recv_value, deadline=time.monotonic() + 5)
        second = Mock(callback=node._recv_value, deadline=time.monotonic() + 5)
        node.ops = [first, second]
        node.do_op()
        self.assertIsInstance(
            first.set_result.call_args.args[0], ArcusNodeConnectionException
        )
        second.set_invalid.assert_called_once_with()
        sock.close.assert_called_once_with()

    def test_socket_errors_invalidate_pending_operations(self):
        for error in (
            socket.timeout("read timed out"),
            ConnectionResetError("connection reset"),
            OSError(errno.EIO, "read failed"),
        ):
            with self.subTest(error=type(error).__name__):
                node, sock = self.make_node(error)
                first = Mock(callback=node._recv_value, deadline=time.monotonic() + 5)
                second = Mock(callback=node._recv_value, deadline=time.monotonic() + 5)
                node.ops = [first, second]
                node.do_op()
                first.set_result.assert_called_once_with(error)
                second.set_invalid.assert_called_once_with()
                self.assertEqual(node.ops, [])
                self.assertTrue(node.handle.disconnected())
                sock.close.assert_called_once_with()

    def test_unsolicited_buffered_response_discards_connection(self):
        node, sock = self.make_node(b"END\r\nEND\r\n")
        operation = Mock(callback=node._recv_value, deadline=time.monotonic() + 5)
        node.ops = [operation]
        node.do_op()
        operation.set_result.assert_called_once_with(None)
        self.assertTrue(node.handle.disconnected())
        self.assertEqual(node.handle.buffer, b"")
        sock.close.assert_called_once_with()


class PollHangupTests(unittest.TestCase):
    def make_poll(self, unregister_error):
        allocator = SimpleNamespace(shutdown=False, snapshot_nodes=lambda: ())
        poll = ArcusMCPoll.__new__(ArcusMCPoll)
        poll.node_allocator = allocator
        poll.epoll = Mock()
        poll.epoll.closed = False
        poll.lock = threading.Lock()
        node = Mock()
        poll.sock_node_map = {7: node}
        node.disconnect.side_effect = lambda: poll.unregister_node(node)
        events = [[(7, 1 | 16)]]

        def next_events(timeout):
            if events:
                return events.pop()
            allocator.shutdown = True
            return []

        poll.epoll.poll.side_effect = next_events
        poll.epoll.unregister.side_effect = unregister_error
        return poll, node

    def test_eof_then_hangup_tolerates_already_closed_descriptor(self):
        for error_number in (errno.EBADF, errno.ENOENT):
            with self.subTest(error_number=error_number):
                poll, node = self.make_poll(OSError(error_number, "already closed"))
                with patch.multiple(
                    "arcus.protocol.worker.select",
                    EPOLLIN=1,
                    EPOLLHUP=16,
                    EPOLLERR=8,
                    create=True,
                ):
                    poll.run()
                node.do_op.assert_called_once_with()
                node.disconnect.assert_called_once_with()
                self.assertEqual(poll.sock_node_map, {})

    def test_hangup_does_not_hide_other_unregister_errors(self):
        poll, _ = self.make_poll(OSError(errno.EIO, "unexpected poll failure"))
        with patch.multiple(
            "arcus.protocol.worker.select",
            EPOLLIN=1,
            EPOLLHUP=16,
            EPOLLERR=8,
            create=True,
        ):
            with self.assertRaises(OSError) as raised:
                poll.run()
        self.assertEqual(raised.exception.errno, errno.EIO)


if __name__ == "__main__":
    unittest.main()
