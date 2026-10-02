import queue
import threading
import unittest
from unittest.mock import Mock, patch

from arcus import ArcusNodeConnectionException, ArcusOperation, ArcusOperationList


def completed_operation(result):
    operation = ArcusOperation(None, b"request", None)
    operation.set_result(result)
    return operation


class OperationTests(unittest.TestCase):
    def make_operation(self):
        return ArcusOperation(None, b"request", None)

    def test_pending_operation_has_no_result(self):
        self.assertFalse(self.make_operation().has_result())

    def test_success_and_miss_are_cached(self):
        for value in (None, False, True, "value", {"key": "value"}):
            with self.subTest(value=value):
                operation = completed_operation(value)
                self.assertTrue(operation.has_result())
                self.assertIs(operation.get_result(), value)
                self.assertIs(operation.get_result(), value)

    def test_exception_is_raised_on_every_read(self):
        error = ValueError("invalid response")
        operation = completed_operation(error)
        for _ in range(2):
            with self.assertRaises(ValueError) as raised:
                operation.get_result(timeout=0.01)
            self.assertIs(raised.exception, error)
        self.assertTrue(operation.has_result())

    def test_positive_timeout_waits_for_completion(self):
        operation = self.make_operation()
        timer = threading.Timer(0.02, operation.set_result, args=("ready",))
        timer.start()
        try:
            self.assertEqual(operation.get_result(timeout=1), "ready")
        finally:
            timer.cancel()
            timer.join(timeout=1)

    def test_timeout_does_not_cancel_later_completion(self):
        operation = self.make_operation()
        with self.assertRaises(queue.Empty):
            operation.get_result(timeout=0.01)
        self.assertFalse(operation.has_result())
        operation.set_result("late")
        self.assertEqual(operation.get_result(timeout=0.01), "late")

    def test_default_timeout_keeps_blocking_until_result(self):
        operation = self.make_operation()
        timer = threading.Timer(0.02, operation.set_result, args=("ready",))
        timer.start()
        try:
            self.assertEqual(operation.get_result(), "ready")
        finally:
            timer.cancel()
            timer.join(timeout=1)

    def test_pending_invalidation_is_an_error_not_a_miss(self):
        operation = self.make_operation()
        self.assertTrue(operation.set_invalid())
        self.assertFalse(operation.set_invalid())
        self.assertTrue(operation.has_result())
        errors = []
        for _ in range(2):
            with self.assertRaises(ArcusNodeConnectionException) as raised:
                operation.get_result(timeout=0.01)
            errors.append(raised.exception)
        self.assertIs(errors[0], errors[1])

    def test_invalidation_preserves_already_completed_result(self):
        operation = completed_operation(None)
        self.assertFalse(operation.set_invalid())
        self.assertFalse(operation.invalid)
        self.assertIsNone(operation.get_result())

    def test_first_completion_wins_without_blocking_on_duplicates(self):
        operation = completed_operation("first")
        self.assertIsNone(operation.set_result("second"))
        self.assertEqual(operation.get_result(), "first")

    def test_late_completion_cannot_overwrite_invalidation(self):
        operation = self.make_operation()
        operation.set_invalid()
        operation.set_result("late success")
        with self.assertRaises(ArcusNodeConnectionException):
            operation.get_result(timeout=0.01)

    def check_waiters(self, operation, complete, expected_value=None, error_type=None):
        barrier = threading.Barrier(4)
        results = []
        errors = []

        def wait_for_result():
            try:
                barrier.wait(timeout=2)
                results.append(operation.get_result(timeout=1))
            except (
                ArcusNodeConnectionException,
                queue.Empty,
                threading.BrokenBarrierError,
            ) as error:
                errors.append(error)

        threads = [
            threading.Thread(target=wait_for_result, daemon=True) for _ in range(3)
        ]
        for thread in threads:
            thread.start()
        try:
            barrier.wait(timeout=2)
            complete()
        finally:
            for thread in threads:
                thread.join(timeout=2)
        self.assertTrue(all(not thread.is_alive() for thread in threads))
        if error_type is None:
            self.assertEqual(errors, [])
            self.assertEqual(results, [expected_value] * 3)
        else:
            self.assertEqual(results, [])
            self.assertEqual(len(errors), 3)
            self.assertTrue(all(isinstance(error, error_type) for error in errors))

    def test_completion_is_visible_to_multiple_waiters(self):
        operation = self.make_operation()
        self.check_waiters(
            operation, lambda: operation.set_result("ready"), expected_value="ready"
        )

    def test_invalidation_wakes_multiple_waiters(self):
        operation = self.make_operation()
        self.check_waiters(
            operation, operation.set_invalid, error_type=ArcusNodeConnectionException
        )

    def test_completion_and_invalidation_race_has_one_final_outcome(self):
        for _ in range(20):
            operation = self.make_operation()
            barrier = threading.Barrier(3)
            invalidated = []

            def complete(current=operation, gate=barrier):
                gate.wait(timeout=2)
                current.set_result("success")

            def invalidate(current=operation, gate=barrier, outcome=invalidated):
                gate.wait(timeout=2)
                outcome.append(current.set_invalid())

            threads = [
                threading.Thread(target=complete, daemon=True),
                threading.Thread(target=invalidate, daemon=True),
            ]
            for thread in threads:
                thread.start()
            barrier.wait(timeout=2)
            for thread in threads:
                thread.join(timeout=2)
            self.assertTrue(all(not thread.is_alive() for thread in threads))
            self.assertEqual(len(invalidated), 1)
            if invalidated[0]:
                for _ in range(2):
                    with self.assertRaises(ArcusNodeConnectionException):
                        operation.get_result(timeout=0.01)
            else:
                self.assertEqual(operation.get_result(timeout=0.01), "success")
                self.assertFalse(operation.invalid)

    def test_repr_does_not_wait_or_raise_cached_error(self):
        self.assertIn("pending", repr(self.make_operation()))
        self.assertIn("ValueError", repr(completed_operation(ValueError("error"))))


class OperationListTests(unittest.TestCase):
    def test_has_result_tracks_pending_children(self):
        operations = ArcusOperationList("bop mget")
        child = ArcusOperation(None, b"request", None)
        operations.add_op(child)
        self.assertFalse(operations.has_result())
        child.set_result(({}, []))
        self.assertTrue(operations.has_result())

    def test_mget_merges_results_and_sorts_missed_keys(self):
        operations = ArcusOperationList("bop mget")
        operations.add_op(completed_operation(({"a": {1: (None, "A")}}, ["z"])))
        operations.add_op(completed_operation(({"b": {2: (None, "B")}}, ["c"])))
        expected = {"a": {1: (None, "A")}, "b": {2: (None, "B")}}
        self.assertEqual(operations.get_result(), expected)
        self.assertIs(operations.get_result(), operations.get_result())
        self.assertEqual(operations.get_missed_key(), ["c", "z"])

    def test_smget_merge_preserves_child_results(self):
        first = [(1, "a", None, "A"), (3, "a", None, "C")]
        second = [(2, "b", None, "B")]
        child = completed_operation((first, ["z"]))
        operations = ArcusOperationList("bop smget")
        operations.add_op(child)
        operations.add_op(completed_operation((second, ["c"])))
        expected = [first[0], second[0], first[1]]
        self.assertEqual(operations.get_result(), expected)
        self.assertEqual(child.get_result(), (first, ["z"]))
        self.assertEqual(len(first), 2)
        self.assertEqual(len(second), 1)
        self.assertEqual(operations.get_missed_key(), ["c", "z"])

    def test_empty_operation_lists_cache_result_and_missed_keys(self):
        for command, expected in (("bop mget", {}), ("bop smget", [])):
            with self.subTest(command=command):
                operations = ArcusOperationList(command)
                self.assertEqual(operations.get_result(), expected)
                self.assertEqual(operations.get_missed_key(), [])
                self.assertTrue(operations.has_result())

    def test_child_exception_is_cached_for_repeated_parent_reads(self):
        error = ValueError("child failed")
        child = completed_operation(error)
        operations = ArcusOperationList("bop mget")
        operations.add_op(child)
        with patch.object(child, "get_result", wraps=child.get_result) as get_result:
            for _ in range(2):
                with self.assertRaises(ValueError) as raised:
                    operations.get_result(timeout=0.1)
                self.assertIs(raised.exception, error)
            get_result.assert_called_once()
        self.assertTrue(operations.has_result())

    def test_invalidation_terminates_pending_children_and_preserves_done_children(self):
        done = completed_operation(({"a": {}}, []))
        pending = ArcusOperation(None, b"request", None)
        operations = ArcusOperationList("bop mget")
        operations.add_op(done)
        operations.add_op(pending)
        self.assertTrue(operations.set_invalidate())
        self.assertFalse(operations.set_invalidate())
        self.assertEqual(done.get_result(), ({"a": {}}, []))
        with self.assertRaises(ArcusNodeConnectionException):
            pending.get_result(timeout=0.01)
        for _ in range(2):
            with self.assertRaises(ArcusNodeConnectionException):
                operations.get_result(timeout=0.01)
        self.assertTrue(operations.has_result())

    def test_invalidation_preserves_ready_child_results(self):
        operations = ArcusOperationList("bop mget")
        operations.add_op(completed_operation(({"a": {}}, [])))
        self.assertFalse(operations.set_invalidate())
        self.assertEqual(operations.get_result(), {"a": {}})

    def test_timeout_is_retryable_after_child_completes(self):
        child = ArcusOperation(None, b"request", None)
        operations = ArcusOperationList("bop mget")
        operations.add_op(child)
        with self.assertRaises(queue.Empty):
            operations.get_result(timeout=0.01)
        self.assertFalse(operations.has_result())
        child.set_result(({"a": {}}, ["b"]))
        self.assertEqual(operations.get_result(timeout=0.1), {"a": {}})
        self.assertEqual(operations.get_missed_key(), ["b"])

    def test_deadline_budget_is_shared_by_all_nodes(self):
        first = Mock(spec=ArcusOperation)
        first.get_result.return_value = ({"a": {}}, [])
        second = Mock(spec=ArcusOperation)
        second.get_result.return_value = ({"b": {}}, [])
        operations = ArcusOperationList("bop mget")
        operations.add_op(first)
        operations.add_op(second)
        with patch("arcus.operation.time.monotonic", side_effect=[10.0, 10.0, 10.7]):
            self.assertEqual(operations.get_result(timeout=1), {"a": {}, "b": {}})
        self.assertAlmostEqual(first.get_result.call_args.args[0], 1.0)
        self.assertAlmostEqual(second.get_result.call_args.args[0], 0.3)

    def test_expired_deadline_never_becomes_an_unlimited_child_wait(self):
        first = completed_operation(({}, []))
        second = Mock(spec=ArcusOperation)
        second.has_result.return_value = False
        operations = ArcusOperationList("bop mget")
        operations.add_op(first)
        operations.add_op(second)
        with (
            patch("arcus.operation.time.monotonic", side_effect=[10.0, 10.0, 11.0]),
            self.assertRaises(queue.Empty),
        ):
            operations.get_result(timeout=1)
        second.get_result.assert_not_called()

    def test_expired_deadline_can_read_an_already_completed_child(self):
        operations = ArcusOperationList("bop mget")
        operations.add_op(completed_operation(({"a": {}}, [])))
        with patch("arcus.operation.time.monotonic", side_effect=[10.0, 11.0]):
            self.assertEqual(operations.get_result(timeout=1), {"a": {}})

    def test_invalidation_wakes_a_blocked_parent_reader(self):
        child = ArcusOperation(None, b"request", None)
        operations = ArcusOperationList("bop mget")
        operations.add_op(child)
        waiting = threading.Event()
        errors = []

        def read_result():
            waiting.set()
            try:
                operations.get_result()
            except ArcusNodeConnectionException as error:
                errors.append(error)

        thread = threading.Thread(target=read_result, daemon=True)
        thread.start()
        try:
            self.assertTrue(waiting.wait(timeout=1))
            self.assertTrue(operations.set_invalidate())
        finally:
            thread.join(timeout=2)
        self.assertFalse(thread.is_alive())
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], ArcusNodeConnectionException)

    def test_concurrent_smget_readers_receive_the_same_complete_result(self):
        operations = ArcusOperationList("bop smget")
        first = [(1, "a", None, "A"), (3, "a", None, "C")]
        second = [(2, "b", None, "B")]
        operations.add_op(completed_operation((first, [])))
        operations.add_op(completed_operation((second, [])))
        OperationTests().check_waiters(
            operations, lambda: None, expected_value=[first[0], second[0], first[1]]
        )
        self.assertEqual(len(first), 2)
        self.assertEqual(len(second), 1)


if __name__ == "__main__":
    unittest.main()
