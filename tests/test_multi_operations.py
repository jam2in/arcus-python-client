import threading
from concurrent.futures import CancelledError
from concurrent.futures import TimeoutError as FutureTimeoutError
from queue import SimpleQueue
from unittest.mock import patch

import pytest

from arcus import ArcusCompositeException, ArcusMultiOperation, ArcusOperation
from arcus.exceptions import (
    ArcusException,
    ArcusNodeConnectionException,
    ArcusTimeoutException,
)


def make_operation(result=None, *, ready=False):
    operation = ArcusOperation(None, b"request", None)
    if ready:
        operation.set_result(result)
    return operation


def completed_operation(result):
    return make_operation(result, ready=True)


def start_readers(operation, count=3, timeout=2):
    outcomes = SimpleQueue()

    def read_result():
        try:
            outcomes.put((True, operation.get_result(timeout=timeout)))
        except (
            ArcusNodeConnectionException,
            ArcusTimeoutException,
            threading.BrokenBarrierError,
        ) as error:
            outcomes.put((False, error))

    threads = [threading.Thread(target=read_result, daemon=True) for _ in range(count)]
    for thread in threads:
        thread.start()
    return threads, outcomes


def finish_readers(threads, outcomes):
    for thread in threads:
        thread.join(timeout=3)
    assert all(not thread.is_alive() for thread in threads)
    return [outcomes.get_nowait() for _ in threads]


def test_has_result_tracks_pending_children():
    child = make_operation()
    operations = ArcusMultiOperation("bop mget", [child])
    assert not operations.has_result()
    child.set_result(({}, []))
    assert operations.has_result()


def test_constructor_snapshots_children():
    children = [completed_operation(({"a": {}}, []))]
    operations = ArcusMultiOperation("bop mget", children)
    children.append(make_operation())
    assert operations.get_result() == {"a": {}}
    assert len(operations.ops) == 1


def test_mget_merges_results_and_sorts_missed_keys_once():
    children = [
        completed_operation(({"a": {1: (None, "A")}}, ["z"])),
        completed_operation(({"b": {2: (None, "B")}}, ["c"])),
    ]
    operations = ArcusMultiOperation("bop mget", children)
    with patch.object(
        children[0], "get_result", wraps=children[0].get_result
    ) as get_result:
        result = operations.get_result()
        assert result == {"a": {1: (None, "A")}, "b": {2: (None, "B")}}
        assert operations.get_result() is result
        assert operations.get_missed_key() == ["c", "z"]
        get_result.assert_called_once()


def test_missed_keys_can_be_read_before_results():
    child = completed_operation(({"a": {}}, ["z", "c"]))
    operations = ArcusMultiOperation("bop mget", [child])
    assert operations.get_missed_key() == ["c", "z"]
    assert operations.get_result() == {"a": {}}


def test_smget_merge_preserves_child_results():
    first = [(1, "a", None, "A"), (3, "a", None, "C")]
    second = [(2, "b", None, "B")]
    first_snapshot = list(first)
    second_snapshot = list(second)
    child = completed_operation((first, ["z"]))
    operations = ArcusMultiOperation(
        "bop smget", [child, completed_operation((second, ["c"]))]
    )
    result = operations.get_result()
    assert result == [first_snapshot[0], second_snapshot[0], first_snapshot[1]]
    assert operations.get_result() is result
    assert child.get_result() == (first_snapshot, ["z"])
    assert first == first_snapshot
    assert second == second_snapshot
    assert operations.get_missed_key() == ["c", "z"]


@pytest.mark.parametrize("command, expected", [("bop mget", {}), ("bop smget", [])])
def test_empty_operations_cache_results_and_missed_keys(command, expected):
    operations = ArcusMultiOperation(command, [])
    assert operations.has_result()
    result = operations.get_result()
    assert result == expected
    assert operations.get_result() is result
    assert operations.get_missed_key() == []


def test_smget_handles_empty_child_results():
    operations = ArcusMultiOperation(
        "bop smget",
        [
            completed_operation(([], ["z"])),
            completed_operation(([(2, "a", None, "A")], [])),
            completed_operation(([], ["b"])),
        ],
    )
    assert operations.get_result() == [(2, "a", None, "A")]
    assert operations.get_missed_key() == ["b", "z"]


@pytest.mark.parametrize(
    "error",
    [
        ValueError("child failed"),
        TimeoutError("socket timed out"),
        FutureTimeoutError("nested operation timed out"),
        ArcusTimeoutException("child failed"),
        CancelledError("child failed"),
    ],
)
def test_child_exception_is_reported_after_siblings_complete(error):
    child = make_operation()
    child.set_exception(error)
    pending = make_operation()
    operations = ArcusMultiOperation("bop mget", [child, pending])

    with pytest.raises(ArcusTimeoutException) as timed_out:
        operations.get_result(timeout=0.01)
    assert timed_out.value is not error
    assert not operations.has_result()

    pending.set_result(({"late": {}}, []))
    with patch.object(child, "get_result", wraps=child.get_result) as get_result:
        for read in (operations.get_result, operations.get_missed_key):
            with pytest.raises(type(error)) as raised:
                read(timeout=0.1)
            assert raised.value is error
        get_result.assert_called_once()
    assert pending.get_result() == ({"late": {}}, [])


@pytest.mark.parametrize("method", ["get_result", "get_missed_key"])
def test_explicit_none_timeout_accepts_completed_results(method):
    child = completed_operation(({"a": {}}, ["b"]))
    operations = ArcusMultiOperation("bop mget", [child])
    expected = {"a": {}} if method == "get_result" else ["b"]
    assert getattr(operations, method)(timeout=None) == expected


@pytest.mark.parametrize("method", ["get_result", "get_missed_key"])
@pytest.mark.parametrize("timeout", [0, -1])
def test_nonpositive_timeout_checks_without_waiting(method, timeout):
    child = make_operation()
    operations = ArcusMultiOperation("bop mget", [child])
    with pytest.raises(ArcusTimeoutException):
        getattr(operations, method)(timeout=timeout)
    assert not operations.has_result()

    child.set_result(({"a": {}}, ["b"]))
    expected = {"a": {}} if method == "get_result" else ["b"]
    assert getattr(operations, method)(timeout=timeout) == expected


def test_merge_error_is_cached():
    child = completed_operation((123, []))
    operations = ArcusMultiOperation("bop mget", [child])
    errors = []
    with patch.object(child, "get_result", wraps=child.get_result) as get_result:
        for _ in range(2):
            with pytest.raises(TypeError) as raised:
                operations.get_result()
            errors.append(raised.value)
        get_result.assert_called_once()
    assert errors[0] is errors[1]


@pytest.mark.parametrize("result", [None, ({}, None)])
def test_malformed_child_result_is_reported_after_siblings_complete(result):
    child = completed_operation(result)
    pending = make_operation()
    operations = ArcusMultiOperation("bop mget", [child, pending])

    with pytest.raises(ArcusTimeoutException):
        operations.get_result(timeout=0)

    pending.set_result(({"b": {}}, []))
    with pytest.raises(TypeError) as first:
        operations.get_result(timeout=0)
    with pytest.raises(TypeError) as second:
        operations.get_missed_key(timeout=0)

    assert second.value is first.value


def test_cancelled_child_uses_the_single_operation_error_after_siblings_complete():
    child = make_operation()
    child._future.cancel()
    pending = make_operation()
    operations = ArcusMultiOperation("bop mget", [child, pending])

    with pytest.raises(ArcusTimeoutException):
        operations.get_result(timeout=0)

    pending.set_result(({"b": {}}, []))
    with pytest.raises(ArcusException) as single:
        child.get_result(timeout=0)
    with pytest.raises(ArcusException) as multi:
        operations.get_result(timeout=0)
    with pytest.raises(ArcusException) as missed:
        operations.get_missed_key(timeout=0)

    assert type(multi.value) is type(single.value)
    assert str(multi.value) == str(single.value)
    assert missed.value is multi.value


def test_multiple_child_failures_are_reported_together():
    connection_error = ArcusNodeConnectionException()
    timeout_error = TimeoutError()
    failed = [make_operation(), make_operation()]
    failed[0].set_exception(connection_error)
    failed[1].set_exception(timeout_error)
    operations = ArcusMultiOperation(
        "bop mget", [failed[0], completed_operation(({"a": {}}, [])), failed[1]]
    )

    with pytest.raises(ArcusCompositeException) as raised:
        operations.get_result()
    with pytest.raises(ArcusCompositeException) as missed:
        operations.get_missed_key()

    assert raised.value.exceptions == (connection_error, timeout_error)
    assert str(raised.value) == (
        "Multiple exceptions (2) reported: Node disconnected, TimeoutError"
    )
    assert missed.value is raised.value


def test_explicit_failure_terminates_pending_children_and_preserves_done_children():
    done = completed_operation(({"a": {}}, []))
    pending = make_operation()
    error = ArcusNodeConnectionException()
    operations = ArcusMultiOperation("bop mget", [done, pending])
    assert operations.set_exception(error) is None
    assert operations.set_exception(ValueError("late failure")) is None
    assert done.get_result() == ({"a": {}}, [])
    for read in (pending.get_result, operations.get_result, operations.get_missed_key):
        with pytest.raises(ArcusNodeConnectionException) as raised:
            read(timeout=0.01)
        assert raised.value is error
    assert operations.has_result()


def test_explicit_failure_can_win_before_ready_children_are_merged():
    child = completed_operation(({"a": {}}, []))
    operations = ArcusMultiOperation("bop mget", [child])
    error = ArcusNodeConnectionException()
    operations.set_exception(error)
    with pytest.raises(ArcusNodeConnectionException) as raised:
        operations.get_result()
    assert raised.value is error
    assert child.get_result() == ({"a": {}}, [])


def test_explicit_failure_cannot_replace_cached_success():
    child = completed_operation(({"a": {}}, ["b"]))
    operations = ArcusMultiOperation("bop mget", [child])
    result = operations.get_result()
    operations.set_exception(ArcusNodeConnectionException())
    assert operations.get_result() is result
    assert operations.get_missed_key() == ["b"]


def test_timeout_is_retryable_after_child_completes():
    child = make_operation()
    operations = ArcusMultiOperation("bop mget", [child])
    with pytest.raises(ArcusTimeoutException):
        operations.get_result(timeout=0.01)
    assert not operations.has_result()
    child.set_result(({"a": {}}, ["b"]))
    assert operations.get_result(timeout=0.1) == {"a": {}}
    assert operations.get_missed_key() == ["b"]


def test_deadline_budget_is_shared_by_all_nodes():
    first = completed_operation(({"a": {}}, []))
    second = completed_operation(({"b": {}}, []))
    operations = ArcusMultiOperation("bop mget", [first, second])
    with (
        patch("arcus.operation.time.monotonic", side_effect=[10.0, 10.0, 10.7]),
        patch.object(
            first._future, "exception", wraps=first._future.exception
        ) as first_wait,
        patch.object(
            second._future, "exception", wraps=second._future.exception
        ) as second_wait,
    ):
        assert operations.get_result(timeout=1) == {"a": {}, "b": {}}
    assert first_wait.call_args_list[0].kwargs["timeout"] == pytest.approx(1.0)
    assert second_wait.call_args_list[0].kwargs["timeout"] == pytest.approx(0.3)


def test_expired_deadline_never_becomes_an_unlimited_child_wait():
    first = completed_operation(({}, []))
    second = make_operation()
    operations = ArcusMultiOperation("bop mget", [first, second])
    with (
        patch("arcus.operation.time.monotonic", side_effect=[10.0, 10.0, 11.0]),
        patch.object(second, "get_result", wraps=second.get_result) as get_result,
        pytest.raises(ArcusTimeoutException),
    ):
        operations.get_result(timeout=1)
    get_result.assert_not_called()


def test_expired_deadline_can_read_an_already_completed_child():
    operations = ArcusMultiOperation("bop mget", [completed_operation(({"a": {}}, []))])
    with patch("arcus.operation.time.monotonic", side_effect=[10.0, 11.0]):
        assert operations.get_result(timeout=1) == {"a": {}}


@pytest.mark.parametrize(
    "error", [FutureTimeoutError("stored timeout"), CancelledError("stored error")]
)
def test_completed_parent_error_wins_over_a_child_wait_timeout(error):
    child = make_operation()
    operations = ArcusMultiOperation("bop mget", [child])

    def fail_during_wait(timeout=None):
        operations.set_exception(error)
        raise FutureTimeoutError()

    with (
        patch.object(child._future, "exception", side_effect=fail_during_wait),
        pytest.raises(type(error)) as raised,
    ):
        operations.get_result(timeout=0)

    assert raised.value is error


@pytest.mark.parametrize("timeout", [None, 2])
def test_explicit_failure_wakes_multiple_parent_readers(timeout):
    child = make_operation()
    operations = ArcusMultiOperation("bop mget", [child])
    registered = threading.Event()
    count = 0

    class CountingCondition(threading.Condition):
        def wait(self, timeout=None):
            nonlocal count
            count += 1
            if count == 3:
                registered.set()
            return super().wait(timeout)

    child._future._condition = CountingCondition()
    threads, outcomes = start_readers(operations, timeout=timeout)
    error = ArcusNodeConnectionException()
    try:
        assert registered.wait(timeout=2)
        assert outcomes.empty()
    finally:
        operations.set_exception(error)
        results = finish_readers(threads, outcomes)
    assert all(not success and value is error for success, value in results)


def test_concurrent_smget_readers_share_one_final_result():
    first = [(1, "a", None, "A"), (3, "a", None, "C")]
    second = [(2, "b", None, "B")]
    child = completed_operation((first, ["z"]))
    operations = ArcusMultiOperation(
        "bop smget", [child, completed_operation((second, ["c"]))]
    )
    entered = threading.Barrier(3)
    get_child_result = child.get_result

    def wait_for_other_readers(timeout=None):
        entered.wait(timeout=2)
        return get_child_result(timeout)

    with patch.object(child, "get_result", side_effect=wait_for_other_readers):
        threads, outcomes = start_readers(operations)
        results = finish_readers(threads, outcomes)
    assert all(success for success, _ in results)
    assert all(value is results[0][1] for _, value in results)
    assert results[0][1] == [first[0], second[0], first[1]]
    assert operations.get_missed_key() == ["c", "z"]
    assert len(first) == 2
    assert len(second) == 1


def test_explicit_failure_wins_while_a_reader_is_merging():
    child = completed_operation(({"a": {}}, []))
    operations = ArcusMultiOperation("bop mget", [child])
    merging = threading.Event()
    resume = threading.Event()
    merge = operations._merge_results

    def pause_before_publish(child_results, missed_keys):
        combined = merge(child_results, missed_keys)
        merging.set()
        assert resume.wait(timeout=2)
        return combined

    error = ArcusNodeConnectionException()
    with patch.object(operations, "_merge_results", side_effect=pause_before_publish):
        threads, outcomes = start_readers(operations, count=1)
        try:
            assert merging.wait(timeout=2)
            operations.set_exception(error)
        finally:
            resume.set()
            results = finish_readers(threads, outcomes)
    assert results == [(False, error)]
    with pytest.raises(ArcusNodeConnectionException) as raised:
        operations.get_result()
    assert raised.value is error


def test_repr_does_not_wait_or_raise_a_stored_error():
    pending = ArcusMultiOperation("bop mget", [make_operation()])
    failed = ArcusMultiOperation("bop smget", [make_operation()])
    error = ArcusNodeConnectionException()
    failed.set_exception(error)

    assert repr(pending) == (
        f"<ArcusMultiOperation[{hex(id(pending))}] result: pending>"
    )
    assert repr(failed) == (
        f"<ArcusMultiOperation[{hex(id(failed))}] result: {error!r}>"
    )


@pytest.mark.parametrize(
    "command, result",
    [("bop mget", {"a": {}}), ("bop smget", [(1, "a", None, "A")])],
)
def test_repr_shows_the_public_result_like_a_single_operation(command, result):
    child = completed_operation((result, ["missing"]))
    operations = ArcusMultiOperation(command, [child])
    merged = operations.get_result()
    single = completed_operation(merged)

    assert repr(operations) == (
        f"<ArcusMultiOperation[{hex(id(operations))}] result: {merged!r}>"
    )
    assert (
        repr(operations).split(" result: ", 1)[1]
        == (repr(single).split(" result: ", 1)[1])
    )
    assert operations.get_missed_key() == ["missing"]


def test_repr_does_not_merge_ready_children():
    child = completed_operation(({"a": {}}, []))
    operations = ArcusMultiOperation("bop mget", [child])

    with patch.object(child, "get_result", wraps=child.get_result) as get_result:
        assert repr(operations) == (
            f"<ArcusMultiOperation[{hex(id(operations))}] result: pending>"
        )
        get_result.assert_not_called()

    assert operations.has_result()
    assert operations.get_result() == {"a": {}}
