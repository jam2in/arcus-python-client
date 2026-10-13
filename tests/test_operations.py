import queue
import threading
from concurrent.futures import InvalidStateError

import pytest

from arcus import ArcusNodeConnectionException, ArcusOperation, ArcusTimeoutException


def make_operation():
    return ArcusOperation(node=None, request=b"set", callback=None)


def test_pending_operation_has_no_result():
    assert not make_operation().has_result()


@pytest.mark.parametrize("value", [None, False, True, "value", {"key": "value"}])
def test_result_is_preserved_on_repeated_reads(value):
    operation = make_operation()
    operation.set_result(value)

    assert operation.has_result()
    assert operation.get_result() is value
    assert operation.get_result() is value


@pytest.mark.parametrize(
    "error",
    [
        ValueError("invalid response"),
        ArcusNodeConnectionException(),
        TimeoutError("socket timed out"),
        ArcusTimeoutException("operation failed"),
    ],
)
def test_exception_is_preserved_on_repeated_reads(error):
    operation = make_operation()
    operation.set_exception(error)

    for _ in range(2):
        with pytest.raises(type(error)) as raised:
            operation.get_result(timeout=0.01)
        assert raised.value is error
    assert operation.has_result()


def test_exception_can_be_returned_as_an_ordinary_value():
    error = ValueError("stored value")
    operation = make_operation()
    operation.set_result(error)

    assert operation.get_result() is error


def test_timeout_does_not_cancel_later_completion():
    operation = make_operation()

    with pytest.raises(ArcusTimeoutException):
        operation.get_result(timeout=0.01)
    assert not operation.has_result()

    operation.set_result("late")
    assert operation.get_result(timeout=0.01) == "late"


@pytest.mark.parametrize("first_is_error", [False, True])
@pytest.mark.parametrize("second_is_error", [False, True])
def test_first_completion_wins(first_is_error, second_is_error):
    operation = make_operation()
    first = ValueError("first") if first_is_error else "first"
    second = RuntimeError("second") if second_is_error else "second"
    complete_first = operation.set_exception if first_is_error else operation.set_result
    complete_second = (
        operation.set_exception if second_is_error else operation.set_result
    )

    assert complete_first(first) is None
    assert complete_second(second) is None
    if first_is_error:
        with pytest.raises(ValueError) as raised:
            operation.get_result(timeout=0.01)
        assert raised.value is first
    else:
        assert operation.get_result(timeout=0.01) is first


def watch_waiter_registration(operation, expected_count):
    """Signal only after readers are blocked on the Future's condition, without sleeps."""
    registered = threading.Event()
    count = 0

    class CountingCondition(threading.Condition):
        def wait(self, timeout=None):
            nonlocal count
            # The condition lock is held here, so a completion cannot run
            # before super().wait() installs this waiter and releases it.
            count += 1
            if count == expected_count:
                registered.set()
            return super().wait(timeout)

    operation._future._condition = CountingCondition()
    return registered


@pytest.mark.parametrize("timeout", [None, 2])
@pytest.mark.parametrize("fails", [False, True])
def test_completion_wakes_all_registered_waiters(timeout, fails):
    operation = make_operation()
    registered = watch_waiter_registration(operation, expected_count=3)
    outcomes = queue.Queue()
    error = ArcusNodeConnectionException()

    def read_result():
        try:
            outcomes.put(("result", operation.get_result(timeout=timeout)))
        except (ArcusNodeConnectionException, ArcusTimeoutException) as caught:
            outcomes.put(("error", caught))

    threads = [threading.Thread(target=read_result, daemon=True) for _ in range(3)]
    for thread in threads:
        thread.start()
    try:
        assert registered.wait(timeout=2)
        assert outcomes.empty()
        if fails:
            operation.set_exception(error)
        else:
            operation.set_result("ready")
    finally:
        # Release readers even if registration unexpectedly failed.
        operation.set_exception(RuntimeError("test cleanup"))
        for thread in threads:
            thread.join(timeout=2)

    assert all(not thread.is_alive() for thread in threads)
    expected = ("error", error) if fails else ("result", "ready")
    assert [outcomes.get_nowait() for _ in threads] == [expected] * len(threads)


def test_success_and_failure_race_has_one_stable_outcome():
    for _ in range(25):
        operation = make_operation()
        gate = threading.Barrier(3)
        producer_errors = queue.Queue()
        error = ArcusNodeConnectionException()

        def complete(setter, value, current_gate=gate, errors=producer_errors):
            try:
                current_gate.wait(timeout=2)
                setter(value)
            except (InvalidStateError, threading.BrokenBarrierError) as caught:
                errors.put(caught)

        threads = [
            threading.Thread(
                target=complete, args=(operation.set_result, "success"), daemon=True
            ),
            threading.Thread(
                target=complete, args=(operation.set_exception, error), daemon=True
            ),
        ]
        for thread in threads:
            thread.start()
        try:
            gate.wait(timeout=2)
        finally:
            for thread in threads:
                thread.join(timeout=2)

        assert all(not thread.is_alive() for thread in threads)
        assert producer_errors.empty()
        assert operation.has_result()
        outcomes = []
        for _ in range(2):
            try:
                outcomes.append(operation.get_result(timeout=0.01))
            except ArcusNodeConnectionException as caught:
                outcomes.append(caught)
        assert outcomes in [["success", "success"], [error, error]]


def test_repr_does_not_wait_or_raise_a_stored_error():
    pending = make_operation()
    succeeded = make_operation()
    failed = make_operation()
    succeeded.set_result(None)
    failed.set_exception(ValueError("invalid response"))

    assert "pending" in repr(pending)
    assert "None" in repr(succeeded)
    assert "ValueError" in repr(failed)
