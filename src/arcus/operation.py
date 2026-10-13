#
# arcus-python-client - Arcus python client drvier
# Copyright 2014 NAVER Corp.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


"""Thread-safe asynchronous operation results and aggregation."""

import heapq
import time
from concurrent.futures import (
    CancelledError,
    Future,
    InvalidStateError,
    TimeoutError,
)

from .exceptions import (
    ArcusCompositeException,
    ArcusException,
    ArcusTimeoutException,
)


class ArcusOperation:
    def __init__(self, node, request, callback):
        self.node = node
        self.request = request
        self.callback = callback
        self.noreply = False
        self.pipe = False

        self._future = Future()

    def __repr__(self):
        if not self._future.done():
            result = "pending"
        elif self._future.cancelled():
            result = "cancelled"
        else:
            error = self._future.exception()
            value = error if error is not None else self._future.result()
            result = repr(value)

        return f"<ArcusOperation[{hex(id(self))}] result: {result}>"

    def has_result(self):
        return self._future.done()

    def set_result(self, result):
        try:
            self._future.set_result(result)
        except InvalidStateError:
            return

    def set_exception(self, error):
        try:
            self._future.set_exception(error)
        except InvalidStateError:
            return

    def get_result(self, timeout=None):
        try:
            self._future.exception(timeout=timeout)
        except TimeoutError:
            raise ArcusTimeoutException() from None
        except CancelledError:
            raise ArcusException("operation cancelled") from None

        return self._future.result()


class ArcusMultiOperation:
    def __init__(self, cmd, operations):
        self.cmd = cmd
        self.ops = tuple(operations)
        self._future = Future()

    def __repr__(self):
        if not self._future.done():
            result = "pending"
        elif self._future.cancelled():
            result = "cancelled"
        else:
            error = self._future.exception()
            value = error if error is not None else self._future.result()[0]
            result = repr(value)

        return f"<ArcusMultiOperation[{hex(id(self))}] result: {result}>"

    def has_result(self):
        return self._future.done() or all(op.has_result() for op in self.ops)

    def set_exception(self, error):
        try:
            self._future.set_exception(error)
        except InvalidStateError:
            return

        for op in self.ops:
            op.set_exception(error)

    def get_result(self, timeout=None):
        try:
            self._complete_result(timeout)
            self._future.exception(timeout=timeout)
        except TimeoutError:
            raise ArcusTimeoutException() from None
        except CancelledError:
            raise ArcusException("operation cancelled") from None

        return self._future.result()[0]

    def get_missed_key(self, timeout=None):
        self.get_result(timeout=timeout)
        return self._future.result()[1]

    def _complete_result(self, timeout):
        if self._future.done():
            return

        deadline = time.monotonic() + timeout if timeout is not None else None
        child_results = []
        missed_keys = []
        errors = []

        for op in self.ops:
            remaining = (
                None if deadline is None else max(0, deadline - time.monotonic())
            )

            try:
                op._future.exception(timeout=remaining)
            except TimeoutError:
                if self._future.done():
                    return
                raise
            except CancelledError:
                errors.append(ArcusException("operation cancelled"))
                continue

            try:
                result, missed = op.get_result()
                child_results.append(result)
                missed_keys.extend(missed)
            except Exception as error:  # noqa: BLE001 - preserve child failures
                errors.append(error)

        try:
            if errors:
                if len(errors) == 1:
                    self._future.set_exception(errors[0])
                else:
                    self._future.set_exception(ArcusCompositeException(errors))
                return

            try:
                combined = self._merge_results(child_results, missed_keys)
            except Exception as e:  # noqa: BLE001 - preserve merge failures
                self._future.set_exception(e)
            else:
                self._future.set_result(combined)
        except InvalidStateError:
            return

    def _merge_results(self, child_results, missed_keys):
        if self.cmd == "bop mget":
            merged = {}
            for result in child_results:
                merged.update(result)

        else:  # bop smget
            merged = list(heapq.merge(*child_results))

        return merged, sorted(missed_keys)
