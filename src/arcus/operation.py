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

import queue
import time
from threading import Event, Lock

from .exceptions import ArcusNodeConnectionException


class ArcusOperation:
    def __init__(self, node, request, callback):
        self.node = node
        self.request = request
        self.callback = callback
        self.result = self  # self.result == self : not received, self.result == None : receive None
        self.invalid = False
        self._result_lock = Lock()
        self._result_ready = Event()

        self.noreply = False
        self.pipe = False

    def __repr__(self):
        return "<ArcusOperation[%s] result: %s>" % (
            hex(id(self)),
            repr(self.result) if self.has_result() else "pending",
        )

    def has_result(self):
        return self._result_ready.is_set()

    def set_result(self, result):
        with self._result_lock:
            if not self._result_ready.is_set():
                self.result = result
                self._result_ready.set()

    def set_invalid(self):
        with self._result_lock:
            if self._result_ready.is_set():
                return False

            self.invalid = True
            self.result = ArcusNodeConnectionException(
                "current async result is unavailable because Arcus node is disconnected now"
            )
            self._result_ready.set()
            return True

    def get_result(self, timeout=0):
        wait_timeout = timeout if timeout > 0 else None
        if not self._result_ready.wait(wait_timeout):
            raise queue.Empty()

        if isinstance(self.result, Exception):
            raise self.result

        return self.result


class ArcusOperationList:
    def __init__(self, cmd):
        self.ops = []
        self.cmd = cmd
        self.result = None
        self.missed_key = None
        self.invalid = False
        self._result_lock = Lock()
        self._result_error = None

        self.noreply = False
        self.pipe = False

    def __repr__(self):
        with self._result_lock:
            result = self._result_error or self.result
        return "<ArcusOperationList[%s] result: %s>" % (
            hex(id(self)),
            repr(result) if result is not None else "pending",
        )

    def add_op(self, op):
        with self._result_lock:
            self.ops.append(op)

    def has_result(self):
        with self._result_lock:
            if self.result is not None or self._result_error is not None:
                return True
            return all(op.has_result() for op in self.ops)

    def set_result(self, result):
        assert False
        pass

    def set_invalidate(self):
        with self._result_lock:
            if (
                self.result is not None
                or self._result_error is not None
                or all(op.has_result() for op in self.ops)
            ):
                return False  # already done

            self.invalid = True
            self._result_error = ArcusNodeConnectionException(
                "current async result is unavailable because Arcus node is disconnected now"
            )
            for op in self.ops:
                op.set_invalid()
            return True

    def get_missed_key(self, timeout=0):
        self.get_result(timeout)
        return self.missed_key

    def get_result(self, timeout=0):
        with self._result_lock:
            if self._result_error is not None:
                raise self._result_error
            if self.result is not None:
                return self.result
            ops = list(self.ops)

        deadline = time.monotonic() + timeout if timeout > 0 else None
        tmp_result = []
        missed_key = []
        try:
            for op in ops:
                if deadline is None:
                    ret, miss = op.get_result()
                else:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        if not op.has_result():
                            raise queue.Empty()
                        ret, miss = op.get_result()
                    else:
                        ret, miss = op.get_result(remaining)
                tmp_result.append(ret)
                missed_key += miss
        except queue.Empty:
            with self._result_lock:
                if self._result_error is not None:
                    raise self._result_error
            raise
        except Exception as error:
            with self._result_lock:
                if self._result_error is None:
                    self._result_error = error
                error = self._result_error
            raise error

        if self.cmd == "bop mget":
            result = {}
            for a in tmp_result:
                result.update(a)

        else:  # bop smget
            # Copy child results so concurrent readers and later reads retain them.
            pending = [list(values) for values in tmp_result if values]
            result = []
            while pending:
                idx = min(range(len(pending)), key=lambda i: pending[i][0])
                result.append(pending[idx].pop(0))
                if not pending[idx]:
                    pending.pop(idx)

        missed_key.sort()
        with self._result_lock:
            if self._result_error is not None:
                raise self._result_error
            if self.result is None:
                self.result = result
                self.missed_key = missed_key
            return self.result
