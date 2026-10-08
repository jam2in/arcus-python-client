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
        self.q = queue.Queue(1)
        self.result = self  # self.result == self : not received, self.result == None : receive None
        self.invalid = False

        self.noreply = False
        self.pipe = False

    def __repr__(self):
        return "<ArcusOperation[%s] result: %s>" % (
            hex(id(self)),
            repr(self.get_result()),
        )

    def has_result(self):
        return self.result != None or self.q.empty() == False

    def set_result(self, result):
        self.q.put(result)

    def set_invalid(self):
        if self.has_result():
            return False

        self.invalid = True
        self.q.put(None)  # wake up blocked callers.
        return True

    def get_result(self, timeout=0):
        if self.result != self:
            return self.result

        if timeout > 0:
            result = self.q.get(timeout=timeout)
        else:
            result = self.q.get()

        if result == self and self.invalid == True:
            raise ArcusNodeConnectionException(
                "current async result is unavailable because Arcus node is disconnected now"
            )

        if isinstance(result, Exception):
            raise result

        self.result = result
        return result


class ArcusOperationList:
    def __init__(self, cmd):
        self.ops = []
        self.cmd = cmd
        self.result = None
        self.missed_key = None
        self.invalid = False

        self.noreply = False
        self.pipe = False

    def __repr__(self):
        return "<ArcusOperationList[%s] result: %s>" % (
            hex(id(self)),
            repr(self.get_result()),
        )

    def add_op(self, op):
        self.ops.append(op)

    def has_result(self):
        if self.result != None:
            return True

        for a in ops:
            if a.has_result() == False:
                return False

        return True

    def set_result(self, result):
        assert False
        pass

    def set_invalidate(self):
        if self.has_result():
            return False  # already done

        self.invalid = True

        # invalidate all ops and wake up blockers.
        for a in ops:
            a.set_invalidate()

        return True

    def get_missed_key(self, timeout=0):
        if self.missed_key != None:
            return self.missed_key

        self.get_result(timeout)
        return self.missed_key

    def get_result(self, timeout=0):
        if self.result != None:
            return self.result

        tmp_result = []
        missed_key = []
        if timeout > 0:
            start_time = time.time()
            end_time = start_tume + timeout

            for a in self.ops:
                curr_time = time.time()
                remain_time = end_time - curr_time
                if remain_time < 0:
                    raise Queue.Empty()

                ret, miss = a.get_result(remain_time)
                tmp_result.append(ret)
                missed_key += miss
        else:
            for a in self.ops:
                ret, miss = a.get_result()
                tmp_result.append(ret)
                missed_key += miss

        if self.cmd == "bop mget":
            result = {}
            for a in tmp_result:
                result.update(a)

        else:  # bop smget
            length = len(tmp_result)

            # empty
            if length <= 0:
                return []

            # merge sort
            result = []
            while True:
                # remove empty list
                while len(tmp_result[0]) == 0:
                    tmp_result.pop(0)
                    if len(tmp_result) == 0:  # all done
                        if self.result == None and self.invalid == True:
                            raise ArcusNodeConnectionException(
                                "current async result is unavailable because Arcus node is disconnected now"
                            )
                        missed_key.sort()
                        self.result = result
                        self.missed_key = missed_key
                        return self.result

                min = tmp_result[0][0]
                idx = 0
                for i in range(0, len(tmp_result)):
                    if len(tmp_result[i]) and tmp_result[i][0] < min:
                        min = tmp_result[i][0]
                        idx = i

                result.append(tmp_result[idx].pop(0))

        if self.result == None and self.invalid == True:
            raise ArcusNodeConnectionException(
                "current async result is unavailable because Arcus node is disconnected now"
            )
        missed_key.sort()
        self.result = result
        self.missed_key = missed_key
        return self.result
