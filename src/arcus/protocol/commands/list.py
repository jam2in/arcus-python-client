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


"""List command construction and submission."""

from ..request import CommandRequest, CommandSubmitter


class ListCommands:
    def __init__(self, executor: CommandSubmitter, collection, responses, status):
        self._executor = executor
        self._collection = collection
        self._responses = responses
        self._status = status

    def create(self, key, flags, exptime=0, noreply=False, attr=None):
        return self._executor.submit(
            self._collection.create("lop create", key, flags, exptime, noreply, attr)
        )

    def insert(self, key, index, value, noreply=False, pipe=False, attr=None):
        return self._executor.submit(
            self._collection.insert(
                "lop insert", key, index, value, noreply, pipe, attr
            )
        )

    def delete(self, key, range, drop=False, noreply=False, pipe=False):
        option = ""
        if drop == True:
            option += "drop"
        if noreply == True:
            option += " noreply"
        if pipe == True:
            assert noreply == False
            option += " pipe"
        if isinstance(range, tuple):
            full_cmd = bytes(
                "lop delete %s %d..%d %s" % (key, range[0], range[1], option), "utf-8"
            )
            return self._executor.submit(
                CommandRequest(
                    "lop delete", full_cmd, self._status.deleted, noreply or pipe
                )
            )
        else:
            full_cmd = bytes("lop delete %s %d %s" % (key, range, option), "utf-8")
            return self._executor.submit(
                CommandRequest(
                    "lop delete", full_cmd, self._status.deleted, noreply or pipe
                )
            )

    def get(self, key, range, delete=False, drop=False):
        return self._executor.submit(
            self._collection.get(
                "lop get", key, range, self._responses.get, delete, drop
            )
        )
