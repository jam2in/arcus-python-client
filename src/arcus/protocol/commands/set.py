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


"""Set command construction and submission."""

from ..request import CommandRequest, CommandSubmitter


class SetCommands:
    def __init__(
        self, executor: CommandSubmitter, transcoder, collection, responses, status
    ):
        self._executor = executor
        self._transcoder = transcoder
        self._collection = collection
        self._responses = responses
        self._status = status

    def create(self, key, flags, exptime=0, noreply=False, attr=None):
        return self._executor.submit(
            self._collection.create("sop create", key, flags, exptime, noreply, attr)
        )

    def insert(self, key, value, noreply=False, pipe=False, attr=None):
        return self._executor.submit(
            self._collection.insert("sop insert", key, None, value, noreply, pipe, attr)
        )

    def get(self, key, count=0, delete=False, drop=False):
        return self._executor.submit(
            self._collection.get(
                "sop get", key, count, self._responses.get, delete, drop
            )
        )

    def delete(self, key, val, drop=False, noreply=False, pipe=False):
        flags, len, value = self._transcoder.encode(val)
        option = "%d" % len
        if drop == True:
            option += " drop"
        if noreply == True:
            option += " noreply"
        if pipe == True:
            assert noreply == False
            option += " pipe"
        option += "\r\n"
        full_cmd = bytes("sop delete %s %s" % (key, option), "utf-8") + value
        return self._executor.submit(
            CommandRequest(
                "sop delete", full_cmd, self._status.deleted, noreply or pipe
            )
        )

    def exist(self, key, val, pipe=False):
        flags, len, value = self._transcoder.encode(val)
        option = "%d" % len
        if pipe == True:
            option += " pipe"
        option += "\r\n"
        full_cmd = bytes("sop exist %s %s" % (key, option), "utf-8") + value
        return self._executor.submit(
            CommandRequest("sop exist", full_cmd, self._responses.exist, pipe)
        )
