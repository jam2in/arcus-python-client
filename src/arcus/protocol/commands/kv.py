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


"""Key/value commands depend only on submission, value encoding and responses."""

from ..request import CommandRequest, CommandSubmitter


class KVCommands:
    def __init__(self, executor: CommandSubmitter, transcoder, responses):
        self._executor = executor
        self._transcoder = transcoder
        self._responses = responses.kv
        self._status = responses.status

    def get(self, key):
        return self._get("get", key)

    def gets(self, key):
        return self._get("gets", key)

    def set(self, key, val, exptime=0):
        return self._set("set", key, val, exptime)

    def cas(self, key, val, cas_id, exptime=0):
        return self._cas("cas", key, val, cas_id, exptime)

    def incr(self, key, value=1):
        return self._incr_decr("incr", key, value)

    def decr(self, key, value=1):
        return self._incr_decr("decr", key, value)

    def add(self, key, val, exptime=0):
        return self._set("add", key, val, exptime)

    def append(self, key, val, exptime=0):
        return self._set("append", key, val, exptime)

    def prepend(self, key, val, exptime=0):
        return self._set("prepend", key, val, exptime)

    def replace(self, key, val, exptime=0):
        return self._set("replace", key, val, exptime)

    def delete(self, key):
        full_cmd = "delete %s" % key
        return self._executor.submit(
            CommandRequest("delete", bytes(full_cmd, "utf-8"), self._status.deleted)
        )

    def _get(self, cmd, key):
        full_cmd = bytes("%s %s" % (cmd, key), "utf-8")
        if cmd == "gets":
            callback = self._responses.cas_value
        else:
            callback = self._responses.value
        op = self._executor.submit(CommandRequest(cmd, full_cmd, callback))
        return op

    def _set(self, cmd, key, val, exptime=0):
        flags, len, value = self._transcoder.encode(val)
        if flags == None:
            return 0
        full_cmd = bytes(
            "%s %s %d %d %d\r\n" % (cmd, key, flags, exptime, len), "utf-8"
        )
        full_cmd += value
        op = self._executor.submit(CommandRequest(cmd, full_cmd, self._status.stored))
        return op

    def _cas(self, cmd, key, val, cas_id, exptime=0):
        flags, len, value = self._transcoder.encode(val)
        if flags == None:
            return 0
        full_cmd = bytes(
            "%s %s %d %d %d %d\r\n" % (cmd, key, flags, exptime, len, int(cas_id)),
            "utf-8",
        )
        full_cmd += value
        op = self._executor.submit(CommandRequest(cmd, full_cmd, self._status.stored))
        return op

    def _incr_decr(self, cmd, key, value):
        full_cmd = "%s %s %d" % (cmd, key, value)
        op = self._executor.submit(
            CommandRequest(cmd, bytes(full_cmd, "utf-8"), self._status.stored)
        )
        return op
