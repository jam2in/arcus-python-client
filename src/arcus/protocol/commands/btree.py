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


"""B+Tree commands, ranges, element flags and multi-key wire encoding."""

from ..request import CommandRequest, CommandSubmitter
from ...exceptions import CollectionHexFormat


class BTreeCommands:
    def __init__(self, executor: CommandSubmitter, collection, responses, status):
        self._executor = executor
        self._collection = collection
        self._responses = responses
        self._status = status

    def create(self, key, flags, exptime=0, noreply=False, attr=None):
        return self._executor.submit(
            self._collection.create("bop create", key, flags, exptime, noreply, attr)
        )

    def insert(
        self, key, bkey, value, eflag=None, noreply=False, pipe=False, attr=None
    ):
        return self._executor.submit(
            self._collection.insert(
                "bop insert", key, None, value, noreply, pipe, attr, bkey, eflag
            )
        )

    def upsert(
        self, key, bkey, value, eflag=None, noreply=False, pipe=False, attr=None
    ):
        return self._executor.submit(
            self._collection.insert(
                "bop upsert", key, None, value, noreply, pipe, attr, bkey, eflag
            )
        )

    def update(
        self, key, bkey, value, eflag=None, noreply=False, pipe=False, attr=None
    ):
        return self._executor.submit(
            self._collection.insert(
                "bop update", key, None, value, noreply, pipe, attr, bkey, eflag
            )
        )

    def delete(
        self, key, range, filter=None, count=None, drop=False, noreply=False, pipe=False
    ):
        option = ""
        if filter != None:
            option += filter.get_expr() + " "
        if count != None:
            option += "%d " % count
        if drop == True:
            option += "drop"
        if noreply == True:
            option += " noreply"
        if pipe == True:
            assert noreply == False
            option += " pipe"
        if isinstance(range, tuple):
            if isinstance(range[0], str):
                if range[0][:2] != "0x" or range[1][:2] != "0x":
                    raise CollectionHexFormat()
                full_cmd = bytes(
                    "bop delete %s %s..%s %s" % (key, range[0], range[1], option),
                    "utf-8",
                )
                return self._executor.submit(
                    CommandRequest(
                        "bop delete", full_cmd, self._status.deleted, noreply or pipe
                    )
                )
            else:
                full_cmd = bytes(
                    "bop delete %s %d..%d %s" % (key, range[0], range[1], option),
                    "utf-8",
                )
                return self._executor.submit(
                    CommandRequest(
                        "bop delete", full_cmd, self._status.deleted, noreply or pipe
                    )
                )
        elif isinstance(range, str):
            if range[:2] != "0x":
                raise CollectionHexFormat()
            full_cmd = bytes("bop delete %s %s %s" % (key, range, option), "utf-8")
            return self._executor.submit(
                CommandRequest(
                    "bop delete", full_cmd, self._status.deleted, noreply or pipe
                )
            )
        else:
            full_cmd = bytes("bop delete %s %d %s" % (key, range, option), "utf-8")
            return self._executor.submit(
                CommandRequest(
                    "bop delete", full_cmd, self._status.deleted, noreply or pipe
                )
            )

    def get(self, key, range, filter=None, delete=False, drop=False):
        return self._executor.submit(
            self._collection.get(
                "bop get", key, range, self._responses.get, delete, drop, filter=filter
            )
        )

    def mget(self, key_list, range, filter=None, offset=None, count=50):
        return self._coll_mget("bop mget", key_list, range, filter, offset, count)

    def smget(self, key_list, range, filter=None, offset=None, count=2000):
        return self._coll_mget("bop smget", key_list, range, filter, offset, count)

    def count(self, key, range, filter):
        return self._executor.submit(
            self._collection.get(
                "bop count", key, range, self._responses.get, filter=filter
            )
        )

    def incr(self, key, bkey, value, noreply=False, pipe=False):
        return self._bop_incrdecr("bop incr", key, bkey, value, noreply, pipe)

    def decr(self, key, bkey, value, noreply=False, pipe=False):
        return self._bop_incrdecr("bop decr", key, bkey, value, noreply, pipe)

    def _bop_incrdecr(self, cmd, key, bkey, val, noreply=False, pipe=False):
        if isinstance(val, int):
            value = "%d" % val
        else:
            value = val
        if isinstance(bkey, int):
            bkey_str = "%d" % bkey
        else:
            if bkey[:2] != "0x":
                raise CollectionHexFormat()
            bkey_str = "%s" % bkey
        option = "%s %s" % (bkey_str, value)
        if noreply == True:
            option += " noreply"
        if pipe == True:
            assert noreply == False
            option += " pipe"
        full_cmd = bytes("%s %s %s" % (cmd, key, option), "utf-8")
        return self._executor.submit(
            CommandRequest(cmd, full_cmd, self._status.stored, noreply or pipe)
        )

    def _coll_mget(self, org_cmd, key_list, range, filter, offset, count):
        comma_sep_keys = ""
        for key in key_list:
            if comma_sep_keys != "":
                comma_sep_keys += ","
            comma_sep_keys += key
        cmd = "%s %d %d " % (org_cmd, len(comma_sep_keys), len(key_list))
        if isinstance(range, tuple):
            if isinstance(range[0], str):
                if range[0][:2] != "0x" or range[1][:2] != "0x":
                    raise CollectionHexFormat()
                cmd += "%s..%s" % range
            else:
                cmd += "%d..%d" % range
        elif isinstance(range, str):
            if range[:2] != "0x":
                raise CollectionHexFormat()
            cmd += "%s" % range
        else:
            cmd += "%d" % range
        if filter != None:
            cmd += " " + filter.get_expr()
        if offset != None:
            cmd += " %d" % offset
        cmd += " %d" % count
        cmd += "\r\n%s" % comma_sep_keys
        cmd = bytes(cmd, "utf-8")
        if org_cmd == "bop mget":
            reply = self._responses.mget
        else:
            reply = self._responses.smget
        op = self._executor.submit(CommandRequest(org_cmd, cmd, reply))
        return op
