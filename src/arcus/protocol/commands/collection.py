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


"""Shared collection wire options; no routing, submission or connection state."""

from ..request import CommandRequest
from ...exceptions import CollectionHexFormat


class CollectionCommandBuilder:
    def __init__(self, transcoder, responses):
        self._transcoder = transcoder
        self._responses = responses

    def create(self, cmd, key, flags, exptime=0, noreply=False, attr=None):
        if attr == None:
            attr = {}
        if "maxcount" not in attr:
            attr["maxcount"] = 4000
        if "ovflaction" not in attr:
            attr["ovflaction"] = "tail_trim"
        if "readable" not in attr:
            attr["readable"] = True
        option = "%d %d %d" % (flags, exptime, attr["maxcount"])
        if attr["ovflaction"] != "tail_trim":
            option += " " + attr["ovflaction"]
        if attr["readable"] == False:
            option += " unreadable"
        if noreply == True:
            option += " noreply"
        full_cmd = bytes("%s %s %s" % (cmd, key, option), "utf-8")
        return CommandRequest(cmd, full_cmd, self._responses.created, noreply)

    def insert(
        self,
        cmd,
        key,
        index,
        val,
        noreply=False,
        pipe=False,
        attr=None,
        bkey=None,
        eflag=None,
    ):
        flags, len, value = self._transcoder.encode(val)
        if bkey != None:
            assert index == None
            if isinstance(bkey, int):
                bkey_str = "%d" % bkey
            else:
                if bkey[:2] != "0x":
                    raise CollectionHexFormat()
                bkey_str = "%s" % bkey
            if eflag != None:
                if eflag[:2] != "0x":
                    raise CollectionHexFormat()
                option = "%s %s %d" % (bkey_str, eflag, len)
            else:
                option = "%s %d" % (bkey_str, len)
        elif index != None:
            option = "%d %d" % (index, len)
        else:
            option = "%d" % len
        if attr != None:
            if "flags" not in attr:
                attr["flags"] = 0
            if "exptime" not in attr:
                attr["exptime"] = 0
            if "maxcount" not in attr:
                attr["maxcount"] = 4000
            option += " create %d %d %d" % (
                attr["flags"],
                attr["exptime"],
                attr["maxcount"],
            )
            if "ovflaction" in attr:
                option += " " + attr["ovflaction"]
            if "readable" in attr and attr["readable"] == False:
                option += " unreadable"
        if noreply == True:
            option += " noreply"
        if pipe == True:
            assert noreply == False
            option += " pipe"
        option += "\r\n"
        full_cmd = bytes("%s %s %s" % (cmd, key, option), "utf-8") + value
        return CommandRequest(cmd, full_cmd, self._responses.stored, noreply or pipe)

    def get(self, cmd, key, range, callback, delete=None, drop=None, filter=None):
        option = ""
        type = cmd[:3]
        if filter != None:
            option += filter.get_expr() + " "
        if delete == True:
            option += "delete"
        if drop == True:
            assert delete == False
            option += "drop"
        if isinstance(range, tuple):
            if type == "bop" and isinstance(range[0], str):
                if range[0][:2] != "0x" or range[1][:2] != "0x":
                    raise CollectionHexFormat()
                full_cmd = bytes(
                    "%s %s %s..%s %s" % (cmd, key, range[0], range[1], option), "utf-8"
                )
                return CommandRequest(cmd, full_cmd, callback)
            else:
                full_cmd = bytes(
                    "%s %s %d..%d %s" % (cmd, key, range[0], range[1], option), "utf-8"
                )
                return CommandRequest(cmd, full_cmd, callback)
        elif type == "bop" and isinstance(range, str):
            if range[:2] != "0x":
                raise CollectionHexFormat()
            full_cmd = bytes("%s %s %s %s" % (cmd, key, range, option), "utf-8")
            return CommandRequest(cmd, full_cmd, callback)
        else:
            full_cmd = bytes("%s %s %d %s" % (cmd, key, range, option), "utf-8")
            return CommandRequest(cmd, full_cmd, callback)
