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


"""Interpret administrative command responses."""

from ...exceptions import ArcusProtocolException


class AdminResponses:
    def __init__(self, reader):
        self.reader = reader

    def ok(self):
        return self.reader.readline() == b"OK"

    def stats(self):
        values = {}
        while True:
            line = self.reader.readline()
            if line == b"END":
                return values
            fields = line.split(b" ", 2)
            if len(fields) != 3 or fields[0] != b"STAT":
                raise ArcusProtocolException(f"invalid stats response: {line!r}")
            values[fields[1].decode("utf-8")] = fields[2].decode("utf-8")
