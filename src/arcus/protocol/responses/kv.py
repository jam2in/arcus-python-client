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


"""Decode scalar values and CAS tokens."""

from ...exceptions import ArcusProtocolException


class KVResponses:
    def __init__(self, reader):
        self.reader = reader

    def value(self):
        fields = self.header(4)
        if fields is None:
            return None
        return self.reader.value(fields[0], fields[1])

    def cas_value(self):
        fields = self.header(5)
        if fields is None:
            return None
        return self.reader.value(fields[0], fields[1]), fields[2]

    def header(self, field_count):
        line = self.reader.readline()
        if line == b"END":
            return None
        fields = line.split()
        if (
            len(fields) != field_count
            or fields[0] != b"VALUE"
            or not fields[2].isdigit()
            or not fields[3].isdigit()
            or (field_count == 5 and not fields[4].isdigit())
        ):
            raise ArcusProtocolException(f"invalid value response header: {line!r}")
        try:
            flags, length = int(fields[2]), int(fields[3])
        except ValueError as error:
            raise ArcusProtocolException(
                f"invalid value response header: {line!r}"
            ) from error
        return flags, length, fields[4] if field_count == 5 else None
