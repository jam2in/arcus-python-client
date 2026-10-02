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


"""Shared collection framing; family handlers determine element/result types."""

from ...exceptions import (
    ArcusProtocolException,
    CollectionExist,
    CollectionIndex,
    CollectionOverflow,
    CollectionType,
)


class CollectionResponses:
    def __init__(self, reader):
        self.reader = reader

    def created(self):
        line = self.reader.readline()
        self.reader.finish()
        if line == b"CREATED":
            return True
        if line == b"EXISTS":
            raise CollectionExist()
        return False

    def stored(self):
        line = self.reader.readline()
        if line[:8] == b"RESPONSE":
            return self.reader.pipeline(line)
        self.reader.finish()
        if line == b"STORED":
            return True
        if line == b"NOT_FOUND":
            return False
        if line == b"TYPE_MISMATCH":
            raise CollectionType()
        if line == b"OVERFLOWED":
            raise CollectionOverflow()
        if line == b"OUT_OF_RANGE":
            raise CollectionIndex()
        return False

    def read(
        self, values, consume, *, allow_count=False, allow_trimmed=False, errors=()
    ):
        """Read common frames and let the family consume each element in order."""
        line = self.reader.readline()
        if line in (
            b"NOT_FOUND",
            b"TYPE_MISMATCH",
            b"UNREADABLE",
            b"OUT_OF_RANGE",
            b"NOT_FOUND_ELEMENT",
            *errors,
        ):
            self.reader.finish()
            return line, values
        if allow_count and line.startswith(b"COUNT="):
            count = self.reader.number(line[len(b"COUNT=") :])
            self.reader.finish()
            return b"COUNT", count
        fields = line.split()
        if len(fields) != 3 or fields[0] != b"VALUE":
            raise ArcusProtocolException(f"invalid collection header: {line!r}")
        flags, count = map(self.reader.number, fields[1:])
        for _ in range(count):
            consume(values, flags)
        terminal = self.reader.readline()
        if terminal not in (b"END", b"DELETED", b"DELETED_DROPPED") and not (
            allow_trimmed and terminal == b"TRIMMED"
        ):
            raise ArcusProtocolException(f"invalid collection terminator: {terminal!r}")
        self.reader.finish()
        return terminal, values

    def value(self, flags):
        length = self.reader.number(self.reader.token())
        payload = self.reader.payload(length)
        return self.reader.decode(flags, payload)
