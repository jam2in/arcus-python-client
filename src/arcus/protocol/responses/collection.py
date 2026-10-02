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
        if line == b"CREATED":
            return True
        if line == b"EXISTS":
            raise CollectionExist()
        return False

    def stored(self):
        line = self.reader.readline()
        if line[:8] == b"RESPONSE":
            return self.reader.pipeline(line)
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

    def read(self, values, consume):
        """Read common frames and let the family consume each element in order."""
        while True:
            line = self.reader.readline()
            if line[:5] not in (b"VALUE", b"COUNT"):
                return line, values
            if line[:5] == b"COUNT":
                command, count = line.split(b"=")
                return command, int(count)
            _, flags, count = line.split()
            flags, count = int(flags), int(count)
            for _ in range(count):
                consume(values, flags, self.reader.readline())

    def value(self, flags, line):
        # Preserve the existing line-framed collection protocol behavior.
        _, payload = line.split(b" ", 1)
        return self.reader.decode(flags, payload)
