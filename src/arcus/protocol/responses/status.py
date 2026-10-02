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


"""Interpret status replies shared by mutation commands."""

from ...exceptions import CollectionIndex, CollectionOverflow, CollectionType


class StatusResponses:
    def __init__(self, reader):
        self.reader = reader

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
        if line.isdigit():
            return int(line)
        return False

    def deleted(self):
        line = self.reader.readline()
        if line[:8] == b"RESPONSE":
            return self.reader.pipeline(line)
        if line in (b"DELETED", b"DELETED_DROPPED", b"NOT_FOUND"):
            return True
        if line == b"TYPE_MISMATCH":
            raise CollectionType()
        if line == b"OVERFLOWED":
            raise CollectionOverflow()
        if line in (b"OUT_OF_RANGE", b"NOT_FOUND_ELEMENT"):
            raise CollectionIndex()
        return False
