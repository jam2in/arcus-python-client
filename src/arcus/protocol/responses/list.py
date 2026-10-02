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


"""Interpret list values and list-specific empty results."""

from ...exceptions import CollectionType, CollectionUnreadable


class ListResponses:
    def __init__(self, reader, collection):
        self.reader = reader
        self.collection = collection

    def decode(self):
        return self.collection.read([], self._append)

    def _append(self, values, flags):
        values.append(self.collection.value(flags))

    def get(self):
        status, values = self.decode()
        if status == b"NOT_FOUND":
            return None
        if status == b"TYPE_MISMATCH":
            raise CollectionType()
        if status == b"UNREADABLE":
            raise CollectionUnreadable()
        if status in (b"OUT_OF_RANGE", b"NOT_FOUND_ELEMENT"):
            return []
        return values
