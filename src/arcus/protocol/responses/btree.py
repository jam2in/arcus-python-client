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


"""Interpret B-tree elements and multi-key response shapes."""

from ...exceptions import CollectionIndex, CollectionType, CollectionUnreadable


class BTreeResponses:
    def __init__(self, reader, collection):
        self.reader = reader
        self.collection = collection

    def element(self, flags, line):
        bkey, eflag, length_payload = line.split(b" ", 2)
        if eflag.isdigit():
            eflag = None
            payload = length_payload
        else:
            eflag = eflag.decode("utf-8")
            _, payload = length_payload.split(b" ", 1)
        bkey = int(bkey) if bkey.isdigit() else bkey.decode("utf-8")
        return bkey, eflag, self.reader.decode(flags, payload)

    def _put(self, values, flags, line):
        bkey, eflag, value = self.element(flags, line)
        values[bkey] = eflag, value

    def decode(self):
        return self.collection.read({}, self._put)

    def get(self):
        status, values = self.decode()
        if status == b"NOT_FOUND":
            return None
        if status == b"TYPE_MISMATCH":
            raise CollectionType()
        if status == b"UNREADABLE":
            raise CollectionUnreadable()
        if status in (b"OUT_OF_RANGE", b"NOT_FOUND_ELEMENT"):
            return {}
        return values

    def _missed(self, header, missed):
        _, count = header.split(b" ")
        for _ in range(int(count)):
            missed.append(self.reader.readline().decode("utf-8"))

    def decode_mget(self):
        values = {}
        missed = []
        while True:
            line = self.reader.readline()
            if line[:11] == b"MISSED_KEYS":
                self._missed(line, missed)
                continue
            if line[:5] not in (b"VALUE", b"COUNT"):
                return line, values, missed
            fields = line.split()
            key = fields[1].decode("utf-8")
            if fields[2] == b"NOT_FOUND":
                missed.append(key)
                continue
            count = 0
            if len(fields) == 5:
                flags = int(fields[3])
                count = int(fields[4])
            elements = {}
            for _ in range(count):
                _, element = self.reader.readline().split(b" ", 1)
                self._put(elements, flags, element)
            values[key] = elements

    def decode_smget(self):
        values = []
        missed = []
        while True:
            line = self.reader.readline()
            if line[:11] == b"MISSED_KEYS":
                self._missed(line, missed)
                continue
            if line[:5] not in (b"VALUE", b"COUNT"):
                return line, values, missed
            fields = line.split()
            for _ in range(int(fields[1])):
                key, flags, element = self.reader.readline().split(b" ", 2)
                bkey, eflag, value = self.element(int(flags), element)
                values.append((bkey, key.decode("utf-8"), eflag, value))

    def _multi_result(self, status, values, missed):
        if status == b"NOT_FOUND":
            return None
        if status == b"TYPE_MISMATCH":
            raise CollectionType()
        if status == b"UNREADABLE":
            raise CollectionUnreadable()
        if status in (b"OUT_OF_RANGE", b"NOT_FOUND_ELEMENT"):
            raise CollectionIndex()
        return values, missed

    def mget(self):
        return self._multi_result(*self.decode_mget())

    def smget(self):
        return self._multi_result(*self.decode_smget())
