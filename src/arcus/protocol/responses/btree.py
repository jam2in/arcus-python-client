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

from ...exceptions import (
    ArcusProtocolException,
    CollectionIndex,
    CollectionType,
    CollectionUnreadable,
)


class BTreeResponses:
    _errors = (
        b"NOT_FOUND",
        b"TYPE_MISMATCH",
        b"UNREADABLE",
        b"OUT_OF_RANGE",
        b"NOT_FOUND_ELEMENT",
        b"BKEY_MISMATCH",
    )

    def __init__(self, reader, collection):
        self.reader = reader
        self.collection = collection

    def _hex(self, field):
        digits = field[2:]
        if (
            not field.startswith(b"0x")
            or not digits
            or len(digits) % 2
            or any(char not in b"0123456789abcdefABCDEF" for char in digits)
        ):
            raise ArcusProtocolException(
                f"invalid hexadecimal element field: {field!r}"
            )
        return field.decode("ascii")

    def element(self, flags):
        bkey = self.reader.token()
        bkey = self.reader.number(bkey) if bkey.isdigit() else self._hex(bkey)
        field = self.reader.token()
        eflag = None
        if field.startswith(b"0x"):
            eflag = self._hex(field)
            field = self.reader.token()
        payload = self.reader.payload(self.reader.number(field))
        return bkey, eflag, self.reader.decode(flags, payload)

    def _put(self, values, flags):
        bkey, eflag, value = self.element(flags)
        values[bkey] = eflag, value

    def decode(self):
        return self.collection.read(
            {},
            self._put,
            allow_count=True,
            allow_trimmed=True,
            errors=(b"BKEY_MISMATCH",),
        )

    def get(self):
        status, values = self.decode()
        if status == b"NOT_FOUND":
            return None
        if status == b"TYPE_MISMATCH":
            raise CollectionType()
        if status == b"BKEY_MISMATCH":
            raise CollectionType("bkey type mismatch")
        if status == b"UNREADABLE":
            raise CollectionUnreadable()
        if status in (b"OUT_OF_RANGE", b"NOT_FOUND_ELEMENT"):
            return {}
        return values

    def _missed(self, header, missed):
        fields = header.split()
        if len(fields) != 2 or fields[0] != b"MISSED_KEYS":
            raise ArcusProtocolException(f"invalid missed-key header: {header!r}")
        for _ in range(self.reader.number(fields[1])):
            missed.append(self.reader.readline().decode("utf-8"))

    def decode_mget(self):
        values = {}
        missed = []
        while True:
            line = self.reader.readline()
            if line.startswith(b"MISSED_KEYS"):
                self._missed(line, missed)
                continue
            if line in (b"END", *self._errors):
                self.reader.finish()
                return line, values, missed
            fields = line.split()
            if len(fields) not in (3, 5) or fields[0] != b"VALUE":
                raise ArcusProtocolException(f"invalid mget header: {line!r}")
            key = fields[1].decode("utf-8")
            status = fields[2]
            if status in (b"OK", b"TRIMMED"):
                if len(fields) != 5:
                    raise ArcusProtocolException(f"incomplete mget header: {line!r}")
                flags, count = map(self.reader.number, fields[3:])
                elements = {}
                for _ in range(count):
                    if self.reader.token() != b"ELEMENT":
                        raise ArcusProtocolException("invalid mget element prefix")
                    self._put(elements, flags)
                values[key] = elements
            elif status in self._errors and len(fields) == 3:
                if status == b"NOT_FOUND":
                    missed.append(key)
                else:
                    values[key] = {}
            else:
                raise ArcusProtocolException(f"invalid mget status: {line!r}")

    def decode_smget(self):
        values = []
        missed = []
        while True:
            line = self.reader.readline()
            if line.startswith(b"MISSED_KEYS"):
                self._missed(line, missed)
                continue
            if line in (
                b"END",
                b"TRIMMED",
                b"DUPLICATED",
                b"DUPLICATED_TRIMMED",
                *self._errors,
            ):
                self.reader.finish()
                return line, values, missed
            fields = line.split()
            if len(fields) != 2 or fields[0] != b"VALUE":
                raise ArcusProtocolException(f"invalid smget header: {line!r}")
            for _ in range(self.reader.number(fields[1])):
                key = self.reader.token().decode("utf-8")
                flags = self.reader.number(self.reader.token())
                bkey, eflag, value = self.element(flags)
                values.append((bkey, key, eflag, value))

    def _multi_result(self, status, values, missed):
        if status == b"NOT_FOUND":
            return None
        if status == b"TYPE_MISMATCH":
            raise CollectionType()
        if status == b"BKEY_MISMATCH":
            raise CollectionType("bkey type mismatch")
        if status == b"UNREADABLE":
            raise CollectionUnreadable()
        if status in (b"OUT_OF_RANGE", b"NOT_FOUND_ELEMENT"):
            raise CollectionIndex()
        return values, missed

    def mget(self):
        return self._multi_result(*self.decode_mget())

    def smget(self):
        return self._multi_result(*self.decode_smget())
