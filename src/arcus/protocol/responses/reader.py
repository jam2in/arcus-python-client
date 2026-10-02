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


"""Read protocol bytes and delegate value decoding without owning a connection."""

from ...exceptions import ArcusNodeSocketException, ArcusProtocolException


class ResponseReader:
    def __init__(self, connection, transcoder):
        self.connection = connection
        self.transcoder = transcoder
        self.complete = False

    def begin(self):
        self.complete = False

    def finish(self):
        self.complete = True

    def readline(self):
        return self.connection.readline()

    def decode(self, flags, payload):
        return self.transcoder.decode(flags, payload)

    def token(self):
        token = self.connection.read_token()
        if not token or any(character in token for character in b"\r\n\t"):
            raise ArcusProtocolException(f"invalid element field: {token!r}")
        return token

    def number(self, field):
        if not field.isdigit():
            raise ArcusProtocolException(f"invalid response number: {field!r}")
        try:
            return int(field)
        except ValueError as error:
            raise ArcusProtocolException(
                f"invalid response number: {field!r}"
            ) from error

    def payload(self, length):
        if length < 0:
            raise ArcusProtocolException(f"invalid response length: {length:d}")
        expected = length + 2
        payload = self.connection.recv(expected)
        if len(payload) != expected:
            raise ArcusNodeSocketException(
                f"received {len(payload):d} bytes when expecting {expected:d}"
            )
        if not payload.endswith(b"\r\n"):
            raise ArcusProtocolException("invalid value payload terminator")
        return payload[:-2]

    def value(self, flags, length):
        payload = self.payload(length)
        line = self.readline()
        if line != b"END":
            raise ArcusProtocolException(
                f"invalid response expect END but recv: {line}"
            )
        self.finish()
        return self.decode(flags, payload)

    def pipeline(self, header):
        fields = header.split()
        if len(fields) != 2 or fields[0] != b"RESPONSE":
            raise ArcusProtocolException(f"invalid pipeline header: {header!r}")
        count = self.number(fields[1])
        if count > 500:
            raise ArcusProtocolException(f"invalid pipeline response count: {count}")
        results = [self.readline().decode("utf-8") for _ in range(count)]
        terminal = self.readline()
        if terminal != b"END":
            raise ArcusProtocolException(f"pipeline failed: {terminal!r}")
        self.finish()
        return results
