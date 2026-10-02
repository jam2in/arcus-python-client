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

    def readline(self):
        return self.connection.readline()

    def decode(self, flags, payload):
        return self.transcoder.decode(flags, payload)

    def value(self, flags, length):
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
        line = self.readline()
        if line != b"END":
            raise ArcusProtocolException(
                f"invalid response expect END but recv: {line}"
            )
        return self.decode(flags, payload[:-2])

    def pipeline(self, header):
        _, count = header.split()
        results = [self.readline().decode("utf-8") for _ in range(int(count))]
        self.readline()  # Existing pipeline replies terminate with END.
        return results
