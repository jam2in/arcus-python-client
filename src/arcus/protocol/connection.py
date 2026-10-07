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


"""Socket connections, buffering, and bounded I/O."""

import math
import socket
import time

from .._logging import arcuslog
from ..exceptions import (
    ArcusNodeConnectionException,
    ArcusNodeSocketException,
    ArcusProtocolException,
)


class Connection(object):
    def __init__(self, host):
        ip, port = host.split(":")
        self.ip = ip
        self.port = int(port)
        self.address = (self.ip, self.port)

        self.socket = None
        self.buffer = b""

        self.connect()

    def connect(self):
        if self.socket:
            disconnect()

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.socket.connect(self.address)
        except socket.timeout as msg:
            self.disconnect()
        except socket.error as msg:
            self.disconnect()

        self.buffer = b""
        return self.socket

    def disconnect(self):
        if self.socket:
            self.socket.close()
            self.socket = None

    def disconnected(self):
        return self.socket == None

    def send_request(self, request):
        arcuslog(self, "send_request: ", request + b"\r\n")
        self.socket.sendall(request + b"\r\n")

    def hasline(self):
        index = self.buffer.find(b"\r\n")
        return index >= 0

    def readline(self):
        buf = self.buffer

        while True:
            index = buf.find(b"\r\n")
            if index >= 0:
                break

            data = self.socket.recv(4096)
            arcuslog(self, 'sock recv: (%d): "' % len(data), data)

            if not data:  # None or b"" (connection closed by peer)
                self.disconnect()
                raise ArcusNodeConnectionException("connection lost")

            buf += data

        self.buffer = buf[index + 2 :]

        arcuslog(self, "readline: ", buf[:index])
        return buf[:index]

    def recv(self, rlen):
        buf = self.buffer
        while len(buf) < rlen:
            foo = self.socket.recv(max(rlen - len(buf), 4096))

            if foo == None:
                raise ArcusNodeSocketException(
                    "Read %d bytes, expecting %d, read returned 0 length bytes"
                    % (len(buf), rlen)
                )

            buf += foo
            arcuslog(self, "sock recv: (%d): " % len(foo), foo)

        self.buffer = buf[rlen:]
        arcuslog(self, "recv: ", buf[:rlen])
        return buf[:rlen]
