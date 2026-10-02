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
from ..exceptions import ArcusNodeConnectionException, ArcusProtocolException


def _positive_timeout(value):
    value = float(value)
    if not math.isfinite(value) or value <= 0:
        raise ValueError("timeouts must be finite and greater than zero")
    return value


class Connection(object):
    def __init__(self, host, *, connect_timeout=1.0, io_timeout=1.0):
        self.connect_timeout = _positive_timeout(connect_timeout)
        self.io_timeout = _positive_timeout(io_timeout)
        self.deadline = None
        ip, port = host.split(":")
        self.ip = ip
        self.port = int(port)
        self.address = (self.ip, self.port)

        self.socket = None
        self.buffer = b""

        self.connect()

    def connect(self):
        if self.socket is not None:
            self.disconnect()

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            timeout = self.connect_timeout
            if self.deadline is not None:
                timeout = min(timeout, self.deadline - time.monotonic())
                if timeout <= 0:
                    raise socket.timeout("operation deadline exceeded before connect")
            self.socket.settimeout(timeout)
            self.socket.connect(self.address)
            self.socket.settimeout(self.io_timeout)
        except socket.timeout as msg:
            self.disconnect()
        except socket.error as msg:
            self.disconnect()

        self.buffer = b""
        return self.socket

    def disconnect(self):
        sock = self.socket
        self.socket = None
        self.buffer = b""
        if sock is not None:
            sock.close()

    def disconnected(self):
        return self.socket == None

    def send_request(self, request):
        if self.disconnected():
            raise ArcusNodeConnectionException("node is disconnected")
        arcuslog(self, "send_request: ", request + b"\r\n")
        self._set_io_timeout()
        self.socket.sendall(request + b"\r\n")

    def _set_io_timeout(self):
        timeout = self.io_timeout
        if self.deadline is not None:
            remaining = self.deadline - time.monotonic()
            if remaining <= 0:
                raise socket.timeout("operation deadline exceeded")
            timeout = min(timeout, remaining)
        self.socket.settimeout(timeout)

    def hasline(self):
        index = self.buffer.find(b"\r\n")
        return index >= 0

    def readline(self):
        buf = self.buffer

        while True:
            index = buf.find(b"\r\n")
            if index >= 0:
                break

            if self.disconnected():
                raise ArcusNodeConnectionException("node is disconnected")
            self._set_io_timeout()
            data = self.socket.recv(4096)
            arcuslog(self, 'sock recv: (%d): "' % len(data), data)

            if data == b"":
                self.disconnect()
                raise ArcusNodeConnectionException("connection lost")

            buf += data

        self.buffer = buf[index + 2 :]

        arcuslog(self, "readline: ", buf[:index])
        return buf[:index]

    def recv(self, rlen):
        if rlen < 0:
            raise ArcusProtocolException("invalid response length: %d" % rlen)
        buf = self.buffer
        while len(buf) < rlen:
            if self.disconnected():
                raise ArcusNodeConnectionException("node is disconnected")
            self._set_io_timeout()
            foo = self.socket.recv(max(rlen - len(buf), 4096))

            if foo == b"":
                self.disconnect()
                raise ArcusNodeConnectionException(
                    "connection lost after reading %d bytes, expecting %d"
                    % (len(buf), rlen)
                )

            buf += foo
            arcuslog(self, "sock recv: (%d): " % len(foo), foo)

        self.buffer = buf[rlen:]
        arcuslog(self, "recv: ", buf[:rlen])
        return buf[:rlen]
