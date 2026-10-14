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
    ArcusProtocolException,
)


class Connection(object):
    sasl_timeout = 5  # seconds to wait for each SASL handshake reply

    def __init__(self, host, auth_user=None, auth_pass=None):
        ip, port = host.split(":")
        self.ip = ip
        self.port = int(port)
        self.address = (self.ip, self.port)
        self.auth_user = auth_user
        self.auth_pass = auth_pass

        self.socket = None
        self.buffer = b""

        try:
            self.connect()
        except ArcusNodeConnectionException as e:
            # SASL failed: stay disconnected, the next request reconnects and
            # reports the error to its caller
            arcuslog(self, "initial connect failed: %s" % e)

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

        if self.socket and self.auth_user:
            try:
                # the handshake reads on the worker thread, so never block forever
                self.socket.settimeout(self.sasl_timeout)
                self._sasl_auth()
                self.socket.settimeout(None)
            except Exception as e:
                self.disconnect()
                raise ArcusNodeConnectionException(
                    "SASL authentication failed: %s" % e
                )

        return self.socket

    def _sasl_auth(self):
        # SCRAM-SHA-256 over 'sasl mech' / 'sasl auth', see tools/arcus_util.py
        from scramp import ScramClient

        self.send_request(b"sasl mech")
        result = self.readline().decode("utf-8")
        if not result.startswith("SASL_MECH"):
            return  # server does not require authentication

        mechanism = "SCRAM-SHA-256"
        if mechanism not in result.split():
            raise ArcusProtocolException("no %s mechanism: %s" % (mechanism, result))

        scram_client = ScramClient([mechanism], self.auth_user, self.auth_pass)
        message = scram_client.get_client_first().encode("utf-8")
        self.send_request(
            b"sasl auth %s %d\r\n%s" % (mechanism.encode(), len(message), message)
        )

        is_server_first = True
        server_verified = False
        while True:
            result = self.readline().decode("utf-8")
            if result.startswith("SASL_OK"):
                if not server_verified:
                    # mutual auth: the server must prove it knows the password
                    raise ArcusProtocolException("SASL_OK before server-final")
                return

            if not result.startswith("SASL_CONTINUE"):
                raise ArcusProtocolException(result)

            server_message = self.readline().decode("utf-8")
            if is_server_first:
                is_server_first = False
                scram_client.set_server_first(server_message)
                message = scram_client.get_client_final().encode("utf-8")
            else:
                scram_client.set_server_final(server_message)
                server_verified = True
                message = b""

            self.send_request(b"sasl auth %d\r\n%s" % (len(message), message))

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

            if not foo:  # None or b"" (connection closed by peer)
                self.disconnect()
                raise ArcusNodeConnectionException(
                    "connection lost: read %d bytes, expecting %d" % (len(buf), rlen)
                )

            buf += foo
            arcuslog(self, "sock recv: (%d): " % len(foo), foo)

        self.buffer = buf[rlen:]
        arcuslog(self, "recv: ", buf[:rlen])
        return buf[:rlen]
