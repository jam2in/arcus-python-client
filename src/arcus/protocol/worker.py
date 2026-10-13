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


"""Request dispatch and selector-based response processing."""

import errno
import queue
import selectors
import socket
import threading
from threading import Lock

from .._logging import arcuslog


class ArcusMCPoll(threading.Thread):
    def __init__(self, node_allocator):
        threading.Thread.__init__(self)
        self.selector = selectors.DefaultSelector()
        self.sock_node_map = {}
        self.node_allocator = node_allocator

    def run(self):
        arcuslog(self, "selector start")

        while True:
            events = self.selector.select(timeout=2)

            if self.node_allocator.shutdown == True:
                arcuslog(self, "selector out")
                self.selector.close()
                return

            for key, mask in events:
                node = key.data
                if mask & selectors.EVENT_READ:
                    if not node.ops and self._peer_closed(key.fileobj):
                        arcuslog(self, "connection closed by peer: ", node)
                        self.selector.unregister(key.fileobj)
                        del self.sock_node_map[key.fd]
                        node.disconnect()
                        continue
                    node.do_op()

    def _peer_closed(self, sock):
        try:
            return sock.recv(1, socket.MSG_PEEK) == b""
        except BlockingIOError:
            return False
        except OSError:
            return True

    def register_node(self, node):
        sock = node.handle.socket
        if sock is None:
            return
        fileno = sock.fileno()
        if fileno == -1:
            return
        # unregister first in case of reconnect (also drops a stale entry with the same fd)
        try:
            self.selector.unregister(sock)
        except (KeyError, ValueError):
            pass
        try:
            self.selector.register(sock, selectors.EVENT_READ, node)
        except (KeyError, ValueError, OSError) as e:
            # socket closed concurrently, selector closed on shutdown, or kernel refused
            arcuslog(self, "register node failed: ", fileno, node, e)
            return
        arcuslog(self, "regist node: ", fileno, node)
        self.sock_node_map[fileno] = node


class ArcusMCWorker(threading.Thread):
    def __init__(self, node_allocator):
        threading.Thread.__init__(self)
        self.q = queue.Queue()
        self.poll = ArcusMCPoll(node_allocator)
        self.poll.start()
        self.node_allocator = node_allocator

    def run(self):
        arcuslog(self, "worker start")

        while True:
            op = self.q.get()
            if self.node_allocator.shutdown == True:
                arcuslog(self, "worker done")
                self.poll.join()
                return

            if op == None:  # maybe shutdown
                continue

            if op.invalid:  # invalidated by disconnect before being sent
                continue

            arcuslog(
                self,
                "get operation %s(%s:%s) from %s"
                % (op.request, op.callback, hex(id(op)), op.node),
            )
            node = op.node

            try:
                node.process_request(op.request)
            except Exception as e:
                arcuslog(self, "operation failed: %s" % str(e))
                op.set_result(e)

    def register_node(self, node):
        self.poll.register_node(node)
