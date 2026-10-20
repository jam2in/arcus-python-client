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
import threading
from threading import Lock

from .._logging import arcuslog


class ArcusMCPoll(threading.Thread):
    def __init__(self, node_allocator):
        threading.Thread.__init__(self)
        self.selector = selectors.DefaultSelector()
        self.sock_node_map = {}
        self.node_allocator = node_allocator
        self.lock = Lock()

    def run(self):
        arcuslog(self, "selector start")
        try:
            while not self.node_allocator.shutdown:
                events = self.selector.select(timeout=2)
                if self.node_allocator.shutdown:
                    return
                for key, mask in events:
                    node = key.data
                    with self.lock:
                        if self.sock_node_map.get(key.fd) is not node:
                            continue
                        try:
                            current_key = self.selector.get_key(key.fd)
                        except KeyError:
                            continue
                        if (
                            current_key is not key
                            or node.handle.socket is not key.fileobj
                        ):
                            continue
                    if mask & selectors.EVENT_READ:
                        # Read independently of writes; the node rechecks this
                        # socket after acquiring its response-reading lock.
                        node.do_op(expected_socket=key.fileobj)
        finally:
            arcuslog(self, "selector out")
            with self.lock:
                self.selector.close()
                self.sock_node_map.clear()

    def _unregister_descriptor(self, descriptor):
        # Caller holds self.lock.
        self.sock_node_map.pop(descriptor, None)
        try:
            self.selector.unregister(descriptor)
        except (KeyError, ValueError):
            pass
        except OSError as error:
            if error.errno not in (errno.EBADF, errno.ENOENT):
                raise

    def unregister_node(self, node):
        with self.lock:
            descriptors = [
                fd for fd, existing in self.sock_node_map.items() if existing is node
            ]
            for fd in descriptors:
                self._unregister_descriptor(fd)

    def register_node(self, node):
        with node._io_lock:
            self.unregister_node(node)
            sock = node.handle.socket
            if sock is None:
                return
            fileno = sock.fileno()
            if fileno == -1:
                return
            with self.lock:
                if self.node_allocator.shutdown:
                    return
                # A replacement socket can reuse a disconnected node's fd.
                self._unregister_descriptor(fileno)
                try:
                    self.selector.register(sock, selectors.EVENT_READ, node)
                except (KeyError, ValueError, OSError) as error:
                    arcuslog(self, "register node failed: ", fileno, node, error)
                    return
                self.sock_node_map[fileno] = node
                arcuslog(self, "regist node: ", fileno, node)


class ArcusMCWorker(threading.Thread):
    def __init__(self, node_allocator):
        threading.Thread.__init__(self)
        self.q = queue.Queue()
        self.node_allocator = node_allocator
        self.poll = ArcusMCPoll(node_allocator)
        self.poll.start()

    def run(self):
        arcuslog(self, "worker start")
        try:
            while True:
                op = self.q.get()
                if self.node_allocator.shutdown:
                    return
                if op is None or (op.has_result() and not op.noreply):
                    continue
                arcuslog(
                    self,
                    "get operation %s(%s:%s) from %s"
                    % (op.request, op.callback, hex(id(op)), op.node),
                )
                op.node.process_operation(op)
        finally:
            arcuslog(self, "worker done")
            self.poll.join()

    def register_node(self, node):
        self.poll.register_node(node)
