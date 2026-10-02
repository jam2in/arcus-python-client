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


"""Cache-node construction and worker lifecycle."""

import queue
import threading

from ..exceptions import ArcusNodeConnectionException
from .connection import _positive_timeout
from .node import ArcusMCNode
from .worker import ArcusMCWorker


class ArcusMCNodeAllocator:
    def __init__(
        self, transcoder, *, connect_timeout=1.0, io_timeout=1.0, operation_timeout=5.0
    ):
        self.transcoder = transcoder
        self.connect_timeout = _positive_timeout(connect_timeout)
        self.io_timeout = _positive_timeout(io_timeout)
        self.operation_timeout = _positive_timeout(operation_timeout)
        self._nodes = set()
        self._lock = threading.RLock()
        self.shutdown = True
        self.worker = None
        self.start()

    def start(self):
        with self._lock:
            if not self.shutdown:
                return
            self.shutdown = False
            self.worker = ArcusMCWorker(self)
            self.worker.start()

    def alloc(self, addr, name):
        with self._lock:
            if self.shutdown:
                raise ArcusNodeConnectionException("allocator is closed")
            node = ArcusMCNode(addr, name, self.transcoder, self)
            self._nodes.add(node)
            self.worker.register_node(node)
            return node

    def snapshot_nodes(self):
        with self._lock:
            return tuple(self._nodes)

    def forget(self, node):
        with self._lock:
            self._nodes.discard(node)

    def close(self):
        with self._lock:
            self.shutdown = True
            nodes = tuple(self._nodes)
            self._nodes.clear()
            worker = self.worker
        for node in nodes:
            with node._io_lock:
                node._closed = True
                node.disconnect()
        if worker is not None:
            worker.q.put(None)
            if threading.current_thread() is not worker:
                worker.join()
            while True:
                try:
                    worker.q.get_nowait()
                except queue.Empty:
                    break

    def join(self):
        if self.worker is not None:
            self.worker.join()
