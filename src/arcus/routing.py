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


"""Consistent hashing, cache-node ownership, and client lifecycle composition."""

import bisect
import hashlib
from threading import Lock

from ._logging import arcuslog
from .discovery import ZooKeeperDiscovery
from .exceptions import ArcusNodeConnectionException, ArcusProtocolException


class ArcusKetemaHash:
    def __init__(self):
        # config
        self.per_node = 40
        self.per_hash = 4

    def hash(self, addr):
        ret = []
        for i in range(0, self.per_node):
            ret = ret + self.__hash(addr + ("-%d" % i))

        return ret

    def __hash(self, input):
        m = hashlib.md5()
        m.update(bytes(input, "utf-8"))
        r = m.digest()
        ret = []

        for i in range(0, self.per_hash):
            hash = (
                (r[3 + i * 4] << 24)
                | (r[2 + i * 4] << 16)
                | (r[1 + i * 4] << 8)
                | r[0 + i * 4]
            )
            ret.append(hash)

        return ret


class ArcusPoint:
    def __init__(self, hash, node):
        self.hash = hash
        self.node = node

    def __lt__(self, rhs):
        return self.hash < rhs.hash

    def __le__(self, rhs):
        return self.hash <= rhs.hash

    def __eq__(self, rhs):
        return self.hash == rhs.hash

    def __ne__(self, rhs):
        return self.hash != rhs.hash

    def __gt__(self, rhs):
        return self.hash > rhs.hash

    def __ge__(self, rhs):
        return self.hash >= rhs.hash

    def __repr__(self):
        return "(%d:%s)" % (self.hash, self.node)


class ConsistentHashRing:
    """Own a cache-node set and its hash points, without a discovery dependency.

    The locator synchronizes access. Replacement publishes only after all new
    nodes and hash points are built, preserving the old ring on allocation errors.
    Retirement errors are logged after publication, so discovery can record the
    committed snapshot even if a removed node cannot be fully closed yet.
    """

    def __init__(self, node_allocator):
        self.hash_method = ArcusKetemaHash()
        self.node_allocator = node_allocator
        self.nodes = {}
        self.points = []

    def replace(self, children):
        arcuslog(self, "hash_nodes with children: ", children)
        nodes = {}
        points = []
        allocated = []
        try:
            for child in children:
                addr, separator, name = child.partition("-")
                if not separator or not addr or not name:
                    raise ArcusProtocolException("invalid cache node: %s" % child)
                if addr in nodes:
                    continue
                node = self.nodes.get(addr)
                if node is None:
                    node = self.node_allocator.alloc(addr, name)
                    allocated.append(node)
                nodes[addr] = node
                points.extend(
                    ArcusPoint(value, node)
                    for value in self.hash_method.hash(node.addr)
                )
            points.sort()
        except BaseException:
            for node in allocated:
                try:
                    node.close()
                except Exception as error:
                    arcuslog(self, "allocation cleanup failed: ", error)
            raise

        removed = [node for addr, node in self.nodes.items() if addr not in nodes]
        self.nodes = nodes
        self.points = points
        for node in nodes.values():
            node.in_use = True
        for node in removed:
            node.in_use = False
            try:
                node.close()
            except Exception as error:
                # The replacement is already visible. Reporting this as a failed
                # publication would let discovery accept an older snapshot.
                # The allocator still owns nodes whose close did not finish and
                # retries their cleanup when the client is disconnected.
                arcuslog(self, "node retirement failed: ", node.addr, error)

    def detach(self):
        """Unpublish nodes so their potentially blocking cleanup can run unlocked."""
        nodes = list(self.nodes.values())
        self.nodes = {}
        self.points = []
        return nodes

    def get_node(self, key):
        key_hash = self._hash_key(key)
        if not self.points:
            raise ArcusNodeConnectionException("no available cache nodes")
        idx = bisect.bisect(self.points, ArcusPoint(key_hash, None))
        if idx >= len(self.points):
            idx = 0
        return self.points[idx].node

    @staticmethod
    def _hash_key(key):
        m = hashlib.md5()
        m.update(bytes(key, "utf-8"))
        r = m.digest()
        return r[3] << 24 | r[2] << 16 | r[1] << 8 | r[0]


class ArcusLocator:
    """Coordinate discovery, routing, and allocator lifetime for the client."""

    def __init__(self, node_allocator):
        self.lock = Lock()
        self._lifecycle_lock = Lock()
        self.node_allocator = node_allocator
        self._ring = ConsistentHashRing(node_allocator)
        self._discovery = ZooKeeperDiscovery(self.hash_nodes)
        self._closed = False

    @property
    def hash_method(self):
        return self._ring.hash_method

    @hash_method.setter
    def hash_method(self, value):
        self._ring.hash_method = value

    @property
    def node_list(self):
        return self._ring.points

    @property
    def addr_node_map(self):
        return self._ring.nodes

    @property
    def zk(self):
        return self._discovery.client

    @property
    def zoo_path(self):
        return self._discovery.path

    def connect(self, addr, code):
        with self._lifecycle_lock:
            if self.zk is not None:
                self._disconnect()
            try:
                self.node_allocator.start()
                with self.lock:
                    self._closed = False
                self._discovery.connect(addr, code)
            except BaseException:
                try:
                    self._disconnect()
                except Exception as error:
                    arcuslog(self, "connection cleanup failed: ", error)
                raise

    def disconnect(self):
        with self._lifecycle_lock:
            self._disconnect()

    def _disconnect(self):
        with self.lock:
            self._closed = True
            nodes = self._ring.detach()
        # Discovery may be delivering a membership callback. Release the routing
        # lock before invalidating the session and waiting for callbacks to stop.
        cleanup = [node.close for node in nodes]
        cleanup.extend([self._discovery.close, self.node_allocator.close])
        first_error = None
        for close in cleanup:
            try:
                close()
            except Exception as error:
                if first_error is None:
                    first_error = error
        if first_error is not None:
            raise first_error

    def hash_nodes(self, children):
        with self.lock:
            if not self._closed:
                self._ring.replace(children)

    def watch_children(self, event):
        self._discovery.watch_children(event)

    def get_node(self, key):
        with self.lock:
            return self._ring.get_node(key)
