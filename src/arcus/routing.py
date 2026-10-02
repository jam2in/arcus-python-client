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


"""Consistent hashing and ZooKeeper cache-node discovery."""

import bisect
import hashlib
import time
from threading import Lock

from kazoo.client import KazooClient, KazooState
from kazoo.protocol.states import EventType

from ._logging import arcuslog
from .exceptions import ArcusNodeConnectionException, ArcusProtocolException


# Match Kazoo's start() default, sharing it across connection and discovery reads.
# Cache socket timeouts and operation deadlines are configured by the allocator.
ZOOKEEPER_CONNECT_TIMEOUT = 15.0


def _remaining_connect_timeout(deadline):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("ZooKeeper connection setup deadline exceeded")
    return remaining


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


class ArcusLocator:
    def __init__(self, node_allocator):
        self.hash_method = ArcusKetemaHash()
        self.lock = Lock()
        self._lifecycle_lock = Lock()
        self.node_list = []
        self.addr_node_map = {}
        self.node_allocator = node_allocator
        self.zk = None
        self._closed = False
        self._generation = 0
        self._refresh_requested = 0
        self._refresh_applied = 0

    def connect(self, addr, code):
        with self._lifecycle_lock:
            if self.zk is not None:
                self._disconnect()
            try:
                self.node_allocator.start()
                zk = KazooClient(hosts=addr)
                with self.lock:
                    self.zk = zk
                    self.zoo_path = "/arcus/cache_list/" + code
                    self._closed = False
                    self._generation += 1
                    generation = self._generation
                    self._watch = self._child_watch(zk, generation)
                zk.add_listener(
                    lambda state: self._state_changed(zk, generation, state)
                )
                deadline = time.monotonic() + ZOOKEEPER_CONNECT_TIMEOUT
                zk.start(timeout=_remaining_connect_timeout(deadline))
                data, stat = zk.get_async(self.zoo_path).get(
                    timeout=_remaining_connect_timeout(deadline)
                )
                arcuslog(self, "zoo keeper node info with stat: ", data, stat)
                # ZooKeeper requests may wait for reconnection. They must never
                # hold the routing lock needed by ordinary cache operations.
                with self.lock:
                    self._refresh_requested += 1
                    request_id = self._refresh_requested
                children = zk.get_children_async(self.zoo_path, watch=self._watch).get(
                    timeout=_remaining_connect_timeout(deadline)
                )
                self._apply_children(zk, generation, request_id, children)
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
            self._generation += 1
            nodes = list(self.addr_node_map.values())
            self.addr_node_map = {}
            self.node_list = []
            zk, self.zk = self.zk, None

        # Do not hold the routing lock while waiting for ZooKeeper callbacks to stop.
        cleanup = [node.close for node in nodes]
        if zk is not None:
            cleanup.extend([zk.stop, zk.close])
        cleanup.append(self.node_allocator.close)
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
                self._hash_nodes(children)

    def _hash_nodes(self, children):
        """Build a replacement ring under self.lock before publishing it."""
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
                node = self.addr_node_map.get(addr)
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

        removed = [
            node for addr, node in self.addr_node_map.items() if addr not in nodes
        ]
        self.addr_node_map = nodes
        self.node_list = points
        for node in nodes.values():
            node.in_use = True
        first_error = None
        for node in removed:
            node.in_use = False
            try:
                node.close()
            except Exception as error:
                if first_error is None:
                    first_error = error
        if first_error is not None:
            raise first_error

    def _child_watch(self, zk, generation):
        return lambda event: self._watch_children(zk, generation, event)

    def _watch_children(self, zk, generation, event):
        if getattr(event, "type", None) == EventType.NONE:
            # Kazoo clears one-shot watches when the connection is suspended.
            # CONNECTED below will refresh the snapshot and reinstall the watch.
            return
        self._refresh_children(zk, generation)

    def _state_changed(self, zk, generation, state):
        if state == KazooState.CONNECTED:
            self._refresh_children(zk, generation)

    def _refresh_children(self, zk, generation):
        with self.lock:
            if self._closed or self.zk is not zk or self._generation != generation:
                return
            self._refresh_requested += 1
            request_id = self._refresh_requested
            path, watch = self.zoo_path, self._watch
        # State listeners run on Kazoo's connection thread, so use only its
        # asynchronous API here. Completion callbacks run on Kazoo's handler.
        try:
            result = zk.get_children_async(path, watch=watch)
            result.rawlink(
                lambda result: self._refresh_complete(
                    zk, generation, request_id, result
                )
            )
        except Exception as error:
            arcuslog(self, "discovery refresh failed: ", error)

    def _refresh_complete(self, zk, generation, request_id, result):
        try:
            children = result.get()
            self._apply_children(zk, generation, request_id, children)
        except Exception as error:
            arcuslog(self, "discovery refresh failed: ", error)

    def _apply_children(self, zk, generation, request_id, children):
        with self.lock:
            if (
                self._closed
                or self.zk is not zk
                or self._generation != generation
                or request_id <= self._refresh_applied
            ):
                return
            self._hash_nodes(children)
            self._refresh_applied = request_id

    def watch_children(self, event):
        with self.lock:
            zk, generation = self.zk, self._generation
        self._watch_children(zk, generation, event)

    def get_node(self, key):
        hash = self.__hash_key(key)
        with self.lock:
            if not self.node_list:
                raise ArcusNodeConnectionException("no available cache nodes")
            idx = bisect.bisect(self.node_list, ArcusPoint(hash, None))
            if idx >= len(self.node_list):
                idx = 0
            return self.node_list[idx].node

    def __hash_key(self, key):
        bkey = bytes(key, "utf-8")

        m = hashlib.md5()
        m.update(bkey)
        r = m.digest()
        ret = r[3] << 24 | r[2] << 16 | r[1] << 8 | r[0]
        return ret
